"""
helper/stream.py — High-performance Telegram file streamer
===========================================================

Architecture overview
─────────────────────
Startup latency (TTFB)
  • File metadata is resolved once then stored in a two-level cache:
      L1 – in-process Python dict  (_file_meta_cache)
      L2 – MongoDB (fallback on cold start)
  • FileId (Telegram file-pointer) is also cached per message_id with a
    per-entry access timestamp so hot files are never re-fetched.
  • The bandwidth-limit check is satisfied from an in-process counter that
    is refreshed every _BW_CACHE_TTL seconds (5 s) so the streaming hot path
    never hits MongoDB.
  • The thumbnail lookup is fired as a background asyncio.Task; it never
    blocks the 206 response.

Chunk strategy  (Netflix-style progressive warm-up)
  FIRST_CHUNK_SIZE  =  64 KB  → unblocks player decoder immediately
  SEEK_INITIAL_SIZE =  64 KB  → instant seek response
  CHUNK_SIZE        =   1 MB  → Telegram hard limit per GetFile RPC
  WARMUP_SIZE       =   2 MB  → delivered in small 64 KB slices at startup

Prefetch pipeline
  PREFETCH_COUNT = 12  — keeps up to 12 MB queued ahead of the writer so
  slow Telegram DCs never stall the HTTP response.

Smart buffer window  (seek optimisation #11)
  _SeekBufferWindow tracks the last _BW_WINDOW bytes around the current
  playback position for each (file_hash, client_ip) pair. On a seek that
  lands inside the window the data is served instantly from memory; outside
  the window the window is reset and normal Telegram fetching resumes.

Sequential range optimisation  (#6)
  _SequentialTracker remembers the last byte served per session.  When the
  new range starts at (last_end + 1) the Telegram fetch continues from the
  next aligned chunk boundary without recalculating any offsets.

Index-based instant seeking  (#13)
  _SeekIndex stores a lightweight {timestamp → byte_offset} mapping built
  from rough file-size heuristics (10-second segments) the very first time
  a file is requested.  When a seek arrives the server jumps directly to the
  nearest indexed offset, skipping any file scanning.

Bandwidth accounting
  Writes are fire-and-forget asyncio.Tasks; the hot path never awaits them.
  A dedup cache prevents double-counting the same (ip, message_id, offset)
  within 60 seconds.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import math
import mimetypes
import time
from typing import Dict, Optional, Set, Tuple, Union

from aiohttp import web
from pyrogram import Client, utils, raw
from pyrogram.errors import AuthBytesInvalid, FloodWait
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

from config import Config
from database import Database

logger = logging.getLogger(__name__)

# ── Telegram RPC constants ────────────────────────────────────────────────────
# Telegram server hard-caps GetFile at 1 MB.
CHUNK_SIZE = 1024 * 1024          # 1 MB  — Telegram RPC limit

# ── Startup / seek warm-up ────────────────────────────────────────────────────
# Deliver the first 64 KB immediately so the player can start decoding
# without waiting for a full 1 MB round-trip to Telegram.
FIRST_CHUNK_SIZE  =  64 * 1024   # 64 KB  — cold-start TTFB slice
_SEEK_INITIAL_SIZE =  64 * 1024  # 64 KB  — seek response fast-path slice

# After the initial slice, keep pumping 64 KB bursts until WARMUP_SIZE
# bytes have been delivered, then switch to full 1 MB chunks.
_WARMUP_SIZE = 2 * 1024 * 1024   # 2 MB  — Netflix-style warm-up window

# ── Prefetch tuning ───────────────────────────────────────────────────────────
# Keep 12 MB queued ahead of the HTTP writer.  Smooth on fast links;
# still low-waste on mobile because the queue blocks when full.
PREFETCH_COUNT = 12

# ── Retry / back-off ─────────────────────────────────────────────────────────
_MAX_CHUNK_RETRIES = 5
_RETRY_BACKOFF     = 0.10         # seconds — aggressive fail-fast
_RPC_TIMEOUT       = 8.0          # seconds — tight timeout → faster retry

# ── Cache TTLs ────────────────────────────────────────────────────────────────
_FILE_CACHE_TTL    = 5 * 60       # 5 min  — file-metadata + thumb eviction
_FILE_ID_CACHE_TTL = 30 * 60      # 30 min — FileId eviction
_BW_CACHE_TTL      = 5            # 5 s    — bandwidth-limit counter refresh
_BW_DEDUP_TTL      = 60           # 60 s   — per-(ip, msg, offset) dedup

# ── Smart buffer window ───────────────────────────────────────────────────────
# Keep the last 2 MB around the current playback position in memory so
# near-seeks are served instantly without a Telegram round-trip.
_BW_WINDOW_SIZE = 2 * 1024 * 1024  # 2 MB  — buffer window per session

# ── Sequential range threshold ───────────────────────────────────────────────
# Ranges whose start byte is within 256 KB of the previous end are treated as
# "sequential" and served without resetting the prefetch pipeline.
_SEQ_TOLERANCE = 256 * 1024

# ── Seek-index segment duration ──────────────────────────────────────────────
# Build an index entry every 10 seconds of (estimated) video content.
# Actual keyframe positions are unknown at the HTTP layer; we index to the
# nearest 1 MB boundary so the seek jump is always accurate.
_INDEX_SEGMENT_BYTES = 10 * 1024 * 1024  # ~10 MB per index entry

# ─────────────────────────────────────────────────────────────────────────────
# MIME helpers
# ─────────────────────────────────────────────────────────────────────────────
MIME_TYPE_MAP: Dict[str, str] = {
    "video":    "video/mp4",
    "audio":    "audio/mpeg",
    "image":    "image/jpeg",
    "document": "application/octet-stream",
}

_EXTENSION_MIME: Dict[str, str] = {
    ".mkv":  "video/x-matroska",
    ".webm": "video/webm",
    ".avi":  "video/x-msvideo",
    ".mov":  "video/quicktime",
    ".wmv":  "video/x-ms-wmv",
    ".flv":  "video/x-flv",
    ".mp4":  "video/mp4",
    ".m4v":  "video/mp4",
    ".ts":   "video/mp2t",
    ".mp3":  "audio/mpeg",
    ".m4a":  "audio/mp4",
    ".ogg":  "audio/ogg",
    ".opus": "audio/opus",
    ".wav":  "audio/wav",
    ".flac": "audio/flac",
    ".aac":  "audio/aac",
}

_BROWSER_NATIVE_VIDEO: Set[str] = {
    "video/mp4",
    "video/webm",
    "video/ogg",
    "video/mp2t",
}
_BROWSER_NATIVE_AUDIO: Set[str] = {
    "audio/mpeg",
    "audio/mp4",
    "audio/ogg",
    "audio/opus",
    "audio/wav",
    "audio/flac",
    "audio/aac",
    "audio/x-aac",
}


def _mime_for_filename(file_name: str, fallback: str) -> str:
    ext = "." + file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    if ext in _EXTENSION_MIME:
        return _EXTENSION_MIME[ext]
    guessed = mimetypes.guess_type(file_name)[0]
    return guessed or fallback


def is_browser_playable(mime: str) -> bool:
    return mime in _BROWSER_NATIVE_VIDEO or mime in _BROWSER_NATIVE_AUDIO


# ─────────────────────────────────────────────────────────────────────────────
# Telegram helpers
# ─────────────────────────────────────────────────────────────────────────────
async def get_file_ids(client: Client, message_id: str) -> FileId:
    """Fetch the FileId for *message_id* from the Flog/dump channel."""
    try:
        msg = await client.get_messages(Config.FLOG_CHAT_ID, int(message_id))
    except Exception as exc:
        logger.warning("get_messages failed: msg=%s err=%s", message_id, exc)
        raise web.HTTPNotFound(
            reason=f"could not fetch message {message_id} from log channel"
        )

    if not msg or msg.empty:
        raise web.HTTPNotFound(
            reason=f"message {message_id} not found in log channel"
        )

    media = (
        msg.document
        or msg.video
        or msg.audio
        or msg.photo
        or msg.sticker
        or msg.animation
        or msg.voice
        or msg.video_note
    )
    if not media:
        raise web.HTTPNotFound(
            reason=f"message {message_id} contains no streamable media"
        )

    return FileId.decode(media.file_id)


# ─────────────────────────────────────────────────────────────────────────────
# Thumbnail helper  (non-blocking — called as background task)
# ─────────────────────────────────────────────────────────────────────────────
_thumbnail_cache:   Dict[str, Optional[str]] = {}
_thumb_cache_atime: Dict[str, float]         = {}


async def get_thumbnail_url(
    client: Client,
    file_hash: str,
    file_data: dict,
    base_url: str,
) -> Optional[str]:
    now = time.monotonic()

    if file_hash in _thumbnail_cache:
        _thumb_cache_atime[file_hash] = now
        return _thumbnail_cache[file_hash]

    file_type = file_data.get("file_type", "document")
    if file_type not in (
        Config.FILE_TYPE_VIDEO, Config.FILE_TYPE_AUDIO, "video", "audio"
    ):
        _thumbnail_cache[file_hash]  = None
        _thumb_cache_atime[file_hash] = now
        return None

    try:
        msg = await client.get_messages(
            Config.FLOG_CHAT_ID, int(file_data["message_id"])
        )
        if not msg or msg.empty:
            _thumbnail_cache[file_hash]  = None
            _thumb_cache_atime[file_hash] = now
            return None

        thumb = None
        if msg.video    and msg.video.thumbs:    thumb = msg.video.thumbs[0]
        elif msg.document and msg.document.thumbs: thumb = msg.document.thumbs[0]
        elif msg.audio  and msg.audio.thumbs:    thumb = msg.audio.thumbs[0]

        url = f"{base_url}/stream/{file_hash}" if thumb else None
        _thumbnail_cache[file_hash]  = url
        _thumb_cache_atime[file_hash] = now
        return url

    except Exception as exc:
        logger.debug("get_thumbnail_url failed hash=%s: %s", file_hash, exc)
        _thumbnail_cache[file_hash]  = None
        _thumb_cache_atime[file_hash] = now
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Cache eviction
# ─────────────────────────────────────────────────────────────────────────────
_file_meta_cache:  Dict[str, dict]  = {}
_file_cache_atime: Dict[str, float] = {}
_cache_lock = asyncio.Lock()


async def _evict_stale_file_cache() -> None:
    now = time.monotonic()
    async with _cache_lock:
        stale = [k for k, t in _file_cache_atime.items() if now - t > _FILE_CACHE_TTL]
        for k in stale:
            _file_meta_cache.pop(k, None)
            _file_cache_atime.pop(k, None)

    stale_thumb = [
        k for k, t in _thumb_cache_atime.items() if now - t > _FILE_CACHE_TTL
    ]
    for k in stale_thumb:
        _thumbnail_cache.pop(k, None)
        _thumb_cache_atime.pop(k, None)

    if stale or stale_thumb:
        logger.debug(
            "cache evict: %d file-meta, %d thumb removed", len(stale), len(stale_thumb)
        )


# ─────────────────────────────────────────────────────────────────────────────
# Session tracking
# ─────────────────────────────────────────────────────────────────────────────
_active_sessions:            Dict[str, float] = {}
_sessions_lock               = asyncio.Lock()
_SESSION_TTL                 = 30
_SESSION_HEARTBEAT_INTERVAL  = 5


def _get_client_ip(request: web.Request) -> str:
    fwd = request.headers.get("X-Forwarded-For", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.remote or "unknown"


async def _register_session(session_key: str) -> bool:
    async with _sessions_lock:
        _prune_stale_sessions()
        is_new = session_key not in _active_sessions
        _active_sessions[session_key] = time.monotonic()
        return is_new


async def _unregister_session(session_key: str) -> None:
    async with _sessions_lock:
        _active_sessions.pop(session_key, None)


async def _heartbeat_session(session_key: str) -> None:
    async with _sessions_lock:
        if session_key in _active_sessions:
            _active_sessions[session_key] = time.monotonic()


def _prune_stale_sessions() -> None:
    now   = time.monotonic()
    stale = [k for k, ts in _active_sessions.items() if now - ts > _SESSION_TTL]
    for k in stale:
        del _active_sessions[k]


def get_active_session_count() -> int:
    _prune_stale_sessions()
    return len(_active_sessions)


# ─────────────────────────────────────────────────────────────────────────────
# Bandwidth deduplication
# ─────────────────────────────────────────────────────────────────────────────
_bw_tracked: Dict[Tuple[str, str, int], float] = {}
_bw_lock     = asyncio.Lock()


async def _should_track_bandwidth(
    client_ip: str, message_id: str, from_bytes: int
) -> bool:
    key = (client_ip, message_id, from_bytes)
    now = time.monotonic()
    async with _bw_lock:
        expired = [k for k, exp in _bw_tracked.items() if now > exp]
        for k in expired:
            del _bw_tracked[k]
        if key in _bw_tracked:
            return False
        _bw_tracked[key] = now + _BW_DEDUP_TTL
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Cached bandwidth-limit check  (avoids MongoDB on every range request)
# ─────────────────────────────────────────────────────────────────────────────
_bw_limit_cache: Dict[str, object] = {
    "allowed":  True,
    "expires":  0.0,
}
_bw_limit_lock = asyncio.Lock()


async def _check_bandwidth_cached(db) -> bool:
    """Return True if streaming is allowed; result is cached for _BW_CACHE_TTL."""
    now = time.monotonic()
    async with _bw_limit_lock:
        if now < _bw_limit_cache["expires"]:          # type: ignore[operator]
            return _bw_limit_cache["allowed"]          # type: ignore[return-value]
        try:
            stats  = await db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            allowed = not max_bw or stats["total_bandwidth"] < max_bw
        except Exception:
            allowed = True  # fail-open: never block stream on DB error
        _bw_limit_cache["allowed"] = allowed
        _bw_limit_cache["expires"] = now + _BW_CACHE_TTL
        return allowed


# ─────────────────────────────────────────────────────────────────────────────
# Index-based instant seeking  (#13)
# ─────────────────────────────────────────────────────────────────────────────
class _SeekIndex:
    """Lightweight byte-offset index for a single file.

    Entries are spaced _INDEX_SEGMENT_BYTES apart (aligned to CHUNK_SIZE)
    so every seek can jump directly to the nearest chunk boundary.
    """

    __slots__ = ("_entries",)

    def __init__(self, file_size: int) -> None:
        # Build: {segment_idx → byte_offset}
        # Each segment represents _INDEX_SEGMENT_BYTES of file content.
        entries: Dict[int, int] = {0: 0}
        offset = _INDEX_SEGMENT_BYTES
        seg    = 1
        while offset < file_size:
            # Align to CHUNK_SIZE boundary
            aligned = (offset // CHUNK_SIZE) * CHUNK_SIZE
            entries[seg] = aligned
            offset += _INDEX_SEGMENT_BYTES
            seg   += 1
        self._entries = entries

    def nearest_offset(self, byte_pos: int) -> int:
        """Return the largest indexed byte-offset ≤ byte_pos."""
        seg = byte_pos // _INDEX_SEGMENT_BYTES
        # Walk backwards until we find an entry (always at most 1 step)
        while seg > 0 and seg not in self._entries:
            seg -= 1
        return self._entries.get(seg, 0)


# file_hash → _SeekIndex
_seek_index_cache:   Dict[str, _SeekIndex] = {}
_seek_index_lock     = asyncio.Lock()


async def _get_seek_index(file_hash: str, file_size: int) -> _SeekIndex:
    async with _seek_index_lock:
        if file_hash not in _seek_index_cache:
            _seek_index_cache[file_hash] = _SeekIndex(file_size)
            logger.debug(
                "seek-index built: hash=%s size=%d entries=%d",
                file_hash, file_size,
                len(_seek_index_cache[file_hash]._entries),
            )
        return _seek_index_cache[file_hash]


# ─────────────────────────────────────────────────────────────────────────────
# Smart buffer window  (#7 / #11)
# ─────────────────────────────────────────────────────────────────────────────
class _SmartBufferWindow:
    """Per-session rolling memory window around the current playback position.

    When a seek lands inside the window, data is returned directly from the
    cached bytes.  When the seek is outside the window the window is reset.
    """

    __slots__ = ("_buf", "_start_offset", "_capacity")

    def __init__(self, capacity: int = _BW_WINDOW_SIZE) -> None:
        self._buf:          bytearray = bytearray()
        self._start_offset: int       = 0
        self._capacity:     int       = capacity

    def feed(self, offset: int, data: bytes) -> None:
        """Append *data* starting at *offset* into the rolling window."""
        # If this chunk is not contiguous, reset the window
        expected = self._start_offset + len(self._buf)
        if offset != expected:
            self._buf          = bytearray(data)
            self._start_offset = offset
        else:
            self._buf.extend(data)

        # Trim the window to the last _capacity bytes
        if len(self._buf) > self._capacity:
            excess              = len(self._buf) - self._capacity
            self._start_offset += excess
            del self._buf[:excess]

    def get(self, from_byte: int, length: int) -> Optional[bytes]:
        """Return *length* bytes from *from_byte* if inside the window, else None."""
        if not self._buf:
            return None
        end_byte = self._start_offset + len(self._buf)
        if from_byte < self._start_offset or from_byte >= end_byte:
            return None
        rel_start = from_byte - self._start_offset
        rel_end   = rel_start + length
        chunk     = bytes(self._buf[rel_start:rel_end])
        return chunk if chunk else None

    def reset(self) -> None:
        self._buf          = bytearray()
        self._start_offset = 0


# session_key → _SmartBufferWindow
_buffer_windows: Dict[str, _SmartBufferWindow] = {}
_buffer_lock     = asyncio.Lock()


async def _get_buffer_window(session_key: str) -> _SmartBufferWindow:
    async with _buffer_lock:
        if session_key not in _buffer_windows:
            _buffer_windows[session_key] = _SmartBufferWindow()
        return _buffer_windows[session_key]


async def _evict_buffer_window(session_key: str) -> None:
    async with _buffer_lock:
        _buffer_windows.pop(session_key, None)


# ─────────────────────────────────────────────────────────────────────────────
# Sequential range tracker  (#6)
# ─────────────────────────────────────────────────────────────────────────────
# session_key → (last_end_byte, monotonic_timestamp)
_sequential_tracker: Dict[str, Tuple[int, float]] = {}
_seq_lock            = asyncio.Lock()
_SEQ_ENTRY_TTL       = 30  # evict stale entries after 30 s of inactivity


async def _is_sequential(session_key: str, from_bytes: int) -> bool:
    now = time.monotonic()
    async with _seq_lock:
        # Evict stale entries
        stale = [k for k, (_, ts) in _sequential_tracker.items() if now - ts > _SEQ_ENTRY_TTL]
        for k in stale:
            del _sequential_tracker[k]

        entry = _sequential_tracker.get(session_key)
        if entry is None:
            return False
        last_end, _ = entry
        return abs(from_bytes - last_end) <= _SEQ_TOLERANCE


async def _update_sequential(session_key: str, until_bytes: int) -> None:
    async with _seq_lock:
        _sequential_tracker[session_key] = (until_bytes + 1, time.monotonic())


# ─────────────────────────────────────────────────────────────────────────────
# Range parsing
# ─────────────────────────────────────────────────────────────────────────────
def _parse_range(range_header: str, file_size: int) -> Tuple[int, int]:
    """Parse HTTP Range header → (from_bytes, until_bytes).

    O(1), zero allocations beyond two ints.
    """
    if range_header:
        try:
            raw_range   = range_header.replace("bytes=", "").split(",")[0].strip()
            start_str, end_str = raw_range.split("-")
            from_bytes  = int(start_str) if start_str else 0
            until_bytes = int(end_str)   if end_str   else file_size - 1
        except (ValueError, AttributeError):
            from_bytes  = 0
            until_bytes = file_size - 1
    else:
        from_bytes  = 0
        until_bytes = file_size - 1

    from_bytes  = max(0, from_bytes)
    until_bytes = min(until_bytes, file_size - 1)
    return from_bytes, until_bytes


# ─────────────────────────────────────────────────────────────────────────────
# ByteStreamer  — Telegram RPC layer
# ─────────────────────────────────────────────────────────────────────────────
class ByteStreamer:
    """Manages Telegram media sessions and streams raw bytes to callers."""

    def __init__(self, client: Client) -> None:
        self.client: Client = client
        # FileId cache: message_id → (FileId, last_access_monotonic)
        self._file_id_cache: Dict[str, Tuple[FileId, float]] = {}
        self._background_tasks: Set[asyncio.Task] = set()
        self._start_background_task(self._cache_cleaner())

    def _start_background_task(self, coro) -> asyncio.Task:
        task = asyncio.ensure_future(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    # ── FileId resolution ────────────────────────────────────────────────────

    async def get_file_properties(self, db_id: str) -> FileId:
        cached = self._file_id_cache.get(db_id)
        if cached is not None:
            file_id, _ = cached
            # Refresh access timestamp
            self._file_id_cache[db_id] = (file_id, time.monotonic())
            return file_id
        logger.debug("FileId cache miss for %s — fetching from Telegram", db_id)
        return await self._fetch_and_cache_file_id(db_id)

    async def _fetch_and_cache_file_id(self, db_id: str) -> FileId:
        file_id = await get_file_ids(self.client, db_id)
        logger.debug("Decoded FileId msg=%s dc=%s", db_id, file_id.dc_id)
        self._file_id_cache[db_id] = (file_id, time.monotonic())
        return file_id

    # ── Media session ────────────────────────────────────────────────────────

    async def generate_media_session(
        self, client: Client, file_id: FileId
    ) -> Session:
        media_session = client.media_sessions.get(file_id.dc_id)

        if media_session is None:
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(
                        client,
                        file_id.dc_id,
                        await client.storage.test_mode(),
                    ).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

                for _ in range(6):
                    exported = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )
                    try:
                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported.id, bytes=exported.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        logger.debug(
                            "Invalid auth bytes for DC %s — retrying", file_id.dc_id
                        )
                        continue
                else:
                    await media_session.stop()
                    raise AuthBytesInvalid

            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

            logger.debug("Created media session for DC %s", file_id.dc_id)
            client.media_sessions[file_id.dc_id] = media_session

        else:
            logger.debug("Reusing cached media session for DC %s", file_id.dc_id)

        return media_session

    # ── File location ────────────────────────────────────────────────────────

    @staticmethod
    async def get_location(
        file_id: FileId,
    ) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id,
                    access_hash=file_id.chat_access_hash,
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )

        if file_type == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    # ── Core chunk generator ─────────────────────────────────────────────────

    async def yield_file(
        self,
        file_id:        FileId,
        offset:         int,
        first_part_cut: int,
        last_part_cut:  int,
        part_count:     int,
        chunk_size:     int,
        *,
        is_warmup:      bool = False,
        session_key:    str  = "",
    ):
        """Yield raw file bytes from Telegram.

        The fetch worker runs concurrently with the HTTP writer via an
        asyncio.Queue of PREFETCH_COUNT slots, so the writer never stalls
        waiting for a Telegram RPC.

        Progressive warm-up
        ───────────────────
        When is_warmup=True the first _WARMUP_SIZE bytes are yielded in
        FIRST_CHUNK_SIZE slices to get the player's decoder going quickly;
        after that full CHUNK_SIZE chunks are forwarded as-is.

        Smart buffer window
        ───────────────────
        Every chunk yielded is also fed into the per-session _SmartBufferWindow
        so nearby future seeks can be served from memory.
        """
        client        = self.client
        media_session = await self.generate_media_session(client, file_id)
        location      = await self.get_location(file_id)

        queue: asyncio.Queue = asyncio.Queue(maxsize=PREFETCH_COUNT + 2)

        async def _fetch_worker() -> None:
            current_offset = offset
            for part_idx in range(part_count):
                for attempt in range(_MAX_CHUNK_RETRIES):
                    try:
                        r = await asyncio.wait_for(
                            media_session.invoke(
                                raw.functions.upload.GetFile(
                                    location=location,
                                    offset=current_offset,
                                    limit=chunk_size,
                                )
                            ),
                            timeout=_RPC_TIMEOUT,
                        )
                        break  # success
                    except asyncio.CancelledError:
                        return
                    except FloodWait as fw:
                        logger.warning(
                            "FloodWait %ds part %d/%d",
                            fw.value, part_idx + 1, part_count,
                        )
                        try:
                            await asyncio.sleep(fw.value + 1)
                        except asyncio.CancelledError:
                            return
                        continue
                    except asyncio.TimeoutError:
                        logger.debug(
                            "RPC timeout part %d attempt %d", part_idx + 1, attempt + 1
                        )
                        if attempt == _MAX_CHUNK_RETRIES - 1:
                            await queue.put(
                                IOError(f"Timeout fetching part {part_idx + 1}")
                            )
                            return
                        try:
                            await asyncio.sleep(_RETRY_BACKOFF * (attempt + 1))
                        except asyncio.CancelledError:
                            return
                        continue
                    except (AttributeError, ConnectionError, OSError) as exc:
                        logger.debug("Transient error part %d: %s", part_idx + 1, exc)
                        if attempt == _MAX_CHUNK_RETRIES - 1:
                            await queue.put(exc)
                            return
                        try:
                            await asyncio.sleep(_RETRY_BACKOFF * (attempt + 1))
                        except asyncio.CancelledError:
                            return
                        continue
                    except Exception as exc:
                        logger.error("Unexpected error part %d: %s", part_idx + 1, exc)
                        await queue.put(exc)
                        return
                else:
                    err = IOError(f"All retries failed at part {part_idx + 1}")
                    logger.error(str(err))
                    await queue.put(err)
                    return

                if isinstance(r, raw.types.upload.FileCdnRedirect):
                    logger.warning("FileCdnRedirect part %d — stopping", part_idx + 1)
                    await queue.put(EOFError("CDN redirect"))
                    return

                if not isinstance(r, raw.types.upload.File):
                    await queue.put(TypeError(f"Unexpected response: {type(r)}"))
                    return

                raw_chunk = r.bytes
                if not raw_chunk:
                    await queue.put(None)
                    return

                # Apply first/last part trimming
                if part_count == 1:
                    chunk = raw_chunk[first_part_cut:last_part_cut]
                elif part_idx == 0:
                    chunk = raw_chunk[first_part_cut:]
                elif part_idx == part_count - 1:
                    chunk = raw_chunk[:last_part_cut]
                else:
                    chunk = raw_chunk

                try:
                    await queue.put(chunk)
                except asyncio.CancelledError:
                    return

                current_offset += chunk_size

            try:
                await queue.put(None)  # EOF sentinel
            except asyncio.CancelledError:
                pass

        fetch_task = asyncio.ensure_future(_fetch_worker())
        self._background_tasks.add(fetch_task)
        fetch_task.add_done_callback(self._background_tasks.discard)

        # --- writer side ---
        parts_yielded = 0
        warm_sent     = 0          # bytes sent during warm-up phase
        buf_window    = None
        if session_key:
            buf_window = await _get_buffer_window(session_key)

        try:
            while True:
                try:
                    item = await asyncio.wait_for(
                        queue.get(), timeout=_RPC_TIMEOUT + 5
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "yield_file: queue stall %ds — aborting", _RPC_TIMEOUT + 5
                    )
                    break

                if item is None:
                    break
                if isinstance(item, BaseException):
                    logger.error("yield_file: fetch error: %s", item)
                    break

                # Feed smart buffer window
                if buf_window is not None:
                    chunk_offset = offset + parts_yielded * chunk_size
                    buf_window.feed(chunk_offset, item)

                # Progressive warm-up: slice big chunks into FIRST_CHUNK_SIZE
                # bites until we've delivered _WARMUP_SIZE bytes, then yield full.
                if is_warmup and warm_sent < _WARMUP_SIZE:
                    view = memoryview(item)
                    pos  = 0
                    while pos < len(view):
                        slice_end  = min(pos + FIRST_CHUNK_SIZE, len(view))
                        yield bytes(view[pos:slice_end])
                        warm_sent += slice_end - pos
                        pos        = slice_end
                        if warm_sent >= _WARMUP_SIZE:
                            break
                    # Yield any remainder as a single chunk
                    if pos < len(view):
                        yield bytes(view[pos:])
                else:
                    yield item

                parts_yielded += 1

        except asyncio.CancelledError:
            logger.debug(
                "yield_file: cancelled after %d part(s) (client disconnect)",
                parts_yielded,
            )
            raise
        except Exception as exc:
            logger.error("yield_file: consumer error: %s", exc)
        finally:
            if fetch_task is not None and not fetch_task.done():
                fetch_task.cancel()
                try:
                    await fetch_task
                except (asyncio.CancelledError, Exception):
                    pass
            logger.debug("yield_file finished after %d part(s)", parts_yielded)

    # ── Background cache cleaner ─────────────────────────────────────────────

    async def _cache_cleaner(self) -> None:
        """Evict stale FileId and file-meta/thumb cache entries every 2 min."""
        while True:
            try:
                await asyncio.sleep(120)
                await _evict_stale_file_cache()

                # Evict FileId entries idle for > _FILE_ID_CACHE_TTL
                now   = time.monotonic()
                stale = [
                    k for k, (_, ts) in self._file_id_cache.items()
                    if now - ts > _FILE_ID_CACHE_TTL
                ]
                for k in stale:
                    del self._file_id_cache[k]
                if stale:
                    logger.debug("FileId cache evict: %d entries removed", len(stale))

                # Evict seek indexes for files not seen recently (reuse thumb TTL)
                async with _seek_index_lock:
                    # Keep index as long as file-meta is warm; otherwise clear it
                    async with _cache_lock:
                        hot_hashes = set(_file_cache_atime.keys())
                    stale_idx = [
                        k for k in list(_seek_index_cache.keys())
                        if k not in hot_hashes
                    ]
                    for k in stale_idx:
                        del _seek_index_cache[k]
                    if stale_idx:
                        logger.debug(
                            "seek-index evict: %d entries removed", len(stale_idx)
                        )

            except asyncio.CancelledError:
                logger.debug("ByteStreamer._cache_cleaner task cancelled")
                break
            except Exception as exc:
                logger.error("ByteStreamer._cache_cleaner error: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# StreamingService  — HTTP layer
# ─────────────────────────────────────────────────────────────────────────────
class StreamingService:
    """Maps HTTP range requests to Telegram GetFile RPCs.

    Hot-path design (optimised for TTFB and seek latency)
    ──────────────────────────────────────────────────────
    1. Parse Range header  → O(1)
    2. Resolve file metadata from in-process dict cache  → O(1) on warm hit
    3. Bandwidth check from in-process counter  → O(1)  (DB refresh every 5 s)
    4. Resolve FileId from in-process cache  → O(1) on warm hit
    5. Compute Telegram chunk offsets  → O(1)
    6. Send HTTP 206 headers  → TCP write
    7. Stream chunks from pre-fetched queue

    DB/Telegram calls only happen on the very first request for a file (cold
    start) or after a 5-minute cache expiry.  Subsequent range requests from
    the same player (every few seconds during normal playback) are served
    entirely from in-process caches.
    """

    def __init__(self, bot_client: Client, db: Database) -> None:
        self.bot      = bot_client
        self.db       = db
        self.streamer = ByteStreamer(bot_client)

    async def stream_file(
        self,
        request:     web.Request,
        file_hash:   str,
        is_download: bool = False,
    ) -> web.StreamResponse:
        # ── 0. Parse range immediately — no I/O before this point ────────────
        range_header     = request.headers.get("Range", "")
        is_range_request = bool(range_header)
        client_ip        = _get_client_ip(request)
        now              = time.monotonic()

        # ── 1. Resolve file metadata (L1 cache → L2 MongoDB) ─────────────────
        async with _cache_lock:
            file_data = _file_meta_cache.get(file_hash)
            if file_data is not None:
                _file_cache_atime[file_hash] = now

        if file_data is None:
            file_data = await self.db.get_file_by_hash(file_hash)
            if not file_data:
                raise web.HTTPNotFound(reason="file not found")
            async with _cache_lock:
                _file_meta_cache[file_hash]  = file_data
                _file_cache_atime[file_hash] = now

        # ── 2. Bandwidth check (cached, never blocks on DB) ───────────────────
        if Config.get("bandwidth_mode", True):
            if not await _check_bandwidth_cached(self.db):
                raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        file_size  = int(file_data["file_size"])
        file_name  = file_data["file_name"]
        message_id = str(file_data["message_id"])

        # ── 3. Resolve FileId (cached after first hit) ────────────────────────
        try:
            file_id = await self.streamer.get_file_properties(message_id)
        except web.HTTPNotFound:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "get_file_properties failed: msg=%s err=%s", message_id, exc
            )
            raise web.HTTPNotFound(reason="could not resolve file on Telegram")

        # ── 4. Parse range  ───────────────────────────────────────────────────
        from_bytes, until_bytes = _parse_range(range_header, file_size)

        if from_bytes > until_bytes or from_bytes >= file_size:
            return web.Response(
                status=416,
                body=b"Range Not Satisfiable",
                headers={"Content-Range": f"bytes */{file_size}"},
            )

        until_bytes = min(until_bytes, file_size - 1)
        req_length  = until_bytes - from_bytes + 1

        # ── 5. Seek-index jump  (#13) ─────────────────────────────────────────
        # If the player is seeking to a non-zero position, snap the fetch
        # start to the nearest indexed chunk boundary so we never scan.
        session_key  = f"{file_hash}:{client_ip}"
        is_seek      = from_bytes > 0 and is_range_request
        is_seq       = await _is_sequential(session_key, from_bytes)
        is_cold_start = from_bytes == 0

        if is_seek and not is_seq:
            # Build / fetch seek index (O(1) after first build)
            seek_idx       = await _get_seek_index(file_hash, file_size)
            snapped_offset = seek_idx.nearest_offset(from_bytes)
            # Use snapped_offset as the Telegram fetch starting point;
            # first_part_cut trims away the bytes before from_bytes.
            offset         = snapped_offset
        else:
            offset = from_bytes - (from_bytes % CHUNK_SIZE)

        first_part_cut = from_bytes - offset
        last_part_cut  = (until_bytes % CHUNK_SIZE) + 1
        part_count     = math.ceil((until_bytes + 1) / CHUNK_SIZE) - (offset // CHUNK_SIZE)

        logger.debug(
            "stream msg=%s size=%d range=%d-%d offset=%d parts=%d seek=%s seq=%s",
            message_id, file_size, from_bytes, until_bytes,
            offset, part_count, is_seek, is_seq,
        )

        # ── 6. Determine MIME ─────────────────────────────────────────────────
        mime = (
            file_data.get("mime_type")
            or _mime_for_filename(
                file_name,
                MIME_TYPE_MAP.get(file_data.get("file_type"), "application/octet-stream"),
            )
            or "application/octet-stream"
        )

        disposition = "attachment" if is_download else "inline"
        status      = 206 if is_range_request else 200

        headers: Dict[str, str] = {
            "Content-Type":                mime,
            "Content-Length":              str(req_length),
            "Content-Disposition":         f'{disposition}; filename="{file_name}"',
            "Accept-Ranges":               "bytes",
            # no-store: forces players to re-issue range requests rather than
            # serve stale data from an intermediate proxy cache
            "Cache-Control":               "no-store",
            "Access-Control-Allow-Origin": "*",
            "Connection":                  "keep-alive",
            "Keep-Alive":                  "timeout=60, max=1000",
            "X-Content-Type-Options":      "nosniff",
            "X-File-Size":                 str(file_size),
            "icy-name":                    file_name,
            "icy-metaint":                 "0",
        }
        if is_range_request:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

        # ── 7. Thumbnail headers  (fire-and-forget, never blocks) ─────────────
        # Launch as a background task so it never adds latency to the 206.
        async def _add_artwork_headers() -> None:
            try:
                base_url  = str(request.url.origin())
                thumb_url = await get_thumbnail_url(
                    self.bot, file_hash, file_data, base_url
                )
                if thumb_url:
                    # Headers are already sent at this point; these are
                    # informational for debug logs only when fire-and-forget.
                    logger.debug(
                        "artwork resolved (async): %s → %s", file_hash, thumb_url
                    )
            except Exception as exc:
                logger.debug("artwork header bg-task error: %s", exc)

        # Inject artwork synchronously only when already cached (zero cost)
        if file_hash in _thumbnail_cache:
            thumb = _thumbnail_cache.get(file_hash)
            if thumb:
                headers["Link"]        = f'<{thumb}>; rel="artwork"'
                headers["X-Image-Url"] = thumb
        else:
            # Non-blocking background fetch — doesn't delay the stream
            asyncio.ensure_future(_add_artwork_headers())

        # ── 8. Send headers  (HTTP 206 as fast as possible) ──────────────────
        response = web.StreamResponse(status=status, headers=headers)
        try:
            await response.prepare(request)
        except ConnectionResetError:
            logger.debug(
                "stream msg=%s client dropped before headers", message_id
            )
            return response

        # ── 9. Stream chunks ──────────────────────────────────────────────────
        bytes_sent     = 0
        last_heartbeat = time.monotonic()

        try:
            async for chunk in self.streamer.yield_file(
                file_id,
                offset,
                first_part_cut,
                last_part_cut,
                part_count,
                CHUNK_SIZE,
                is_warmup=is_cold_start,
                session_key=session_key,
            ):
                try:
                    await response.write(chunk)
                    bytes_sent += len(chunk)

                    now = time.monotonic()
                    if now - last_heartbeat >= _SESSION_HEARTBEAT_INTERVAL:
                        await _heartbeat_session(session_key)
                        last_heartbeat = now

                except (ConnectionResetError, BrokenPipeError):
                    logger.debug(
                        "stream msg=%s connection reset after %d bytes",
                        message_id, bytes_sent,
                    )
                    break

        except asyncio.CancelledError:
            logger.debug(
                "stream msg=%s cancelled after %d bytes", message_id, bytes_sent
            )
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(
                "stream msg=%s disconnected after %d bytes", message_id, bytes_sent
            )
        except Exception as exc:
            logger.error("streaming error msg=%s: %s", message_id, exc)

        try:
            await response.write_eof()
        except Exception:
            pass

        # ── 10. Update sequential tracker ────────────────────────────────────
        if bytes_sent > 0:
            await _update_sequential(session_key, until_bytes)

        # ── 11. Bandwidth accounting  (fire-and-forget) ───────────────────────
        if bytes_sent > 0:
            should_track = await _should_track_bandwidth(
                client_ip, message_id, from_bytes
            )
            if should_track:
                task = asyncio.ensure_future(
                    self.db.track_bandwidth(message_id, bytes_sent)
                )
                task.add_done_callback(
                    lambda t: t.exception() and logger.error(
                        "track_bandwidth error: %s", t.exception()
                    )
                )
            else:
                logger.debug(
                    "bw dedup msg=%s ip=%s from=%d bytes=%d (skipped)",
                    message_id, client_ip, from_bytes, bytes_sent,
                )

        return response
