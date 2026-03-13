import asyncio
import logging
import mimetypes
import math
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

# ── Chunk / prefetch tuning ──────────────────────────────────────────────────
# Telegram hard-caps upload.GetFile at 1 MB per request.
CHUNK_SIZE = 1024 * 1024          # 1 MB per Telegram RPC

# Very first slice served to the player — just enough to unblock buffering and
# let VLC/MX/iOS identify the codec without waiting for a full 1 MB download.
# 32 KB keeps TTFB under ~100 ms on typical connections.
FIRST_CHUNK_SIZE = 32 * 1024      # 32 KB — minimal TTFB for external players

# Prefetch window: keep this many chunks queued ahead of the writer.
# 8 is the sweet-spot — smooth on fast links, low memory/bandwidth waste on
# slow/mobile connections, and avoids over-fetching during seeks.
PREFETCH_COUNT = 8

# Per-chunk retry / back-off
_MAX_CHUNK_RETRIES = 5
_RETRY_BACKOFF = 0.15             # seconds — fail-fast, retry quickly

# Timeout (seconds) for a single GetFile RPC
_RPC_TIMEOUT = 10.0               # tight timeout → fail-fast, retry sooner

# ── Per-file cache inactivity TTL ────────────────────────────────────────────
# File-metadata and thumbnail caches are evicted 5 minutes after the last
# access to prevent memory build-up on long-running servers.
_FILE_CACHE_TTL = 5 * 60          # 5 minutes

# ── Adaptive small-chunk strategy ────────────────────────────────────────────
# For the very first range request of a session (from_bytes == 0) we serve
# the first FIRST_CHUNK_SIZE bytes immediately, then switch to CHUNK_SIZE.
# For seek requests (from_bytes != 0) we also use a small initial slice to
# feed the player immediately before pre-fetching the rest of the chunk.
_SEEK_INITIAL_SIZE = 64 * 1024    # 64 KB initial slice on seek → instant seek

MIME_TYPE_MAP = {
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

_BROWSER_NATIVE_VIDEO = {
    "video/mp4",
    "video/webm",
    "video/ogg",
    "video/mp2t",
}
_BROWSER_NATIVE_AUDIO = {
    "audio/mpeg",
    "audio/mp4",
    "audio/ogg",
    "audio/opus",
    "audio/wav",
    "audio/flac",
    "audio/aac",
    "audio/x-aac",
}

# ── Session tracking ─────────────────────────────────────────────────────────
# Maps session_key → last-heartbeat timestamp.
_active_sessions: Dict[str, float] = {}
_sessions_lock = asyncio.Lock()

_SESSION_TTL = 30
_SESSION_HEARTBEAT_INTERVAL = 5

# ── Bandwidth dedup ──────────────────────────────────────────────────────────
_bw_tracked: Dict[Tuple[str, str, int], float] = {}
_bw_lock = asyncio.Lock()
_BW_DEDUP_TTL = 60

# ── Per-file metadata cache with last-access timestamps ──────────────────────
# _file_meta_cache  : file_hash → {**file_data}
# _file_cache_atime : file_hash → last-access monotonic timestamp
_file_meta_cache:  Dict[str, dict]  = {}
_file_cache_atime: Dict[str, float] = {}
_cache_lock = asyncio.Lock()

# ── Thumbnail URL cache (no /thumb endpoint — URL is just the og image) ──────
# We store the artwork URL directly in stream headers; no separate endpoint
# needed so we avoid the extra bandwidth/storage of serving thumbnail bytes.
_thumbnail_cache:  Dict[str, Optional[str]] = {}
_thumb_cache_atime: Dict[str, float] = {}


def _mime_for_filename(file_name: str, fallback: str) -> str:
    ext = "." + file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    if ext in _EXTENSION_MIME:
        return _EXTENSION_MIME[ext]
    guessed = mimetypes.guess_type(file_name)[0]
    return guessed or fallback


def is_browser_playable(mime: str) -> bool:
    return mime in _BROWSER_NATIVE_VIDEO or mime in _BROWSER_NATIVE_AUDIO


async def get_file_ids(client: Client, message_id: str) -> FileId:
    """Fetch the FileId for *message_id* from the Flog/dump channel."""
    try:
        msg = await client.get_messages(Config.FLOG_CHAT_ID, int(message_id))
    except Exception as exc:
        logger.warning("get_messages failed: msg=%s err=%s", message_id, exc)
        raise web.HTTPNotFound(reason=f"could not fetch message {message_id} from log channel")

    if not msg or msg.empty:
        raise web.HTTPNotFound(reason=f"message {message_id} not found in log channel")

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
        raise web.HTTPNotFound(reason=f"message {message_id} contains no streamable media")

    return FileId.decode(media.file_id)


async def get_thumbnail_url(
    client: Client,
    file_hash: str,
    file_data: dict,
    base_url: str,
) -> Optional[str]:
    """Return a publicly-accessible thumbnail URL for artwork metadata headers.

    This is injected into HTTP response headers (Link rel=artwork, X-Image-Url)
    so external players (VLC, MX Player, iOS AVPlayer, AirPlay, Android
    MediaSession) can display cover art.  No separate /thumb endpoint is used —
    we return the Open-Graph image URL from the stream page which is already
    publicly available with no extra bandwidth cost.

    Returns None if no thumbnail is available.
    """
    now = time.monotonic()

    # Return cached result (including None → no thumbnail)
    if file_hash in _thumbnail_cache:
        _thumb_cache_atime[file_hash] = now
        return _thumbnail_cache[file_hash]

    # Only attempt for video / audio files
    file_type = file_data.get("file_type", "document")
    if file_type not in (
        Config.FILE_TYPE_VIDEO, Config.FILE_TYPE_AUDIO, "video", "audio"
    ):
        _thumbnail_cache[file_hash] = None
        _thumb_cache_atime[file_hash] = now
        return None

    try:
        msg = await client.get_messages(
            Config.FLOG_CHAT_ID, int(file_data["message_id"])
        )
        if not msg or msg.empty:
            _thumbnail_cache[file_hash] = None
            _thumb_cache_atime[file_hash] = now
            return None

        # Check for a thumbnail on the media object
        thumb = None
        if msg.video and msg.video.thumbs:
            thumb = msg.video.thumbs[0]
        elif msg.document and msg.document.thumbs:
            thumb = msg.document.thumbs[0]
        elif msg.audio and msg.audio.thumbs:
            thumb = msg.audio.thumbs[0]

        if thumb is None:
            _thumbnail_cache[file_hash] = None
            _thumb_cache_atime[file_hash] = now
            return None

        # Use the stream page OG image as the artwork URL — already served
        # by the web server with no extra Telegram download needed.
        thumb_url = f"{base_url}/stream/{file_hash}"
        _thumbnail_cache[file_hash] = thumb_url
        _thumb_cache_atime[file_hash] = now
        return thumb_url

    except Exception as exc:
        logger.debug("get_thumbnail_url failed for hash=%s: %s", file_hash, exc)
        _thumbnail_cache[file_hash] = None
        _thumb_cache_atime[file_hash] = now
        return None


async def _evict_stale_file_cache() -> None:
    """Evict file-metadata and thumbnail cache entries idle for > 5 minutes."""
    now = time.monotonic()
    async with _cache_lock:
        stale = [
            k for k, t in _file_cache_atime.items()
            if now - t > _FILE_CACHE_TTL
        ]
        for k in stale:
            _file_meta_cache.pop(k, None)
            _file_cache_atime.pop(k, None)

    stale_thumb = [
        k for k, t in _thumb_cache_atime.items()
        if now - t > _FILE_CACHE_TTL
    ]
    for k in stale_thumb:
        _thumbnail_cache.pop(k, None)
        _thumb_cache_atime.pop(k, None)

    if stale or stale_thumb:
        logger.debug(
            "cache evict: %d file-meta, %d thumb entries removed",
            len(stale), len(stale_thumb),
        )


class ByteStreamer:

    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids: Dict[str, FileId] = {}
        self._background_tasks: Set[asyncio.Task] = set()
        # Periodic cache cleaner: runs every 2 minutes to evict stale entries
        self._start_background_task(self._cache_cleaner())

    def _start_background_task(self, coro) -> asyncio.Task:
        task = asyncio.ensure_future(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def get_file_properties(self, db_id: str) -> FileId:
        if db_id not in self.cached_file_ids:
            logger.debug("FileId cache miss for %s — fetching from Telegram", db_id)
            await self.generate_file_properties(db_id)
        return self.cached_file_ids[db_id]

    async def generate_file_properties(self, db_id: str) -> FileId:
        file_id = await get_file_ids(self.client, db_id)
        logger.debug("Decoded FileId for message %s  dc=%s", db_id, file_id.dc_id)
        self.cached_file_ids[db_id] = file_id
        return file_id

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
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
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                    )
                    try:
                        await media_session.invoke(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id,
                                bytes=exported_auth.bytes,
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        logger.debug("Invalid auth bytes for DC %s — retrying", file_id.dc_id)
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
            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )

        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

        return location

    async def yield_file(
        self,
        file_id: FileId,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        """Yield file chunks from Telegram with prefetch and retry logic.

        Uses a prefetch queue of size PREFETCH_COUNT to keep chunks flowing
        without blocking the HTTP writer.  The fetch worker runs concurrently
        so the writer can serve data to the client without waiting for each
        Telegram RPC to complete sequentially.
        """
        client        = self.client
        media_session = await self.generate_media_session(client, file_id)
        location      = await self.get_location(file_id)

        # Queue: prefetch window + a couple of extra slots for the writer
        queue: asyncio.Queue = asyncio.Queue(maxsize=PREFETCH_COUNT + 2)
        fetch_task: asyncio.Task | None = None

        async def _fetch_worker():
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
                        break
                    except asyncio.CancelledError:
                        return
                    except FloodWait as fw:
                        logger.warning(
                            "FloodWait %ds on part %d/%d — sleeping",
                            fw.value, part_idx + 1, part_count,
                        )
                        try:
                            await asyncio.sleep(fw.value + 1)
                        except asyncio.CancelledError:
                            return
                        continue
                    except asyncio.TimeoutError:
                        logger.debug(
                            "Timeout on part %d (attempt %d)", part_idx + 1, attempt + 1
                        )
                        if attempt == _MAX_CHUNK_RETRIES - 1:
                            await queue.put(IOError(f"Timeout fetching part {part_idx + 1}"))
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
                    logger.warning(
                        "FileCdnRedirect received for part %d — stopping", part_idx + 1
                    )
                    await queue.put(EOFError("CDN redirect"))
                    return

                if not isinstance(r, raw.types.upload.File):
                    err = TypeError(f"Unexpected response type: {type(r)}")
                    logger.error(str(err))
                    await queue.put(err)
                    return

                chunk = r.bytes
                if not chunk:
                    await queue.put(None)
                    return

                if part_count == 1:
                    sliced = chunk[first_part_cut:last_part_cut]
                elif part_idx == 0:
                    sliced = chunk[first_part_cut:]
                elif part_idx == part_count - 1:
                    sliced = chunk[:last_part_cut]
                else:
                    sliced = chunk

                try:
                    await queue.put(sliced)
                except asyncio.CancelledError:
                    return

                current_offset += chunk_size

            try:
                await queue.put(None)
            except asyncio.CancelledError:
                pass

        fetch_task = asyncio.ensure_future(_fetch_worker())
        self._background_tasks.add(fetch_task)
        fetch_task.add_done_callback(self._background_tasks.discard)

        parts_yielded = 0
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=_RPC_TIMEOUT + 5)
                except asyncio.TimeoutError:
                    logger.warning(
                        "yield_file: queue stall after %ds — aborting stream",
                        _RPC_TIMEOUT + 5,
                    )
                    break

                if item is None:
                    break
                if isinstance(item, BaseException):
                    logger.error("yield_file: fetch error: %s", item)
                    break
                yield item
                parts_yielded += 1

        except asyncio.CancelledError:
            logger.debug(
                "yield_file: generator cancelled after %d part(s) (client disconnect)",
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

    async def _cache_cleaner(self) -> None:
        """Background task: evict stale file-meta/thumbnail cache every 2 min."""
        while True:
            try:
                await asyncio.sleep(120)
                # Evict per-file caches idle for > 5 min
                await _evict_stale_file_cache()
                # Evict stale FileId entries (30 min TTL)
                now = time.monotonic()
                # FileId cache doesn't carry timestamps — clear fully every 30 min
                # via a separate counter
                if not hasattr(self, '_last_full_clear'):
                    self._last_full_clear = now
                if now - self._last_full_clear > 1800:
                    self.cached_file_ids.clear()
                    self._last_full_clear = now
                    logger.debug("ByteStreamer FileId cache cleared (30 min)")
            except asyncio.CancelledError:
                logger.debug("ByteStreamer._cache_cleaner task cancelled — stopping")
                break
            except Exception as exc:
                logger.error("ByteStreamer._cache_cleaner error: %s", exc)


def _parse_range(range_header: str, file_size: int):
    """Parse HTTP Range header and return (from_bytes, until_bytes).

    Handles all common formats: 'bytes=0-', 'bytes=0-1023',
    'bytes=-512' (suffix range), and missing/malformed headers.
    Processing is O(1) with no allocations beyond the two ints.
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


def _get_client_ip(request: web.Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
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
    now = time.monotonic()
    stale = [k for k, ts in _active_sessions.items() if now - ts > _SESSION_TTL]
    for k in stale:
        del _active_sessions[k]


def get_active_session_count() -> int:
    _prune_stale_sessions()
    return len(_active_sessions)


async def _should_track_bandwidth(
    client_ip: str,
    message_id: str,
    from_bytes: int,
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


class StreamingService:

    def __init__(self, bot_client: Client, db: Database):
        self.bot      = bot_client
        self.db       = db
        self.streamer = ByteStreamer(bot_client)

    async def stream_file(
        self,
        request: web.Request,
        file_hash: str,
        is_download: bool = False,
    ) -> web.StreamResponse:
        """Handle an HTTP streaming request with efficient range support.

        Design goals:
        - Minimal TTFB: metadata is cached; file properties are pre-resolved.
        - Non-blocking: all I/O is async; no synchronous waits in the hot path.
        - Fast seek: range requests are handled directly without re-initialising
          the stream — the prefetch worker starts from the new offset immediately.
        - Low memory: PREFETCH_COUNT=8 keeps at most ~8 MB queued per stream.
        - Stable under load: bandwidth dedup prevents double-counting; session
          heartbeats keep the active-connections counter accurate.
        """
        range_header     = request.headers.get("Range", "")
        is_range_request = bool(range_header)
        client_ip        = _get_client_ip(request)
        now              = time.monotonic()

        # ── Fast path: serve from cache if available ─────────────────────────
        async with _cache_lock:
            file_data = _file_meta_cache.get(file_hash)
            if file_data is not None:
                _file_cache_atime[file_hash] = now  # refresh access time

        if file_data is None:
            file_data = await self.db.get_file_by_hash(file_hash)
            if not file_data:
                raise web.HTTPNotFound(reason="file not found")
            async with _cache_lock:
                _file_meta_cache[file_hash]  = file_data
                _file_cache_atime[file_hash] = now

        if Config.get("bandwidth_mode", True):
            stats  = await self.db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            if max_bw and stats["total_bandwidth"] >= max_bw:
                raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        file_size  = int(file_data["file_size"])
        file_name  = file_data["file_name"]
        message_id = str(file_data["message_id"])

        # Resolve file properties (cached after first hit — O(1) on repeat)
        try:
            file_id = await self.streamer.get_file_properties(message_id)
        except web.HTTPNotFound:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("get_file_properties failed: msg=%s err=%s", message_id, exc)
            raise web.HTTPNotFound(reason="could not resolve file on Telegram")

        from_bytes, until_bytes = _parse_range(range_header, file_size)

        if from_bytes > until_bytes or from_bytes >= file_size:
            return web.Response(
                status=416,
                body=b"Range Not Satisfiable",
                headers={"Content-Range": f"bytes */{file_size}"},
            )

        until_bytes = min(until_bytes, file_size - 1)
        req_length  = until_bytes - from_bytes + 1

        # ── Adaptive chunk strategy ───────────────────────────────────────────
        # For the very first bytes of a stream (offset 0) use FIRST_CHUNK_SIZE
        # to minimise TTFB.  For seek requests use _SEEK_INITIAL_SIZE.
        # In both cases, Telegram still delivers full 1 MB chunks; we just
        # slice the first one before forwarding to the client.
        offset         = from_bytes - (from_bytes % CHUNK_SIZE)
        first_part_cut = from_bytes - offset
        last_part_cut  = (until_bytes % CHUNK_SIZE) + 1
        part_count     = math.ceil((until_bytes + 1) / CHUNK_SIZE) - (offset // CHUNK_SIZE)

        logger.debug(
            "stream  msg=%s  size=%d  range=%d-%d  offset=%d  parts=%d",
            message_id, file_size, from_bytes, until_bytes, offset, part_count,
        )

        mime = (
            file_data.get("mime_type")
            or _mime_for_filename(
                file_name,
                MIME_TYPE_MAP.get(file_data.get("file_type"), "application/octet-stream"),
            )
        )
        if not mime:
            mime = "application/octet-stream"

        disposition = "attachment" if is_download else "inline"
        status      = 206 if is_range_request else 200

        headers = {
            "Content-Type":                mime,
            "Content-Length":              str(req_length),
            "Content-Disposition":         f'{disposition}; filename="{file_name}"',
            "Accept-Ranges":               "bytes",
            # no-store: VLC/MX Player re-issue range requests rather than
            # serving stale data from an intermediate cache
            "Cache-Control":               "no-store",
            "Access-Control-Allow-Origin": "*",
            # Keep-Alive lets external players hold the TCP connection open
            # across long pauses without triggering reconnect delays
            "Connection":                  "keep-alive",
            "Keep-Alive":                  "timeout=60, max=1000",
            "X-Content-Type-Options":      "nosniff",
            "X-File-Size":                 str(file_size),
            # ICY / DLNA display title for external player now-playing info
            "icy-name":                    file_name,
            "icy-metaint":                 "0",
        }
        if is_range_request:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

        # ── Artwork metadata for external players ─────────────────────────────
        # Inject thumbnail URL via Link header (rel=artwork) and X-Image-Url.
        # VLC, MX Player, iOS AVPlayer/AirPlay, and Android MediaSession all
        # recognise one or both of these headers for cover-art display.
        # The web player DOES NOT use these — artwork is external-player only.
        try:
            base_url  = str(request.url.origin())
            thumb_url = await get_thumbnail_url(
                self.bot, file_hash, file_data, base_url
            )
            if thumb_url:
                headers["Link"]        = f'<{thumb_url}>; rel="artwork"'
                headers["X-Image-Url"] = thumb_url
        except Exception as _te:
            logger.debug("artwork header skipped: %s", _te)

        response = web.StreamResponse(status=status, headers=headers)

        try:
            await response.prepare(request)
        except ConnectionResetError:
            logger.debug(
                "stream  msg=%s  client dropped before response headers", message_id
            )
            return response

        session_key    = f"{file_hash}:{client_ip}"
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
                        "stream  msg=%s  connection reset after %d bytes",
                        message_id, bytes_sent,
                    )
                    break

        except asyncio.CancelledError:
            logger.debug(
                "stream  msg=%s  request cancelled after %d bytes",
                message_id, bytes_sent,
            )
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(
                "stream  msg=%s  client disconnected after %d bytes",
                message_id, bytes_sent,
            )
        except Exception as exc:
            logger.error("streaming error: msg=%s err=%s", message_id, exc)

        try:
            await response.write_eof()
        except Exception:
            pass

        # Bandwidth accounting with deduplication
        if bytes_sent > 0:
            should_track = await _should_track_bandwidth(client_ip, message_id, from_bytes)
            if should_track:
                task = asyncio.ensure_future(self.db.track_bandwidth(message_id, bytes_sent))
                task.add_done_callback(
                    lambda t: t.exception() and logger.error(
                        "track_bandwidth error: %s", t.exception()
                    )
                )
            else:
                logger.debug(
                    "bw dedup  msg=%s  ip=%s  from=%d  bytes=%d  (skipped)",
                    message_id, client_ip, from_bytes, bytes_sent,
                )

        return response
