import asyncio
import logging
import mimetypes
import math
import time
from typing import Dict, Set, Tuple, Union

from aiohttp import web
from pyrogram import Client, utils, raw
from pyrogram.errors import AuthBytesInvalid, FloodWait
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

from config import Config
from database import Database

logger = logging.getLogger(__name__)

# Telegram hard-caps upload.GetFile at 1 MB per request.
CHUNK_SIZE = 1024 * 1024

# For the very first chunk of a fresh stream (offset=0) we only need a tiny
# slice of data to unblock the browser and let it start rendering.  After that
# the normal 1 MB chunk size is used.  Setting this too small wastes RTTs;
# setting it too large delays first-byte.  128 KB is a good sweet-spot that
# satisfies HTTP range-sniff probes AND lets browsers start decoding quickly.
FIRST_CHUNK_SIZE = 128 * 1024   # 128 KB for lowest TTFB on the initial request

# Keep this many chunks pre-fetched ahead of the writer.
# Higher values smooth playback on fast connections at the cost of a little
# extra memory per stream.
PREFETCH_COUNT = 8

# Per-chunk retry cap and back-off base (seconds).
_MAX_CHUNK_RETRIES = 6
_RETRY_BACKOFF = 0.3

# Timeout (seconds) for a single GetFile RPC.
_RPC_TIMEOUT = 15.0

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

# Session dedup — maps session_key → last-heartbeat timestamp.
# A "session" is (file_hash, client_ip).  Multiple consecutive range requests
# from the same player are merged into one entry so the active-connections
# counter never oscillates 0→1→0 during normal seek/probe sequences.
_active_sessions: Dict[str, float] = {}
_sessions_lock = asyncio.Lock()

# A session is considered live while its heartbeat was updated within this many
# seconds.  The heartbeat is refreshed every _SESSION_HEARTBEAT_INTERVAL seconds
# while data is flowing.  When the stream ends the entry is removed immediately.
_SESSION_TTL = 30
_SESSION_HEARTBEAT_INTERVAL = 5

# Bandwidth dedup — prevents counting the same byte range twice when a player
# issues probe/prefetch requests before settling on the real playback position.
# Key: (client_ip, message_id, from_bytes)  Value: expiry timestamp
_bw_tracked: Dict[Tuple[str, str, int], float] = {}
_bw_lock = asyncio.Lock()
_BW_DEDUP_TTL = 60

# In-memory file-metadata cache (keyed by file_hash).
_file_meta_cache: Dict[str, dict] = {}


def _mime_for_filename(file_name: str, fallback: str) -> str:
    ext = "." + file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    if ext in _EXTENSION_MIME:
        return _EXTENSION_MIME[ext]
    guessed = mimetypes.guess_type(file_name)[0]
    return guessed or fallback


def is_browser_playable(mime: str) -> bool:
    return mime in _BROWSER_NATIVE_VIDEO or mime in _BROWSER_NATIVE_AUDIO


async def get_file_ids(client: Client, message_id: str) -> FileId:
    """Fetch the FileId for *message_id* from the Flog/dump channel.

    Raises ``web.HTTPNotFound`` (instead of a plain ValueError) when the
    message cannot be found or contains no streamable media, so callers can
    surface a clean 404 to the browser rather than a 500.
    """
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


class ByteStreamer:

    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids: Dict[str, FileId] = {}
        self.clean_timer: int = 30 * 60
        self._background_tasks: Set[asyncio.Task] = set()
        self._start_background_task(self.clean_cache())

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
        client        = self.client
        media_session = await self.generate_media_session(client, file_id)
        location      = await self.get_location(file_id)

        # Queue capacity: prefetch window + a couple of writer slots.
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
                        "yield_file: queue stall after %ds — aborting stream", _RPC_TIMEOUT + 5
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

    async def clean_cache(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.clean_timer)
                self.cached_file_ids.clear()
                _file_meta_cache.clear()
                logger.debug("ByteStreamer cache cleared")
            except asyncio.CancelledError:
                logger.debug("ByteStreamer.clean_cache task cancelled — stopping")
                break
            except Exception as exc:
                logger.error("ByteStreamer.clean_cache error: %s", exc)


def _parse_range(range_header: str, file_size: int):
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

        range_header     = request.headers.get("Range", "")
        is_range_request = bool(range_header)
        client_ip        = _get_client_ip(request)

        if file_hash in _file_meta_cache:
            file_data = _file_meta_cache[file_hash]
        else:
            file_data = await self.db.get_file_by_hash(file_hash)
            if not file_data:
                raise web.HTTPNotFound(reason="file not found")
            _file_meta_cache[file_hash] = file_data

        if Config.get("bandwidth_mode", True):
            stats  = await self.db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            if max_bw and stats["total_bandwidth"] >= max_bw:
                raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        file_size  = int(file_data["file_size"])
        file_name  = file_data["file_name"]
        message_id = str(file_data["message_id"])

        # Verify file properties from the Flog/dump channel.
        # get_file_properties first checks in-memory cache; on miss it calls
        # get_file_ids which fetches the Telegram message and raises
        # web.HTTPNotFound if the message is gone or contains no media.
        try:
            file_id = await self.streamer.get_file_properties(message_id)
        except web.HTTPNotFound:
            # Re-raise directly — the message is gone from the log channel.
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

        # Build the thumbnail URL — used by external players (VLC, MX Player)
        # that read HTTP response headers to locate poster/cover art.
        _base_url = str(request.url.origin())
        _thumb_url = (
            file_data.get("thumbnail_url")
            or f"{_base_url}/thumbnail/{file_hash}"
        )

        headers = {
            "Content-Type":                mime,
            "Content-Length":              str(req_length),
            "Content-Disposition":         f'{disposition}; filename="{file_name}"',
            "Accept-Ranges":               "bytes",
            # no-store ensures VLC / MX Player re-issues range requests
            # instead of serving stale cached data.
            "Cache-Control":               "no-store",
            "Access-Control-Allow-Origin": "*",
            # TCP keepalive helps external players maintain the connection
            # across long pauses between user interactions.
            "Connection":                  "keep-alive",
            "Keep-Alive":                  "timeout=60, max=1000",
            "X-Content-Type-Options":      "nosniff",
            "X-File-Size":                 str(file_size),
            # Expose thumbnail to external players.
            # VLC reads the artwork URL from custom headers when served inline.
            "X-Thumbnail-URL":             _thumb_url,
        }
        if is_range_request:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

        response = web.StreamResponse(status=status, headers=headers)

        try:
            await response.prepare(request)
        except ConnectionResetError:
            logger.debug("stream  msg=%s  client dropped before response headers", message_id)
            return response

        # Session key for the active-connections counter.
        session_key = f"{file_hash}:{client_ip}"

        bytes_sent      = 0
        last_heartbeat  = time.monotonic()

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

                    # Refresh the session heartbeat periodically so the
                    # active-connections counter stays at 1 for the full
                    # duration of a long stream instead of dropping to 0
                    # between chunk fetches.
                    now = time.monotonic()
                    if now - last_heartbeat >= _SESSION_HEARTBEAT_INTERVAL:
                        await _heartbeat_session(session_key)
                        last_heartbeat = now

                except (ConnectionResetError, BrokenPipeError):
                    logger.debug(
                        "stream  msg=%s  connection reset after %d bytes", message_id, bytes_sent
                    )
                    break

        except asyncio.CancelledError:
            logger.debug(
                "stream  msg=%s  request cancelled after %d bytes", message_id, bytes_sent
            )
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(
                "stream  msg=%s  client disconnected after %d bytes", message_id, bytes_sent
            )
        except Exception as exc:
            logger.error("streaming error: msg=%s err=%s", message_id, exc)

        try:
            await response.write_eof()
        except Exception:
            pass

        # Bandwidth accounting — always record the actual bytes transferred,
        # deduplicated so the same (client, file, offset) range is not double-
        # counted when a player issues multiple probe requests.
        if bytes_sent > 0:
            should_track = await _should_track_bandwidth(client_ip, message_id, from_bytes)
            if should_track:
                task = asyncio.ensure_future(self.db.track_bandwidth(message_id, bytes_sent))
                task.add_done_callback(
                    lambda t: t.exception() and logger.error("track_bandwidth error: %s", t.exception())
                )
            else:
                logger.debug(
                    "bw dedup  msg=%s  ip=%s  from=%d  bytes=%d  (skipped)",
                    message_id, client_ip, from_bytes, bytes_sent,
                )

        return response
