import asyncio
import logging
import mimetypes
import math
from typing import Dict, Set, Union

from aiohttp import web
from pyrogram import Client, utils, raw
from pyrogram.errors import AuthBytesInvalid, FloodWait
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

from config import Config
from database import Database

logger = logging.getLogger(__name__)

# Telegram hard-caps upload.GetFile at 1 MB per call.
CHUNK_SIZE = 1024 * 1024  # 1 MB

# How many chunks to prefetch ahead while streaming to keep the pipe full.
# Higher = smoother playback; 3 is a safe sweet spot that avoids re-buffering
# without consuming too much memory per connection.
PREFETCH_CHUNKS = 3

MIME_TYPE_MAP = {
    "video":    "video/mp4",
    "audio":    "audio/mpeg",
    "image":    "image/jpeg",
    "document": "application/octet-stream",
}

# Cached file metadata keyed by file_hash to avoid repeated DB lookups.
# Cleared every 30 minutes together with cached_file_ids.
_file_meta_cache: Dict[str, dict] = {}

# ── Session tracking ──────────────────────────────────────────────────────────
# A "session" is one browser/player tab that opened the /stream/<hash> HTML page
# and is now actively fetching byte ranges from it.  We deduplicate by
# (file_hash, client_ip) so that the normal pattern of
#   1. HTML page load  (Accept: text/html — no range, no counter bump)
#   2. HEAD / first 0-byte probe by some players
#   3. Ongoing range requests during playback
# all collapse to exactly ONE logical viewer per IP per file.
#
# Structure: { file_hash: set_of_ip_strings }
_active_sessions: Dict[str, Set[str]] = {}
# Lock to prevent concurrent mutation of the sessions dict
_sessions_lock = asyncio.Lock()


async def register_session(file_hash: str, client_ip: str) -> None:
    async with _sessions_lock:
        if file_hash not in _active_sessions:
            _active_sessions[file_hash] = set()
        _active_sessions[file_hash].add(client_ip)


async def unregister_session(file_hash: str, client_ip: str) -> None:
    async with _sessions_lock:
        bucket = _active_sessions.get(file_hash)
        if bucket:
            bucket.discard(client_ip)
            if not bucket:
                del _active_sessions[file_hash]


def get_active_session_count() -> int:
    """Return the number of *unique* viewer IPs currently streaming any file."""
    seen: Set[str] = set()
    for ips in _active_sessions.values():
        seen |= ips
    return len(seen)


async def get_file_ids(client: Client, message_id: str) -> FileId:
    msg = await client.get_messages(Config.FLOG_CHAT_ID, int(message_id))
    if not msg or msg.empty:
        raise ValueError(f"message {message_id} not found in dump chat")

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
        raise ValueError(f"message {message_id} contains no streamable media")

    return FileId.decode(media.file_id)


class ByteStreamer:

    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids: Dict[str, FileId] = {}
        self.clean_timer: int = 30 * 60
        asyncio.create_task(self.clean_cache())

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
        """
        Continuously stream file chunks using a prefetch queue so that the
        network write and the Telegram download always overlap.  This eliminates
        the mid-stream stalls that occurred when the previous implementation
        fetched one chunk, wrote it, then waited for the *next* fetch before
        writing again.

        The prefetch queue holds up to PREFETCH_CHUNKS futures at a time.
        As soon as one chunk is consumed and sent to the client, a new fetch is
        kicked off in the background so the pipeline stays full.
        """
        client        = self.client
        media_session = await self.generate_media_session(client, file_id)
        location      = await self.get_location(file_id)

        async def fetch_chunk(off: int) -> bytes:
            """Fetch a single chunk with retry logic; returns b'' on permanent failure."""
            for attempt in range(5):
                try:
                    r = await media_session.invoke(
                        raw.functions.upload.GetFile(
                            location=location,
                            offset=off,
                            limit=chunk_size,
                        )
                    )

                    if isinstance(r, raw.types.upload.FileCdnRedirect):
                        logger.warning("FileCdnRedirect at offset %d — stopping", off)
                        return b""

                    if not isinstance(r, raw.types.upload.File):
                        logger.error("Unexpected response type: %s", type(r))
                        return b""

                    return r.bytes or b""

                except FloodWait as fw:
                    logger.warning("FloodWait %ds at offset %d — sleeping", fw.value, off)
                    await asyncio.sleep(fw.value + 1)
                except (TimeoutError, AttributeError) as exc:
                    logger.debug("Transient error at offset %d attempt %d: %s", off, attempt, exc)
                    if attempt == 4:
                        return b""
                    await asyncio.sleep(0.5 * (attempt + 1))

            logger.error("All retries failed at offset %d", off)
            return b""

        # Build an initial queue of PREFETCH_CHUNKS concurrent fetch tasks.
        # Each element is (task, part_index, part_offset).
        queue: asyncio.Queue = asyncio.Queue(maxsize=PREFETCH_CHUNKS + 1)

        async def producer():
            """Push fetch tasks into the queue in order."""
            cur_offset   = offset
            cur_part     = 1
            pending      = 0
            tasks        = []
            part_offsets = []

            # Prime the pipeline
            while cur_part <= part_count and pending < PREFETCH_CHUNKS:
                task = asyncio.create_task(fetch_chunk(cur_offset))
                tasks.append((task, cur_part, cur_offset))
                cur_offset += chunk_size
                cur_part   += 1
                pending    += 1

            for task, part_idx, p_offset in tasks:
                chunk = await task

                # Trim the first and last parts to the exact requested byte range
                if part_count == 1:
                    chunk = chunk[first_part_cut:last_part_cut]
                elif part_idx == 1:
                    chunk = chunk[first_part_cut:]
                elif part_idx == part_count:
                    chunk = chunk[:last_part_cut]

                await queue.put((chunk, part_idx))

                # Immediately kick off the next fetch so it runs in parallel
                if cur_part <= part_count:
                    next_task = asyncio.create_task(fetch_chunk(cur_offset))
                    # We await it in the next loop iteration but start it now
                    tasks.append((next_task, cur_part, cur_offset))
                    cur_offset += chunk_size
                    cur_part   += 1

            await queue.put(None)  # sentinel

        prod_task = asyncio.create_task(producer())

        current_part = 0
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                chunk, current_part = item
                if chunk:
                    yield chunk
        except Exception as exc:
            logger.error("yield_file consumer error at part %d: %s", current_part, exc)
        finally:
            prod_task.cancel()
            try:
                await prod_task
            except asyncio.CancelledError:
                pass
            logger.debug("yield_file finished after %d part(s)", current_part)

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            _file_meta_cache.clear()
            logger.debug("ByteStreamer cache cleared")


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
    """Extract the real client IP, respecting X-Forwarded-For when present."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote or "unknown"


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

        # ── File metadata (memory-cached after first lookup) ──────────────────
        if file_hash in _file_meta_cache:
            file_data = _file_meta_cache[file_hash]
        else:
            file_data = await self.db.get_file_by_hash(file_hash)
            if not file_data:
                raise web.HTTPNotFound(reason="file not found")
            _file_meta_cache[file_hash] = file_data

        # ── Bandwidth guard (only on first / non-ranged request) ──────────────
        if not is_range_request and Config.get("bandwidth_mode", True):
            stats  = await self.db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            if max_bw and stats["total_bandwidth"] >= max_bw:
                raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        file_size  = int(file_data["file_size"])
        file_name  = file_data["file_name"]
        message_id = str(file_data["message_id"])

        # Resolve FileId before preparing response
        try:
            file_id = await self.streamer.get_file_properties(message_id)
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
            "stream  msg=%s  size=%d  range=%d-%d  offset=%d  parts=%d  ip=%s",
            message_id, file_size, from_bytes, until_bytes, offset, part_count, client_ip,
        )

        mime = (
            file_data.get("mime_type")
            or mimetypes.guess_type(file_name)[0]
            or MIME_TYPE_MAP.get(file_data.get("file_type"), "application/octet-stream")
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
            "Cache-Control":               "public, max-age=3600",
            "Access-Control-Allow-Origin": "*",
            "Connection":                  "keep-alive",
        }
        if is_range_request:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)

        # ── Register this IP as an active viewer ──────────────────────────────
        # We only register range requests (actual playback), not HTML page loads.
        # This prevents the HTML page fetch from inflating the counter.
        if is_range_request:
            await register_session(file_hash, client_ip)

        bytes_sent = 0
        try:
            async for chunk in self.streamer.yield_file(
                file_id,
                offset,
                first_part_cut,
                last_part_cut,
                part_count,
                CHUNK_SIZE,
            ):
                await response.write(chunk)
                bytes_sent += len(chunk)
        except Exception as exc:
            logger.error("streaming error: msg=%s err=%s", message_id, exc)
        finally:
            # Unregister this viewer once streaming ends (normal or error)
            if is_range_request:
                await unregister_session(file_hash, client_ip)

        await response.write_eof()

        # ── Bandwidth accounting ───────────────────────────────────────────────
        # Track ONLY the bytes actually delivered — never estimate the full range.
        if bytes_sent > 0:
            asyncio.create_task(self.db.track_bandwidth(message_id, bytes_sent))

        return response
