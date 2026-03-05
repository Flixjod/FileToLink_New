"""
stream.py — Telegram file streaming helper
==========================================
Key design decisions
--------------------
* No packet/chunk counter heuristic.  Every Range request is served the same
  way regardless of how many previous requests came from the same player.
  The old "_packet_counters" approach caused mid-stream buffering because the
  first few requests went through the full slow path while later ones skipped
  bandwidth/DB checks — the inconsistency confused the player's buffer.

* Continuous, lock-step streaming.  yield_file() fetches from Telegram and
  immediately writes to the HTTP response without any artificial delays or
  "packet" grouping.  aiohttp's StreamResponse handles TCP back-pressure
  automatically, so we never pile up data in memory.

* Bandwidth tracking: only the bytes we actually write() are recorded.  We
  track progress inside the streaming loop and call track_bandwidth() once at
  the very end with the final byte count — no per-chunk DB updates that could
  cause double-counting if a connection is retried.

* Session counter: counted at the _connection_ level (one HTTP request = one
  session), not at the chunk level.  The counter lives in app.py and is
  incremented once when stream_file() is entered and decremented once when it
  returns, irrespective of how many Range sub-requests the player makes.
"""

import asyncio
import logging
import mimetypes
import math
from typing import Dict, Optional, Union

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

MIME_TYPE_MAP = {
    "video":    "video/mp4",
    "audio":    "audio/mpeg",
    "image":    "image/jpeg",
    "document": "application/octet-stream",
}

# In-memory file-metadata cache (DB lookup once per file_hash, cleared every
# 30 minutes by ByteStreamer.clean_cache).
_file_meta_cache: Dict[str, dict] = {}


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
        Yield raw bytes for the requested byte range in a continuous loop.

        Each iteration fetches exactly one 1 MB block from Telegram and
        immediately yields the slice that belongs to the requested range.
        There are no artificial pauses, no packet counters, and no grouping —
        this ensures the HTTP layer can stream chunks to the browser as fast
        as Telegram delivers them, preventing mid-playback buffering.
        """
        client        = self.client
        media_session = await self.generate_media_session(client, file_id)
        location      = await self.get_location(file_id)
        current_part  = 1

        try:
            while current_part <= part_count:
                for attempt in range(5):
                    try:
                        r = await media_session.invoke(
                            raw.functions.upload.GetFile(
                                location=location,
                                offset=offset,
                                limit=chunk_size,
                            )
                        )
                        break
                    except FloodWait as fw:
                        logger.warning(
                            "FloodWait %ds on part %d/%d — sleeping",
                            fw.value, current_part, part_count,
                        )
                        await asyncio.sleep(fw.value + 1)
                    except (TimeoutError, AttributeError) as exc:
                        logger.debug("Transient error part %d: %s", current_part, exc)
                        if attempt == 4:
                            return
                        await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    logger.error("All retries failed at part %d", current_part)
                    return

                if isinstance(r, raw.types.upload.FileCdnRedirect):
                    logger.warning(
                        "FileCdnRedirect received for part %d — CDN streaming not supported; stopping",
                        current_part,
                    )
                    return

                if not isinstance(r, raw.types.upload.File):
                    logger.error("Unexpected response type: %s", type(r))
                    return

                chunk = r.bytes
                if not chunk:
                    break

                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk

                current_part += 1
                offset += chunk_size

        except Exception as exc:
            logger.error("yield_file error at part %d: %s", current_part, exc)
        finally:
            logger.debug("yield_file finished after %d part(s)", current_part - 1)

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            _file_meta_cache.clear()
            logger.debug("ByteStreamer caches cleared")


def _parse_range(range_header: str, file_size: int):
    if range_header:
        try:
            raw_range          = range_header.replace("bytes=", "").split(",")[0].strip()
            start_str, end_str = raw_range.split("-")
            from_bytes         = int(start_str) if start_str else 0
            until_bytes        = int(end_str)   if end_str   else file_size - 1
        except (ValueError, AttributeError):
            from_bytes  = 0
            until_bytes = file_size - 1
    else:
        from_bytes  = 0
        until_bytes = file_size - 1

    from_bytes  = max(0, from_bytes)
    until_bytes = min(until_bytes, file_size - 1)
    return from_bytes, until_bytes


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
        """
        Serve a file (or a byte range of it) from Telegram storage.

        Design notes
        ~~~~~~~~~~~~
        * Metadata (DB lookup) is cached in _file_meta_cache so only the
          first request for a given file_hash hits MongoDB.
        * Bandwidth limit is checked only once per unique streaming URL — on
          the initial (non-Range) request or on the first Range request for a
          file not yet cached — to avoid unnecessary DB reads on every 1 MB
          chunk during playback.
        * bytes_sent is incremented inside the write loop and recorded in the
          DB only once, at the end, to prevent double-counting when a player
          retries a request or issues overlapping Range requests.
        """
        range_header     = request.headers.get("Range", "")
        is_range_request = bool(range_header)

        # ── File metadata (cached after first DB hit) ─────────────────────
        if file_hash in _file_meta_cache:
            file_data = _file_meta_cache[file_hash]
        else:
            file_data = await self.db.get_file_by_hash(file_hash)
            if not file_data:
                raise web.HTTPNotFound(reason="file not found")
            _file_meta_cache[file_hash] = file_data

        # ── Bandwidth guard (only on fresh / non-cached requests) ─────────
        # We skip the guard once the metadata is already cached — that means
        # we already checked it for this file in the recent past.
        if file_hash not in _file_meta_cache and Config.get("bandwidth_mode", True):
            stats  = await self.db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            if max_bw and stats["total_bandwidth"] >= max_bw:
                raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")
        elif not is_range_request and Config.get("bandwidth_mode", True):
            # Also check on initial non-range page loads
            stats  = await self.db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            if max_bw and stats["total_bandwidth"] >= max_bw:
                raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        file_size  = int(file_data["file_size"])
        file_name  = file_data["file_name"]
        message_id = str(file_data["message_id"])

        # Resolve FileId before preparing response (HTTP errors can't be sent after prepare)
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
            "stream  msg=%s  size=%d  range=%d-%d  offset=%d  parts=%d",
            message_id, file_size, from_bytes, until_bytes, offset, part_count,
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
        except (ConnectionResetError, asyncio.CancelledError):
            # Client disconnected mid-stream — normal for seeking/closing.
            logger.debug("Client disconnected mid-stream: msg=%s bytes_sent=%d", message_id, bytes_sent)
        except Exception as exc:
            logger.error("streaming error: msg=%s err=%s", message_id, exc)

        try:
            await response.write_eof()
        except Exception:
            pass

        # Record only the bytes actually delivered — once, at the end.
        # This prevents over-counting caused by:
        #   • partial/aborted requests
        #   • player retrying the same byte range
        #   • multiple concurrent chunk fetches for the same file
        if bytes_sent > 0:
            asyncio.create_task(self.db.track_bandwidth(message_id, bytes_sent))

        return response
