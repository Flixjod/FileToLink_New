import json
import logging
import time
import uuid
import asyncio
from pathlib import Path

import psutil
from aiohttp import web
import aiohttp_jinja2
import jinja2

from bot import Bot
from config import Config
from database import Database
from helper import StreamingService, check_bandwidth_limit, format_size

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR    = Path(__file__).parent / "static"

# ── Live-session tracker ────────────────────────────────────────────────────
# We count *unique viewers*, not individual HTTP requests.
# A viewer is identified by the session token stored in their Cookie.
# The set _active_sessions holds currently-streaming session IDs.
# The set is incremented when the first streaming byte is about to be sent
# and decremented when the response is fully written.
#
# Why this is better than a simple integer counter:
#   • A media player sends dozens of Range requests for every video it plays
#     (seek, pre-buffer, quality switch). Counting each request would give
#     wildly inflated numbers (e.g. 3 shown when only 1 viewer is watching).
#   • With a Set<session_id>, multiple requests from the same browser session
#     keep the count at 1 for that viewer until all their requests complete.
_active_sessions: set = set()


def _get_or_create_session_id(request: web.Request) -> str:
    """Return the viewer's session cookie, or generate a new one."""
    return request.cookies.get("flix_sid") or str(uuid.uuid4())


def _bot_info(bot: Bot) -> dict:
    me = getattr(bot, "me", None)
    return {
        "bot_name":     (me.first_name if me else None) or "File2Link",
        "bot_username": (me.username   if me else None) or "file2link_bot",
        "bot_id":       str(me.id)    if me else "N/A",
        "bot_dc":       str(me.dc_id) if me else "N/A",
    }


def build_app(bot: Bot, database) -> web.Application:
    streaming_service = StreamingService(bot, database)

    @web.middleware
    async def not_found_middleware(request: web.Request, handler):
        try:
            return await handler(request)
        except web.HTTPNotFound:
            return await _render_not_found(request)
        except web.HTTPServiceUnavailable:
            return await _render_bandwidth_exceeded(request)

    async def _render_not_found(request: web.Request) -> web.Response:
        try:
            info = _bot_info(bot)
            return aiohttp_jinja2.render_template(
                "not_found.html",
                request,
                {"bot_name": info["bot_name"], "bot_username": info["bot_username"]},
            )
        except Exception as exc:
            logger.error("not_found template error: %s", exc)
            return web.Response(status=404, text="404 — File not found", content_type="text/plain")

    async def _render_bandwidth_exceeded(request: web.Request) -> web.Response:
        try:
            info = _bot_info(bot)
            return aiohttp_jinja2.render_template(
                "bandwidth_exceeded.html",
                request,
                {
                    "bot_name":       info["bot_name"],
                    "bot_username":   info["bot_username"],
                    "owner_username": "FLiX_LY",
                },
            )
        except Exception as exc:
            logger.error("bandwidth_exceeded template error: %s", exc)
            return web.Response(
                status=503,
                text="Bandwidth limit exceeded",
                content_type="text/plain",
            )

    app = web.Application(middlewares=[not_found_middleware])
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)))

    @aiohttp_jinja2.template("home.html")
    async def home(request: web.Request):
        info = _bot_info(bot)
        return {
            "bot_name":       info["bot_name"],
            "bot_username":   info["bot_username"],
            "owner_username": "FLiX_LY",
        }

    async def _tracked_stream(request: web.Request, file_hash: str, is_download: bool):
        """
        Stream a file while accurately tracking unique live viewer sessions.

        Session tracking logic
        ~~~~~~~~~~~~~~~~~~~~~~
        * Each browser/client carries a `flix_sid` cookie that uniquely
          identifies a viewing session.  If the cookie is absent we generate
          a new UUID and set it in the response.
        * We use a module-level set (_active_sessions) instead of a plain
          integer counter.  This means:
            - A single viewer seeking / buffering sends many Range requests
              but only adds ONE entry to the set.
            - The count drops back to zero as soon as all connections from
              that session finish (i.e. the Set removes the ID).
          This fixes the "I watched alone but the panel showed 3 sessions"
          bug that was caused by counting every HTTP sub-request separately.
        """
        sid = _get_or_create_session_id(request)
        _active_sessions.add(sid)
        try:
            response = await streaming_service.stream_file(
                request, file_hash, is_download=is_download
            )
            # Persist the session cookie so the browser re-sends it on the
            # next Range request (seek / pre-buffer).
            if "flix_sid" not in request.cookies:
                response.set_cookie(
                    "flix_sid", sid,
                    max_age=3600,
                    httponly=True,
                    samesite="Lax",
                )
            return response
        finally:
            # Remove the session only when this specific connection ends.
            # If the viewer has other in-flight requests they share the same
            # sid and those will also call discard() when they finish — that
            # is safe because set.discard() is idempotent.
            _active_sessions.discard(sid)

    async def stream_page(request: web.Request):
        file_hash = request.match_info["file_hash"]
        accept    = request.headers.get("Accept", "")
        range_h   = request.headers.get("Range", "")

        if range_h or "text/html" not in accept:
            return await _tracked_stream(request, file_hash, is_download=False)

        file_data = await database.get_file_by_hash(file_hash)
        if not file_data:
            raise web.HTTPNotFound(reason="File not found")

        allowed, _ = await check_bandwidth_limit(database)
        if not allowed:
            raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        base      = str(request.url.origin())
        file_type = (
            "video"    if file_data["file_type"] == Config.FILE_TYPE_VIDEO
            else "audio" if file_data["file_type"] == Config.FILE_TYPE_AUDIO
            else "document"
        )
        info = _bot_info(bot)
        context = {
            "bot_name":       info["bot_name"],
            "bot_username":   info["bot_username"],
            "owner_username": "FLiX_LY",
            "file_name":      file_data["file_name"],
            "file_size":      format_size(file_data["file_size"]),
            "file_type":      file_type,
            "stream_url":     f"{base}/stream/{file_hash}",
            "download_url":   f"{base}/dl/{file_hash}",
            "telegram_url":   f"https://t.me/{info['bot_username']}?start={file_hash}",
        }
        return aiohttp_jinja2.render_template("stream.html", request, context)

    async def download_file(request: web.Request):
        file_hash = request.match_info["file_hash"]
        return await _tracked_stream(request, file_hash, is_download=True)

    # ── Static icons (Issue 3 — local hosting of inline-query thumbnails) ──
    async def static_icon(request: web.Request):
        """
        Serve pre-rendered PNG icons from /home/user/webapp/static/icons/.
        These are used as thumbnails in Telegram inline query results instead
        of fetching from GitHub raw CDN — eliminates external latency.
        """
        name      = request.match_info["name"]
        icon_path = STATIC_DIR / "icons" / name
        if not icon_path.exists() or not icon_path.is_file():
            raise web.HTTPNotFound()
        return web.FileResponse(
            icon_path,
            headers={
                "Cache-Control": "public, max-age=86400",
                "Content-Type":  "image/png",
            },
        )

    async def _collect_panel_data():
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
        except Exception:
            stats    = {"total_users": 0, "total_files": 0}
            bw_stats = {"total_bandwidth": 0, "today_bandwidth": 0}

        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_mode   = Config.get("bandwidth_mode", True)
        bw_used   = bw_stats["total_bandwidth"]
        bw_today  = bw_stats["today_bandwidth"]
        remaining = max(0, max_bw - bw_used)
        bw_pct    = round((bw_used / max_bw * 100) if max_bw else 0, 1)

        try:
            ram          = psutil.virtual_memory()
            ram_pct      = ram.percent
            ram_used_fmt = format_size(ram.used)
            cpu_pct      = psutil.cpu_percent(interval=None)
        except Exception:
            ram_pct      = 0
            ram_used_fmt = "N/A"
            cpu_pct      = 0

        uptime_seconds = time.time() - Config.UPTIME if Config.UPTIME else 0
        uptime_str     = _format_uptime(uptime_seconds)

        info = _bot_info(bot)

        return {
            **info,
            "total_users":  stats.get("total_users",  0),
            "total_chats":  stats.get("total_users",  0),
            "total_files":  stats.get("total_files",  0),
            "ram_used":     ram_used_fmt,
            "ram_pct":      ram_pct,
            "cpu_pct":      cpu_pct,
            "uptime":       uptime_str,
            "bw_mode":      bw_mode,
            "bw_limit":     format_size(max_bw),
            "bw_used":      format_size(bw_used),
            "bw_today":     format_size(bw_today),
            "bw_remaining": format_size(remaining),
            "bw_pct":       bw_pct,
            "bot_status":   "running" if getattr(bot, "me", None) else "initializing",
            # Unique live viewer count — number of distinct session IDs that
            # currently have an in-flight streaming response.
            "active_conns": len(_active_sessions),
        }

    def _format_uptime(seconds: float) -> str:
        seconds = int(seconds)
        d, seconds = divmod(seconds, 86400)
        h, seconds = divmod(seconds, 3600)
        m, s       = divmod(seconds, 60)
        parts = []
        if d: parts.append(f"{d}d")
        if h: parts.append(f"{h}h")
        if m: parts.append(f"{m}m")
        parts.append(f"{s}s")
        return " ".join(parts)

    async def bot_settings_page(request: web.Request):
        try:
            ctx = await _collect_panel_data()
            return aiohttp_jinja2.render_template("bot_settings.html", request, ctx)
        except Exception as exc:
            logger.error("bot_settings page error: %s", exc)
            return web.Response(status=500, text="Internal server error")

    async def api_stats(request: web.Request):
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
            max_bw   = Config.get("max_bandwidth", 107374182400)
            bw_used  = bw_stats["total_bandwidth"]
            bw_today = bw_stats["today_bandwidth"]
            bw_pct   = round((bw_used / max_bw * 100) if max_bw else 0, 1)

            try:
                ram          = psutil.virtual_memory()
                cpu_pct      = psutil.cpu_percent(interval=None)
                ram_used_fmt = format_size(ram.used)
            except Exception:
                cpu_pct      = 0
                ram_used_fmt = "N/A"

            uptime_str = _format_uptime(time.time() - Config.UPTIME if Config.UPTIME else 0)

            payload = {
                "total_users": stats.get("total_users", 0),
                "total_chats": stats.get("total_users", 0),
                "total_files": stats.get("total_files", 0),
                "ram_used":    ram_used_fmt,
                "cpu_pct":     cpu_pct,
                "uptime":      uptime_str,
                "bw_pct":      bw_pct,
                "bw_used":     format_size(bw_used),
                "bw_today":    format_size(bw_today),
                "bw_limit":    format_size(max_bw),
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_stats error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def api_bandwidth(request: web.Request):
        try:
            stats     = await database.get_bandwidth_stats()
            max_bw    = Config.get("max_bandwidth", 107374182400)
            bw_mode   = Config.get("bandwidth_mode", True)
            used      = stats["total_bandwidth"]
            today     = stats["today_bandwidth"]
            remaining = max(0, max_bw - used)
            pct       = round((used / max_bw * 100) if max_bw else 0, 1)
            payload = {
                **stats,
                "limit":          max_bw,
                "remaining":      remaining,
                "percentage":     pct,
                "bandwidth_mode": bw_mode,
                "formatted": {
                    "total_bandwidth": format_size(used),
                    "today_bandwidth": format_size(today),
                    "limit":           format_size(max_bw),
                    "remaining":       format_size(remaining),
                },
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_bandwidth error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def api_health(request: web.Request):
        try:
            info = _bot_info(bot)
            payload = {
                "status":       "ok",
                "bot_status":   "running" if getattr(bot, "me", None) else "initializing",
                "bot_name":     info["bot_name"],
                "bot_username": info["bot_username"],
                "bot_id":       info["bot_id"],
                "bot_dc":       info["bot_dc"],
                "active_conns": len(_active_sessions),
                "active_conns_description": (
                    "Number of unique viewer sessions currently streaming. "
                    "Uses session cookies to prevent counting multiple Range "
                    "requests from the same browser as separate viewers."
                ),
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_health error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def stats_endpoint(request: web.Request):
        if "application/json" in request.headers.get("Accept", ""):
            return await api_stats(request)
        raise web.HTTPFound("/bot_settings")

    async def bandwidth_endpoint(request: web.Request):
        if "application/json" in request.headers.get("Accept", ""):
            return await api_bandwidth(request)
        raise web.HTTPFound("/bot_settings")

    async def health_endpoint(request: web.Request):
        if "application/json" in request.headers.get("Accept", ""):
            return await api_health(request)
        raise web.HTTPFound("/bot_settings")

    app.router.add_get("/",                        home)
    app.router.add_get("/stream/{file_hash}",      stream_page)
    app.router.add_get("/dl/{file_hash}",          download_file)
    app.router.add_get("/bot_settings",            bot_settings_page)
    app.router.add_get("/api/stats",               api_stats)
    app.router.add_get("/api/bandwidth",           api_bandwidth)
    app.router.add_get("/api/health",              api_health)
    app.router.add_get("/stats",                   stats_endpoint)
    app.router.add_get("/bandwidth",               bandwidth_endpoint)
    app.router.add_get("/health",                  health_endpoint)
    # Static icon route for inline-query thumbnails (Issue 3)
    app.router.add_get("/icons/{name}",            static_icon)

    return app
