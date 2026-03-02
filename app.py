import json
import logging
import time
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

# Track active streaming connections
_active_connections = 0


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
            return aiohttp_jinja2.render_template(
                "not_found.html",
                request,
                {
                    "bot_name":     Config.BOT_NAME     or Config.DEFAULT_BOT_NAME,
                    "bot_username": Config.BOT_USERNAME or Config.DEFAULT_BOT_USERNAME,
                },
            )
        except Exception as exc:
            logger.error("not_found template error: %s", exc)
            return web.Response(status=404, text="404 — File not found", content_type="text/plain")

    async def _render_bandwidth_exceeded(request: web.Request) -> web.Response:
        try:
            return aiohttp_jinja2.render_template(
                "bandwidth_exceeded.html",
                request,
                {
                    "bot_name":       Config.BOT_NAME     or Config.DEFAULT_BOT_NAME,
                    "bot_username":   Config.BOT_USERNAME or Config.DEFAULT_BOT_USERNAME,
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
        return {
            "bot_name":       Config.BOT_NAME     or Config.DEFAULT_BOT_NAME,
            "bot_username":   Config.BOT_USERNAME or Config.DEFAULT_BOT_USERNAME,
            "owner_username": "FLiX_LY",
        }

    async def stream_page(request: web.Request):
        file_hash = request.match_info["file_hash"]
        accept    = request.headers.get("Accept", "")
        range_h   = request.headers.get("Range", "")

        if range_h or "text/html" not in accept:
            return await streaming_service.stream_file(
                request, file_hash, is_download=False
            )

        file_data = await database.get_file_by_hash(file_hash)
        if not file_data:
            raise web.HTTPNotFound(reason="File not found")

        allowed, _ = await check_bandwidth_limit(database)
        if not allowed:
            raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        base      = str(request.url.origin())
        file_type = (
            "video"   if file_data["file_type"] == Config.FILE_TYPE_VIDEO
            else "audio" if file_data["file_type"] == Config.FILE_TYPE_AUDIO
            else "document"
        )

        context = {
            "bot_name":       Config.BOT_NAME     or Config.DEFAULT_BOT_NAME,
            "bot_username":   Config.BOT_USERNAME or Config.DEFAULT_BOT_USERNAME,
            "owner_username": "FLiX_LY",
            "file_name":      file_data["file_name"],
            "file_size":      format_size(file_data["file_size"]),
            "file_type":      file_type,
            "stream_url":     f"{base}/stream/{file_hash}",
            "download_url":   f"{base}/dl/{file_hash}",
            "telegram_url":   f"https://t.me/{Config.BOT_USERNAME}?start={file_hash}",
        }
        return aiohttp_jinja2.render_template("stream.html", request, context)

    async def download_file(request: web.Request):
        file_hash = request.match_info["file_hash"]
        return await streaming_service.stream_file(request, file_hash, is_download=True)

    # ─────────────────────────────────────────────────────────
    # Helper: gather all stats for the unified panel
    # ─────────────────────────────────────────────────────────
    async def _collect_panel_data():
        """Return a dict with all data needed for the bot_settings panel."""
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

        # System metrics
        try:
            ram     = psutil.virtual_memory()
            ram_pct = ram.percent
            ram_used_fmt = format_size(ram.used)
            cpu_pct = psutil.cpu_percent(interval=None)
        except Exception:
            ram_pct      = 0
            ram_used_fmt = "N/A"
            cpu_pct      = 0

        # Uptime
        uptime_seconds = time.time() - Config.UPTIME if Config.UPTIME else 0
        uptime_str     = _format_uptime(uptime_seconds)

        # Bot info
        bot_me  = getattr(bot, "me", None)
        bot_id  = str(bot_me.id)   if bot_me else "N/A"
        bot_dc  = str(bot_me.dc_id) if bot_me else "N/A"

        return {
            "bot_name":     Config.BOT_NAME     or Config.DEFAULT_BOT_NAME,
            "bot_username": Config.BOT_USERNAME or Config.DEFAULT_BOT_USERNAME,
            "bot_id":       bot_id,
            "bot_dc":       bot_dc,
            # Stats
            "total_users":  stats.get("total_users",  0),
            "total_chats":  stats.get("total_users",  0),  # chats ≈ unique users
            "total_files":  stats.get("total_files",  0),
            "ram_used":     ram_used_fmt,
            "ram_pct":      ram_pct,
            "cpu_pct":      cpu_pct,
            "uptime":       uptime_str,
            # Bandwidth
            "bw_mode":      bw_mode,
            "bw_limit":     format_size(max_bw),
            "bw_used":      format_size(bw_used),
            "bw_today":     format_size(bw_today),
            "bw_remaining": format_size(remaining),
            "bw_pct":       bw_pct,
            # Health / live
            "bot_status":   "running" if Config.BOT_USERNAME else "initializing",
            "active_conns": _active_connections,
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

    # ─────────────────────────────────────────────────────────
    # /bot_settings  — unified HTML panel
    # ─────────────────────────────────────────────────────────
    async def bot_settings_page(request: web.Request):
        try:
            ctx = await _collect_panel_data()
            return aiohttp_jinja2.render_template("bot_settings.html", request, ctx)
        except Exception as exc:
            logger.error("bot_settings page error: %s", exc)
            return web.Response(status=500, text="Internal server error")

    # ─────────────────────────────────────────────────────────
    # JSON API: /api/stats
    # ─────────────────────────────────────────────────────────
    async def api_stats(request: web.Request):
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
            max_bw   = Config.get("max_bandwidth", 107374182400)
            bw_used  = bw_stats["total_bandwidth"]
            bw_today = bw_stats["today_bandwidth"]
            bw_pct   = round((bw_used / max_bw * 100) if max_bw else 0, 1)

            try:
                ram     = psutil.virtual_memory()
                cpu_pct = psutil.cpu_percent(interval=None)
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
            return web.Response(
                text=json.dumps(payload),
                content_type="application/json",
            )
        except Exception as exc:
            logger.error("api_stats error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    # ─────────────────────────────────────────────────────────
    # JSON API: /api/bandwidth
    # ─────────────────────────────────────────────────────────
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

    # ─────────────────────────────────────────────────────────
    # JSON API: /api/health
    # ─────────────────────────────────────────────────────────
    async def api_health(request: web.Request):
        try:
            bot_me    = getattr(bot, "me", None)
            bot_id    = str(bot_me.id)    if bot_me else "N/A"
            bot_dc    = str(bot_me.dc_id)  if bot_me else "N/A"
            payload = {
                "status":       "ok",
                "bot_status":   "running" if Config.BOT_USERNAME else "initializing",
                "bot_name":     Config.BOT_NAME     or Config.DEFAULT_BOT_NAME,
                "bot_username": Config.BOT_USERNAME or Config.DEFAULT_BOT_USERNAME,
                "bot_id":       bot_id,
                "bot_dc":       bot_dc,
                "active_conns": _active_connections,
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_health error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    # ─────────────────────────────────────────────────────────
    # Legacy HTML routes — kept as redirects / backward compat
    # ─────────────────────────────────────────────────────────
    async def stats_endpoint(request: web.Request):
        """Legacy /stats — redirect to unified panel."""
        if "application/json" in request.headers.get("Accept", ""):
            return await api_stats(request)
        raise web.HTTPFound("/bot_settings")

    async def bandwidth_endpoint(request: web.Request):
        """Legacy /bandwidth — redirect to unified panel."""
        if "application/json" in request.headers.get("Accept", ""):
            return await api_bandwidth(request)
        raise web.HTTPFound("/bot_settings")

    async def health_endpoint(request: web.Request):
        """Legacy /health — redirect to unified panel."""
        if "application/json" in request.headers.get("Accept", ""):
            return await api_health(request)
        raise web.HTTPFound("/bot_settings")

    # ─────────────────────────────────────────────────────────
    # Route table
    # ─────────────────────────────────────────────────────────
    app.router.add_get("/",                   home)
    app.router.add_get("/stream/{file_hash}", stream_page)
    app.router.add_get("/dl/{file_hash}",     download_file)

    # Unified panel
    app.router.add_get("/bot_settings",       bot_settings_page)

    # JSON API
    app.router.add_get("/api/stats",          api_stats)
    app.router.add_get("/api/bandwidth",      api_bandwidth)
    app.router.add_get("/api/health",         api_health)

    # Legacy (redirect → panel for browsers, JSON for API clients)
    app.router.add_get("/stats",              stats_endpoint)
    app.router.add_get("/bandwidth",          bandwidth_endpoint)
    app.router.add_get("/health",             health_endpoint)

    return app
