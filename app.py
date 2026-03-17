import json
import logging
import time
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import psutil
from aiohttp import web
import aiohttp_jinja2
import jinja2

from bot import Bot
from config import Config
from database import Database
from helper import StreamingService, check_bandwidth_limit, format_size
from helper.stream import (
    get_active_session_count,
    _register_session,
    _unregister_session,
    _get_client_ip,
    _mime_for_filename,
    is_browser_playable,
    MIME_TYPE_MAP,
)

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


DEFAULT_BOT_NAME     = "FLiX File Bot"
DEFAULT_BOT_USERNAME = "FLiXFileBot"


def _bot_info(bot: Bot) -> dict:
    """Return bot identity from the in-memory cache (Config.BOT_INFO).
    Falls back to bot.me for backward compatibility, then to static defaults.
    Using the cached value avoids redundant Telegram API calls."""
    me = Config.BOT_INFO or getattr(bot, "me", None)
    return {
        "bot_name":     (me.first_name if me else None) or DEFAULT_BOT_NAME,
        "bot_username": (me.username   if me else None) or DEFAULT_BOT_USERNAME,
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
        # One (file_hash, client_ip) pair = one unique session.
        # Registration is idempotent: repeated range-requests from the same
        # player only refresh the heartbeat, they never increment the counter.
        client_ip   = _get_client_ip(request)
        session_key = f"{file_hash}:{client_ip}"
        await _register_session(session_key)
        try:
            return await streaming_service.stream_file(request, file_hash, is_download=is_download)
        finally:
            await _unregister_session(session_key)

    async def stream_page(request: web.Request):
        file_hash = request.match_info["file_hash"]
        accept    = request.headers.get("Accept", "")
        range_h   = request.headers.get("Range", "")

        if range_h or "text/html" not in accept:
            return await _tracked_stream(request, file_hash, is_download=False)

        file_data = await database.get_file_by_hash(file_hash)
        if not file_data:
            raise web.HTTPNotFound(reason="File not found")

        # Also verify the file exists in the Flog/dump channel so we can
        # surface a clean 404 instead of a player error mid-stream.
        try:
            from helper.stream import get_file_ids
            await get_file_ids(bot, str(file_data["message_id"]))
        except web.HTTPNotFound:
            raise
        except Exception as exc:
            logger.warning(
                "stream_page Flog verification failed: hash=%s err=%s", file_hash, exc
            )
            raise web.HTTPNotFound(reason="File no longer available on Telegram")

        allowed, _ = await check_bandwidth_limit(database)
        if not allowed:
            raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        base      = str(request.url.origin())
        file_type = (
            "video"   if file_data["file_type"] == Config.FILE_TYPE_VIDEO
            else "audio" if file_data["file_type"] == Config.FILE_TYPE_AUDIO
            else "document"
        )

        mime = (
            file_data.get("mime_type")
            or _mime_for_filename(
                file_data["file_name"],
                MIME_TYPE_MAP.get(file_data.get("file_type"), "application/octet-stream"),
            )
            or "application/octet-stream"
        )
        playable = is_browser_playable(mime)

        info = _bot_info(bot)
        # Build thumbnail URL for the template (used ONLY as metadata for external
        # players — the built-in web player does NOT display it)
        # thumbnail_url is used only as OG metadata (no /thumb endpoint)
        thumbnail_url = f"{base}/stream/{file_hash}"
        context = {
            "bot_name":         info["bot_name"],
            "bot_username":     info["bot_username"],
            "owner_username":   "FLiX_LY",
            "file_name":        file_data["file_name"],
            "file_size":        format_size(file_data["file_size"]),
            "file_type":        file_type,
            "mime_type":        mime,
            "browser_playable": playable,
            "stream_url":       f"{base}/stream/{file_hash}",
            "download_url":     f"{base}/dl/{file_hash}",
            "thumbnail_url":    thumbnail_url,
            "telegram_url":     f"https://t.me/{info['bot_username']}?start={file_hash}",
        }
        return aiohttp_jinja2.render_template("stream.html", request, context)

    async def download_file(request: web.Request):
        file_hash = request.match_info["file_hash"]
        return await _tracked_stream(request, file_hash, is_download=True)

    def _fmt_date(dt) -> str:
        """Format a datetime (aware or naive) as 'DD Mon YYYY', e.g. '12 Mar 2026'."""
        if dt is None:
            return "N/A"
        if hasattr(dt, "strftime"):
            return dt.strftime("%d %b %Y")
        return str(dt)

    def _days_remaining(next_reset) -> str:
        """Return human-readable days left until next_reset."""
        if next_reset is None:
            return "N/A"
        now = datetime.now(timezone.utc)
        if next_reset.tzinfo is None:
            next_reset = next_reset.replace(tzinfo=timezone.utc)
        delta = next_reset - now
        days  = max(0, delta.days)
        return f"{days} Day{'s' if days != 1 else ''}"

    async def _collect_panel_data():
        try:
            stats    = await database.get_stats()
            bw_stats = await database.get_bandwidth_stats()
        except Exception:
            stats    = {"total_users": 0, "total_files": 0}
            bw_stats = {
                "total_bandwidth":            0,
                "today_bandwidth":            0,
                "cycle_bandwidth_used":       0,
                "bandwidth_cycle_start":      None,
                "bandwidth_cycle_next_reset": None,
            }

        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_mode   = Config.get("bandwidth_mode", True)
        bw_used   = bw_stats["total_bandwidth"]
        bw_today  = bw_stats["today_bandwidth"]
        remaining = max(0, max_bw - bw_used)
        bw_pct    = round((bw_used / max_bw * 100) if max_bw else 0, 1)

        # Cycle fields
        cycle_used       = bw_stats.get("cycle_bandwidth_used", 0)
        cycle_start      = bw_stats.get("bandwidth_cycle_start")
        cycle_next_reset = bw_stats.get("bandwidth_cycle_next_reset")
        cycle_pct        = round((cycle_used / max_bw * 100) if max_bw else 0, 1)

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
            "total_users":       stats.get("total_users",  0),
            "total_chats":       stats.get("total_users",  0),
            "total_files":       stats.get("total_files",  0),
            "ram_used":          ram_used_fmt,
            "ram_pct":           ram_pct,
            "cpu_pct":           cpu_pct,
            "uptime":            uptime_str,
            "bw_mode":           bw_mode,
            "bw_limit":          format_size(max_bw),
            "bw_used":           format_size(bw_used),
            "bw_today":          format_size(bw_today),
            "bw_remaining":      format_size(remaining),
            "bw_pct":            bw_pct,
            # 30-day cycle fields
            "cycle_used":        format_size(cycle_used),
            "cycle_used_raw":    cycle_used,
            "cycle_pct":         cycle_pct,
            "cycle_start":       _fmt_date(cycle_start),
            "cycle_next_reset":  _fmt_date(cycle_next_reset),
            "cycle_days_left":   _days_remaining(cycle_next_reset),
            "bot_status":        "running" if getattr(bot, "me", None) else "initializing",
            "active_conns":      get_active_session_count(),
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
            stats      = await database.get_bandwidth_stats()
            max_bw     = Config.get("max_bandwidth", 107374182400)
            bw_mode    = Config.get("bandwidth_mode", True)
            used       = stats["total_bandwidth"]
            today      = stats["today_bandwidth"]
            remaining  = max(0, max_bw - used)
            pct        = round((used / max_bw * 100) if max_bw else 0, 1)

            cycle_used       = stats.get("cycle_bandwidth_used", 0)
            cycle_start      = stats.get("bandwidth_cycle_start")
            cycle_next_reset = stats.get("bandwidth_cycle_next_reset")
            cycle_pct        = round((cycle_used / max_bw * 100) if max_bw else 0, 1)

            payload = {
                "total_bandwidth":  used,
                "today_bandwidth":  today,
                "limit":            max_bw,
                "remaining":        remaining,
                "percentage":       pct,
                "bandwidth_mode":   bw_mode,
                # 30-day cycle
                "cycle_bandwidth_used":       cycle_used,
                "cycle_percentage":           cycle_pct,
                "bandwidth_cycle_start":      _fmt_date(cycle_start),
                "bandwidth_cycle_next_reset": _fmt_date(cycle_next_reset),
                "cycle_days_left":            _days_remaining(cycle_next_reset),
                "formatted": {
                    "total_bandwidth": format_size(used),
                    "today_bandwidth": format_size(today),
                    "limit":           format_size(max_bw),
                    "remaining":       format_size(remaining),
                    "cycle_used":      format_size(cycle_used),
                },
            }
            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_bandwidth error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    async def api_bandwidth_analytics(request: web.Request):
        """Return bandwidth totals for last-7d, last-30d, or a custom date range.

        Query params:
          range  = '7d' | '30d' | 'custom'  (default: '7d')
          from   = 'YYYY-MM-DD'              (required for custom)
          to     = 'YYYY-MM-DD'              (required for custom)
        """
        try:
            rng      = request.rel_url.query.get("range", "7d")
            now      = datetime.now(timezone.utc)

            if rng == "7d":
                total = await database.get_bandwidth_last_n_days(7)
                payload = {
                    "range": "7d",
                    "total_bytes": total,
                    "formatted":   format_size(total),
                }
            elif rng == "30d":
                total = await database.get_bandwidth_last_n_days(30)
                payload = {
                    "range": "30d",
                    "total_bytes": total,
                    "formatted":   format_size(total),
                }
            elif rng == "custom":
                from_str = request.rel_url.query.get("from")
                to_str   = request.rel_url.query.get("to")
                if not from_str:
                    return web.json_response({"error": "'from' param required for custom range"}, status=400)
                try:
                    from_dt = datetime.strptime(from_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    to_dt   = (
                        datetime.strptime(to_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if to_str else now
                    )
                except ValueError:
                    return web.json_response({"error": "Invalid date format, use YYYY-MM-DD"}, status=400)

                result = await database.get_bandwidth_by_range(from_date=from_dt, to_date=to_dt)
                total  = result.get("total_bytes", 0)
                payload = {
                    "range":       "custom",
                    "from":        from_dt.strftime("%Y-%m-%d"),
                    "to":          to_dt.strftime("%Y-%m-%d"),
                    "total_bytes": total,
                    "formatted":   format_size(total),
                }
            else:
                return web.json_response({"error": f"Unknown range '{rng}'"}, status=400)

            return web.Response(text=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            logger.error("api_bandwidth_analytics error: %s", exc)
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
                "active_conns": get_active_session_count(),
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

    app.router.add_get("/",                          home)
    app.router.add_get("/stream/{file_hash}",        stream_page)
    app.router.add_get("/dl/{file_hash}",            download_file)
    app.router.add_get("/bot_settings",              bot_settings_page)
    app.router.add_get("/api/stats",                 api_stats)
    app.router.add_get("/api/bandwidth",             api_bandwidth)
    app.router.add_get("/api/bandwidth/analytics",   api_bandwidth_analytics)
    app.router.add_get("/api/health",                api_health)
    app.router.add_get("/stats",                     stats_endpoint)
    app.router.add_get("/bandwidth",                 bandwidth_endpoint)
    app.router.add_get("/health",                    health_endpoint)

    return app
