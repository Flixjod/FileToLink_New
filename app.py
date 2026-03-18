import json
import logging
import time
import asyncio
from datetime import datetime, timedelta
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
    get_live_bandwidth_bps,
    get_live_bytes_total,
)

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _bot_info(bot: Bot) -> dict:
    """Use Config.BOT_INFO (cached at startup) — avoid repeated get_me() calls."""
    me = Config.BOT_INFO or getattr(bot, "me", None)
    return {
        "bot_name":     (me.first_name if me else None) or "FLiX Bot",
        "bot_username": (me.username   if me else None) or "FLiX_Bot",
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

        # Bandwidth cycle info
        try:
            cycle = await database.get_bw_cycle()
            cycle_start   = cycle.get("cycle_start", datetime.utcnow())
            cycle_end     = cycle.get("cycle_end",   datetime.utcnow() + timedelta(days=30))
            cycle_bytes   = cycle.get("bytes_used",  0)
            if isinstance(cycle_start, str):
                cycle_start = datetime.fromisoformat(cycle_start)
            if isinstance(cycle_end, str):
                cycle_end = datetime.fromisoformat(cycle_end)
            days_left = max(0, (cycle_end - datetime.utcnow()).days)
        except Exception:
            cycle_bytes = 0
            days_left   = 30
            cycle_start = datetime.utcnow()
            cycle_end   = datetime.utcnow() + timedelta(days=30)

        info = _bot_info(bot)

        return {
            **info,
            "total_users":   stats.get("total_users",  0),
            "total_chats":   stats.get("total_users",  0),
            "total_files":   stats.get("total_files",  0),
            "ram_used":      ram_used_fmt,
            "ram_pct":       ram_pct,
            "cpu_pct":       cpu_pct,
            "uptime":        uptime_str,
            "bw_mode":       bw_mode,
            "bw_limit":      format_size(max_bw),
            "bw_used":       format_size(bw_used),
            "bw_today":      format_size(bw_today),
            "bw_remaining":  format_size(remaining),
            "bw_pct":        bw_pct,
            "bot_status":    "running" if Config.BOT_INFO or getattr(bot, "me", None) else "initializing",
            "active_conns":  get_active_session_count(),
            # Cycle fields
            "cycle_used":    format_size(cycle_bytes),
            "cycle_days_left": days_left,
            "cycle_start":   cycle_start.strftime("%Y-%m-%d") if cycle_start else "—",
            "cycle_end":     cycle_end.strftime("%Y-%m-%d")   if cycle_end   else "—",
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

    # ── SSE: Real-time bandwidth stream ────────────────────────────────────
    async def api_live_bandwidth(request: web.Request):
        """
        Server-Sent Events endpoint for real-time bandwidth analytics.
        Pushes JSON every second: {bps, active_sessions, total_bytes}.
        """
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type":  "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection":    "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "X-Accel-Buffering": "no",     # disable nginx buffering
            },
        )
        await response.prepare(request)

        try:
            while True:
                bps      = get_live_bandwidth_bps()
                sessions = get_active_session_count()
                total    = await get_live_bytes_total()

                payload = json.dumps({
                    "bps":             round(bps, 1),
                    "bps_fmt":         _format_bps(bps),
                    "active_sessions": sessions,
                    "total_bytes":     total,
                    "total_fmt":       format_size(total),
                    "ts":              time.time(),
                })
                try:
                    await response.write(f"data: {payload}\n\n".encode())
                except (ConnectionResetError, BrokenPipeError):
                    break

                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("SSE stream closed: %s", exc)

        return response

    def _format_bps(bps: float) -> str:
        if bps >= 1024 * 1024:
            return f"{bps / (1024*1024):.2f} MB/s"
        if bps >= 1024:
            return f"{bps / 1024:.1f} KB/s"
        return f"{bps:.0f} B/s"

    # ── API: bandwidth date-range analytics ────────────────────────────────
    async def api_bandwidth_analytics(request: web.Request):
        """
        Returns bandwidth totals and a daily series for a date range.

        Query params:
          filter  = today | 7d | 15d | 30d | custom
          start   = YYYY-MM-DD  (only for custom)
          end     = YYYY-MM-DD  (only for custom; if omitted = same as start)
        """
        try:
            now   = datetime.utcnow().date()
            filt  = request.rel_url.query.get("filter", "today")

            if filt == "today":
                start = end = now
            elif filt == "7d":
                start = now - timedelta(days=6)
                end   = now
            elif filt == "15d":
                start = now - timedelta(days=14)
                end   = now
            elif filt == "30d":
                start = now - timedelta(days=29)
                end   = now
            else:  # custom
                try:
                    start = datetime.strptime(
                        request.rel_url.query.get("start", str(now)), "%Y-%m-%d"
                    ).date()
                    end   = datetime.strptime(
                        request.rel_url.query.get("end", str(start)), "%Y-%m-%d"
                    ).date()
                except ValueError:
                    start = end = now

            start_str = start.isoformat()
            end_str   = end.isoformat()

            total_bytes = await database.get_bandwidth_by_range(start_str, end_str)
            daily       = await database.get_bandwidth_daily_series(start_str, end_str)

            # Cycle info
            cycle = await database.get_bw_cycle()
            cycle_start = cycle.get("cycle_start", datetime.utcnow())
            cycle_end   = cycle.get("cycle_end",   datetime.utcnow() + timedelta(days=30))
            cycle_bytes = cycle.get("bytes_used",  0)
            if isinstance(cycle_start, str):
                cycle_start = datetime.fromisoformat(cycle_start)
            if isinstance(cycle_end, str):
                cycle_end = datetime.fromisoformat(cycle_end)
            days_left = max(0, (cycle_end.date() - now).days)

            payload = {
                "filter":      filt,
                "start":       start_str,
                "end":         end_str,
                "total_bytes": total_bytes,
                "total_fmt":   format_size(total_bytes),
                "daily":       daily,
                "cycle": {
                    "bytes_used": cycle_bytes,
                    "fmt":        format_size(cycle_bytes),
                    "start":      cycle_start.strftime("%Y-%m-%d") if cycle_start else "—",
                    "end":        cycle_end.strftime("%Y-%m-%d")   if cycle_end   else "—",
                    "days_left":  days_left,
                },
            }
            return web.Response(
                text=json.dumps(payload),
                content_type="application/json",
            )
        except Exception as exc:
            logger.error("api_bandwidth_analytics error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

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
                "active_conns": get_active_session_count(),
                "bps":         round(get_live_bandwidth_bps(), 1),
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

            # Cycle data
            cycle = await database.get_bw_cycle()
            cycle_bytes = cycle.get("bytes_used", 0)
            cycle_end   = cycle.get("cycle_end", datetime.utcnow() + timedelta(days=30))
            if isinstance(cycle_end, str):
                cycle_end = datetime.fromisoformat(cycle_end)
            days_left = max(0, (cycle_end.date() - datetime.utcnow().date()).days)

            payload = {
                **stats,
                "limit":          max_bw,
                "remaining":      remaining,
                "percentage":     pct,
                "bandwidth_mode": bw_mode,
                "bps":            round(get_live_bandwidth_bps(), 1),
                "active_sessions": get_active_session_count(),
                "cycle": {
                    "bytes_used": cycle_bytes,
                    "fmt":        format_size(cycle_bytes),
                    "days_left":  days_left,
                },
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
                "bot_status":   "running" if (Config.BOT_INFO or getattr(bot, "me", None)) else "initializing",
                "bot_name":     info["bot_name"],
                "bot_username": info["bot_username"],
                "bot_id":       info["bot_id"],
                "bot_dc":       info["bot_dc"],
                "active_conns": get_active_session_count(),
                "bps":          round(get_live_bandwidth_bps(), 1),
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
    app.router.add_get("/api/live/bandwidth",        api_live_bandwidth)
    app.router.add_get("/api/health",                api_health)
    app.router.add_get("/stats",                     stats_endpoint)
    app.router.add_get("/bandwidth",                 bandwidth_endpoint)
    app.router.add_get("/health",                    health_endpoint)

    return app
