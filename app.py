import hashlib
import hmac
import json
import logging
import time
import asyncio
from pathlib import Path
from urllib.parse import parse_qs, unquote

import psutil
from aiohttp import web
import aiohttp_jinja2
import jinja2

from bot import Bot
from config import Config
from database import Database
from helper import (
    StreamingService, check_bandwidth_limit, format_size,
    should_warn_global_bw, is_privileged_user_async, small_caps,
)
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


def _bot_info(bot: Bot) -> dict:
    me = getattr(bot, "me", None)
    return {
        "bot_name":     (me.first_name if me else None) or "FLiX Bot",
        "bot_username": (me.username   if me else None) or "FLiX_LY",
        "bot_id":       str(me.id)    if me else "N/A",
        "bot_dc":       str(me.dc_id) if me else "N/A",
    }


def _validate_telegram_init_data(init_data: str, bot_token: str) -> dict | None:
    """
    Validate Telegram Web App initData using HMAC-SHA256.
    Returns parsed user data dict on success, None on failure.
    """
    try:
        parsed = parse_qs(init_data)
        data_check_string_parts = []
        user_data = None

        for key, values in sorted(parsed.items()):
            if key == "hash":
                continue
            val = values[0] if values else ""
            data_check_string_parts.append(f"{key}={val}")
            if key == "user":
                try:
                    user_data = json.loads(unquote(val))
                except Exception:
                    user_data = json.loads(val)

        data_check_string = "\n".join(data_check_string_parts)
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        expected_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        provided_hash = parsed.get("hash", [""])[0]
        if not hmac.compare_digest(expected_hash, provided_hash):
            return None

        return user_data or {}
    except Exception as exc:
        logger.debug("TWA init_data validation failed: %s", exc)
        return None


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

        # Verify file still exists in Telegram
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

        # ── User bandwidth enforcement ─────────────────────────────
        user_id = str(file_data.get("user_id", ""))
        if user_id and Config.get("user_bw_mode", True):
            # Skip privileged users from per-user bandwidth enforcement
            uid_int        = int(user_id) if user_id else 0
            _is_privileged = await is_privileged_user_async(database, uid_int)
            if not _is_privileged:
                allowed_user, user_stats = await database.check_user_bw_limit(user_id)
                if not allowed_user:
                    max_ubw = Config.get("max_user_bandwidth", 10737418240)
                    days_r  = user_stats.get("days_remaining", "?")
                    # Fire-and-forget warning — only once per exceeded cycle
                    async def _warn_user_limit():
                        try:
                            already = await database.has_sent_user_exceeded_warning(user_id)
                            if not already:
                                await database.mark_user_exceeded_notified(user_id)
                                await bot.send_message(
                                    uid_int,
                                    f"⛔ **{small_caps('bandwidth limit reached')}**\n\n"
                                    f"📊 **{small_caps('used')}:** `{format_size(user_stats.get('used', 0))}` / `{format_size(max_ubw)}`\n"
                                    f"🔄 **{small_caps('resets in')}:** `{days_r}` ᴅᴀʏꜱ\n\n"
                                    "ꜱᴛʀᴇᴀᴍɪɴɢ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅꜱ ᴀʀᴇ ᴛᴇᴍᴘᴏʀᴀʀɪʟʏ ʙʟᴏᴄᴋᴇᴅ.\n"
                                    "ᴄᴏɴᴛᴀᴄᴛ ᴀɴ ᴀᴅᴍɪɴ ᴛᴏ ʀᴇꜱᴇᴛ ʏᴏᴜʀ ʟɪᴍɪᴛ ᴇᴀʀʟʏ.",
                                )
                        except Exception:
                            pass
                    asyncio.ensure_future(_warn_user_limit())
                    raise web.HTTPServiceUnavailable(reason="user bandwidth limit exceeded")

        # ── Global bandwidth enforcement ───────────────────────────
        allowed, cycle_stats = await check_bandwidth_limit(database)
        if not allowed:
            raise web.HTTPServiceUnavailable(reason="bandwidth limit exceeded")

        # Warn owner if approaching limit (fire-and-forget, one-shot per threshold)
        async def _maybe_warn_owner():
            try:
                should_warn, g_bracket = await should_warn_global_bw(database)
                if should_warn:
                    days_r = cycle_stats.get("days_remaining", "?")
                    pct    = cycle_stats.get("pct", 0)
                    g_used = format_size(cycle_stats.get("used", 0))
                    g_lim  = format_size(cycle_stats.get("limit", 0))
                    await database.mark_global_bw_warning_sent(g_bracket)
                    for owner_id in Config.OWNER_ID:
                        try:
                            await bot.send_message(
                                owner_id,
                                f"⚠️ **{small_caps('global bandwidth warning')}**\n\n"
                                f"ɢʟᴏʙᴀʟ ʙᴀɴᴅᴡɪᴅᴛʜ ɪꜱ ᴀᴛ **{pct:.1f}%** ᴏꜰ ᴍᴏɴᴛʜʟʏ ʟɪᴍɪᴛ!\n\n"
                                f"📊 **{small_caps('used')}:** `{g_used}` / `{g_lim}`\n"
                                f"🔄 **{small_caps('resets in')}:** `{days_r}` ᴅᴀʏꜱ",
                            )
                        except Exception:
                            pass
            except Exception:
                pass
        asyncio.ensure_future(_maybe_warn_owner())

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

        # ── User bandwidth enforcement for downloads ───────────────
        file_data = await database.get_file_by_hash(file_hash)
        if file_data:
            user_id = str(file_data.get("user_id", ""))
            if user_id and Config.get("user_bw_mode", True):
                uid_int        = int(user_id) if user_id else 0
                _is_privileged = await is_privileged_user_async(database, uid_int)
                if not _is_privileged:
                    allowed_user, user_stats = await database.check_user_bw_limit(user_id)
                    if not allowed_user:
                        max_ubw = Config.get("max_user_bandwidth", 10737418240)
                        days_r  = user_stats.get("days_remaining", "?")
                        async def _warn_dl_limit():
                            try:
                                already = await database.has_sent_user_exceeded_warning(user_id)
                                if not already:
                                    await database.mark_user_exceeded_notified(user_id)
                                    await bot.send_message(
                                        uid_int,
                                        f"⛔ **{small_caps('download blocked — bandwidth limit reached')}**\n\n"
                                        f"📊 **{small_caps('used')}:** `{format_size(user_stats.get('used', 0))}` / `{format_size(max_ubw)}`\n"
                                        f"🔄 **{small_caps('resets in')}:** `{days_r}` ᴅᴀʏꜱ\n\n"
                                        "ʏᴏᴜʀ ʟɪᴍɪᴛ ᴡɪʟʟ ʀᴇꜱᴇᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ.",
                                    )
                            except Exception:
                                pass
                        asyncio.ensure_future(_warn_dl_limit())
                        raise web.HTTPServiceUnavailable(reason="user bandwidth limit exceeded")

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
        try:
            cycle   = await database.get_global_bw_cycle()
            bw_used = cycle.get("used", bw_stats["total_bandwidth"])
            days_r  = cycle.get("days_remaining", 30)
        except Exception:
            bw_used = bw_stats["total_bandwidth"]
            days_r  = 30

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
            "bw_mode":        bw_mode,
            "bw_limit":       format_size(max_bw),
            "bw_used":        format_size(bw_used),
            "bw_today":       format_size(bw_today),
            "bw_remaining":   format_size(remaining),
            "bw_pct":         bw_pct,
            "bw_days_reset":  days_r,
            "bot_status":   "running" if getattr(bot, "me", None) else "initializing",
            "active_conns": get_active_session_count(),
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

    # ── Telegram Web App page ──────────────────────────────────
    async def twa_page(request: web.Request):
        """Serve the Telegram Web App SPA."""
        try:
            info = _bot_info(bot)
            return aiohttp_jinja2.render_template(
                "twa.html",
                request,
                {
                    "bot_name":     info["bot_name"],
                    "bot_username": info["bot_username"],
                },
            )
        except Exception as exc:
            logger.error("twa page error: %s", exc)
            return web.Response(status=500, text="Internal server error")

    # ── API: Stats ─────────────────────────────────────────────
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

    # ── API: Basic Bandwidth ───────────────────────────────────
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

    # ── API: Full Bandwidth (enhanced for bot_settings BW panel) ─
    async def api_bandwidth_full(request: web.Request):
        """
        Extended bandwidth API used by the bot_settings bandwidth panel.
        Includes:
          - Global monthly cycle stats with seconds_to_reset
          - Per-user limit config
          - users_over_limit count
        """
        try:
            from datetime import datetime
            stats      = await database.get_bandwidth_stats()
            cycle      = await database.get_global_bw_cycle()
            max_bw     = Config.get("max_bandwidth", 107374182400)
            bw_mode    = Config.get("bandwidth_mode", True)
            user_bw_mode = Config.get("user_bw_mode", True)
            max_ubw    = Config.get("max_user_bandwidth", 10737418240)

            used      = cycle.get("used", stats["total_bandwidth"])
            today     = stats["today_bandwidth"]
            remaining = max(0, max_bw - used)
            pct       = round((used / max_bw * 100) if max_bw else 0, 1)
            days_r    = cycle.get("days_remaining", 30)

            # Calculate seconds to reset (precise)
            cycle_end = cycle.get("cycle_end")
            if cycle_end:
                now = datetime.utcnow()
                delta = cycle_end - now
                seconds_to_reset = max(0, int(delta.total_seconds()))
            else:
                seconds_to_reset = days_r * 86400

            # Count users over limit (async, best-effort)
            users_over_limit = 0
            try:
                cursor = database.user_bw.find({})
                docs   = await cursor.to_list(length=None)
                for doc in docs:
                    used_b = doc.get("used_bytes", 0)
                    if max_ubw > 0 and used_b >= max_ubw:
                        users_over_limit += 1
            except Exception:
                pass

            payload = {
                "limit":            max_bw,
                "used":             used,
                "remaining":        remaining,
                "today":            today,
                "percentage":       pct,
                "days_remaining":   days_r,
                "seconds_to_reset": seconds_to_reset,
                "bandwidth_mode":   bw_mode,
                "user_bw_mode":     user_bw_mode,
                "max_user_bw":      max_ubw,
                "users_over_limit": users_over_limit,
                "formatted": {
                    "total_bandwidth": format_size(used),
                    "today_bandwidth": format_size(today),
                    "limit":           format_size(max_bw),
                    "remaining":       format_size(remaining),
                    "user_limit":      format_size(max_ubw) if max_ubw else "Unlimited",
                },
            }
            return web.Response(
                text=json.dumps(payload),
                content_type="application/json",
                headers={"Access-Control-Allow-Origin": "*"},
            )
        except Exception as exc:
            logger.error("api_bandwidth_full error: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

    # ── API: Health ────────────────────────────────────────────
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

    # ── TWA API: Validate + get user data ─────────────────────
    async def twa_api_auth(request: web.Request):
        """Validate Telegram initData and return user info + limits + role."""
        try:
            body = await request.json()
            init_data = body.get("initData", "")
            if not init_data:
                return web.json_response({"ok": False, "error": "no initData"}, status=400)

            user_data = _validate_telegram_init_data(init_data, Config.BOT_TOKEN)
            if user_data is None:
                return web.json_response({"ok": False, "error": "invalid initData"}, status=403)

            user_id     = str(user_data.get("id", ""))
            user_id_int = int(user_id) if user_id else 0
            bw_info     = {}

            # Determine role
            is_owner = user_id_int in Config.OWNER_ID
            is_sudo  = (not is_owner) and (await database.is_sudo_user(user_id) if user_id else False)
            is_privileged = is_owner or is_sudo
            role = "owner" if is_owner else ("sudo" if is_sudo else "user")

            if user_id:
                try:
                    if is_privileged:
                        # Privileged users: show unlimited / no cap stats
                        bw_info = {
                            "used":           0,
                            "limit":          0,
                            "remaining":      0,
                            "pct":            0,
                            "days_remaining": 30,
                            "unlimited":      True,
                            "formatted": {
                                "used":      "N/A",
                                "limit":     "Unlimited",
                                "remaining": "Unlimited",
                            },
                        }
                    else:
                        bw_stats = await database.get_user_bw(user_id)
                        max_ubw  = Config.get("max_user_bandwidth", 10737418240)
                        bw_info  = {
                            "used":           bw_stats.get("used", 0),
                            "limit":          max_ubw,
                            "remaining":      max(0, max_ubw - bw_stats.get("used", 0)),
                            "pct":            bw_stats.get("pct", 0),
                            "days_remaining": bw_stats.get("days_remaining", 30),
                            "unlimited":      False,
                            "formatted": {
                                "used":      format_size(bw_stats.get("used", 0)),
                                "limit":     format_size(max_ubw),
                                "remaining": format_size(max(0, max_ubw - bw_stats.get("used", 0))),
                            },
                        }
                except Exception:
                    pass

            return web.json_response({
                "ok":             True,
                "user":           user_data,
                "role":           role,
                "is_privileged":  is_privileged,
                "bw_info":        bw_info,
                "bw_mode":        Config.get("bandwidth_mode", True),
                "user_bw_mode":   Config.get("user_bw_mode", True),
            }, headers={"Access-Control-Allow-Origin": "*"})

        except Exception as exc:
            logger.error("twa_api_auth error: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    # ── TWA API: List user files ───────────────────────────────
    async def twa_api_files(request: web.Request):
        """Return paginated file list for authenticated TWA user (with role + share URL)."""
        try:
            # Get auth header or query param
            init_data = request.headers.get("X-Telegram-Init-Data", "")
            if not init_data:
                init_data = request.rel_url.query.get("initData", "")

            user_data = _validate_telegram_init_data(init_data, Config.BOT_TOKEN)
            if user_data is None:
                return web.json_response({"ok": False, "error": "unauthorized"}, status=403)

            user_id     = str(user_data.get("id", ""))
            user_id_int = int(user_id) if user_id else 0
            page        = int(request.rel_url.query.get("page", 1))
            limit       = min(int(request.rel_url.query.get("limit", 20)), 50)
            skip        = (page - 1) * limit

            # Determine role
            is_owner      = user_id_int in Config.OWNER_ID
            is_sudo       = (not is_owner) and (await database.is_sudo_user(user_id) if user_id else False)
            is_privileged = is_owner or is_sudo
            role          = "owner" if is_owner else ("sudo" if is_sudo else "user")

            # Check user bw limit — privileged users are always allowed
            allowed = True
            if user_id and not is_privileged and Config.get("user_bw_mode", True):
                allowed, _ = await database.check_user_bw_limit(user_id)

            # Bot username for share links
            bot_username = ""
            try:
                from bot import Bot
                me = getattr(bot, "me", None)
                if me:
                    bot_username = me.username or ""
            except Exception:
                pass

            cursor, total = await database.find_files(user_id, [skip + 1, limit])
            files = []
            async for doc in cursor:
                base       = str(request.url.origin())
                file_hash  = doc["file_id"]
                share_url  = f"https://t.me/{bot_username}?start=file_{file_hash}" if bot_username else ""
                files.append({
                    "file_id":        file_hash,
                    "file_name":      doc["file_name"],
                    "file_size":      doc["file_size"],
                    "file_size_fmt":  format_size(doc["file_size"]),
                    "file_type":      doc.get("file_type", "document"),
                    "mime_type":      doc.get("mime_type", ""),
                    "stream_url":     f"{base}/stream/{file_hash}",
                    "download_url":   f"{base}/dl/{file_hash}",
                    "share_url":      share_url,
                    "created_at":     doc.get("created_at", "").isoformat() if hasattr(doc.get("created_at", ""), "isoformat") else str(doc.get("created_at", "")),
                })

            return web.json_response({
                "ok":            True,
                "files":         files,
                "total":         total,
                "page":          page,
                "pages":         max(1, -(-total // limit)),
                "allowed":       allowed,
                "role":          role,
                "is_privileged": is_privileged,
            }, headers={"Access-Control-Allow-Origin": "*"})

        except Exception as exc:
            logger.error("twa_api_files error: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    # ── TWA API: Global bandwidth info (public) ────────────────
    async def twa_api_bandwidth(request: web.Request):
        """Public global bandwidth info for TWA display."""
        try:
            cycle  = await database.get_global_bw_cycle()
            max_bw = Config.get("max_bandwidth", 107374182400)
            used   = cycle.get("used", 0)
            pct    = cycle.get("pct", 0)
            days_r = cycle.get("days_remaining", 30)

            return web.json_response({
                "ok": True,
                "used":      used,
                "limit":     max_bw,
                "remaining": max(0, max_bw - used),
                "pct":       pct,
                "days_remaining": days_r,
                "formatted": {
                    "used":      format_size(used),
                    "limit":     format_size(max_bw),
                    "remaining": format_size(max(0, max_bw - used)),
                },
            }, headers={"Access-Control-Allow-Origin": "*"})

        except Exception as exc:
            logger.error("twa_api_bandwidth error: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    # ── Preflight CORS handler ─────────────────────────────────
    async def cors_preflight(request: web.Request):
        return web.Response(
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
            },
        )

    # ── Legacy redirect endpoints ──────────────────────────────
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

    # ── Routes ────────────────────────────────────────────────
    app.router.add_get("/",                          home)
    app.router.add_get("/stream/{file_hash}",        stream_page)
    app.router.add_get("/dl/{file_hash}",            download_file)
    app.router.add_get("/bot_settings",              bot_settings_page)
    app.router.add_get("/twa",                       twa_page)

    # Core API
    app.router.add_get("/api/stats",                 api_stats)
    app.router.add_get("/api/bandwidth",             api_bandwidth)
    app.router.add_get("/api/bandwidth/full",        api_bandwidth_full)
    app.router.add_get("/api/health",                api_health)

    # TWA API
    app.router.add_post("/api/twa/auth",             twa_api_auth)
    app.router.add_get("/api/twa/files",             twa_api_files)
    app.router.add_get("/api/twa/bandwidth",         twa_api_bandwidth)
    app.router.add_route("OPTIONS", "/api/twa/{tail:.*}", cors_preflight)

    # Legacy redirects
    app.router.add_get("/stats",                     stats_endpoint)
    app.router.add_get("/bandwidth",                 bandwidth_endpoint)
    app.router.add_get("/health",                    health_endpoint)

    return app
