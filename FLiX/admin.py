import asyncio
import logging
import os
import time

from pyrogram import Client, filters, StopPropagation
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from config import Config
from database import db
from helper import (
    small_caps, format_size, escape_markdown,
    format_uptime, human_size, check_owner,
    get_global_bandwidth_warning,
)

logger = logging.getLogger(__name__)

# ─── Ban reason presets ───────────────────────────────────────────────────────
BAN_REASON_PRESETS = [
    ("🚨 Service Abuse",             "Service Abuse"),
    ("🔞 Restricted/Prohibited Content", "Restricted/Prohibited Content"),
    ("🤖 Spam/Bot Activity",         "Spam/Bot Activity"),
    ("⚠️ Policy Violation",          "Policy Violation"),
]

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _mention(user_id: str, name: str = None) -> str:
    """Build a clickable Telegram mention link."""
    display = name or f"User {user_id}"
    return f"[{display}](tg://user?id={user_id})"


def _fmt_cycle_info(days: int, hours: int) -> str:
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h"
    return "< 1h"


async def _try_get_name(client: Client, user_id: str) -> str:
    """Try to resolve a display name for a Telegram user_id."""
    try:
        u = await client.get_users(int(user_id))
        name = (u.first_name or "").strip()
        if u.last_name:
            name = f"{name} {u.last_name}".strip()
        return name or f"User {user_id}"
    except Exception:
        return f"User {user_id}"


async def show_panel(client: Client, source, panel_type: str, **kwargs):
    config = Config.all()
    msg    = source.message if isinstance(source, CallbackQuery) else source

    if panel_type == "main_panel":
        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_toggle = Config.get("bandwidth_mode", True)
        text = (
            f"✨ **{small_caps('bot settings panel')}** ✨\n\n"
            f"📡 **{small_caps('bandwidth')}**  : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'} | `{format_size(max_bw)}`\n"
            f"👥 **{small_caps('sudo users')}** : ᴍᴀɴᴀɢᴇ ᴀᴄᴄᴇꜱꜱ\n"
            f"🤖 **{small_caps('bot mode')}**  : {'🟢 ᴘᴜʙʟɪᴄ' if config.get('public_bot') else '🔴 ᴘʀɪᴠᴀᴛᴇ'}\n"
            f"📢 **{small_caps('force sub')}** : {'🟢 ᴀᴄᴛɪᴠᴇ' if config.get('fsub_mode') else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n\n"
            "👇 ᴄʜᴏᴏꜱᴇ ᴀ ᴄᴀᴛᴇɢᴏʀʏ ᴛᴏ ᴄᴏɴꜰɪɢᴜʀᴇ."
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📡 ʙᴀɴᴅᴡɪᴅᴛʜ",  callback_data="settings_bandwidth"),
                InlineKeyboardButton("👥 ꜱᴜᴅᴏ ᴜꜱᴇʀꜱ", callback_data="settings_sudo"),
            ],
            [
                InlineKeyboardButton("🤖 ʙᴏᴛ ᴍᴏᴅᴇ",   callback_data="settings_botmode"),
                InlineKeyboardButton("📢 ꜰᴏʀᴄᴇ ꜱᴜʙ",  callback_data="settings_fsub"),
            ],
            [
                InlineKeyboardButton("🚫 ʙᴀɴ ꜱʏꜱᴛᴇᴍ", callback_data="settings_bans"),
            ],
            [InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="settings_close")],
        ])

    elif panel_type == "bandwidth_panel":
        max_bw      = Config.get("max_bandwidth", 107374182400)
        bw_toggle   = Config.get("bandwidth_mode", True)
        ubw_toggle  = Config.get("user_bandwidth_mode", False)
        max_user_bw = Config.get("max_user_bandwidth", 10737418240)
        bw_stats    = await db.get_bandwidth_stats()
        bw_used     = bw_stats["total_bandwidth"]
        bw_today    = bw_stats["today_bandwidth"]
        bw_pct      = (bw_used / max_bw * 100) if max_bw else 0
        days_rem    = bw_stats.get("days_remaining",  30)
        hours_rem   = bw_stats.get("hours_remaining", 0)
        reset_str   = _fmt_cycle_info(days_rem, hours_rem)
        cs          = bw_stats.get("cycle_start")
        ce          = bw_stats.get("cycle_end")
        cs_str      = cs.strftime("%Y-%m-%d") if hasattr(cs, "strftime") else "N/A"
        ce_str      = ce.strftime("%Y-%m-%d") if hasattr(ce, "strftime") else "N/A"

        text = (
            f"💠 **{small_caps('bandwidth settings')}** 💠\n\n"
            f"⚡ **{small_caps('global mode')}**      : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"📊 **{small_caps('global limit')}**     : `{format_size(max_bw)}`\n"
            f"📤 **{small_caps('used (total)')}**     : `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
            f"📅 **{small_caps('used today')}**       : `{format_size(bw_today)}`\n"
            f"🔄 **{small_caps('resets in')}**        : `{reset_str}` _(on `{ce_str}`)_\n"
            f"📆 **{small_caps('cycle')}**            : `{cs_str}` → `{ce_str}`\n\n"
            f"👤 **{small_caps('per-user mode')}**    : {'🟢 ᴀᴄᴛɪᴠᴇ' if ubw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"📏 **{small_caps('per-user limit')}**   : `{format_size(max_user_bw)}` / 30 ᴅᴀʏꜱ\n"
            f"ℹ️ _ꜱᴜᴅᴏ/ᴏᴡɴᴇʀ ᴀʀᴇ ᴇxᴇᴍᴘᴛ ꜰʀᴏᴍ ᴘᴇʀ-ᴜꜱᴇʀ ʟɪᴍɪᴛ_"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ ɢʟᴏʙᴀʟ",    callback_data="toggle_bandwidth")],
            [
                InlineKeyboardButton("✏️ ꜱᴇᴛ ɢʟᴏʙᴀʟ ʟɪᴍɪᴛ", callback_data="set_bandwidth_limit"),
                InlineKeyboardButton("🔄 ʀᴇꜱᴇᴛ ᴜꜱᴀɢᴇ",       callback_data="reset_bandwidth"),
            ],
            [InlineKeyboardButton("👤 ᴛᴏɢɢʟᴇ ᴘᴇʀ-ᴜꜱᴇʀ",    callback_data="toggle_user_bandwidth")],
            [InlineKeyboardButton("✏️ ꜱᴇᴛ ᴘᴇʀ-ᴜꜱᴇʀ ʟɪᴍɪᴛ", callback_data="set_user_bandwidth_limit")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",                callback_data="settings_back")],
        ])

    elif panel_type == "sudo_panel":
        sudo_users = await db.get_sudo_users()
        count      = len(sudo_users)
        recent     = sudo_users[:5]  # show 5 most recent
        lines_parts = []
        for u in recent:
            uid  = u["user_id"]
            name = await _try_get_name(client, uid)
            ts   = u.get("added_at")
            ts_s = ts.strftime("%m-%d") if hasattr(ts, "strftime") else ""
            lines_parts.append(f"  • {_mention(uid, name)} `{uid}` {f'_{ts_s}_' if ts_s else ''}")
        lines = "\n".join(lines_parts) if lines_parts else "  ɴᴏɴᴇ"
        if count > 5:
            lines += f"\n  _…ᴀɴᴅ {count - 5} ᴍᴏʀᴇ_"

        # Recent history
        hist = await db.get_history("sudo_add", limit=3)
        hist_lines = []
        for h in hist:
            ts = h.get("created_at")
            ts_s = ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else ""
            hist_lines.append(f"  ➕ `{h['target_id']}` ʙʏ `{h['by_id']}` _{ts_s}_")
        hist_text = "\n".join(hist_lines) if hist_lines else "  ɴᴏɴᴇ"

        text = (
            f"💠 **{small_caps('sudo users')}** 💠\n\n"
            f"👥 **{small_caps('count')}** : `{count}`\n\n"
            f"**{small_caps('recent sudo')}:**\n{lines}\n\n"
            f"**{small_caps('recent additions')}:**\n{hist_text}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➕ ᴀᴅᴅ",    callback_data="sudo_add"),
                InlineKeyboardButton("➖ ʀᴇᴍᴏᴠᴇ", callback_data="sudo_remove"),
            ],
            [InlineKeyboardButton("📋 ꜰᴜʟʟ ʟɪꜱᴛ",   callback_data="sudo_list_full")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
        ])

    elif panel_type == "sudo_list_panel":
        sudo_users = await db.get_sudo_users()
        count = len(sudo_users)
        lines_parts = []
        for i, u in enumerate(sudo_users, 1):
            uid  = u["user_id"]
            name = await _try_get_name(client, uid)
            ts   = u.get("added_at")
            ts_s = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else ""
            lines_parts.append(f"  {i}. {_mention(uid, name)} `{uid}`\n     _Added: {ts_s} by `{u.get('added_by','?')}`_")
        lines = "\n".join(lines_parts) if lines_parts else "  ɴᴏɴᴇ"

        # Full history
        hist = await db.get_history("sudo_remove", limit=5)
        hist_lines = []
        for h in hist:
            ts   = h.get("created_at")
            ts_s = ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else ""
            hist_lines.append(f"  ➖ `{h['target_id']}` ʙʏ `{h['by_id']}` _{ts_s}_")
        hist_text = "\n".join(hist_lines) if hist_lines else "  ɴᴏɴᴇ"

        text = (
            f"💠 **{small_caps('sudo users — full list')}** 💠\n\n"
            f"👥 **{small_caps('total')}** : `{count}`\n\n"
            f"**{small_caps('all sudo users')}:**\n{lines}\n\n"
            f"**{small_caps('recent removals')}:**\n{hist_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_sudo")],
        ])

    elif panel_type == "botmode_panel":
        public = config.get("public_bot", False)
        text = (
            f"💠 **{small_caps('bot mode settings')}** 💠\n\n"
            f"⚡ **{small_caps('current mode')}** : {'🌍 ᴘᴜʙʟɪᴄ' if public else '🔒 ᴘʀɪᴠᴀᴛᴇ'}\n\n"
            f"🌍 **{small_caps('public')}** — ᴀɴʏᴏɴᴇ ᴄᴀɴ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ\n"
            f"🔒 **{small_caps('private')}** — ᴏɴʟʏ ꜱᴜᴅᴏ/ᴏᴡɴᴇʀ ᴄᴀɴ ᴜꜱᴇ"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"🔓 {small_caps('set public')}" if not public else f"🔒 {small_caps('set private')}",
                callback_data="toggle_botmode",
            )],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
        ])

    elif panel_type == "fsub_panel":
        fsub_id   = config.get("fsub_chat_id", 0)
        fsub_name = "ɴᴏᴛ ꜱᴇᴛ"
        if fsub_id:
            try:
                fsub_name = (await client.get_chat(fsub_id)).title
            except Exception:
                fsub_name = "❓ ᴜɴᴋɴᴏᴡɴ"

        text = (
            f"💠 **{small_caps('force sub settings')}** 💠\n\n"
            f"⚡ **{small_caps('mode')}**          : {'🟢 ᴀᴄᴛɪᴠᴇ' if config.get('fsub_mode') else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"🆔 **{small_caps('channel id')}**   : `{fsub_id or 'ɴᴏᴛ ꜱᴇᴛ'}`\n"
            f"📛 **{small_caps('channel name')}** : `{fsub_name}`\n"
            f"🔗 **{small_caps('invite link')}**  : `{config.get('fsub_inv_link') or 'ɴᴏᴛ ꜱᴇᴛ'}`"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ", callback_data="toggle_fsub")],
            [
                InlineKeyboardButton(f"🆔 {small_caps('channel id')}", callback_data="set_fsub_id"),
                InlineKeyboardButton(f"🔗 {small_caps('invite link')}",  callback_data="set_fsub_link"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
        ])

    elif panel_type == "bans_panel":
        banned_users = await db.get_banned_users(limit=5)
        count        = await db.banned.count_documents({})
        lines_parts  = []
        for u in banned_users:
            uid    = u["user_id"]
            name   = await _try_get_name(client, uid)
            reason = u.get("reason", "N/A")[:25]
            ts     = u.get("banned_at")
            ts_s   = ts.strftime("%m-%d") if hasattr(ts, "strftime") else ""
            lines_parts.append(f"  • {_mention(uid, name)} `{uid}`\n    📋 `{reason}` _{ts_s}_")
        lines = "\n".join(lines_parts) if lines_parts else "  ɴᴏɴᴇ"

        # Recent history
        hist = await db.get_history("unban", limit=3)
        hist_lines = []
        for h in hist:
            ts   = h.get("created_at")
            ts_s = ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else ""
            hist_lines.append(f"  ✅ `{h['target_id']}` ᴜɴʙᴀɴɴᴇᴅ ʙʏ `{h['by_id']}` _{ts_s}_")
        hist_text = "\n".join(hist_lines) if hist_lines else "  ɴᴏɴᴇ"

        text = (
            f"🚫 **{small_caps('ban system')}** 🚫\n\n"
            f"📋 **{small_caps('total banned')}** : `{count}`\n\n"
            f"**{small_caps('recently banned')}:**\n{lines}\n\n"
            f"**{small_caps('recent unbans')}:**\n{hist_text}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔨 ʙᴀɴ ᴜꜱᴇʀ",     callback_data="ban_user_prompt"),
                InlineKeyboardButton("✅ ᴜɴʙᴀɴ ᴜꜱᴇʀ",   callback_data="unban_user_prompt"),
            ],
            [InlineKeyboardButton("🔍 ᴄʜᴇᴄᴋ ʙᴀɴ",       callback_data="checkban_user_prompt")],
            [InlineKeyboardButton("📋 ꜰᴜʟʟ ʙᴀɴ ʟɪꜱᴛ",   callback_data="ban_list_full")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",             callback_data="settings_back")],
        ])

    elif panel_type == "bans_list_panel":
        all_banned  = await db.get_banned_users()
        count       = len(all_banned)
        lines_parts = []
        for i, u in enumerate(all_banned, 1):
            uid    = u["user_id"]
            name   = await _try_get_name(client, uid)
            reason = u.get("reason", "N/A")[:30]
            ts     = u.get("banned_at")
            ts_s   = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else ""
            lines_parts.append(
                f"  {i}. {_mention(uid, name)} `{uid}`\n"
                f"     📋 `{reason}`\n"
                f"     👮 `{u.get('banned_by','?')}` · _{ts_s}_"
            )
        lines = "\n".join(lines_parts) if lines_parts else "  ɴᴏɴᴇ"

        # Full ban history
        hist = await db.get_history("ban", limit=5)
        hist_lines = []
        for h in hist:
            ts   = h.get("created_at")
            ts_s = ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else ""
            note = h.get("note", "")[:20]
            hist_lines.append(f"  🔨 `{h['target_id']}` ʙʏ `{h['by_id']}` _{ts_s}_ — `{note}`")
        hist_text = "\n".join(hist_lines) if hist_lines else "  ɴᴏɴᴇ"

        text = (
            f"🚫 **{small_caps('banned users — full list')}** 🚫\n\n"
            f"📋 **{small_caps('total')}** : `{count}`\n\n"
            f"**{small_caps('all banned users')}:**\n{lines}\n\n"
            f"**{small_caps('recent bans (history)')}:**\n{hist_text}"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_bans")],
        ])

    elif panel_type == "ban_reason_panel":
        # Choose ban reason preset or custom — target_id passed via kwargs
        target_id = kwargs.get("target_id", "?")
        text = (
            f"🔨 **{small_caps('choose ban reason')}**\n\n"
            f"🆔 ᴛᴀʀɢᴇᴛ: `{target_id}`\n\n"
            "ꜱᴇʟᴇᴄᴛ ᴀ ʀᴇᴀꜱᴏɴ ᴏʀ ᴄʜᴏᴏꜱᴇ **ᴄᴜꜱᴛᴏᴍ** ᴛᴏ ᴛʏᴘᴇ ʏᴏᴜʀ ᴏᴡɴ:"
        )
        preset_btns = [
            [InlineKeyboardButton(label, callback_data=f"ban_preset_{target_id}_{value}")]
            for label, value in BAN_REASON_PRESETS
        ]
        preset_btns += [
            [InlineKeyboardButton("✏️ ᴄᴜꜱᴛᴏᴍ ʀᴇᴀꜱᴏɴ", callback_data=f"ban_custom_{target_id}")],
            [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ",          callback_data="settings_bans")],
        ]
        buttons = InlineKeyboardMarkup(preset_btns)

    else:
        return

    if isinstance(source, CallbackQuery):
        try:
            await source.message.edit_text(text, reply_markup=buttons)
        except Exception:
            await client.send_message(
                chat_id=source.message.chat.id,
                text=text,
                reply_markup=buttons,
            )
    else:
        await client.send_message(
            chat_id=source.chat.id,
            text=text,
            reply_to_message_id=source.id,
            reply_markup=buttons,
        )


_pending: dict[int, asyncio.Future] = {}


@Client.on_message(filters.text & filters.private, group=99)
async def _catch_pending(client: Client, message: Message):
    uid = message.from_user.id
    if uid in _pending and not _pending[uid].done():
        _pending[uid].set_result(message)
        raise StopPropagation


async def ask_input(
    client: Client, user_id: int, prompt: str, timeout: int = 60
) -> str | None:
    loop   = asyncio.get_event_loop()
    future = loop.create_future()
    _pending[user_id] = future

    ask_msg = None
    reply   = None
    try:
        ask_msg = await client.send_message(user_id, prompt)
        reply   = await asyncio.wait_for(future, timeout=timeout)
        return reply.text.strip() if reply and reply.text else None
    except asyncio.TimeoutError:
        logger.debug("ask_input timed out for user %s", user_id)
        return None
    except Exception as exc:
        logger.debug("ask_input error for user %s: %s", user_id, exc)
        return None
    finally:
        _pending.pop(user_id, None)
        for m in (ask_msg, reply):
            if m:
                try:
                    await m.delete()
                except Exception:
                    pass


async def _do_ban(client: Client, target_id: str, by_id: str, reason: str, callback: CallbackQuery = None):
    """Perform the ban, notify the banned user, and refresh the bans panel."""
    if int(target_id) in Config.OWNER_ID:
        if callback:
            await callback.answer(f"❌ {small_caps('cannot ban owner')}!", show_alert=True)
        return

    await db.ban_user(target_id, by_id, reason)

    # Notify the banned user
    try:
        await client.send_message(
            int(target_id),
            f"🚫 **{small_caps('you have been banned')}**\n\n"
            f"📋 **{small_caps('reason')}:** `{reason}`\n\n"
            "ꜰᴏʀ ᴀᴘᴘᴇᴀʟꜱ, ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴɪꜱᴛʀᴀᴛᴏʀ.",
        )
    except Exception:
        pass  # User may have blocked the bot

    if callback:
        await callback.answer(f"🔨 `{target_id}` {small_caps('has been banned')}!", show_alert=True)
        await show_panel(client, callback, "bans_panel")


@Client.on_message(filters.command("bot_settings") & filters.private, group=2)
async def open_settings(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    # Show global bandwidth warning if near limit
    warn = await get_global_bandwidth_warning(db)
    if warn:
        pct  = warn["pct"] * 100
        dr   = _fmt_cycle_info(warn["days_remaining"], warn["hours_remaining"])
        from helper import format_size as _fs
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"⚠️ **{small_caps('global bandwidth warning')}** ⚠️\n\n"
                f"📊 **{small_caps('used')}:** `{_fs(warn['used'])}` / `{_fs(warn['limit'])}` "
                f"({pct:.1f}%)\n"
                f"⏳ **{small_caps('resets in')}:** `{dr}`\n\n"
                "🔴 ᴛʜᴇ ʙᴏᴛ-ᴡɪᴅᴇ ʙᴀɴᴅᴡɪᴅᴛʜ ɪꜱ ʀᴜɴɴɪɴɢ ʟᴏᴡ!\n"
                "ᴄᴏɴꜱɪᴅᴇʀ ɪɴᴄʀᴇᴀꜱɪɴɢ ᴛʜᴇ ʟɪᴍɪᴛ ᴏʀ ᴡᴀɪᴛɪɴɢ ꜰᴏʀ ᴛʜᴇ ᴄʏᴄʟᴇ ᴛᴏ ʀᴇꜱᴇᴛ."
            ),
        )

    await show_panel(client, message, "main_panel")


@Client.on_callback_query(
    filters.regex(r"^(settings_|toggle_|set_|sudo_|reset_|ban_|unban_|checkban_).+"),
    group=2,
)
async def settings_callback(client: Client, callback: CallbackQuery):
    data   = callback.data
    config = Config.all()

    if not await check_owner(client, callback):
        return

    panel_nav = {
        "settings_bandwidth": ("bandwidth_panel", f"📡 {small_caps('bandwidth settings')}"),
        "settings_sudo":      ("sudo_panel",      f"👥 {small_caps('sudo users')}"),
        "settings_botmode":   ("botmode_panel",   f"🤖 {small_caps('bot mode settings')}"),
        "settings_fsub":      ("fsub_panel",      f"📌 {small_caps('force sub settings')}"),
        "settings_bans":      ("bans_panel",      f"🚫 {small_caps('ban system')}"),
        "settings_back":      ("main_panel",      f"⬅️ {small_caps('back to main menu')}"),
    }
    if data in panel_nav:
        panel, toast = panel_nav[data]
        await callback.answer(toast, show_alert=False)
        return await show_panel(client, callback, panel)

    if data == "settings_close":
        try:
            await callback.answer(f"❌ {small_caps('closing')}", show_alert=True)
            await callback.message.delete()
        except Exception:
            pass
        return

    # Full-list panels
    if data == "sudo_list_full":
        await callback.answer(f"📋 {small_caps('loading full list')}…", show_alert=False)
        return await show_panel(client, callback, "sudo_list_panel")

    if data == "ban_list_full":
        await callback.answer(f"📋 {small_caps('loading full ban list')}…", show_alert=False)
        return await show_panel(client, callback, "bans_list_panel")

    # ── Global Bandwidth ────────────────────────────────────────────────────

    if data == "toggle_bandwidth":
        new_val = not config.get("bandwidth_mode", True)
        await Config.update(db.db, {"bandwidth_mode": new_val})
        await callback.answer(f"✅ {small_caps('bandwidth mode toggled')}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "toggle_botmode":
        new_val = not config.get("public_bot", False)
        await Config.update(db.db, {"public_bot": new_val})
        mode = small_caps("public") if new_val else small_caps("private")
        await callback.answer(f"✅ {small_caps('bot set to')} {mode}!", show_alert=True)
        return await show_panel(client, callback, "botmode_panel")

    if data == "toggle_fsub":
        new_val = not config.get("fsub_mode", False)
        await Config.update(db.db, {"fsub_mode": new_val})
        await callback.answer(f"✅ {small_caps('force sub toggled')}!", show_alert=True)
        return await show_panel(client, callback, "fsub_panel")

    if data == "set_bandwidth_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"📡 **{small_caps('send bandwidth limit in bytes')}**\n\n"
            f"{small_caps('examples')}:\n"
            "`107374182400` — 100 GB\n"
            "`53687091200`  — 50 GB\n"
            "`10737418240`  — 10 GB\n\n"
            f"{small_caps('send')} `0` {small_caps('to reset to 100 gb')}.",
        )
        if text is None:
            return
        if not text.isdigit():
            await callback.answer(f"❌ {small_caps('invalid number')}!", show_alert=True)
            return
        new_limit = int(text) or 107374182400
        await Config.update(db.db, {"max_bandwidth": new_limit})
        await callback.answer(f"✅ {small_caps('limit set to')} {format_size(new_limit)}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "reset_bandwidth":
        await callback.answer(f"🔄 {small_caps('resetting bandwidth usage')}…", show_alert=False)
        ok = await db.reset_bandwidth()
        if ok:
            await callback.answer(f"✅ {small_caps('bandwidth usage reset to zero')}!", show_alert=True)
        else:
            await callback.answer(f"❌ {small_caps('failed to reset bandwidth')}.", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    # ── Per-User Bandwidth ───────────────────────────────────────────────────

    if data == "toggle_user_bandwidth":
        new_val = not config.get("user_bandwidth_mode", False)
        await Config.update(db.db, {"user_bandwidth_mode": new_val})
        state = small_caps("enabled") if new_val else small_caps("disabled")
        await callback.answer(f"✅ {small_caps('per-user bandwidth')} {state}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "set_user_bandwidth_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"👤 **{small_caps('send per-user bandwidth limit in bytes')}**\n\n"
            f"_{small_caps('applies to free users only — sudo/owner are exempt')}_\n\n"
            f"{small_caps('examples')}:\n"
            "`10737418240`  — 10 GB / 30 ᴅᴀʏꜱ\n"
            "`5368709120`   — 5 GB / 30 ᴅᴀʏꜱ\n"
            "`2147483648`   — 2 GB / 30 ᴅᴀʏꜱ\n\n"
            f"{small_caps('send')} `0` {small_caps('to reset to 10 gb')}.",
        )
        if text is None:
            return
        if not text.isdigit():
            await callback.answer(f"❌ {small_caps('invalid number')}!", show_alert=True)
            return
        new_limit = int(text) or 10737418240
        await Config.update(db.db, {"max_user_bandwidth": new_limit})
        await callback.answer(
            f"✅ {small_caps('per-user limit set to')} {format_size(new_limit)}/30d!",
            show_alert=True,
        )
        return await show_panel(client, callback, "bandwidth_panel")

    # ── Sudo Users ───────────────────────────────────────────────────────────

    if data == "sudo_add":
        text = await ask_input(
            client, callback.from_user.id,
            f"👥 **{small_caps('send user id to add as sudo')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return
        await db.add_sudo_user(text, str(callback.from_user.id))
        await callback.answer(f"✅ `{text}` {small_caps('added as sudo')}!", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

    if data == "sudo_remove":
        text = await ask_input(
            client, callback.from_user.id,
            f"👥 **{small_caps('send user id to remove from sudo')}**",
        )
        if text is None:
            return
        result = await db.remove_sudo_user(text, str(callback.from_user.id))
        if result:
            await callback.answer(f"✅ `{text}` {small_caps('removed from sudo')}!", show_alert=True)
        else:
            await callback.answer(f"❌ `{text}` {small_caps('not found in sudo list')}.", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

    # ── Force Sub ────────────────────────────────────────────────────────────

    if data == "set_fsub_id":
        text = await ask_input(
            client, callback.from_user.id,
            f"📢 **{small_caps('send the channel id')}**\n\n"
            f"📌 {small_caps('format')}: `-100xxxxxxxxxx`\n"
            f"➡️ {small_caps('send')} `0` {small_caps('to unset')}.",
        )
        if text is None:
            return

        value = int(text) if text != "0" and text.lstrip("-").isdigit() else 0

        if value == 0:
            await Config.update(db.db, {"fsub_chat_id": 0, "fsub_inv_link": ""})
            await callback.answer(f"✅ {small_caps('force sub channel unset')}!", show_alert=True)
            return await show_panel(client, callback, "fsub_panel")

        if not str(value).startswith("-100"):
            return await callback.answer(
                f"❌ {small_caps('invalid id')}!\n\n📌 {small_caps('channel id must start with')} `-100`",
                show_alert=True,
            )

        try:
            me     = await client.get_me()
            member = await client.get_chat_member(value, me.id)

            if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                return await callback.answer(
                    f"❌ {small_caps('no admin rights')}!\n\n⚡ {small_caps('i must be admin in that channel')}.",
                    show_alert=True,
                )

            rights = getattr(member, "privileges", None)
            if rights and not rights.can_invite_users:
                return await callback.answer(
                    f"❌ {small_caps('missing permission')}!\n\n"
                    f"👤 {small_caps('please grant')}: 🔑 `{small_caps('add subscribers')}` {small_caps('right')}",
                    show_alert=True,
                )

            try:
                inv = await client.export_chat_invite_link(value)
            except Exception:
                inv = ""

            await Config.update(db.db, {"fsub_chat_id": value, "fsub_inv_link": inv})
            await callback.answer(
                f"✅ {small_caps('force sub channel saved')}!\n\n🆔 {small_caps('id')} + 🔗 {small_caps('invite link added')}.",
                show_alert=True,
            )

        except Exception as exc:
            return await callback.answer(f"❌ {small_caps('error')}:\n`{exc}`", show_alert=True)

        return await show_panel(client, callback, "fsub_panel")

    if data == "set_fsub_link":
        text = await ask_input(
            client, callback.from_user.id,
            f"🔗 **{small_caps('send invite link')}**\n\n{small_caps('send')} `0` {small_caps('to unset')}.",
        )
        if text is not None:
            await Config.update(db.db, {"fsub_inv_link": "" if text == "0" else text})
            await callback.answer(f"✅ {small_caps('force sub invite link updated')}!", show_alert=True)
            return await show_panel(client, callback, "fsub_panel")
        return

    # ── Ban System — Preset selection ────────────────────────────────────────

    if data.startswith("ban_preset_"):
        # Format: ban_preset_<target_id>_<reason_encoded>
        rest      = data[len("ban_preset_"):]
        parts     = rest.split("_", 1)
        target_id = parts[0]
        reason    = parts[1].replace("_", " ") if len(parts) > 1 else "Policy Violation"
        await _do_ban(client, target_id, str(callback.from_user.id), reason, callback)
        return

    if data.startswith("ban_custom_"):
        target_id = data[len("ban_custom_"):]
        custom = await ask_input(
            client, callback.from_user.id,
            f"✏️ **{small_caps('type a custom ban reason')}** for `{target_id}`:",
        )
        if custom is None:
            return await show_panel(client, callback, "bans_panel")
        await _do_ban(client, target_id, str(callback.from_user.id), custom, callback)
        return

    # ── Ban System via Settings Panel ────────────────────────────────────────

    if data == "ban_user_prompt":
        text = await ask_input(
            client, callback.from_user.id,
            f"🔨 **{small_caps('send user id to ban')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return
        if int(text) in Config.OWNER_ID:
            await callback.answer(f"❌ {small_caps('cannot ban owner')}!", show_alert=True)
            return
        # Show reason picker
        return await show_panel(client, callback, "ban_reason_panel", target_id=text)

    if data == "unban_user_prompt":
        text = await ask_input(
            client, callback.from_user.id,
            f"✅ **{small_caps('send user id to unban')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return
        result = await db.unban_user(text, str(callback.from_user.id))
        if result:
            # Try to notify unbanned user
            try:
                await client.send_message(
                    int(text),
                    f"✅ **{small_caps('you have been unbanned')}**\n\n"
                    "ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ ᴀɢᴀɪɴ.",
                )
            except Exception:
                pass
            await callback.answer(f"✅ `{text}` {small_caps('has been unbanned')}!", show_alert=True)
        else:
            await callback.answer(f"❌ `{text}` {small_caps('was not banned')}.", show_alert=True)
        return await show_panel(client, callback, "bans_panel")

    if data == "checkban_user_prompt":
        text = await ask_input(
            client, callback.from_user.id,
            f"🔍 **{small_caps('send user id to check ban status')}**",
        )
        if text is None:
            return
        if not text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return
        ban_info = await db.get_ban_info(text)
        if ban_info:
            reason    = ban_info.get("reason", "N/A")
            banned_at = ban_info.get("banned_at", "N/A")
            dt = banned_at.strftime("%Y-%m-%d %H:%M") if hasattr(banned_at, "strftime") else str(banned_at)
            await callback.answer(
                f"🔨 `{text}` {small_caps('is banned')}\n"
                f"{small_caps('reason')}: {reason}\n"
                f"{small_caps('since')}: {dt}",
                show_alert=True,
            )
        else:
            await callback.answer(f"✅ `{text}` {small_caps('is not banned')}.", show_alert=True)
        return await show_panel(client, callback, "bans_panel")


# ─── Ban / Unban Commands ────────────────────────────────────────────────────

@Client.on_message(filters.command("ban") & filters.private, group=2)
async def ban_command(client: Client, message: Message):
    """Ban a user. Usage: /ban <user_id> [reason]"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/ban <user_id> [reason]`\n\n"
                f"{small_caps('example')}: `/ban 123456789 spam`"
            ),
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    reason    = " ".join(message.command[2:]) if len(message.command) > 2 else "No reason provided"

    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    if int(target_id) in Config.OWNER_ID:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('cannot ban the owner')}**",
            reply_to_message_id=message.id,
        )
        return

    already_banned = await db.is_banned(target_id)
    await db.ban_user(target_id, str(message.from_user.id), reason)

    # Notify banned user
    try:
        await client.send_message(
            int(target_id),
            f"🚫 **{small_caps('you have been banned')}**\n\n"
            f"📋 **{small_caps('reason')}:** `{reason}`\n\n"
            "ꜰᴏʀ ᴀᴘᴘᴇᴀʟꜱ, ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴɪꜱᴛʀᴀᴛᴏʀ.",
        )
    except Exception:
        pass

    status = small_caps("updated") if already_banned else small_caps("banned")
    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"🔨 **{small_caps('user banned')}**\n\n"
            f"🆔 **{small_caps('user id')}:** `{target_id}`\n"
            f"📋 **{small_caps('reason')}:** `{reason}`\n"
            f"📌 **{small_caps('status')}:** {status}"
        ),
        reply_to_message_id=message.id,
    )


@Client.on_message(filters.command("unban") & filters.private, group=2)
async def unban_command(client: Client, message: Message):
    """Unban a user. Usage: /unban <user_id>"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/unban <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    result = await db.unban_user(target_id, str(message.from_user.id))
    if result:
        # Notify unbanned user
        try:
            await client.send_message(
                int(target_id),
                f"✅ **{small_caps('you have been unbanned')}**\n\n"
                "ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ ᴀɢᴀɪɴ.",
            )
        except Exception:
            pass
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"✅ **{small_caps('user unbanned')}**\n\n"
                f"🆔 **{small_caps('user id')}:** `{target_id}`\n"
                f"📌 {small_caps('the user can now use the bot again')}."
            ),
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('user not found in ban list')}**\n\n"
                f"🆔 `{target_id}` {small_caps('was not banned')}."
            ),
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("checkban") & filters.private, group=2)
async def checkban_command(client: Client, message: Message):
    """Check ban status of a user. Usage: /checkban <user_id>"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/checkban <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    ban_info = await db.get_ban_info(target_id)
    if ban_info:
        reason    = ban_info.get("reason", "N/A")
        banned_by = ban_info.get("banned_by", "N/A")
        banned_at = ban_info.get("banned_at")
        dt = banned_at.strftime("%Y-%m-%d %H:%M UTC") if hasattr(banned_at, "strftime") else str(banned_at)
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"🔨 **{small_caps('user is banned')}**\n\n"
                f"🆔 **{small_caps('user id')}:**   `{target_id}`\n"
                f"📋 **{small_caps('reason')}:**    `{reason}`\n"
                f"👮 **{small_caps('banned by')}:** `{banned_by}`\n"
                f"📅 **{small_caps('since')}:**     `{dt}`"
            ),
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"✅ **{small_caps('user is not banned')}**\n\n"
                f"🆔 `{target_id}` {small_caps('has no active ban')}."
            ),
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("bwcheck") & filters.private, group=2)
async def bwcheck_command(client: Client, message: Message):
    """Check per-user bandwidth. Usage: /bwcheck <user_id>"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/bwcheck <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    user_bw      = await db.get_user_bandwidth(target_id)
    max_user_bw  = Config.get("max_user_bandwidth", 10737418240)
    used         = user_bw["used"]
    cycle_start  = user_bw["cycle_start"]
    cycle_end    = user_bw["cycle_end"]
    days_rem     = user_bw.get("days_remaining", 30)
    hours_rem    = user_bw.get("hours_remaining", 0)
    pct          = (used / max_user_bw * 100) if max_user_bw else 0
    remaining    = max(0, max_user_bw - used)
    cs = cycle_start.strftime("%Y-%m-%d") if hasattr(cycle_start, "strftime") else str(cycle_start)
    ce = cycle_end.strftime("%Y-%m-%d")   if hasattr(cycle_end,   "strftime") else str(cycle_end)
    reset_str    = _fmt_cycle_info(days_rem, hours_rem)

    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"📊 **{small_caps('user bandwidth')}**\n\n"
            f"🆔 **{small_caps('user id')}:**       `{target_id}`\n"
            f"📤 **{small_caps('used')}:**           `{format_size(used)}` ({pct:.1f}%)\n"
            f"📏 **{small_caps('limit')}:**          `{format_size(max_user_bw)}` / 30d\n"
            f"📉 **{small_caps('remaining')}:**      `{format_size(remaining)}`\n"
            f"🔄 **{small_caps('resets in')}:**      `{reset_str}`\n"
            f"📅 **{small_caps('cycle')}:**          `{cs}` → `{ce}`"
        ),
        reply_to_message_id=message.id,
    )


@Client.on_message(filters.command("bwreset") & filters.private, group=2)
async def bwreset_command(client: Client, message: Message):
    """Manually reset a user's bandwidth cycle. Usage: /bwreset <user_id>"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/bwreset <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    target_id = message.command[1]
    if not target_id.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    ok = await db.reset_user_bandwidth(target_id)
    if ok:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"🔄 **{small_caps('bandwidth reset')}**\n\n"
                f"🆔 `{target_id}` — {small_caps('cycle reset to zero')}."
            ),
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('failed to reset bandwidth')}**",
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("adminstats") & filters.private, group=2)
async def adminstats_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    uptime_str = format_uptime(time.time() - Config.UPTIME)
    stats      = await db.get_stats()
    bw_stats   = await db.get_bandwidth_stats()

    max_bw      = Config.get("max_bandwidth", 107374182400)
    max_user_bw = Config.get("max_user_bandwidth", 10737418240)
    bw_used     = bw_stats["total_bandwidth"]
    bw_pct      = (bw_used / max_bw * 100) if max_bw else 0
    bw_mode     = f"🟢 {small_caps('active')}" if Config.get("bandwidth_mode", True) else f"🔴 {small_caps('inactive')}"
    ubw_mode    = f"🟢 {small_caps('active')}" if Config.get("user_bandwidth_mode", False) else f"🔴 {small_caps('inactive')}"
    banned_cnt  = await db.banned.count_documents({})
    days_rem    = bw_stats.get("days_remaining",  30)
    hours_rem   = bw_stats.get("hours_remaining", 0)
    reset_str   = _fmt_cycle_info(days_rem, hours_rem)
    ce          = bw_stats.get("cycle_end")
    ce_str      = ce.strftime("%Y-%m-%d") if hasattr(ce, "strftime") else "N/A"

    text = (
        f"📊 **{small_caps('admin statistics')}**\n\n"
        f"⏱️ **{small_caps('uptime')}:**             `{uptime_str}`\n\n"
        f"👥 **{small_caps('total users')}:**         `{stats['total_users']}`\n"
        f"📂 **{small_caps('total files')}:**         `{stats['total_files']}`\n"
        f"🚫 **{small_caps('banned users')}:**        `{banned_cnt}`\n\n"
        f"📡 **{small_caps('global bw mode')}:**      {bw_mode}\n"
        f"📶 **{small_caps('global bw limit')}:**     `{format_size(max_bw)}`\n"
        f"📤 **{small_caps('bw used total')}:**       `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
        f"📅 **{small_caps('bw used today')}:**       `{format_size(bw_stats['today_bandwidth'])}`\n"
        f"🔄 **{small_caps('bw resets in')}:**        `{reset_str}` _(on `{ce_str}`)_\n\n"
        f"👤 **{small_caps('per-user bw mode')}:**    {ubw_mode}\n"
        f"📏 **{small_caps('per-user bw limit')}:**   `{format_size(max_user_bw)}` / 30d"
    )

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
    )


@Client.on_message(filters.command("revoke") & filters.private, group=0)
async def revoke_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**\n\n`/revoke <file_hash>`",
            reply_to_message_id=message.id,
        )
        return

    file_hash = message.command[1]
    file_data = await db.get_file_by_hash(file_hash)

    if not file_data:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('file not found')}**\n\n"
                "ᴛʜᴇ ꜰɪʟᴇ ᴅᴏᴇꜱɴ'ᴛ ᴇxɪꜱᴛ ᴏʀ ʜᴀꜱ ᴀʟʀᴇᴀᴅʏ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ."
            ),
            reply_to_message_id=message.id,
        )
        return

    safe_name = escape_markdown(file_data["file_name"])
    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"⚠️ **{small_caps('confirm revoke')}**\n\n"
            f"🚫 ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ **ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ʀᴇᴠᴏᴋᴇ ᴀᴄᴄᴇꜱꜱ** ᴛᴏ ᴛʜɪꜱ ꜰɪʟᴇ?\n\n"
            f"📂 **{small_caps('file')}:** `{safe_name}`\n\n"
            "⚠️ **ᴛʜɪꜱ ᴀᴄᴛɪᴏɴ ᴄᴀɴɴᴏᴛ ʙᴇ ᴜɴᴅᴏɴᴇ.**\n"
            "ᴀʟʟ ꜱᴛʀᴇᴀᴍ ᴀɴᴅ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ ꜰᴏʀ ᴛʜɪꜱ ꜰɪʟᴇ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ ɪᴍᴍᴇᴅɪᴀᴛᴇʟʏ."
        ),
        reply_to_message_id=message.id,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ {small_caps('yes, revoke')}", callback_data=f"revoke_{file_hash}"),
                InlineKeyboardButton(f"❌ {small_caps('cancel')}",  callback_data="revoke_no_1"),
            ]
        ]),
    )


@Client.on_message(filters.command("revokeall") & filters.private, group=2)
async def revokeall_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) > 1:
        raw = message.command[1]
        if not raw.lstrip("-").isdigit():
            await client.send_message(
                chat_id=message.chat.id,
                text=f"❌ **{small_caps('invalid user id')}**\n\n`/revokeall <user_id>`",
                reply_to_message_id=message.id,
            )
            return

        target_id = raw
        files     = await db.get_user_files(target_id, limit=0)
        count     = len(files)

        if count == 0:
            await client.send_message(
                chat_id=message.chat.id,
                text=f"📂 **{small_caps('no files found')}** {small_caps('for user')} `{target_id}`.",
                reply_to_message_id=message.id,
            )
            return

        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"⚠️ **{small_caps('confirm revokeall')}**\n\n"
                f"ᴛʜɪꜱ ᴡɪʟʟ ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ᴅᴇʟᴇᴛᴇ **{count}** ꜰɪʟᴇꜱ "
                f"ʙᴇʟᴏɴɢɪɴɢ ᴛᴏ ᴜꜱᴇʀ `{target_id}`.\n"
                "ᴀʟʟ ꜱᴛʀᴇᴀᴍ/ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ.\n\n"
                "ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ?"
            ),
            reply_to_message_id=message.id,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(f"✅ {small_caps('confirm')}", callback_data=f"revokeuser_confirm_{target_id}"),
                    InlineKeyboardButton(f"❌ {small_caps('cancel')}",  callback_data="revokeall_cancel"),
                ]
            ]),
        )
        return

    stats       = await db.get_stats()
    total_files = stats["total_files"]

    if total_files == 0:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"📂 **{small_caps('no files to delete')}**.",
            reply_to_message_id=message.id,
        )
        return

    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"⚠️ **{small_caps('confirm revokeall')}**\n\n"
            f"ᴛʜɪꜱ ᴡɪʟʟ ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ᴅᴇʟᴇᴛᴇ **{total_files}** ꜰɪʟᴇꜱ ꜰʀᴏᴍ ᴛʜᴇ ᴅᴀᴛᴀʙᴀꜱᴇ.\n"
            "ᴀʟʟ ꜱᴛʀᴇᴀᴍ/ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ.\n\n"
            "ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ?"
        ),
        reply_to_message_id=message.id,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ {small_caps('confirm')}", callback_data="revokeall_confirm"),
                InlineKeyboardButton(f"❌ {small_caps('cancel')}",  callback_data="revokeall_cancel"),
            ]
        ]),
    )


@Client.on_callback_query(filters.regex(r"^revokeall_(confirm|cancel)$"), group=2)
async def revokeall_callback(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    if callback.data == "revokeall_cancel":
        await callback.answer(f"❌ {small_caps('cancelled')}.", show_alert=False)
        try:
            await callback.message.edit_text(f"❌ **{small_caps('revokeall cancelled')}.**")
        except Exception:
            pass
        return

    await callback.answer(f"🗑️ {small_caps('deleting all files')}…", show_alert=False)
    try:
        await callback.message.edit_text(f"🗑️ {small_caps('deleting all files')}…")
    except Exception:
        pass

    deleted_count = await db.delete_all_files()
    try:
        await callback.message.edit_text(
            f"🗑️ **{small_caps('all files deleted')}!**\n\n"
            f"{small_caps('deleted')} `{deleted_count}` {small_caps('files successfully')}."
        )
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^revokeuser_confirm_"), group=2)
async def revokeuser_confirm_callback(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    target_id = callback.data.replace("revokeuser_confirm_", "", 1)

    await callback.answer(f"🗑️ {small_caps('deleting')}…", show_alert=False)
    try:
        await callback.message.edit_text(
            f"🗑️ {small_caps('deleting all files for user')} `{target_id}`…"
        )
    except Exception:
        pass

    deleted_count = await db.delete_user_files(target_id)
    try:
        await callback.message.edit_text(
            f"🗑️ **{small_caps('done')}!**\n\n"
            f"{small_caps('deleted')} `{deleted_count}` {small_caps('files for user')} `{target_id}`."
        )
    except Exception:
        pass


@Client.on_message(filters.command("logs") & filters.private, group=2)
async def logs_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    log_file = "bot.log"

    if not os.path.isfile(log_file) or os.path.getsize(log_file) == 0:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('log file not found or empty')}.**",
            reply_to_message_id=message.id,
        )
        return

    try:
        await client.send_document(
            chat_id=message.chat.id,
            document=log_file,
            file_name="bot.log",
            caption=(
                f"📋 **{small_caps('bot logs')}**\n\n"
                f"📁 **{small_caps('file')}:** `bot.log`\n"
                f"📦 **{small_caps('size')}:** `{human_size(os.path.getsize(log_file))}`"
            ),
            reply_to_message_id=message.id,
        )
    except Exception as exc:
        logger.error("logs_command send document error: %s", exc)
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as fh:
                tail = fh.read()[-4000:]
            await client.send_message(
                chat_id=message.chat.id,
                text=f"📋 **{small_caps('bot logs')}** *({small_caps('last 4000 chars')})*\n\n```\n{tail}\n```",
                reply_to_message_id=message.id,
            )
        except Exception as exc2:
            logger.error("logs_command fallback error: %s", exc2)
            await client.send_message(
                chat_id=message.chat.id,
                text=f"❌ **{small_caps('error reading logs')}:** `{exc2}`",
                reply_to_message_id=message.id,
            )
