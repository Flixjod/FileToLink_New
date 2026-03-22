import asyncio
import logging
import os
import time
from datetime import datetime, timedelta

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
from helper import small_caps, format_size, escape_markdown, format_uptime, human_size, check_owner

logger = logging.getLogger(__name__)


# ─── Panel rendering ─────────────────────────────────────────────────────────

async def show_panel(client: Client, source, panel_type: str):
    config = Config.all()
    msg    = source.message if isinstance(source, CallbackQuery) else source

    if panel_type == "main_panel":
        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_toggle = Config.get("bandwidth_mode", True)
        ubw_mode  = Config.get("user_bw_mode", False)
        text = (
            f"✨ **{small_caps('bot settings panel')}** ✨\n\n"
            f"📡 **{small_caps('global bandwidth')}** : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'} | `{format_size(max_bw)}`\n"
            f"👤 **{small_caps('user bandwidth')}**   : {'🟢 ᴀᴄᴛɪᴠᴇ' if ubw_mode else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"👥 **{small_caps('sudo users')}**       : ᴍᴀɴᴀɢᴇ ᴀᴄᴄᴇꜱꜱ\n"
            f"🚫 **{small_caps('ban system')}**       : ᴍᴀɴᴀɢᴇ ʙᴀɴꜱ\n"
            f"🤖 **{small_caps('bot mode')}**         : {'🟢 ᴘᴜʙʟɪᴄ' if config.get('public_bot') else '🔴 ᴘʀɪᴠᴀᴛᴇ'}\n"
            f"📢 **{small_caps('force sub')}**        : {'🟢 ᴀᴄᴛɪᴠᴇ' if config.get('fsub_mode') else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n\n"
            "👇 ᴄʜᴏᴏꜱᴇ ᴀ ᴄᴀᴛᴇɢᴏʀʏ ᴛᴏ ᴄᴏɴꜰɪɢᴜʀᴇ."
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📡 ɢʟᴏʙᴀʟ ʙᴡ",   callback_data="settings_bandwidth"),
                InlineKeyboardButton("👤 ᴜꜱᴇʀ ʙᴡ",      callback_data="settings_user_bw"),
            ],
            [
                InlineKeyboardButton("👥 ꜱᴜᴅᴏ ᴜꜱᴇʀꜱ",   callback_data="settings_sudo"),
                InlineKeyboardButton("🚫 ʙᴀɴ ꜱʏꜱᴛᴇᴍ",   callback_data="settings_ban"),
            ],
            [
                InlineKeyboardButton("🤖 ʙᴏᴛ ᴍᴏᴅᴇ",     callback_data="settings_botmode"),
                InlineKeyboardButton("📢 ꜰᴏʀᴄᴇ ꜱᴜʙ",    callback_data="settings_fsub"),
            ],
            [InlineKeyboardButton("❌ ᴄʟᴏꜱᴇ", callback_data="settings_close")],
        ])

    elif panel_type == "bandwidth_panel":
        max_bw    = Config.get("max_bandwidth", 107374182400)
        bw_toggle = Config.get("bandwidth_mode", True)
        bw_stats  = await db.get_bandwidth_stats()
        bw_used   = bw_stats["total_bandwidth"]
        bw_today  = bw_stats["today_bandwidth"]
        bw_pct    = (bw_used / max_bw * 100) if max_bw else 0
        reset_date = Config.get("global_bw_reset_date")
        reset_str  = reset_date[:10] if reset_date else "ɴᴇᴠᴇʀ"
        text = (
            f"📡 **{small_caps('global bandwidth settings')}** 📡\n\n"
            f"⚡ **{small_caps('mode')}**        : {'🟢 ᴀᴄᴛɪᴠᴇ' if bw_toggle else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"📊 **{small_caps('limit')}**       : `{format_size(max_bw)}`\n"
            f"📤 **{small_caps('used (total)')}** : `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
            f"📅 **{small_caps('used today')}**  : `{format_size(bw_today)}`\n"
            f"🔄 **{small_caps('last reset')}**  : `{reset_str}`"
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ",           callback_data="toggle_bandwidth")],
            [
                InlineKeyboardButton("✏️ ꜱᴇᴛ ʟɪᴍɪᴛ",      callback_data="set_bandwidth_limit"),
                InlineKeyboardButton("🔄 ʀᴇꜱᴇᴛ ᴜꜱᴀɢᴇ",    callback_data="reset_bandwidth"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",             callback_data="settings_back")],
        ])

    elif panel_type == "user_bw_panel":
        ubw_mode  = Config.get("user_bw_mode", False)
        ubw_limit = Config.get("user_bw_limit", 10737418240)
        text = (
            f"👤 **{small_caps('per-user bandwidth')}** 👤\n\n"
            f"⚡ **{small_caps('mode')}**       : {'🟢 ᴀᴄᴛɪᴠᴇ' if ubw_mode else '🔴 ɪɴᴀᴄᴛɪᴠᴇ'}\n"
            f"📊 **{small_caps('limit')}**      : `{format_size(ubw_limit)}`\n\n"
            f"ℹ️ ᴀᴘᴘʟɪᴇꜱ ᴛᴏ ꜰʀᴇᴇ ᴜꜱᴇʀꜱ ᴏɴʟʏ.\n"
            f"ꜱᴜᴅᴏ ᴜꜱᴇʀꜱ ᴀɴᴅ ᴏᴡɴᴇʀ ᴀʀᴇ ᴇxᴇᴍᴘᴛ.\n"
            f"🔄 ᴀᴜᴛᴏ-ʀᴇꜱᴇᴛꜱ ᴇᴠᴇʀʏ **30 ᴅᴀʏꜱ** ꜰʀᴏᴍ ꜰɪʀꜱᴛ ᴜꜱᴀɢᴇ."
        )
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ ᴛᴏɢɢʟᴇ",            callback_data="toggle_user_bw")],
            [
                InlineKeyboardButton("✏️ ꜱᴇᴛ ʟɪᴍɪᴛ",      callback_data="set_user_bw_limit"),
                InlineKeyboardButton("👁️ ᴠɪᴇᴡ ᴜꜱᴇʀꜱ",     callback_data="view_user_bw_stats"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ",             callback_data="settings_back")],
        ])

    elif panel_type == "sudo_panel":
        sudo_users = await db.get_sudo_users()
        count = len(sudo_users)
        lines = "\n".join(f"  • `{u['user_id']}`" for u in sudo_users) if sudo_users else "  ɴᴏɴᴇ"
        text = (
            f"💠 **{small_caps('sudo users')}** 💠\n\n"
            f"👥 **{small_caps('count')}** : `{count}`\n\n"
            f"**{small_caps('list')}:**\n{lines}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("➕ ᴀᴅᴅ",    callback_data="sudo_add"),
                InlineKeyboardButton("➖ ʀᴇᴍᴏᴠᴇ", callback_data="sudo_remove"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
        ])

    elif panel_type == "ban_panel":
        banned_data = await db.get_banned_users(page=1, per_page=5)
        total_banned = banned_data["total"]
        lines = ""
        for b in banned_data["records"][:5]:
            expires = ""
            if b.get("expires_at"):
                exp = b["expires_at"]
                if isinstance(exp, str):
                    exp = datetime.fromisoformat(exp)
                expires = f" *(ᴜɴᴛɪʟ {exp.strftime('%Y-%m-%d')})*"
            lines += f"  • `{b['user_id']}`{expires} — {b.get('reason','')[:30]}\n"
        if not lines:
            lines = "  ɴᴏɴᴇ"
        text = (
            f"🚫 **{small_caps('ban system')}** 🚫\n\n"
            f"👤 **{small_caps('total banned')}** : `{total_banned}`\n\n"
            f"**{small_caps('recent bans')}:**\n{lines}"
        )
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🚫 ʙᴀɴ ᴜꜱᴇʀ",    callback_data="ban_user"),
                InlineKeyboardButton("✅ ᴜɴʙᴀɴ ᴜꜱᴇʀ",  callback_data="unban_user"),
            ],
            [
                InlineKeyboardButton("🔍 ᴄʜᴇᴄᴋ ʙᴀɴ",   callback_data="check_ban"),
                InlineKeyboardButton("📋 ʟɪꜱᴛ ʙᴀɴꜱ",   callback_data="list_bans_1"),
            ],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_back")],
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

    else:
        return

    if isinstance(source, CallbackQuery):
        try:
            await source.message.edit_text(
                text,
                reply_markup=buttons,
            )
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


# ─── Ask-input helper ────────────────────────────────────────────────────────

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


# ─── /bot_settings command ───────────────────────────────────────────────────

@Client.on_message(filters.command("bot_settings") & filters.private, group=2)
async def open_settings(client: Client, message: Message):
    if not await check_owner(client, message):
        return
    await show_panel(client, message, "main_panel")


# ─── Settings callbacks ──────────────────────────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^(settings_|toggle_|set_|sudo_|reset_|ban_|unban_|check_ban|list_bans_|view_user_bw).+"),
    group=2,
)
async def settings_callback(client: Client, callback: CallbackQuery):
    data   = callback.data
    config = Config.all()

    if not await check_owner(client, callback):
        return

    panel_nav = {
        "settings_bandwidth": ("bandwidth_panel",  f"📡 {small_caps('global bandwidth')}"),
        "settings_user_bw":   ("user_bw_panel",    f"👤 {small_caps('per-user bandwidth')}"),
        "settings_sudo":      ("sudo_panel",        f"👥 {small_caps('sudo users')}"),
        "settings_ban":       ("ban_panel",         f"🚫 {small_caps('ban system')}"),
        "settings_botmode":   ("botmode_panel",     f"🤖 {small_caps('bot mode settings')}"),
        "settings_fsub":      ("fsub_panel",        f"📌 {small_caps('force sub settings')}"),
        "settings_back":      ("main_panel",        f"⬅️ {small_caps('back to main menu')}"),
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

    # ── Global bandwidth ────────────────────────────────────────────────────

    if data == "toggle_bandwidth":
        new_val = not config.get("bandwidth_mode", True)
        await Config.update(db.db, {"bandwidth_mode": new_val})
        await callback.answer(f"✅ {small_caps('global bandwidth toggled')}!", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    if data == "set_bandwidth_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"📡 **{small_caps('send global bandwidth limit in bytes')}**\n\n"
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
        await callback.answer(f"🔄 {small_caps('resetting global bandwidth usage')}…", show_alert=False)
        ok = await db.reset_bandwidth()
        # Record the reset timestamp so rolling-window logic uses it
        reset_ts = datetime.utcnow().isoformat()
        await Config.update(db.db, {"global_bw_reset_date": reset_ts})
        if ok:
            await callback.answer(f"✅ {small_caps('bandwidth usage reset to zero')}!", show_alert=True)
        else:
            await callback.answer(f"❌ {small_caps('failed to reset bandwidth')}.", show_alert=True)
        return await show_panel(client, callback, "bandwidth_panel")

    # ── Per-user bandwidth ──────────────────────────────────────────────────

    if data == "toggle_user_bw":
        new_val = not config.get("user_bw_mode", False)
        await Config.update(db.db, {"user_bw_mode": new_val})
        await callback.answer(
            f"✅ {small_caps('per-user bandwidth')} {'ᴇɴᴀʙʟᴇᴅ' if new_val else 'ᴅɪꜱᴀʙʟᴇᴅ'}!",
            show_alert=True,
        )
        return await show_panel(client, callback, "user_bw_panel")

    if data == "set_user_bw_limit":
        text = await ask_input(
            client, callback.from_user.id,
            f"👤 **{small_caps('send per-user bandwidth limit in bytes')}**\n\n"
            f"{small_caps('examples')}:\n"
            "`10737418240`  — 10 GB\n"
            "`5368709120`   — 5 GB\n"
            "`2147483648`   — 2 GB\n\n"
            f"{small_caps('rolling 30-day window per free user')}.",
        )
        if text is None:
            return
        if not text.isdigit():
            await callback.answer(f"❌ {small_caps('invalid number')}!", show_alert=True)
            return
        new_limit = int(text) or 10737418240
        await Config.update(db.db, {"user_bw_limit": new_limit})
        await callback.answer(
            f"✅ {small_caps('user limit set to')} {format_size(new_limit)}!",
            show_alert=True,
        )
        return await show_panel(client, callback, "user_bw_panel")

    if data == "view_user_bw_stats":
        stats_data = await db.get_all_user_bandwidth_stats(page=1, per_page=10)
        limit = Config.get("user_bw_limit", 10737418240)
        lines = ""
        for rec in stats_data["records"]:
            pct = round((rec["used_bytes"] / limit * 100) if limit else 0, 1)
            exp = rec.get("window_end")
            if isinstance(exp, datetime):
                exp_str = exp.strftime("%Y-%m-%d")
            else:
                exp_str = "?"
            lines += f"  • `{rec['user_id']}` — `{format_size(rec['used_bytes'])}` ({pct}%) → _{exp_str}_\n"
        if not lines:
            lines = "  ɴᴏ ᴅᴀᴛᴀ ʏᴇᴛ"
        text = (
            f"📊 **{small_caps('per-user bandwidth stats')}** (ᴛᴏᴘ 10)\n\n"
            f"📏 **{small_caps('limit per user')}:** `{format_size(limit)}`\n"
            f"📋 **{small_caps('total tracked')}:** `{stats_data['total']}`\n\n"
            f"{lines}"
        )
        try:
            await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_user_bw")]
            ]))
        except Exception:
            pass
        await callback.answer()
        return

    # ── Bot mode ────────────────────────────────────────────────────────────

    if data == "toggle_botmode":
        new_val = not config.get("public_bot", False)
        await Config.update(db.db, {"public_bot": new_val})
        mode = small_caps("public") if new_val else small_caps("private")
        await callback.answer(f"✅ {small_caps('bot set to')} {mode}!", show_alert=True)
        return await show_panel(client, callback, "botmode_panel")

    # ── Force sub ───────────────────────────────────────────────────────────

    if data == "toggle_fsub":
        new_val = not config.get("fsub_mode", False)
        await Config.update(db.db, {"fsub_mode": new_val})
        await callback.answer(f"✅ {small_caps('force sub toggled')}!", show_alert=True)
        return await show_panel(client, callback, "fsub_panel")

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

    # ── Sudo users ──────────────────────────────────────────────────────────

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
        result = await db.remove_sudo_user(text)
        if result:
            await callback.answer(f"✅ `{text}` {small_caps('removed from sudo')}!", show_alert=True)
        else:
            await callback.answer(f"❌ `{text}` {small_caps('not found in sudo list')}.", show_alert=True)
        return await show_panel(client, callback, "sudo_panel")

    # ── Ban system ──────────────────────────────────────────────────────────

    if data == "ban_user":
        uid_text = await ask_input(
            client, callback.from_user.id,
            f"🚫 **{small_caps('enter user id to ban')}**\n\n"
            f"ꜰᴏʀᴍᴀᴛ: `123456789`",
        )
        if uid_text is None:
            return
        if not uid_text.lstrip("-").isdigit():
            await callback.answer(f"❌ {small_caps('invalid user id')}!", show_alert=True)
            return

        reason_text = await ask_input(
            client, callback.from_user.id,
            f"📝 **{small_caps('enter ban reason')}** *(ᴏᴘᴛɪᴏɴᴀʟ)*\n\n"
            f"{small_caps('send')} `-` {small_caps('to skip')}.",
        )
        reason = "" if (reason_text is None or reason_text == "-") else reason_text

        duration_text = await ask_input(
            client, callback.from_user.id,
            f"⏰ **{small_caps('temp ban duration')}** *(ᴏᴘᴛɪᴏɴᴀʟ)*\n\n"
            f"{small_caps('examples')}: `1d` `12h` `30m`\n"
            f"{small_caps('send')} `0` {small_caps('for permanent ban')}.",
        )
        expires_at = _parse_duration(duration_text)

        ok = await db.ban_user(
            uid_text,
            reason=reason,
            banned_by=str(callback.from_user.id),
            expires_at=expires_at,
        )
        if ok:
            exp_str = f" *(ᴜɴᴛɪʟ {expires_at.strftime('%Y-%m-%d %H:%M')} ᴜᴛᴄ)*" if expires_at else " *(ᴘᴇʀᴍᴀɴᴇɴᴛ)*"
            await callback.answer(
                f"✅ {small_caps('user')} `{uid_text}` {small_caps('banned')}!{exp_str}",
                show_alert=True,
            )
        else:
            await callback.answer(f"❌ {small_caps('failed to ban user')}.", show_alert=True)
        return await show_panel(client, callback, "ban_panel")

    if data == "unban_user":
        uid_text = await ask_input(
            client, callback.from_user.id,
            f"✅ **{small_caps('enter user id to unban')}**",
        )
        if uid_text is None:
            return
        ok = await db.unban_user(uid_text)
        if ok:
            await callback.answer(f"✅ `{uid_text}` {small_caps('unbanned')}!", show_alert=True)
        else:
            await callback.answer(f"❌ `{uid_text}` {small_caps('was not banned')}.", show_alert=True)
        return await show_panel(client, callback, "ban_panel")

    if data == "check_ban":
        uid_text = await ask_input(
            client, callback.from_user.id,
            f"🔍 **{small_caps('enter user id to check ban status')}**",
        )
        if uid_text is None:
            return
        is_b, ban_doc = await db.is_banned(uid_text)
        if is_b and ban_doc:
            exp = ban_doc.get("expires_at")
            exp_str = ""
            if exp:
                if isinstance(exp, str):
                    exp = datetime.fromisoformat(exp)
                exp_str = f"\n⏰ **{small_caps('expires')}:** `{exp.strftime('%Y-%m-%d %H:%M')} UTC`"
            try:
                await callback.message.edit_text(
                    f"🚫 **{small_caps('user is banned')}**\n\n"
                    f"🆔 **{small_caps('user id')}:**  `{uid_text}`\n"
                    f"📝 **{small_caps('reason')}:**    {ban_doc.get('reason','ɴ/ᴀ')}\n"
                    f"🕐 **{small_caps('banned at')}:** `{ban_doc.get('banned_at','?')}`\n"
                    f"👮 **{small_caps('by')}:**        `{ban_doc.get('banned_by','?')}`"
                    f"{exp_str}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_ban")]
                    ]),
                )
            except Exception:
                pass
        else:
            await callback.answer(f"✅ `{uid_text}` {small_caps('is not banned')}.", show_alert=True)
        return

    if data.startswith("list_bans_"):
        page = int(data.split("_")[-1]) or 1
        banned_data = await db.get_banned_users(page=page, per_page=10)
        total = banned_data["total"]
        lines = ""
        for b in banned_data["records"]:
            exp = b.get("expires_at")
            exp_str = ""
            if exp:
                if isinstance(exp, str):
                    try:
                        exp = datetime.fromisoformat(exp)
                    except Exception:
                        exp = None
                if exp:
                    exp_str = f" [ᴜɴᴛɪʟ {exp.strftime('%m/%d')}]"
            lines += f"• `{b['user_id']}`{exp_str} — _{b.get('reason','')[:40]}_\n"
        if not lines:
            lines = "ɴᴏ ʙᴀɴɴᴇᴅ ᴜꜱᴇʀꜱ"

        total_pages = max(1, (total + 9) // 10)
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("◄", callback_data=f"list_bans_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="N/A"))
        if page < total_pages:
            nav.append(InlineKeyboardButton("►", callback_data=f"list_bans_{page+1}"))

        text = (
            f"📋 **{small_caps('banned users')}** ({total} ᴛᴏᴛᴀʟ)\n\n"
            f"{lines}"
        )
        try:
            rows = []
            if nav:
                rows.append(nav)
            rows.append([InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="settings_ban")])
            await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            pass
        await callback.answer()
        return


# ─── /ban and /unban commands ────────────────────────────────────────────────

@Client.on_message(filters.command("ban") & filters.private, group=2)
async def ban_command(client: Client, message: Message):
    """
    Usage:
      /ban <user_id> [reason]
      /ban <user_id> 1d [reason]   — temp ban for 1 day
      /ban <user_id> 12h [reason]  — temp ban for 12 hours
    """
    if not await check_owner(client, message):
        return

    args = message.command[1:] if len(message.command) > 1 else []
    if not args:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/ban <user_id> [duration] [reason]`\n\n"
                f"**{small_caps('duration examples')}:** `1d` `12h` `30m`\n"
                f"**{small_caps('leave blank for permanent ban')}**"
            ),
            reply_to_message_id=message.id,
        )
        return

    user_id_str = args[0]
    if not user_id_str.lstrip("-").isdigit():
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('invalid user id')}**",
            reply_to_message_id=message.id,
        )
        return

    # Check if second arg is a duration
    duration_str = None
    reason_parts = args[1:]
    if reason_parts and _is_duration(reason_parts[0]):
        duration_str = reason_parts[0]
        reason_parts = reason_parts[1:]
    reason = " ".join(reason_parts) if reason_parts else "Banned by admin"

    expires_at = _parse_duration(duration_str)

    ok = await db.ban_user(
        user_id_str,
        reason=reason,
        banned_by=str(message.from_user.id),
        expires_at=expires_at,
    )
    if ok:
        exp_str = f"\n⏰ **{small_caps('expires')}:** `{expires_at.strftime('%Y-%m-%d %H:%M')} UTC`" if expires_at else f"\n🔒 **{small_caps('type')}:** ᴘᴇʀᴍᴀɴᴇɴᴛ"
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"✅ **{small_caps('user banned')}**\n\n"
                f"🆔 **{small_caps('user id')}:** `{user_id_str}`\n"
                f"📝 **{small_caps('reason')}:** {reason}"
                f"{exp_str}"
            ),
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('failed to ban user')}**",
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("unban") & filters.private, group=2)
async def unban_command(client: Client, message: Message):
    """Usage: /unban <user_id>"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**: `/unban <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    user_id_str = message.command[1]
    ok = await db.unban_user(user_id_str)
    if ok:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"✅ **{small_caps('user')}** `{user_id_str}` **{small_caps('has been unbanned')}**.",
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('user')}** `{user_id_str}` **{small_caps('was not banned')}**.",
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("checkban") & filters.private, group=2)
async def checkban_command(client: Client, message: Message):
    """Usage: /checkban <user_id>"""
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('usage')}**: `/checkban <user_id>`",
            reply_to_message_id=message.id,
        )
        return

    user_id_str = message.command[1]
    is_b, ban_doc = await db.is_banned(user_id_str)
    if is_b and ban_doc:
        exp = ban_doc.get("expires_at")
        exp_str = ""
        if exp:
            if isinstance(exp, str):
                exp = datetime.fromisoformat(exp)
            exp_str = f"\n⏰ **{small_caps('expires')}:** `{exp.strftime('%Y-%m-%d %H:%M')} UTC`"
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"🚫 **{small_caps('user is banned')}**\n\n"
                f"🆔 **{small_caps('user id')}:**  `{user_id_str}`\n"
                f"📝 **{small_caps('reason')}:**    {ban_doc.get('reason','N/A')}\n"
                f"👮 **{small_caps('banned by')}:** `{ban_doc.get('banned_by','?')}`\n"
                f"🕐 **{small_caps('banned at')}:** `{ban_doc.get('banned_at','?')}`"
                f"{exp_str}"
            ),
            reply_to_message_id=message.id,
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=f"✅ **{small_caps('user')}** `{user_id_str}` **{small_caps('is not banned')}**.",
            reply_to_message_id=message.id,
        )


@Client.on_message(filters.command("userbw") & filters.private, group=2)
async def userbw_command(client: Client, message: Message):
    """
    Usage:
      /userbw <user_id>          — check user's bandwidth
      /userbw reset <user_id>    — reset user's bandwidth
    """
    if not await check_owner(client, message):
        return

    args = message.command[1:]
    if not args:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/userbw <user_id>`\n"
                f"`/userbw reset <user_id>`"
            ),
            reply_to_message_id=message.id,
        )
        return

    if args[0].lower() == "reset" and len(args) > 1:
        uid = args[1]
        ok  = await db.reset_user_bandwidth(uid)
        if ok:
            await client.send_message(
                chat_id=message.chat.id,
                text=f"✅ **{small_caps('bandwidth reset for user')}** `{uid}`",
                reply_to_message_id=message.id,
            )
        else:
            await client.send_message(
                chat_id=message.chat.id,
                text=f"❌ **{small_caps('failed to reset bandwidth for')}** `{uid}`",
                reply_to_message_id=message.id,
            )
        return

    uid = args[0]
    limit = Config.get("user_bw_limit", 10737418240)
    result = await db.check_user_bandwidth_limit(uid, limit)

    exp = result.get("window_end")
    if isinstance(exp, datetime):
        exp_str = exp.strftime("%Y-%m-%d %H:%M UTC")
    else:
        exp_str = "N/A"

    await client.send_message(
        chat_id=message.chat.id,
        text=(
            f"📊 **{small_caps('user bandwidth info')}**\n\n"
            f"🆔 **{small_caps('user id')}:**    `{uid}`\n"
            f"📤 **{small_caps('used')}:**        `{format_size(result['used'])}` ({result['pct']}%)\n"
            f"📏 **{small_caps('limit')}:**       `{format_size(result['limit'])}`\n"
            f"📉 **{small_caps('remaining')}:**   `{format_size(result['remaining'])}`\n"
            f"📅 **{small_caps('window ends')}:** `{exp_str}`\n"
            f"✅ **{small_caps('status')}:**      {'🟢 ᴀʟʟᴏᴡᴇᴅ' if result['allowed'] else '🔴 ʙʟᴏᴄᴋᴇᴅ'}"
            + (f"\n⚠️ _{small_caps('warning: approaching limit')}_" if result.get('warning') else "")
        ),
        reply_to_message_id=message.id,
    )


# ─── Duration parsing helpers ─────────────────────────────────────────────────

def _is_duration(s: str) -> bool:
    """Check if a string looks like a duration (e.g. 1d, 12h, 30m)."""
    if not s:
        return False
    s = s.strip().lower()
    if s == "0":
        return True
    return len(s) > 1 and s[:-1].isdigit() and s[-1] in ("d", "h", "m")


def _parse_duration(s: str) -> datetime | None:
    """
    Parse a duration string into an absolute expiry datetime.
    Returns None for permanent (s is None, "0", or empty).
    """
    if not s or s.strip() in ("0", "-", ""):
        return None
    s = s.strip().lower()
    if not s:
        return None
    try:
        unit = s[-1]
        val  = int(s[:-1])
        if unit == "d":
            return datetime.utcnow() + timedelta(days=val)
        elif unit == "h":
            return datetime.utcnow() + timedelta(hours=val)
        elif unit == "m":
            return datetime.utcnow() + timedelta(minutes=val)
    except (ValueError, IndexError):
        pass
    return None


# ─── /adminstats ──────────────────────────────────────────────────────────────

@Client.on_message(filters.command("adminstats") & filters.private, group=2)
async def adminstats_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    uptime_str = format_uptime(time.time() - Config.UPTIME)
    stats      = await db.get_stats()
    bw_stats   = await db.get_bandwidth_stats()

    max_bw    = Config.get("max_bandwidth", 107374182400)
    bw_used   = bw_stats["total_bandwidth"]
    bw_pct    = (bw_used / max_bw * 100) if max_bw else 0
    bw_mode   = f"🟢 {small_caps('active')}" if Config.get("bandwidth_mode", True) else f"🔴 {small_caps('inactive')}"
    ubw_mode  = f"🟢 {small_caps('active')}" if Config.get("user_bw_mode", False) else f"🔴 {small_caps('inactive')}"
    ubw_limit = Config.get("user_bw_limit", 10737418240)

    text = (
        f"📊 **{small_caps('admin statistics')}**\n\n"
        f"⏱️ **{small_caps('uptime')}:**          `{uptime_str}`\n\n"
        f"👥 **{small_caps('total users')}:**      `{stats['total_users']}`\n"
        f"📂 **{small_caps('total files')}:**      `{stats['total_files']}`\n"
        f"🚫 **{small_caps('banned users')}:**     `{stats.get('total_banned',0)}`\n\n"
        f"📡 **{small_caps('global bw mode')}:**   {bw_mode}\n"
        f"📶 **{small_caps('global bw limit')}:**  `{format_size(max_bw)}`\n"
        f"📤 **{small_caps('global bw used')}:**   `{format_size(bw_used)}` ({bw_pct:.1f}%)\n"
        f"📅 **{small_caps('bw used today')}:**    `{format_size(bw_stats['today_bandwidth'])}`\n\n"
        f"👤 **{small_caps('user bw mode')}:**     {ubw_mode}\n"
        f"📏 **{small_caps('user bw limit')}:**    `{format_size(ubw_limit)}`"
    )

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
    )


# ─── /revoke, /revokeall, /logs ──────────────────────────────────────────────

@Client.on_message(filters.command("revoke") & filters.private, group=0)
async def revoke_command(client: Client, message: Message):
    if not await check_owner(client, message):
        return

    if len(message.command) < 2:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('usage')}**\n\n"
                f"`/revoke <file_hash>`"
            ),
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
                text=(
                    f"❌ **{small_caps('invalid user id')}**\n\n"
                    f"`/revokeall <user_id>`"
                ),
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
                    InlineKeyboardButton(
                        f"✅ {small_caps('confirm')}",
                        callback_data=f"revokeuser_confirm_{target_id}",
                    ),
                    InlineKeyboardButton(
                        f"❌ {small_caps('cancel')}",
                        callback_data="revokeall_cancel",
                    ),
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
            await callback.message.edit_text(
                f"❌ **{small_caps('revokeall cancelled')}.**"
            )
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
