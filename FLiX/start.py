import logging
from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from config import Config
from database import db
from helper import format_size, escape_markdown, check_fsub

logger = logging.getLogger(__name__)


START_TEXT = "**👋 ʜᴇʏ, {}**\n\n**ɪ'ᴍ ᴛᴇʟᴇɢʀᴀᴍ ꜰɪʟᴇꜱ ꜱᴛʀᴇᴀᴍɪɴɢ ʙᴏᴛ ᴀꜱ ᴡᴇʟʟ ᴅɪʀᴇᴄᴛ ʟɪɴᴋꜱ ɢᴇɴᴇʀᴀᴛᴏʀ**\n\n**ᴡᴏʀᴋɪɴɢ ᴏɴ ᴄʜᴀɴɴᴇʟꜱ ᴀɴᴅ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀᴛ**\n\n**💕 @{}**"

HELP_TEXT = "**- ᴀᴅᴅ ᴍᴇ ᴀꜱ ᴀɴ ᴀᴅᴍɪɴ ᴏɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ**\n**- ꜱᴇɴᴅ ᴍᴇ ᴀɴʏ ᴅᴏᴄᴜᴍᴇɴᴛ ᴏʀ ᴍᴇᴅɪᴀ**\n**- ɪ'ʟʟ ᴘʀᴏᴠɪᴅᴇ ꜱᴛʀᴇᴀᴍᴀʙʟᴇ ʟɪɴᴋ**\n\n**🔞 ᴀᴅᴜʟᴛ ᴄᴏɴᴛᴇɴᴛ ꜱᴛʀɪᴄᴛʟʏ ᴘʀᴏʜɪʙɪᴛᴇᴅ.**\n\n**ʀᴇᴘᴏʀᴛ ʙᴜɢꜱ ᴛᴏ [ᴅᴇᴠᴇʟᴏᴘᴇʀ](https://t.me/FLiX_LY)**"

ABOUT_TEXT = "**⚜ ᴍʏ ɴᴀᴍᴇ : {}**\n\n**✦ ᴠᴇʀꜱɪᴏɴ : `2.1.0`**\n**✦ ᴜᴘᴅᴀᴛᴇᴅ ᴏɴ : `06-ᴊᴀɴᴜᴀʀʏ-2024`**\n**✦ ᴅᴇᴠᴇʟᴏᴘᴇʀ : [ꜰʟɪx ᴏᴘ](https://t.me/FLiX_LY)**"



def show_nav(page: str, user_mention: str, bot_name: str, bot_username: str):
    if page == "start":
        text = START_TEXT.format(user_mention, bot_username)
        btns = [
            [
                InlineKeyboardButton("📖 ʜᴇʟᴘ", callback_data="help"),
                InlineKeyboardButton("💎 ᴀʙᴏᴜᴛ", callback_data="about"),
                InlineKeyboardButton("✖️ ᴄʟᴏꜱᴇ", callback_data="close")
            ],
            [InlineKeyboardButton("👨‍💻 ᴅᴇᴠᴇʟᴏᴘᴇʀ", url="https://t.me/FLiX_LY")]
        ]
    elif page == "help":
        text = HELP_TEXT
        btns = [[
            InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="start"),
            InlineKeyboardButton("💎 ᴀʙᴏᴜᴛ", callback_data="about"),
            InlineKeyboardButton("✖️ ᴄʟᴏꜱᴇ", callback_data="close")
        ],
            [InlineKeyboardButton("👨‍💻 ᴅᴇᴠᴇʟᴏᴘᴇʀ", url="https://t.me/FLiX_LY")]
        ]
    elif page == "about":
        text = ABOUT_TEXT.format(bot_name)
        btns = [[
            InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="start"),
            InlineKeyboardButton("📖 ʜᴇʟᴘ", callback_data="help"),
            InlineKeyboardButton("✖️ ᴄʟᴏꜱᴇ", callback_data="close")
        ],
            [InlineKeyboardButton("👨‍💻 ᴅᴇᴠᴇʟᴏᴘᴇʀ", url="https://t.me/FLiX_LY")]
        ]
    return text, InlineKeyboardMarkup(btns)




@Client.on_message(filters.command("start") & filters.private, group=1)
async def start_command(client: Client, message: Message):
    user = message.from_user
    user_id = user.id
    _me = await client.get_me()

    # 1. Register User & Log to Admin Chat
    is_new = await db.register_user_on_start({
        "user_id": str(user_id),
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
    })

    if is_new and Config.LOGS_CHAT_ID:
        try:
            full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
            await client.send_message(
                chat_id=Config.LOGS_CHAT_ID,
                text=(
                    "**#ɴᴇᴡ_ᴜꜱᴇʀ**\n\n"
                    f"👤 **ᴜꜱᴇʀ:** {user.mention}\n"
                    f"🆔 **ɪᴅ:** `{user_id}`\n"
                    f"👤 **ᴜꜱᴇʀɴᴀᴍᴇ:** @{user.username or 'ɴ/ᴀ'}\n"
                    f"📛 **ɴᴀᴍᴇ:** `{full_name}`"
                ),
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.error("failed to log new user: %s", exc)

    # 2. Deep Link Logic
    if len(message.command) > 1:
        arg = message.command[1]
        file_hash = arg[5:] if arg.startswith("file_") else arg

        if Config.get("fsub_mode", False):
            if not await check_fsub(client, message):
                return

        try:
            file_data = await db.get_file_by_hash(file_hash)
            if not file_data:
                await client.send_message(
                    chat_id=message.chat.id,
                    text="**❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ**\n\n**ᴛʜᴇ ꜰɪʟᴇ ʟɪɴᴋ ɪꜱ ɪɴᴠᴀʟɪᴅ ᴏʀ ʜᴀꜱ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ.**",
                    reply_to_message_id=message.id,
                )
                return

            base_url = Config.URL or f"http://localhost:{Config.PORT}"
            stream_link = f"{base_url}/stream/{file_hash}"
            download_link = f"{base_url}/dl/{file_hash}"

            file_type = file_data.get("file_type", "document")
            is_streamable = file_type in ("video", "audio")
            
            text = (
                "**✅ ꜰɪʟᴇ ꜰᴏᴜɴᴅ!**\n\n"
                f"**📂 ɴᴀᴍᴇ:** `{escape_markdown(file_data['file_name'])}`\n"
                f"**💾 ꜱɪᴢᴇ:** `{format_size(file_data['file_size'])}`\n"
                f"**📊 ᴛʏᴘᴇ:** `{file_type}`\n\n"
            )

            btn_rows = []
            if is_streamable:
                text += f"**🎬 ꜱᴛʀᴇᴀᴍ ʟɪɴᴋ:**\n`{stream_link}`"
                btn_rows.append([
                    InlineKeyboardButton("🎬 ꜱᴛʀᴇᴀᴍ", url=stream_link),
                    InlineKeyboardButton("📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=download_link),
                ])
            else:
                text += f"**🔗 ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋ:**\n`{download_link}`"
                btn_rows.append([InlineKeyboardButton("📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=download_link)])

            await client.send_message(
                chat_id=message.chat.id,
                text=text,
                reply_markup=InlineKeyboardMarkup(btn_rows),
                reply_to_message_id=message.id,
                disable_web_page_preview=True
            )
            return

        except Exception as exc:
            logger.error("deep-link error: %s", exc)
            await client.send_message(
                chat_id=message.chat.id,
                text="**❌ ᴇʀʀᴏʀ: ɪɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ ʟɪɴᴋ**",
                reply_to_message_id=message.id,
            )
            return

    # 3. Standard Start Message
    text, buttons = show_nav("start", user.mention, _me.first_name, _me.username)
    if Config.Start_IMG:
        await client.send_photo(
            chat_id=message.chat.id,
            photo=Config.Start_IMG,
            caption=text,
            reply_markup=buttons,
            reply_to_message_id=message.id
        )
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text=text,
            reply_markup=buttons,
            reply_to_message_id=message.id
        )

@Client.on_message(filters.command("help") & filters.private, group=1)
async def help_command(client: Client, message: Message):
    _me = await client.get_me()
    text, buttons = show_nav("help", message.from_user.mention, _me.first_name, _me.username)
    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_markup=buttons,
        reply_to_message_id=message.id
    )

@Client.on_message(filters.command("about") & filters.private, group=1)
async def about_command(client: Client, message: Message):
    _me = await client.get_me()
    text, buttons = show_nav("about", message.from_user.mention, _me.first_name, _me.username)
    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_markup=buttons,
        reply_to_message_id=message.id
    )


@Client.on_message(filters.command("info") & filters.private, group=1)
async def info_command(client: Client, message: Message):
    if len(message.command) > 1:
        user_input = message.command[1]
        try:
            target_user = await client.get_users(user_input)
        except (PeerIdInvalid, UsernameInvalid, Exception):
            return await client.send_message(message.chat.id, "**❌ ɪɴᴠᴀʟɪᴅ ᴜꜱᴇʀ ɪᴅ ᴏʀ ᴜꜱᴇʀɴᴀᴍᴇ**", reply_to_message_id=message.id)
    elif message.reply_to_message:
        target_user = message.reply_to_message.from_user
    else:
        target_user = message.from_user

    info_text = (
        "**👤 ᴜꜱᴇʀ ɪɴꜰᴏʀᴍᴀᴛɪᴏɴ**\n\n"
        f"**🆔 ɪᴅ:** `{target_user.id}`\n"
        f"**✨ ꜰɪʀꜱᴛ ɴᴀᴍᴇ:** `{target_user.first_name}`\n"
        f"**👤 ʟᴀꜱᴛ ɴᴀᴍᴇ:** `{target_user.last_name or 'ɴ/ᴀ'}`\n"
        f"**🔗 ᴜꜱᴇʀɴᴀᴍᴇ:** @{target_user.username or 'ɴ/ᴀ'}\n"
        f"**🛰 ᴜꜱᴇʀ ʟɪɴᴋ:** {target_user.mention}"
    )
    await client.send_message(message.chat.id, info_text, reply_to_message_id=message.id)




@Client.on_callback_query(filters.regex(r"^(start|help|about|close)$"))
async def cb_handler(client: Client, query: CallbackQuery):
    if query.data == "close":
        await query.message.delete()
        return await query.answer("ᴄʟᴏꜱᴇᴅ")

    _me = await client.get_me()
    text, markup = show_nav(query.data, query.from_user.mention, _me.first_name, _me.username)
    
    try:
        if query.message.photo:
            await query.message.edit_caption(caption=text, reply_markup=markup)
        else:
            await query.message.edit_text(text=text, reply_markup=markup)
    except Exception:
        pass
    await query.answer()