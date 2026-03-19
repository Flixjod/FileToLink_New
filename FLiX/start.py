import logging
from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from config import Config
from database import db
from helper import format_size, escape_markdown, check_fsub

logger = logging.getLogger(__name__)


START_TEXT = "**👋 ʜᴇʏ, {}**\n\n**ɪ'ᴍ ᴛᴇʟᴇɢʀᴀᴍ ꜰɪʟᴇꜱ ꜱᴛʀᴇᴀᴍɪɴɢ ʙᴏᴛ ᴀꜱ ᴡᴇʟʟ ᴅɪʀᴇᴄᴛ ʟɪɴᴋꜱ ɢᴇɴᴇʀᴀᴛᴏʀ**\n\n**ᴡᴏʀᴋɪɴɢ ᴏɴ ᴄʜᴀɴɴᴇʟꜱ ᴀɴᴅ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀᴛ**\n\n**💕 @{}**"
HELP_TEXT = "**- ᴀᴅᴅ ᴍᴇ ᴀꜱ ᴀɴ ᴀᴅᴍɪɴ ᴏɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ**\n**- ꜱᴇɴᴅ ᴍᴇ ᴀɴʏ ᴅᴏᴄᴜᴍᴇɴᴛ ᴏʀ ᴍᴇᴅɪᴀ**\n**- ɪ'ʟʟ ᴘʀᴏᴠɪᴅᴇ ꜱᴛʀᴇᴀᴍᴀʙʟᴇ ʟɪɴᴋ**\n\n**🔞 ᴀᴅᴜʟᴛ ᴄᴏɴᴛᴇɴᴛ ꜱᴛʀɪᴄᴛʟʏ ᴘʀᴏʜɪʙɪᴛᴇᴅ.**\n\n**ʀᴇᴘᴏʀᴛ ʙᴜɢꜱ ᴛᴏ [ᴅᴇᴠᴇʟᴏᴘᴇʀ](https://t.me/FLiX_LY)**"
ABOUT_TEXT = "**⚜ ᴍʏ ɴᴀᴍᴇ : {}**\n\n**✦ ᴠᴇʀꜱɪᴏɴ : `2.1.0`**\n**✦ ᴜᴘᴅᴀᴛᴇᴅ ᴏɴ : `06-ᴊᴀɴᴜᴀʀʏ-2024`**\n**✦ ᴅᴇᴠᴇʟᴏᴘᴇʀ : [٭千🅻丨乂٭](https://t.me/FLiX_LY)**"



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
        btns = [
            [
                InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data="start"),
                InlineKeyboardButton("💎 ᴀʙᴏᴜᴛ", callback_data="about"),
                InlineKeyboardButton("✖️ ᴄʟᴏꜱᴇ", callback_data="close")
            ],
            [InlineKeyboardButton("👨‍💻 ᴅᴇᴠᴇʟᴏᴘᴇʀ", url="https://t.me/FLiX_LY")]
        ]
    elif page == "about":
        text = ABOUT_TEXT.format(bot_name)
        btns = [
            [
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
    bot = Config.BOT_INFO 

    # 1. Register User
    is_new = await db.register_user_on_start({
        "user_id": str(user.id),
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
    })

    if is_new and getattr(Config, "LOGS_CHAT_ID", None):
        try:
            full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
            await client.send_message(
                chat_id=Config.LOGS_CHAT_ID,
                text=f"**#ɴᴇᴡ_ᴜꜱᴇʀ**\n\n👤 **ᴜꜱᴇʀ:** {user.mention}\n🆔 **ɪᴅ:** `{user.id}`\n👤 **ᴜꜱᴇʀɴᴀᴍᴇ:** @{user.username or 'ɴ/ᴀ'}\n📛 **ɴᴀᴍᴇ:** `{full_name}`",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.error(f"Failed to log: {exc}")

    # 2. Deep Link Logic
    if len(message.command) > 1:
        arg = message.command[1]
        file_hash = arg.replace("file_", "") if arg.startswith("file_") else arg

        if getattr(Config, "FSUB_MODE", False):
            if not await check_fsub(client, message):
                return

        file_data = await db.get_file_by_hash(file_hash)
        if not file_data:
            return await client.send_message(
                chat_id=message.chat.id,
                text="**❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ**",
                reply_to_message_id=message.id
            )

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

        btn_rows = [[InlineKeyboardButton("🎬 ꜱᴛʀᴇᴀᴍ" if is_streamable else "📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=stream_link if is_streamable else download_link)]]
        if is_streamable: btn_rows[0].append(InlineKeyboardButton("📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=download_link))

        return await client.send_message(
            chat_id=message.chat.id,
            text=text,
            reply_markup=InlineKeyboardMarkup(btn_rows),
            reply_to_message_id=message.id,
            disable_web_page_preview=True
        )

    # 3. Standard Start Message
    text, buttons = show_nav("start", user.mention, bot.first_name, bot.username)
    if getattr(Config, "START_IMG", None):
        await client.send_photo(
            chat_id=message.chat.id,
            photo=Config.START_IMG,
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


@Client.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    bot = Config.BOT_INFO
    text, buttons = show_nav("help", message.from_user.mention, bot.first_name, bot.username)
    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_markup=buttons,
        reply_to_message_id=message.id
    )


@Client.on_message(filters.command("about") & filters.private)
async def about_command(client: Client, message: Message):
    bot = Config.BOT_INFO
    text, buttons = show_nav("about", message.from_user.mention, bot.first_name, bot.username)
    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_markup=buttons,
        reply_to_message_id=message.id
    )


@Client.on_message(filters.command("info") & filters.private)
async def info_command(client: Client, message: Message):
    target_user = message.from_user
    if len(message.command) > 1:
        try: target_user = await client.get_users(message.command[1])
        except Exception: return await client.send_message(message.chat.id, "**❌ ɴᴏᴛ ꜰᴏᴜɴᴅ**", reply_to_message_id=message.id)
    elif message.reply_to_message:
        target_user = message.reply_to_message.from_user

    info_text = f"**👤 ᴜꜱᴇʀ ɪᴅ:** `{target_user.id}`\n**✨ ɴᴀᴍᴇ:** `{target_user.first_name}`"
    await client.send_message(message.chat.id, info_text, reply_to_message_id=message.id)


@Client.on_callback_query(filters.regex(r"^(start|help|about|close)$"))
async def cb_handler(client: Client, query: CallbackQuery):
    if query.data == "close":
        return await query.message.delete()

    bot = Config.BOT_INFO
    text, markup = show_nav(query.data, query.from_user.mention, bot.first_name, bot.username)
    
    try:
        if query.message.photo:
            await query.message.edit_caption(caption=text, reply_markup=markup)
        else:
            await query.message.edit_text(text=text, reply_markup=markup)
    except Exception: pass
    await query.answer()