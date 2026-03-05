import logging
import math

from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InlineQueryResultPhoto,
    InputTextMessageContent,
    Message,
)

from config import Config
from helper import Cryptic, format_size, escape_markdown, small_caps, check_fsub, check_owner
from database import db

logger = logging.getLogger(__name__)

STREAMABLE_TYPES = ("video", "audio")
PAGE_SIZE = 10


async def check_access(user_id: int) -> bool:
    if Config.get("public_bot", False):
        return True
    if user_id in Config.OWNER_ID:
        return True
    return await db.is_sudo_user(str(user_id))


@Client.on_message(
    (filters.document | filters.video | filters.audio | filters.photo) & filters.private,
    group=0,
)
async def file_handler(client: Client, message: Message):
    user    = message.from_user
    user_id = user.id

    if Config.get("fsub_mode", False):
        if not await check_fsub(client, message):
            return

    if not await check_access(user_id):
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('access forbidden')}**\n\n📡 ᴛʜɪꜱ ɪꜱ ᴀ ᴘʀɪᴠᴀᴛᴇ ʙᴏᴛ.",
            reply_to_message_id=message.id,
            disable_web_page_preview=True,
        )
        return

    stats         = await db.get_bandwidth_stats()
    max_bandwidth = Config.get("max_bandwidth", 107374182400)
    if Config.get("bandwidth_mode", True) and stats["total_bandwidth"] >= max_bandwidth:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('bandwidth limit reached')}!**\n\n"
                "ᴘʟᴇᴀꜱᴇ ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴɪꜱᴛʀᴀᴛᴏʀ."
            ),
            reply_to_message_id=message.id,
            disable_web_page_preview=True,
        )
        return

    if message.document:
        file       = message.document
        file_name  = file.file_name or "Document"
        file_size  = file.file_size
        file_type  = file.mime_type.split("/")[0] if file.mime_type else "document"
        tg_file_id = file.file_id
    elif message.video:
        file       = message.video
        file_name  = file.file_name or "Video File"
        file_size  = file.file_size
        file_type  = "video"
        tg_file_id = file.file_id
    elif message.audio:
        file       = message.audio
        file_name  = file.file_name or "Audio File"
        file_size  = file.file_size
        file_type  = "audio"
        tg_file_id = file.file_id
    elif message.photo:
        file       = message.photo
        file_name  = f"{file.file_unique_id}.jpg"
        file_size  = file.file_size
        file_type  = "image"
        tg_file_id = file.file_id
    else:
        await client.send_message(
            chat_id=message.chat.id,
            text="❌ ᴜɴꜱᴜᴘᴘᴏʀᴛᴇᴅ ꜰɪʟᴇ ᴛʏᴘᴇ",
            reply_to_message_id=message.id,
            disable_web_page_preview=True,
        )
        return

    max_file_size = Config.get("max_file_size", 4294967296)
    if file_size > max_file_size:
        await client.send_message(
            chat_id=message.chat.id,
            text=(
                f"❌ **{small_caps('file too large')}**\n\n"
                f"📊 **{small_caps('file size')}:** `{format_size(file_size)}`\n"
                f"⚠️ **{small_caps('max allowed')}:** `{format_size(max_file_size)}`"
            ),
            reply_to_message_id=message.id,
            disable_web_page_preview=True,
        )
        return

    processing_msg = await client.send_message(
        chat_id=message.chat.id,
        text="⏳ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ʏᴏᴜʀ ꜰɪʟᴇ…",
        reply_to_message_id=message.id,
        disable_web_page_preview=True,
    )

    try:
        file_info = await client.send_cached_media(
            chat_id=Config.FLOG_CHAT_ID,
            file_id=tg_file_id,
        )
    except Exception as exc:
        logger.error("send_cached_media failed: user=%s err=%s", user_id, exc)
        await processing_msg.edit_text(
            f"❌ **{small_caps('failed to process file')}**\n\n"
            "ᴄᴏᴜʟᴅ ɴᴏᴛ ꜰᴏʀᴡᴀʀᴅ ꜰɪʟᴇ ᴛᴏ ꜱᴛᴏʀᴀɢᴇ.\n"
            f"`{exc}`",
        )
        return

    media = (
        getattr(file_info, "document", None)
        or getattr(file_info, "video",    None)
        or getattr(file_info, "audio",    None)
        or getattr(file_info, "photo",    None)
    )
    if not media:
        logger.error("send_cached_media returned no media: user=%s msg=%s", user_id, file_info.id)
        try:
            await client.delete_messages(Config.FLOG_CHAT_ID, file_info.id)
        except Exception:
            pass
        await processing_msg.edit_text(
            f"❌ **{small_caps('file processing failed')}**\n\n"
            "ꜰɪʟᴇ ᴄᴏᴜʟᴅ ɴᴏᴛ ʙᴇ ʀᴇᴀᴅ ꜰʀᴏᴍ ᴛᴇʟᴇɢʀᴀᴍ ᴀꜰᴛᴇʀ ꜰᴏʀᴡᴀʀᴅɪɴɢ.\n"
            "ᴛʜɪꜱ ᴜꜱᴜᴀʟʟʏ ʜᴀᴘᴘᴇɴꜱ ᴡɪᴛʜ ᴠᴇʀʏ ʟᴀʀɢᴇ ꜰɪʟᴇꜱ. ᴘʟᴇᴀꜱᴇ ᴛʀʏ ᴀɢᴀɪɴ.",
        )
        return

    file_hash = Cryptic.hash_file_id(str(file_info.id))

    await client.send_message(
        chat_id=Config.FLOG_CHAT_ID,
        text=(
            f"**RᴇQᴜᴇꜱᴛᴇᴅ ʙʏ** : [{user.first_name}](tg://user?id={user.id})\n"
            f"**Uꜱᴇʀ ɪᴅ** : `{user_id}`\n"
            f"**Fɪʟᴇ ɪᴅ** : `{file_hash}`"
        ),
        reply_to_message_id=file_info.id,
    )

    base_url      = Config.URL or f"http://localhost:{Config.PORT}"
    stream_link   = f"{base_url}/stream/{file_hash}"
    download_link = f"{base_url}/dl/{file_hash}"

    await db.add_file({
        "file_id":          file_hash,
        "message_id":       str(file_info.id),
        "telegram_file_id": tg_file_id,
        "user_id":          str(user_id),
        "username":         user.username or "",
        "file_name":        file_name,
        "file_size":        file_size,
        "file_type":        file_type,
        "mime_type":        getattr(file, "mime_type", ""),
    })

    is_streamable = file_type in STREAMABLE_TYPES
    buttons       = []

    if is_streamable:
        buttons.append([
            InlineKeyboardButton(f"🎬 {small_caps('stream')}",   url=stream_link),
            InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
        ])
    else:
        buttons.append([
            InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
        ])

    buttons.extend([
        [
            InlineKeyboardButton(f"📩 {small_caps('get file')}", callback_data=f"getfile_{file_hash}"),
            InlineKeyboardButton(f"🔁 {small_caps('share')}", switch_inline_query=f"file_{file_hash}"),
        ],
    ])

    safe_name = escape_markdown(file_name)
    fmt_size  = format_size(file_size)

    text = (
        f"✅ **{small_caps('file successfully processed')}!**\n\n"
        f"📂 **{small_caps('file name')}:** `{safe_name}`\n"
        f"💾 **{small_caps('file size')}:** `{fmt_size}`\n"
        f"📊 **{small_caps('file type')}:** `{file_type}`\n"
    )
    if is_streamable:
        text += (
            f"🎬 **{small_caps('streaming')}:** `Available`\n\n"
            f"🔗 **{small_caps('stream link')}:**\n`{stream_link}`"
        )
    else:
        text += f"\n🔗 **{small_caps('download link')}:**\n`{download_link}`"

    await processing_msg.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


@Client.on_message(filters.command("files") & filters.private, group=0)
async def files_command(client: Client, message: Message):
    user_id = message.from_user.id

    if len(message.command) > 1:
        if not await check_owner(client, message):
            return

        raw = message.command[1]
        if not raw.lstrip("-").isdigit():
            await client.send_message(
                chat_id=message.chat.id,
                text=(
                    f"❌ **{small_caps('invalid user id')}**\n\n"
                    f"ᴜꜱᴀɢᴇ: `/files <user_id>`"
                ),
                reply_to_message_id=message.id,
            )
            return

        target_id = raw
        markup, caption = await _build_user_files_markup(
            client, target_id, page=1, owner_view=True
        )

        if Config.Files_IMG:
            try:
                await client.send_photo(
                    chat_id=message.chat.id,
                    photo=Config.Files_IMG,
                    caption=caption,
                    reply_to_message_id=message.id,
                    reply_markup=markup,
                )
                return
            except Exception as exc:
                logger.warning("failed to send Files_IMG: %s", exc)

        await client.send_message(
            chat_id=message.chat.id,
            text=caption,
            reply_to_message_id=message.id,
            reply_markup=markup,
        )
        return

    if not await check_access(user_id):
        await client.send_message(
            chat_id=message.chat.id,
            text=f"❌ **{small_caps('access forbidden')}**",
            reply_to_message_id=message.id,
        )
        return

    markup, caption = await _build_user_files_markup(
        client, str(user_id), page=1, owner_view=False
    )

    if Config.Files_IMG:
        try:
            await client.send_photo(
                chat_id=message.chat.id,
                photo=Config.Files_IMG,
                caption=caption,
                reply_to_message_id=message.id,
                reply_markup=markup,
            )
            return
        except Exception as exc:
            logger.warning("failed to send Files_IMG: %s", exc)

    await client.send_message(
        chat_id=message.chat.id,
        text=caption,
        reply_to_message_id=message.id,
        reply_markup=markup,
    )


async def _build_user_files_markup(
    client,
    user_id: str,
    page: int,
    owner_view: bool,
) -> tuple:
    skip           = (page - 1) * PAGE_SIZE
    user_files_cur, total_files = await db.find_files(user_id, [skip + 1, PAGE_SIZE])

    file_list = []
    async for x in user_files_cur:
        name = x.get("file_name", "Unknown")
        if len(name) > 30:
            name = name[:27] + "…"
        cb = (
            f"ownview_{x['message_id']}_{user_id}"
            if owner_view
            else f"myfile_{x['_id']}_{page}"
        )
        file_list.append([InlineKeyboardButton(f"📄 {name}", callback_data=cb)])

    total_pages = math.ceil(total_files / PAGE_SIZE) if total_files else 1

    if total_files > PAGE_SIZE:
        nav = []
        if page > 1:
            prev_cb = (
                f"ownfiles_{user_id}_{page - 1}"
                if owner_view
                else f"userfiles_{page - 1}"
            )
            nav.append(InlineKeyboardButton("◄", callback_data=prev_cb))
        else:
            nav.append(InlineKeyboardButton("◄", callback_data="N/A"))

        nav.append(InlineKeyboardButton(
            f"{page}/{total_pages}", callback_data="N/A"
        ))

        if page < total_pages:
            next_cb = (
                f"ownfiles_{user_id}_{page + 1}"
                if owner_view
                else f"userfiles_{page + 1}"
            )
            nav.append(InlineKeyboardButton("►", callback_data=next_cb))
        else:
            nav.append(InlineKeyboardButton("►", callback_data="N/A"))

        file_list.append(nav)

    if not file_list or (len(file_list) == 1 and file_list[0][0].callback_data == "N/A"):
        file_list = [[InlineKeyboardButton("ᴇᴍᴘᴛʏ", callback_data="N/A")]]

    file_list.append([InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close")])

    markup = InlineKeyboardMarkup(file_list)

    if owner_view:
        caption = (
            f"📂 **{small_caps('files for user')}** `{user_id}`\n"
            f"📊 **{small_caps('total')}:** `{total_files}` "
            f"| **{small_caps('page')}:** `{page}/{total_pages}`\n\n"
            "ᴄʟɪᴄᴋ ᴀ ꜰɪʟᴇ ᴛᴏ ᴠɪᴇᴡ ᴏʀ ʀᴇᴠᴏᴋᴇ ɪᴛ:"
        ) if total_files else (
            f"📂 **{small_caps('files for user')}** `{user_id}`\n\n"
            "ᴛʜɪꜱ ᴜꜱᴇʀ ʜᴀꜱ ɴᴏ ꜰɪʟᴇꜱ ʏᴇᴛ."
        )
    else:
        caption = (
            f"📂 **{small_caps('your files')}**\n"
            f"📊 **{small_caps('total')}:** `{total_files}` "
            f"| **{small_caps('page')}:** `{page}/{total_pages}`\n\n"
            "ᴄʟɪᴄᴋ ᴏɴ ᴀɴʏ ꜰɪʟᴇ ᴛᴏ ᴠɪᴇᴡ ᴅᴇᴛᴀɪʟꜱ:"
        ) if total_files else (
            f"📂 **{small_caps('your files')}**\n\n"
            "ʏᴏᴜ ᴅᴏɴ'ᴛ ʜᴀᴠᴇ ᴀɴʏ ꜰɪʟᴇꜱ ʏᴇᴛ. "
            "ꜱᴇɴᴅ ᴍᴇ ᴀ ꜰɪʟᴇ ᴛᴏ ɢᴇᴛ ꜱᴛᴀʀᴛᴇᴅ!"
        )

    return markup, caption


@Client.on_callback_query(filters.regex(r"^userfiles_\d+$"), group=0)
async def cb_user_files_page(client: Client, callback: CallbackQuery):
    page    = int(callback.data.replace("userfiles_", ""))
    user_id = str(callback.from_user.id)

    markup, caption = await _build_user_files_markup(
        client, user_id, page=page, owner_view=False
    )
    try:
        await callback.message.edit_text(caption, reply_markup=markup)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^ownfiles_"), group=0)
async def cb_owner_files_page(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    # Format: ownfiles_<user_id>_<page>
    parts     = callback.data.split("_", 2)
    target_id = parts[1]
    page      = int(parts[2]) if len(parts) > 2 else 1

    markup, caption = await _build_user_files_markup(
        client, target_id, page=page, owner_view=True
    )
    try:
        await callback.message.edit_text(caption, reply_markup=markup)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^myfile_"), group=0)
async def cb_user_file_detail(client: Client, callback: CallbackQuery):
    # Format: myfile_<_id_hex>_<page>
    parts     = callback.data.split("_", 2)
    oid_str   = parts[1]
    back_page = int(parts[2]) if len(parts) > 2 else 1

    from bson import ObjectId
    try:
        file_data = await db.files.find_one({"_id": ObjectId(oid_str)})
    except Exception:
        file_data = None

    if not file_data:
        await callback.answer("❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ", show_alert=True)
        return

    file_hash     = file_data["file_id"]
    file_type     = file_data.get("file_type", "document")
    is_streamable = file_type in STREAMABLE_TYPES
    base_url      = Config.URL or f"http://localhost:{Config.PORT}"
    stream_link   = f"{base_url}/stream/{file_hash}"
    download_link = f"{base_url}/dl/{file_hash}"

    safe_name      = escape_markdown(file_data["file_name"])
    formatted_size = format_size(file_data["file_size"])

    link_buttons = []
    if is_streamable:
        link_buttons.append([
            InlineKeyboardButton(f"🎬 {small_caps('stream')}",   url=stream_link),
            InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
        ])
    else:
        link_buttons.append([
            InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
        ])

    buttons = link_buttons + [
        [
            InlineKeyboardButton(f"📩 {small_caps('get file')}", callback_data=f"getfile_{file_hash}"),
            InlineKeyboardButton(f"🔁 {small_caps('share')}",    switch_inline_query=f"file_{file_hash}"),
        ],
        [InlineKeyboardButton(f"🗑️ {small_caps('revoke')}",  callback_data=f"revoke_{file_hash}_{back_page}")],
        [InlineKeyboardButton(f"⬅️ {small_caps('back')}",    callback_data=f"userfiles_{back_page}")],
    ]

    text = (
        f"✅ **{small_caps('file details')}**\n\n"
        f"📂 **{small_caps('name')}:** `{safe_name}`\n"
        f"💾 **{small_caps('size')}:** `{formatted_size}`\n"
        f"📊 **{small_caps('type')}:** `{file_data['file_type']}`\n"
        f"📅 **{small_caps('uploaded')}:** `{file_data['created_at'].strftime('%Y-%m-%d')}`"
    )
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^close$"), group=0)
async def cb_close(client: Client, callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^ownview_"), group=0)
async def cb_owner_file_detail(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    # Format: ownview_<message_id>_<target_user_id>
    parts      = callback.data.split("_", 2)
    message_id = parts[1]
    target_id  = parts[2] if len(parts) > 2 else ""

    file_data = await db.get_file(message_id)
    if not file_data:
        await callback.answer("❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ", show_alert=True)
        return

    file_hash     = file_data["file_id"]
    file_type     = file_data.get("file_type", "document")
    is_streamable = file_type in STREAMABLE_TYPES
    base_url      = Config.URL or f"http://localhost:{Config.PORT}"
    stream_link   = f"{base_url}/stream/{file_hash}"
    download_link = f"{base_url}/dl/{file_hash}"
    telegram_link = f"https://t.me/{(await client.get_me()).username}?start=file_{file_hash}"

    safe_name      = escape_markdown(file_data["file_name"])
    formatted_size = format_size(file_data["file_size"])

    link_buttons = []
    if is_streamable:
        link_buttons.append([
            InlineKeyboardButton(f"🎬 {small_caps('stream')}",   url=stream_link),
            InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
        ])
    else:
        link_buttons.append([
            InlineKeyboardButton(f"📥 {small_caps('download')}", url=download_link),
        ])

    buttons = link_buttons + [
        [
            InlineKeyboardButton(f"💬 {small_caps('telegram')}", url=telegram_link),
        ],
        [InlineKeyboardButton(
            f"🗑️ {small_caps('revoke this file')}",
            callback_data=f"ownrevoke_{file_hash}_{target_id}",
        )],
        [InlineKeyboardButton(
            f"⬅️ {small_caps('back')}",
            callback_data=f"ownback_{target_id}",
        )],
    ]

    text = (
        f"✅ **{small_caps('file details')}** *(owner view)*\n\n"
        f"📂 **{small_caps('name')}:** `{safe_name}`\n"
        f"💾 **{small_caps('size')}:** `{formatted_size}`\n"
        f"📊 **{small_caps('type')}:** `{file_data['file_type']}`\n"
        f"👤 **{small_caps('owner')}:** `{file_data.get('user_id', 'N/A')}`\n"
        f"📅 **{small_caps('uploaded')}:** `{file_data['created_at'].strftime('%Y-%m-%d')}`"
    )
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^ownrevoke_(?!yes_|no_)"), group=0)
async def cb_owner_revoke_confirm(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    # Format: ownrevoke_<file_hash>_<target_user_id>
    raw       = callback.data[len("ownrevoke_"):]
    parts     = raw.split("_", 1)
    file_hash = parts[0]
    target_id = parts[1] if len(parts) > 1 else ""

    file_data = await db.get_file_by_hash(file_hash)
    if not file_data:
        await callback.answer("❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ ᴏʀ ᴀʟʀᴇᴀᴅʏ ᴅᴇʟᴇᴛᴇᴅ", show_alert=True)
        return

    safe_name = escape_markdown(file_data["file_name"])
    await callback.message.edit_text(
        f"⚠️ **{small_caps('confirm revoke')}**\n\n"
        f"ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ʀᴇᴠᴏᴋᴇ:\n\n"
        f"📂 **{small_caps('file')}:** `{safe_name}`\n\n"
        "ᴀʟʟ ʟɪɴᴋꜱ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ {small_caps('yes')}", callback_data=f"ownrevoke_yes_{file_hash}_{target_id}"),
                InlineKeyboardButton(f"❌ {small_caps('no')}",  callback_data=f"ownrevoke_no_{target_id}"),
            ]
        ]),
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^ownrevoke_yes_"), group=0)
async def cb_owner_revoke_yes(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    # Format: ownrevoke_yes_<file_hash>_<target_user_id>
    raw       = callback.data[len("ownrevoke_yes_"):]
    parts     = raw.split("_", 1)
    file_hash = parts[0]
    target_id = parts[1] if len(parts) > 1 else ""

    file_data = await db.get_file_by_hash(file_hash)
    if not file_data:
        await callback.answer("❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ ᴏʀ ᴀʟʀᴇᴀᴅʏ ᴅᴇʟᴇᴛᴇᴅ", show_alert=True)
        return

    try:
        await client.delete_messages(Config.FLOG_CHAT_ID, int(file_data["message_id"]))
    except Exception as exc:
        logger.error("owner revoke dump delete: msg=%s err=%s", file_data["message_id"], exc)

    await db.delete_file(file_data["message_id"])

    safe_name = escape_markdown(file_data["file_name"])
    await callback.message.edit_text(
        f"🗑️ **{small_caps('file revoked successfully')}!**\n\n"
        f"📂 **{small_caps('file')}:** `{safe_name}`\n\n"
        "ᴀʟʟ ʟɪɴᴋꜱ ʜᴀᴠᴇ ʙᴇᴇɴ ɪɴᴠᴀʟɪᴅᴀᴛᴇᴅ.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"⬅️ {small_caps('back to user files')}",
                callback_data=f"ownfiles_{target_id}_1",
            )],
        ]),
    )
    await callback.answer("✅ ꜰɪʟᴇ ʀᴇᴠᴏᴋᴇᴅ!", show_alert=False)


@Client.on_callback_query(filters.regex(r"^ownrevoke_no_"), group=0)
async def cb_owner_revoke_no(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    target_id = callback.data[len("ownrevoke_no_"):]
    markup, caption = await _build_user_files_markup(
        client, target_id, page=1, owner_view=True
    )
    try:
        await callback.message.edit_text(caption, reply_markup=markup)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^ownback_"), group=0)
async def cb_owner_back(client: Client, callback: CallbackQuery):
    if not await check_owner(client, callback):
        return

    target_id = callback.data[len("ownback_"):]
    markup, caption = await _build_user_files_markup(
        client, target_id, page=1, owner_view=True
    )
    try:
        await callback.message.edit_text(caption, reply_markup=markup)
    except Exception:
        pass
    await callback.answer()


# NOTE: The /revoke command (admin.py) also routes to this handler via the
#       shared callback_data prefix "revoke_<file_hash>".  Both the inline
#       Revoke button and the /revoke command emit the same pattern so
#       confirmation and execution are handled in one place.
@Client.on_callback_query(filters.regex(r"^revoke_(?!yes_|no_)"), group=0)
async def cb_revoke_confirm(client: Client, callback: CallbackQuery):
    # Format (from file detail): revoke_<file_hash>_<back_page>
    # Format (from /revoke cmd): revoke_<file_hash>   (no back_page)
    raw       = callback.data[len("revoke_"):]
    parts     = raw.split("_", 1)
    file_hash = parts[0]
    back_page = parts[1] if len(parts) > 1 else "1"

    file_data = await db.get_file_by_hash(file_hash)
    if not file_data:
        await callback.answer("❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ ᴏʀ ᴀʟʀᴇᴀᴅʏ ᴅᴇʟᴇᴛᴇᴅ", show_alert=True)
        return

    safe_name = escape_markdown(file_data["file_name"])
    await callback.message.edit_text(
        f"⚠️ **{small_caps('confirm revoke')}**\n\n"
        f"ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴘᴇʀᴍᴀɴᴇɴᴛʟʏ ʀᴇᴠᴏᴋᴇ:\n\n"
        f"📂 **{small_caps('file')}:** `{safe_name}`\n\n"
        "ᴀʟʟ ʟɪɴᴋꜱ ᴡɪʟʟ ʙᴇᴄᴏᴍᴇ ɪɴᴠᴀʟɪᴅ.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"✅ {small_caps('yes')}", callback_data=f"revoke_yes_{file_hash}_{back_page}"),
                InlineKeyboardButton(f"❌ {small_caps('no')}",  callback_data=f"revoke_no_{back_page}"),
            ]
        ]),
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^revoke_yes_"), group=0)
async def cb_revoke_yes(client: Client, callback: CallbackQuery):
    # Format: revoke_yes_<file_hash>_<back_page>
    raw       = callback.data[len("revoke_yes_"):]
    parts     = raw.split("_", 1)
    file_hash = parts[0]
    back_page = parts[1] if len(parts) > 1 else "1"

    file_data = await db.get_file_by_hash(file_hash)
    if not file_data:
        await callback.answer("❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ ᴏʀ ᴀʟʀᴇᴀᴅʏ ᴅᴇʟᴇᴛᴇᴅ", show_alert=True)
        try:
            await callback.message.delete()
        except Exception:
            pass
        return

    await db.delete_file(file_data["message_id"])

    safe_name = escape_markdown(file_data["file_name"])
    await callback.message.edit_text(
        f"🗑️ **{small_caps('file revoked successfully')}!**\n\n"
        f"📂 **{small_caps('file')}:** `{safe_name}`\n\n"
        "ᴀʟʟ ʟɪɴᴋꜱ ʜᴀᴠᴇ ʙᴇᴇɴ ɪɴᴠᴀʟɪᴅᴀᴛᴇᴅ.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⬅️ {small_caps('back to files')}", callback_data=f"userfiles_{back_page}")],
        ]),
    )
    await callback.answer("✅ ꜰɪʟᴇ ʀᴇᴠᴏᴋᴇᴅ!", show_alert=False)


@Client.on_callback_query(filters.regex(r"^revoke_no_"), group=0)
async def cb_revoke_no(client: Client, callback: CallbackQuery):
    back_page_str = callback.data[len("revoke_no_"):]
    try:
        back_page = int(back_page_str)
    except ValueError:
        back_page = 1

    user_id = str(callback.from_user.id)
    markup, caption = await _build_user_files_markup(
        client, user_id, page=back_page, owner_view=False
    )
    try:
        await callback.message.edit_text(caption, reply_markup=markup)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^getfile_"), group=0)
async def cb_get_file(client: Client, callback: CallbackQuery):
    file_hash = callback.data[len("getfile_"):]
    user_id   = callback.from_user.id

    file_data = await db.get_file_by_hash(file_hash)
    if not file_data:
        await callback.answer("❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ ᴏʀ ʜᴀꜱ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ", show_alert=True)
        return

    await callback.answer("📩 ꜱᴇɴᴅɪɴɢ ꜰɪʟᴇ…", show_alert=False)

    try:
        await client.copy_message(
            chat_id=user_id,
            from_chat_id=Config.FLOG_CHAT_ID,
            message_id=int(file_data["message_id"]),
        )
    except Exception as exc:
        logger.error("getfile copy_message failed: user=%s hash=%s err=%s", user_id, file_hash, exc)
        try:
            await client.send_message(
                chat_id=user_id,
                text=f"❌ **{small_caps('could not send file')}**\n\n`{exc}`",
            )
        except Exception:
            pass


@Client.on_inline_query(group=0)
async def inline_query_handler(client: Client, inline_query):
    query = (inline_query.query or "").strip()
    if not query:
        await inline_query.answer(
            results=[],
            cache_time=1,
            switch_pm_text="📂 ꜱᴇɴᴅ ᴀ ꜰɪʟᴇ ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ᴀ ʟɪɴᴋ",
            switch_pm_parameter="start",
        )
        return

    # Strip the "file_" prefix that the Share button injects
    file_hash_query = query[5:] if query.startswith("file_") else query

    file_data = await db.get_file_by_hash(file_hash_query)
    if not file_data:
        await inline_query.answer(
            results=[],
            cache_time=5,
            switch_pm_text="❌ ꜰɪʟᴇ ɴᴏᴛ ꜰᴏᴜɴᴅ — ᴛʀʏ ᴀɢᴀɪɴ",
            switch_pm_parameter="start",
        )
        return

    base_url      = Config.URL or f"http://localhost:{Config.PORT}"
    file_hash     = file_data["file_id"]
    stream_link   = f"{base_url}/stream/{file_hash}"
    download_link = f"{base_url}/dl/{file_hash}"
    bot_username  = (await client.get_me()).username
    telegram_link = f"https://t.me/{bot_username}?start=file_{file_hash}"
    file_type     = file_data.get("file_type", "document")
    is_streamable = file_type in STREAMABLE_TYPES
    safe_name     = escape_markdown(file_data["file_name"])
    fmt_size      = format_size(file_data["file_size"])
    tg_file_id    = file_data.get("telegram_file_id", "")

    TYPE_META = {
        "video":    {"icon": "🎬", "label": "ᴠɪᴅᴇᴏ",    "action": "ꜱᴛʀᴇᴀᴍ / ᴅᴏᴡɴʟᴏᴀᴅ"},
        "audio":    {"icon": "🎵", "label": "ᴀᴜᴅɪᴏ",    "action": "ꜱᴛʀᴇᴀᴍ / ᴅᴏᴡɴʟᴏᴀᴅ"},
        "image":    {"icon": "🖼️", "label": "ɪᴍᴀɢᴇ",    "action": "ᴠɪᴇᴡ / ᴅᴏᴡɴʟᴏᴀᴅ"},
        "document": {"icon": "📄", "label": "ᴅᴏᴄᴜᴍᴇɴᴛ", "action": "ᴅᴏᴡɴʟᴏᴀᴅ"},
    }
    meta = TYPE_META.get(file_type, {"icon": "📁", "label": "ꜰɪʟᴇ", "action": "ᴅᴏᴡɴʟᴏᴀᴅ"})
    type_icon  = meta["icon"]
    type_label = meta["label"]

    divider = "─" * 20
    text = (
        f"{type_icon} **{safe_name}**\n"
        f"{divider}\n"
        f"📦 `{fmt_size}`  ·  🏷️ {type_label}\n"
    )
    if is_streamable:
        text += (
            f"\n🔗 **{small_caps('stream')}**\n`{stream_link}`\n"
            f"\n⬇️ **{small_caps('download')}**\n`{download_link}`"
        )
    else:
        text += (
            f"\n⬇️ **{small_caps('download')}**\n`{download_link}`"
        )
    text += f"\n\n🤖 _ᴠɪᴀ @{bot_username}_"

    btn_rows = []
    if is_streamable:
        btn_rows.append([
            InlineKeyboardButton(f"▶️ {small_caps('stream')}",   url=stream_link),
            InlineKeyboardButton(f"⬇️ {small_caps('download')}", url=download_link),
        ])
    else:
        btn_rows.append([
            InlineKeyboardButton(f"⬇️ {small_caps('download')}", url=download_link),
        ])
    btn_rows.append([
        InlineKeyboardButton(f"🤖 {small_caps('get via bot')}", url=telegram_link),
    ])
    markup = InlineKeyboardMarkup(btn_rows)

    # Thumbnail icons for Telegram inline query results.
    # Served from our own /icons/ static route (local PNG files) for:
    #   • Zero external CDN latency
    #   • No GitHub raw rate-limits
    #   • Instant load for inline results
    # Falls back to the folder icon for unknown types.
    _icon_base = (Config.URL or f"http://localhost:{Config.PORT}").rstrip("/")
    THUMBS = {
        "video":    f"{_icon_base}/icons/media.png",
        "audio":    f"{_icon_base}/icons/audio.png",
        "image":    f"{_icon_base}/icons/photo.png",
        "document": f"{_icon_base}/icons/document.png",
    }
    DEFAULT_THUMB = f"{_icon_base}/icons/folder.png"
    thumb_url = THUMBS.get(file_type, DEFAULT_THUMB)

    display_name = file_data["file_name"]
    if len(display_name) > 48:
        display_name = display_name[:45] + "…"
    result_title = f"{type_icon}  {display_name}"
    result_desc  = f"💾 {fmt_size}   ·   {type_label.upper()}   ·   ᴛᴀᴘ ᴛᴏ ꜱʜᴀʀᴇ ɪɴꜱᴛᴀɴᴛʟʏ"

    result_item = None

    if file_type == "image" and tg_file_id:
        try:
            result_item = InlineQueryResultPhoto(
                photo_url=stream_link,
                thumb_url=stream_link,
                title=result_title,
                description=result_desc,
                caption=text,
                disable_web_page_preview=True,
                reply_markup=markup,
            )
        except Exception as exc:
            logger.debug("InlineQueryResultPhoto build failed: %s", exc)

    if result_item is None:
        result_item = InlineQueryResultArticle(
            title=result_title,
            description=result_desc,
            input_message_content=InputTextMessageContent(
                message_text=text,
                disable_web_page_preview=True,
            ),
            reply_markup=markup,
            thumb_url=thumb_url,
            thumb_width=320,
            thumb_height=320,
        )

    await inline_query.answer(results=[result_item], cache_time=30)
