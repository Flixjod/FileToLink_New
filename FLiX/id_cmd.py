"""
/id command — returns User ID, Chat ID, and a clickable mention.
Works in private chats, groups, and when replying to another user.
"""
import logging

from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from helper import small_caps

logger = logging.getLogger(__name__)


def _mention_html(user) -> str:
    """Return a tg://user?id= mention string."""
    name = (
        f"{user.first_name or ''} {user.last_name or ''}".strip()
        or user.username
        or str(user.id)
    )
    return f"[{name}](tg://user?id={user.id})"


def _build_id_text(user, chat) -> str:
    """Compose the /id response text."""
    mention = _mention_html(user)
    lines = [
        f"🪪 **{small_caps('id lookup')}**\n",
        f"👤 **{small_caps('user id')}:** `{user.id}`",
        f"🔗 **{small_caps('mention')}:** {mention}",
    ]
    # Also show username if available
    if user.username:
        lines.append(f"📎 **{small_caps('username')}:** @{user.username}")
    # Chat ID (only meaningful in groups / channels)
    if chat and chat.id != user.id:
        lines.append(f"\n💬 **{small_caps('chat id')}:** `{chat.id}`")
        if getattr(chat, "title", None):
            lines.append(f"📛 **{small_caps('chat name')}:** {chat.title}")
    return "\n".join(lines)


@Client.on_message(filters.command("id"), group=1)
async def id_command(client: Client, message: Message):
    """
    /id  ← about yourself
    /id  (reply to someone) ← about that user
    """
    reply = message.reply_to_message

    if reply and reply.from_user:
        # Info about the replied-to user
        target_user = reply.from_user
        target_chat = message.chat
        text = _build_id_text(target_user, target_chat)
        # Extra row: info about the requester
        requester = message.from_user
        text += (
            f"\n\n━━━━━━━━━━━━━━━━━\n"
            f"🔍 **{small_caps('requested by')}:** {_mention_html(requester)}\n"
            f"👤 **{small_caps('your id')}:** `{requester.id}`"
        )
    else:
        # Info about the sender themselves
        target_user = message.from_user
        target_chat = message.chat
        text = _build_id_text(target_user, target_chat)

    # Inline buttons: deep-link to the user & copy-friendly note
    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"🔗 {small_caps('open profile')}",
                url=f"tg://user?id={target_user.id}",
            ),
        ],
    ])

    await client.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_to_message_id=message.id,
        reply_markup=buttons,
        disable_web_page_preview=True,
    )
