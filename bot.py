import time
from pyrogram import Client
from pyrogram.types import BotCommand, BotCommandScopeChat
from config import Config
import logging

logger = logging.getLogger(__name__)


class Bot(Client):
    def __init__(self):
        super().__init__(
            name="FileStreamBot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            plugins=dict(root="FLiX"),
            workers=50,
            sleep_threshold=10,
        )

    async def start(self):
        await super().start()
        me = await self.get_me()
        # Persist bot identity for health/stats endpoints
        self.me                 = me
        Config.BOT_USERNAME     = me.username   or Config.DEFAULT_BOT_USERNAME
        Config.BOT_NAME         = me.first_name or Config.DEFAULT_BOT_NAME
        Config.UPTIME           = time.time()
        logger.info("⚡  ʙᴏᴛ: @%s  │  ɴᴀᴍᴇ: %s  │  ɪᴅ: %s  │  ᴡᴏʀᴋᴇʀs: %s",
                    me.username, me.first_name, me.id, "50")
        await self._resolve_log_channel()
        await self._set_commands()
        return me

    async def _resolve_log_channel(self):
        if not Config.FLOG_CHAT_ID:
            return
        try:
            chat = await self.get_chat(Config.FLOG_CHAT_ID)
            logger.info(
                "✅  ʟᴏɢ ᴄʜᴀɴɴᴇʟ ʀᴇꜱᴏʟᴠᴇᴅ  │  ɪᴅ: %s  │  ɴᴀᴍᴇ: %s",
                Config.FLOG_CHAT_ID,
                getattr(chat, "title", None) or getattr(chat, "first_name", "?"),
            )
        except Exception as exc:
            logger.warning(
                "⚠️  ᴄᴏᴜʟᴅ ɴᴏᴛ ʀᴇꜱᴏʟᴠᴇ FLOG_CHAT_ID %s — ʙᴏᴛ ᴍᴜꜱᴛ ʙᴇ ᴀ ᴍᴇᴍʙᴇʀ/ᴀᴅᴍɪɴ: %s",
                Config.FLOG_CHAT_ID,
                exc,
            )

    async def stop(self, *args):
        await super().stop()
        logger.info("🛑  ʙᴏᴛ sᴛᴏᴘᴘᴇᴅ")

    async def _set_commands(self):
        user_commands = [
            BotCommand("start",     "🚀 ꜱᴛᴀʀᴛ ᴛʜᴇ ʙᴏᴛ"),
            BotCommand("help",      "📚 ɢᴇᴛ ʜᴇʟᴘ ɪɴꜰᴏ"),
            BotCommand("about",     "ℹ️ ᴀʙᴏᴜᴛ ᴛʜɪꜱ ʙᴏᴛ"),
            BotCommand("files",     "📂 ᴠɪᴇᴡ ʏᴏᴜʀ ꜰɪʟᴇꜱ"),
            BotCommand("id",        "🪪 ɢᴇᴛ ʏᴏᴜʀ ᴜꜱᴇʀ & ᴄʜᴀᴛ ɪᴅ"),
        ]

        owner_commands = user_commands + [
            BotCommand("adminstats",   "🔐 ᴀᴅᴍɪɴ ꜱᴛᴀᴛꜱ (ᴜᴘᴛɪᴍᴇ, ʙᴡ, ᴜꜱᴇʀꜱ, ꜰɪʟᴇꜱ)"),
            BotCommand("bot_settings", "⚙️ ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ ᴘᴀɴᴇʟ"),
            BotCommand("broadcast",    "📢 ʙʀᴏᴀᴅᴄᴀꜱᴛ ᴍᴇꜱꜱᴀɢᴇ"),
            BotCommand("revoke",       "🗑️ ʀᴇᴠᴏᴋᴇ ꜰɪʟᴇ ʙʏ ʜᴀꜱʜ"),
            BotCommand("revokeall",    "🗑️ ʙᴜʟᴋ ʀᴇᴠᴏᴋᴇ [ᴀʟʟ | ᴜꜱᴇʀ_ɪᴅ]"),
            BotCommand("logs",         "📄 ɢᴇᴛ ʙᴏᴛ ʟᴏɢꜱ"),
        ]
        try:
            await self.set_bot_commands(user_commands)

            for owner_id in Config.OWNER_ID:
                try:
                    await self.set_bot_commands(
                        owner_commands,
                        scope=BotCommandScopeChat(chat_id=owner_id),
                    )
                except Exception as e:
                    logger.warning(
                        "⚠️  ᴄᴏᴜʟᴅ ɴᴏᴛ ꜱᴇᴛ ᴏᴡɴᴇʀ ᴄᴏᴍᴍᴀɴᴅꜱ ꜰᴏʀ %s: %s",
                        owner_id, e,
                    )

            logger.info("✅  ʙᴏᴛ ᴄᴏᴍᴍᴀɴᴅꜱ ʀᴇɢɪꜱᴛᴇʀᴇᴅ")
        except Exception as e:
            logger.error("❌  ꜰᴀɪʟᴇᴅ ᴛᴏ ʀᴇɢɪꜱᴛᴇʀ ᴄᴏᴍᴍᴀɴᴅꜱ: %s", e)


