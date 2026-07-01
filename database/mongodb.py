from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, mongo_uri: str, database_name: str):
        self.client = AsyncIOMotorClient(
            mongo_uri,
            appname="FileToLink_Bot",
            maxPoolSize=10,
            minPoolSize=1,
            maxIdleTimeMS=30000,
            waitQueueTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
        )
        self.db            = self.client[database_name]
        self.files         = self.db.files
        self.users         = self.db.users
        self.bandwidth     = self.db.bandwidth
        self.sudo_users    = self.db.sudo_users
        self.config        = self.db.config
        self.banned_users  = self.db.banned_users
        self.ban_history   = self.db.ban_history
        self.sudo_history  = self.db.sudo_history
        self.user_bw       = self.db.user_bandwidth   # per-user monthly bandwidth
        self.global_bw     = self.db.global_bandwidth  # global monthly bandwidth cycle

    async def init_db(self):
        try:
            async def _existing(col):
                info = await col.index_information()
                return {v['key'][0][0] for v in info.values() if v.get('key')}

            files_idx = await _existing(self.files)
            if 'file_id'    not in files_idx:
                await self.files.create_index('file_id',    unique=True)
            if 'message_id' not in files_idx:
                await self.files.create_index('message_id', unique=True)
            if 'user_id'    not in files_idx:
                await self.files.create_index('user_id')
            if 'created_at' not in files_idx:
                await self.files.create_index('created_at')

            users_idx = await _existing(self.users)
            if 'user_id'       not in users_idx:
                await self.users.create_index('user_id',      unique=True)
            if 'last_activity' not in users_idx:
                await self.users.create_index('last_activity')

            bw_idx = await _existing(self.bandwidth)
            if 'date' not in bw_idx:
                await self.bandwidth.create_index('date')

            sudo_idx = await _existing(self.sudo_users)
            if 'user_id' not in sudo_idx:
                await self.sudo_users.create_index('user_id', unique=True)

            # ── banned_users ───────────────────────────────────────
            ban_idx = await _existing(self.banned_users)
            if 'user_id' not in ban_idx:
                await self.banned_users.create_index('user_id', unique=True)

            # ── ban_history ────────────────────────────────────────
            bh_idx = await _existing(self.ban_history)
            if 'user_id' not in bh_idx:
                await self.ban_history.create_index('user_id')
            if 'timestamp' not in bh_idx:
                await self.ban_history.create_index('timestamp')

            # ── sudo_history ───────────────────────────────────────
            sh_idx = await _existing(self.sudo_history)
            if 'user_id' not in sh_idx:
                await self.sudo_history.create_index('user_id')
            if 'timestamp' not in sh_idx:
                await self.sudo_history.create_index('timestamp')

            # ── user_bandwidth (per-user monthly) ─────────────────
            ubw_idx = await _existing(self.user_bw)
            if 'user_id' not in ubw_idx:
                await self.user_bw.create_index('user_id', unique=True)

            # ── global_bandwidth (monthly cycle) ──────────────────
            gbw_idx = await _existing(self.global_bw)
            if 'cycle_start' not in gbw_idx:
                await self.global_bw.create_index('cycle_start')

            logger.info("✅ ᴅʙ ɪɴᴅᴇxᴇꜱ ʀᴇᴀᴅˏ ᴀʟʟ ɪɴꜱᴛᴀɴᴛ — ꜱᴄɪᴘᴘᴇᴅ ɴᴇᴡ ᴄʀᴇᴀᴛɪᴏɴ ᴏɴʟˏ")
            return True
        except Exception as e:
            logger.error("❌ ᴅʙ ɪɴɪᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    # ══════════════════════════════════════════════════════════════
    # FILE OPERATIONS
    # ══════════════════════════════════════════════════════════════

    async def add_file(self, file_data: Dict) -> bool:
        try:
            doc = {
                "file_id":          file_data["file_id"],
                "message_id":       file_data["message_id"],
                "telegram_file_id": file_data.get("telegram_file_id", ""),
                "user_id":          file_data["user_id"],
                "username":         file_data.get("username", ""),
                "file_name":        file_data["file_name"],
                "file_size":        file_data["file_size"],
                "file_type":        file_data["file_type"],
                "mime_type":        file_data.get("mime_type", ""),
                "created_at":       datetime.utcnow(),
                "bandwidth_used":   0,
            }
            await self.files.insert_one(doc)
            return True
        except Exception as e:
            logger.error("add file error: %s", e)
            return False

    async def get_file(self, message_id: str) -> Optional[Dict]:
        try:
            return await self.files.find_one({"message_id": message_id})
        except Exception as e:
            logger.error("get file error: %s", e)
            return None

    async def get_file_by_hash(self, file_hash: str) -> Optional[Dict]:
        try:
            return await self.files.find_one({"file_id": file_hash})
        except Exception as e:
            logger.error("get file by hash error: %s", e)
            return None

    async def delete_file(self, message_id: str) -> bool:
        try:
            result = await self.files.delete_one({"message_id": message_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error("delete file error: %s", e)
            return False

    async def delete_all_files(self) -> int:
        try:
            result = await self.files.delete_many({})
            return result.deleted_count
        except Exception as e:
            logger.error("delete all files error: %s", e)
            return 0

    async def get_user_files(self, user_id: str, limit: int = 50) -> List[Dict]:
        try:
            cursor = self.files.find({"user_id": user_id}).sort("created_at", -1)
            if limit and limit > 0:
                cursor = cursor.limit(limit)
                return await cursor.to_list(length=limit)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error("get user files error: %s", e)
            return []

    async def find_files(self, user_id, page_range: list) -> tuple:
        try:
            skip  = page_range[0] - 1 if page_range[0] > 0 else 0
            limit = page_range[1]
            total = await self.files.count_documents({"user_id": str(user_id)})
            cursor = (
                self.files.find({"user_id": str(user_id)})
                .sort("created_at", -1)
                .skip(skip)
                .limit(limit)
            )
            return cursor, total
        except Exception as e:
            logger.error("find_files error: %s", e)
            return self.files.find({"user_id": str(user_id)}).limit(0), 0

    async def delete_user_files(self, user_id: str) -> int:
        try:
            result = await self.files.delete_many({"user_id": str(user_id)})
            return result.deleted_count
        except Exception as e:
            logger.error("delete user files error: %s", e)
            return 0

    # ══════════════════════════════════════════════════════════════
    # LEGACY BANDWIDTH (daily tracking — kept for backward compat)
    # ══════════════════════════════════════════════════════════════

    async def update_bandwidth(self, size: int) -> bool:
        try:
            today = datetime.utcnow().date().isoformat()
            await self.bandwidth.update_one(
                {"date": today},
                {
                    "$inc": {"total_bytes": size},
                    "$set": {"last_updated": datetime.utcnow()},
                },
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("update bandwidth error: %s", e)
            return False

    async def track_bandwidth(self, message_id: str, size: int) -> bool:
        try:
            await self.files.update_one(
                {"message_id": message_id},
                {"$inc": {"bandwidth_used": size}},
            )
            await self.update_bandwidth(size)
            return True
        except Exception as e:
            logger.error("track bandwidth error: %s", e)
            return False

    async def reset_bandwidth(self) -> bool:
        try:
            await self.bandwidth.delete_many({})
            await self.files.update_many({}, {"$set": {"bandwidth_used": 0}})
            # Also reset the global monthly cycle and all per-user cycles
            await self.global_bw.delete_many({})
            await self.user_bw.delete_many({})
            return True
        except Exception as e:
            logger.error("reset bandwidth error: %s", e)
            return False

    async def get_total_bandwidth(self) -> int:
        try:
            pipeline = [{"$group": {"_id": None, "total": {"$sum": "$total_bytes"}}}]
            result   = await self.bandwidth.aggregate(pipeline).to_list(length=1)
            return result[0]["total"] if result else 0
        except Exception as e:
            logger.error("get total bandwidth error: %s", e)
            return 0

    async def get_bandwidth_stats(self) -> Dict:
        try:
            total       = await self.get_total_bandwidth()
            today       = datetime.utcnow().date().isoformat()
            today_stats = await self.bandwidth.find_one({"date": today})
            return {
                "total_bandwidth": total,
                "today_bandwidth": today_stats.get("total_bytes", 0) if today_stats else 0,
            }
        except Exception as e:
            logger.error("get bandwidth stats error: %s", e)
            return {"total_bandwidth": 0, "today_bandwidth": 0}

    async def get_stats(self) -> Dict:
        try:
            total_files = await self.files.count_documents({})
            total_users = await self.users.count_documents({})
            bw          = await self.get_bandwidth_stats()
            return {
                "total_files":     total_files,
                "total_users":     total_users,
                "total_bandwidth": bw["total_bandwidth"],
                "today_bandwidth": bw["today_bandwidth"],
            }
        except Exception as e:
            logger.error("get stats error: %s", e)
            return {
                "total_files": 0, "total_users": 0,
                "total_bandwidth": 0, "today_bandwidth": 0,
            }

    # ══════════════════════════════════════════════════════════════
    # GLOBAL MONTHLY BANDWIDTH (30-day auto-reset cycle)
    # ══════════════════════════════════════════════════════════════

    async def _ensure_global_cycle(self) -> Dict:
        """
        Ensure an active global bandwidth cycle exists.
        A cycle lasts 30 days.  If there is no active cycle, or the current
        one has expired, a fresh cycle is created automatically.
        Returns the active cycle document.
        """
        now = datetime.utcnow()
        cycle = await self.global_bw.find_one({"active": True})
        if cycle:
            cycle_start = cycle["cycle_start"]
            if (now - cycle_start).days < 30:
                return cycle
            # Expired — deactivate old cycle
            await self.global_bw.update_one(
                {"_id": cycle["_id"]},
                {"$set": {"active": False, "ended_at": now}},
            )

        # Create new cycle
        new_cycle = {
            "cycle_start":  now,
            "cycle_end":    now + timedelta(days=30),
            "total_bytes":  0,
            "active":       True,
            "created_at":   now,
        }
        await self.global_bw.insert_one(new_cycle)
        fresh = await self.global_bw.find_one({"active": True})
        return fresh

    async def get_global_bw_cycle(self) -> Dict:
        """Return current cycle stats: used, limit, days_remaining, pct."""
        try:
            from config import Config
            cycle   = await self._ensure_global_cycle()
            max_bw  = Config.get("max_bandwidth", 107374182400)
            now     = datetime.utcnow()
            used    = cycle.get("total_bytes", 0)
            end     = cycle["cycle_end"]
            days_r  = max(0, (end - now).days)
            pct     = round((used / max_bw * 100) if max_bw else 0, 1)
            return {
                "used":           used,
                "limit":          max_bw,
                "days_remaining": days_r,
                "cycle_start":    cycle["cycle_start"],
                "cycle_end":      end,
                "pct":            pct,
                "remaining":      max(0, max_bw - used),
            }
        except Exception as e:
            logger.error("get_global_bw_cycle error: %s", e)
            return {"used": 0, "limit": 0, "days_remaining": 30, "pct": 0, "remaining": 0}

    async def record_global_bw(self, size: int) -> bool:
        """Atomically add `size` bytes to the current global monthly cycle."""
        try:
            cycle = await self._ensure_global_cycle()
            await self.global_bw.update_one(
                {"_id": cycle["_id"]},
                {"$inc": {"total_bytes": size}, "$set": {"last_updated": datetime.utcnow()}},
            )
            return True
        except Exception as e:
            logger.error("record_global_bw error: %s", e)
            return False

    # ══════════════════════════════════════════════════════════════
    # PER-USER MONTHLY BANDWIDTH
    # ══════════════════════════════════════════════════════════════

    async def _ensure_user_bw_cycle(self, user_id: str) -> Dict:
        """
        Ensure the user has an active monthly bandwidth record.
        Each user's 30-day window starts the first time they consume bandwidth.
        """
        now  = datetime.utcnow()
        doc  = await self.user_bw.find_one({"user_id": user_id})

        if doc:
            cycle_start = doc["cycle_start"]
            if (now - cycle_start).days < 30:
                return doc
            # Cycle expired — reset in-place (preserve history via cycle_start update)
            await self.user_bw.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "cycle_start": now,
                        "cycle_end":   now + timedelta(days=30),
                        "used_bytes":  0,
                        "last_reset":  now,
                    }
                },
            )
            return await self.user_bw.find_one({"user_id": user_id})

        # First-ever record for this user
        new_doc = {
            "user_id":     user_id,
            "cycle_start": now,
            "cycle_end":   now + timedelta(days=30),
            "used_bytes":  0,
            "last_reset":  now,
            "created_at":  now,
        }
        await self.user_bw.insert_one(new_doc)
        return await self.user_bw.find_one({"user_id": user_id})

    async def get_user_bw(self, user_id: str) -> Dict:
        """Return per-user cycle stats."""
        try:
            from config import Config
            doc       = await self._ensure_user_bw_cycle(user_id)
            max_ubw   = Config.get("max_user_bandwidth", 10737418240)  # default 10 GB
            now       = datetime.utcnow()
            used      = doc.get("used_bytes", 0)
            end       = doc["cycle_end"]
            days_r    = max(0, (end - now).days)
            pct       = round((used / max_ubw * 100) if max_ubw else 0, 1)
            return {
                "used":           used,
                "limit":          max_ubw,
                "days_remaining": days_r,
                "cycle_start":    doc["cycle_start"],
                "cycle_end":      end,
                "pct":            pct,
                "remaining":      max(0, max_ubw - used),
            }
        except Exception as e:
            logger.error("get_user_bw error: %s", e)
            return {"used": 0, "limit": 0, "days_remaining": 30, "pct": 0, "remaining": 0}

    async def record_user_bw(self, user_id: str, size: int) -> bool:
        """Atomically add `size` bytes to the user's monthly cycle."""
        try:
            doc = await self._ensure_user_bw_cycle(user_id)
            await self.user_bw.update_one(
                {"user_id": user_id},
                {"$inc": {"used_bytes": size}, "$set": {"last_updated": datetime.utcnow()}},
            )
            return True
        except Exception as e:
            logger.error("record_user_bw error: %s", e)
            return False

    async def check_user_bw_limit(self, user_id: str) -> tuple[bool, Dict]:
        """
        Check whether user has exceeded their per-user bandwidth.
        Returns (allowed: bool, stats: dict).
        """
        try:
            stats     = await self.get_user_bw(user_id)
            max_ubw   = stats["limit"]
            if max_ubw <= 0:
                return True, stats
            allowed   = stats["used"] < max_ubw
            return allowed, stats
        except Exception as e:
            logger.error("check_user_bw_limit error: %s", e)
            return True, {}

    async def reset_user_bw(self, user_id: str) -> bool:
        """Manually reset a single user's bandwidth cycle."""
        try:
            now = datetime.utcnow()
            await self.user_bw.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "cycle_start": now,
                        "cycle_end":   now + timedelta(days=30),
                        "used_bytes":  0,
                        "last_reset":  now,
                    }
                },
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("reset_user_bw error: %s", e)
            return False

    # ══════════════════════════════════════════════════════════════
    # USER MANAGEMENT
    # ══════════════════════════════════════════════════════════════

    async def register_user_on_start(self, user_data: Dict) -> bool:
        try:
            existing = await self.users.find_one({"user_id": user_data["user_id"]})
            if existing:
                await self.users.update_one(
                    {"user_id": user_data["user_id"]},
                    {"$set": {"last_activity": datetime.utcnow()}},
                )
                return False  # not new

            await self.users.insert_one({
                "user_id":       user_data["user_id"],
                "username":      user_data.get("username", ""),
                "first_name":    user_data.get("first_name", ""),
                "last_name":     user_data.get("last_name", ""),
                "first_used":    datetime.utcnow(),
                "last_activity": datetime.utcnow(),
            })
            logger.info("👤 ɴᴇᴡ ᴜꜱᴇʀ ʀᴇɢɪꜱᴛᴇʀᴇᴅ: %s", user_data["user_id"])
            return True  # new user
        except Exception as e:
            logger.error("❌ ʀᴇɢɪꜱᴛᴇʀ_ᴜꜱᴇʀ_ᴏɴ_ꜱᴛᴀʀᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    async def get_user(self, user_id: str) -> Optional[Dict]:
        try:
            return await self.users.find_one({"user_id": user_id})
        except Exception as e:
            logger.error("get user error: %s", e)
            return None

    async def get_user_count(self) -> int:
        try:
            return await self.users.count_documents({})
        except Exception as e:
            logger.error("get user count error: %s", e)
            return 0

    # ══════════════════════════════════════════════════════════════
    # SUDO USER MANAGEMENT
    # ══════════════════════════════════════════════════════════════

    async def add_sudo_user(self, user_id: str, added_by: str,
                            username: str = "", first_name: str = "") -> bool:
        try:
            now = datetime.utcnow()
            await self.sudo_users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id":    user_id,
                        "username":   username,
                        "first_name": first_name,
                        "added_by":   added_by,
                        "added_at":   now,
                    }
                },
                upsert=True,
            )
            # Record history
            await self.sudo_history.insert_one({
                "action":     "add",
                "user_id":    user_id,
                "username":   username,
                "first_name": first_name,
                "actor_id":   added_by,
                "timestamp":  now,
            })
            return True
        except Exception as e:
            logger.error("add sudo user error: %s", e)
            return False

    async def remove_sudo_user(self, user_id: str, removed_by: str = "") -> bool:
        try:
            doc = await self.sudo_users.find_one({"user_id": user_id})
            if not doc:
                return False
            now = datetime.utcnow()
            result = await self.sudo_users.delete_one({"user_id": user_id})
            if result.deleted_count > 0:
                await self.sudo_history.insert_one({
                    "action":     "remove",
                    "user_id":    user_id,
                    "username":   doc.get("username", ""),
                    "first_name": doc.get("first_name", ""),
                    "actor_id":   removed_by,
                    "timestamp":  now,
                })
                return True
            return False
        except Exception as e:
            logger.error("remove sudo user error: %s", e)
            return False

    async def is_sudo_user(self, user_id: str) -> bool:
        try:
            result = await self.sudo_users.find_one({"user_id": user_id})
            return result is not None
        except Exception as e:
            logger.error("is sudo user error: %s", e)
            return False

    async def get_sudo_users(self) -> List[Dict]:
        try:
            cursor = self.sudo_users.find({}).sort("added_at", -1)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error("get sudo users error: %s", e)
            return []

    async def get_sudo_history(self, limit: int = 20) -> List[Dict]:
        """Return recent sudo add/remove actions."""
        try:
            cursor = self.sudo_history.find({}).sort("timestamp", -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error("get_sudo_history error: %s", e)
            return []

    # ══════════════════════════════════════════════════════════════
    # BAN SYSTEM
    # ══════════════════════════════════════════════════════════════

    async def ban_user(self, user_id: str, banned_by: str, reason: str,
                       username: str = "", first_name: str = "") -> bool:
        try:
            now = datetime.utcnow()
            await self.banned_users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "user_id":    user_id,
                        "username":   username,
                        "first_name": first_name,
                        "banned_by":  banned_by,
                        "reason":     reason,
                        "banned_at":  now,
                    }
                },
                upsert=True,
            )
            await self.ban_history.insert_one({
                "action":     "ban",
                "user_id":    user_id,
                "username":   username,
                "first_name": first_name,
                "actor_id":   banned_by,
                "reason":     reason,
                "timestamp":  now,
            })
            return True
        except Exception as e:
            logger.error("ban_user error: %s", e)
            return False

    async def unban_user(self, user_id: str, unbanned_by: str) -> bool:
        try:
            doc = await self.banned_users.find_one({"user_id": user_id})
            if not doc:
                return False
            now = datetime.utcnow()
            result = await self.banned_users.delete_one({"user_id": user_id})
            if result.deleted_count > 0:
                await self.ban_history.insert_one({
                    "action":     "unban",
                    "user_id":    user_id,
                    "username":   doc.get("username", ""),
                    "first_name": doc.get("first_name", ""),
                    "actor_id":   unbanned_by,
                    "reason":     "",
                    "timestamp":  now,
                })
                return True
            return False
        except Exception as e:
            logger.error("unban_user error: %s", e)
            return False

    async def is_banned(self, user_id: str) -> bool:
        try:
            result = await self.banned_users.find_one({"user_id": user_id})
            return result is not None
        except Exception as e:
            logger.error("is_banned error: %s", e)
            return False

    async def get_ban_info(self, user_id: str) -> Optional[Dict]:
        try:
            return await self.banned_users.find_one({"user_id": user_id})
        except Exception as e:
            logger.error("get_ban_info error: %s", e)
            return None

    async def get_banned_users(self) -> List[Dict]:
        try:
            cursor = self.banned_users.find({}).sort("banned_at", -1)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error("get_banned_users error: %s", e)
            return []

    async def get_ban_history(self, limit: int = 20) -> List[Dict]:
        """Return recent ban/unban actions."""
        try:
            cursor = self.ban_history.find({}).sort("timestamp", -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error("get_ban_history error: %s", e)
            return []

    # ══════════════════════════════════════════════════════════════
    # CLOSE
    # ══════════════════════════════════════════════════════════════

    async def close(self):
        self.client.close()
