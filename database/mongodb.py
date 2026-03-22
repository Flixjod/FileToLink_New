from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, mongo_uri: str, database_name: str):
        self.client = AsyncIOMotorClient(
            mongo_uri,
            maxPoolSize=50,
            minPoolSize=10,
            maxIdleTimeMS=45000,
            waitQueueTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
        )
        self.db           = self.client[database_name]
        self.files        = self.db.files
        self.users        = self.db.users
        self.bandwidth    = self.db.bandwidth
        self.sudo_users   = self.db.sudo_users
        self.config       = self.db.config
        self.user_bw      = self.db.user_bandwidth   # per-user bandwidth tracking
        self.banned_users = self.db.banned_users     # ban system

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

            # Per-user bandwidth index
            ubw_idx = await _existing(self.user_bw)
            if 'user_id' not in ubw_idx:
                await self.user_bw.create_index('user_id', unique=True)

            # Banned users index
            ban_idx = await _existing(self.banned_users)
            if 'user_id' not in ban_idx:
                await self.banned_users.create_index('user_id', unique=True)

            logger.info("✅ ᴅʙ ɪɴᴅᴇxᴇꜱ ʀᴇᴀᴅʏ")
            return True
        except Exception as e:
            logger.error("❌ ᴅʙ ɪɴɪᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    # ─── Files ────────────────────────────────────────────────────────────────

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

    # ─── Global Bandwidth ────────────────────────────────────────────────────

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
            return True
        except Exception as e:
            logger.error("reset bandwidth error: %s", e)
            return False

    async def get_total_bandwidth(self) -> int:
        """
        Global bandwidth: sum all daily records that fall within the current
        rolling 30-day window (from the last global reset date).
        """
        try:
            from config import Config
            last_reset_str = Config.get("global_bw_reset_date", None)
            if last_reset_str:
                try:
                    last_reset = datetime.fromisoformat(last_reset_str)
                except Exception:
                    last_reset = None
            else:
                last_reset = None

            if last_reset:
                reset_date_str = last_reset.date().isoformat()
                pipeline = [
                    {"$match": {"date": {"$gte": reset_date_str}}},
                    {"$group": {"_id": None, "total": {"$sum": "$total_bytes"}}},
                ]
            else:
                pipeline = [{"$group": {"_id": None, "total": {"$sum": "$total_bytes"}}}]

            result = await self.bandwidth.aggregate(pipeline).to_list(length=1)
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

    # ─── Per-User Bandwidth (Free Users) ─────────────────────────────────────

    async def get_user_bandwidth(self, user_id: str) -> Dict:
        """
        Returns the user's bandwidth record. Creates a fresh 30-day window
        if none exists or the 30-day window has expired.
        """
        try:
            now = datetime.utcnow()
            doc = await self.user_bw.find_one({"user_id": user_id})

            if not doc:
                # First use — create the window
                doc = {
                    "user_id":      user_id,
                    "used_bytes":   0,
                    "window_start": now,
                    "window_end":   now + timedelta(days=30),
                    "last_updated": now,
                }
                await self.user_bw.insert_one(doc)
                return doc

            # Auto-reset if the 30-day window has expired
            window_end = doc.get("window_end", now)
            if isinstance(window_end, str):
                window_end = datetime.fromisoformat(window_end)

            if now >= window_end:
                new_start = now
                new_end   = now + timedelta(days=30)
                await self.user_bw.update_one(
                    {"user_id": user_id},
                    {"$set": {
                        "used_bytes":   0,
                        "window_start": new_start,
                        "window_end":   new_end,
                        "last_updated": now,
                    }},
                )
                doc.update({
                    "used_bytes":   0,
                    "window_start": new_start,
                    "window_end":   new_end,
                    "last_updated": now,
                })

            return doc
        except Exception as e:
            logger.error("get_user_bandwidth error: %s", e)
            return {"user_id": user_id, "used_bytes": 0,
                    "window_start": datetime.utcnow(),
                    "window_end":   datetime.utcnow() + timedelta(days=30)}

    async def update_user_bandwidth(self, user_id: str, size: int) -> Dict:
        """
        Increment the user's rolling-window bandwidth usage.
        Returns the updated record.
        """
        try:
            now = datetime.utcnow()
            doc = await self.get_user_bandwidth(user_id)

            await self.user_bw.update_one(
                {"user_id": user_id},
                {"$inc": {"used_bytes": size},
                 "$set": {"last_updated": now}},
                upsert=True,
            )
            doc["used_bytes"] = doc.get("used_bytes", 0) + size
            return doc
        except Exception as e:
            logger.error("update_user_bandwidth error: %s", e)
            return {}

    async def reset_user_bandwidth(self, user_id: str) -> bool:
        """Manually reset a user's bandwidth window right now."""
        try:
            now = datetime.utcnow()
            await self.user_bw.update_one(
                {"user_id": user_id},
                {"$set": {
                    "used_bytes":   0,
                    "window_start": now,
                    "window_end":   now + timedelta(days=30),
                    "last_updated": now,
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("reset_user_bandwidth error: %s", e)
            return False

    async def check_user_bandwidth_limit(self, user_id: str, limit_bytes: int) -> Dict:
        """
        Returns:
          {
            "allowed":   bool,
            "used":      int (bytes),
            "limit":     int (bytes),
            "remaining": int (bytes),
            "pct":       float (0–100),
            "warning":   bool  (True if 80–90 % used),
            "window_end": datetime,
          }
        """
        try:
            doc       = await self.get_user_bandwidth(user_id)
            used      = doc.get("used_bytes", 0)
            remaining = max(0, limit_bytes - used)
            pct       = round((used / limit_bytes * 100) if limit_bytes else 0, 1)
            allowed   = used < limit_bytes
            warning   = 80.0 <= pct < 100.0
            window_end = doc.get("window_end", datetime.utcnow() + timedelta(days=30))
            return {
                "allowed":    allowed,
                "used":       used,
                "limit":      limit_bytes,
                "remaining":  remaining,
                "pct":        pct,
                "warning":    warning,
                "window_end": window_end,
            }
        except Exception as e:
            logger.error("check_user_bandwidth_limit error: %s", e)
            return {"allowed": True, "used": 0, "limit": limit_bytes,
                    "remaining": limit_bytes, "pct": 0.0, "warning": False,
                    "window_end": datetime.utcnow() + timedelta(days=30)}

    async def get_all_user_bandwidth_stats(self, page: int = 1, per_page: int = 20) -> Dict:
        """Return paginated per-user bandwidth stats for admin view."""
        try:
            skip  = (page - 1) * per_page
            total = await self.user_bw.count_documents({})
            cursor = (
                self.user_bw.find({})
                .sort("used_bytes", -1)
                .skip(skip)
                .limit(per_page)
            )
            records = await cursor.to_list(length=per_page)
            return {"records": records, "total": total, "page": page, "per_page": per_page}
        except Exception as e:
            logger.error("get_all_user_bandwidth_stats error: %s", e)
            return {"records": [], "total": 0, "page": page, "per_page": per_page}

    # ─── Ban System ───────────────────────────────────────────────────────────

    async def ban_user(
        self,
        user_id: str,
        reason: str = "",
        banned_by: str = "",
        expires_at: Optional[datetime] = None,
    ) -> bool:
        """Ban a user. Pass expires_at for a temporary ban (None = permanent)."""
        try:
            doc = {
                "user_id":   user_id,
                "reason":    reason or "No reason provided",
                "banned_by": banned_by,
                "banned_at": datetime.utcnow(),
                "expires_at": expires_at,  # None means permanent
            }
            await self.banned_users.update_one(
                {"user_id": user_id},
                {"$set": doc},
                upsert=True,
            )
            logger.info("🚫 User %s banned by %s. Reason: %s", user_id, banned_by, reason)
            return True
        except Exception as e:
            logger.error("ban_user error: %s", e)
            return False

    async def unban_user(self, user_id: str) -> bool:
        """Remove a user's ban."""
        try:
            result = await self.banned_users.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error("unban_user error: %s", e)
            return False

    async def is_banned(self, user_id: str) -> tuple:
        """
        Returns (is_banned: bool, ban_doc: dict | None).
        Automatically lifts expired temporary bans.
        """
        try:
            doc = await self.banned_users.find_one({"user_id": str(user_id)})
            if not doc:
                return False, None

            expires_at = doc.get("expires_at")
            if expires_at:
                if isinstance(expires_at, str):
                    try:
                        expires_at = datetime.fromisoformat(expires_at)
                    except Exception:
                        expires_at = None

                if expires_at and datetime.utcnow() >= expires_at:
                    # Expired — auto-remove ban
                    await self.banned_users.delete_one({"user_id": str(user_id)})
                    logger.info("⏰ Temporary ban for user %s has expired — auto-lifted.", user_id)
                    return False, None

            return True, doc
        except Exception as e:
            logger.error("is_banned error: %s", e)
            return False, None

    async def get_banned_users(self, page: int = 1, per_page: int = 20) -> Dict:
        """Return paginated list of banned users for admin view."""
        try:
            skip  = (page - 1) * per_page
            total = await self.banned_users.count_documents({})
            cursor = (
                self.banned_users.find({})
                .sort("banned_at", -1)
                .skip(skip)
                .limit(per_page)
            )
            records = await cursor.to_list(length=per_page)
            return {"records": records, "total": total, "page": page, "per_page": per_page}
        except Exception as e:
            logger.error("get_banned_users error: %s", e)
            return {"records": [], "total": 0, "page": page, "per_page": per_page}

    async def get_ban_info(self, user_id: str) -> Optional[Dict]:
        """Return ban document for a specific user."""
        try:
            return await self.banned_users.find_one({"user_id": str(user_id)})
        except Exception as e:
            logger.error("get_ban_info error: %s", e)
            return None

    # ─── Users ────────────────────────────────────────────────────────────────

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

    # ─── Stats ────────────────────────────────────────────────────────────────

    async def get_stats(self) -> Dict:
        try:
            total_files   = await self.files.count_documents({})
            total_users   = await self.users.count_documents({})
            total_banned  = await self.banned_users.count_documents({})
            bw            = await self.get_bandwidth_stats()
            return {
                "total_files":     total_files,
                "total_users":     total_users,
                "total_banned":    total_banned,
                "total_bandwidth": bw["total_bandwidth"],
                "today_bandwidth": bw["today_bandwidth"],
            }
        except Exception as e:
            logger.error("get stats error: %s", e)
            return {
                "total_files": 0, "total_users": 0, "total_banned": 0,
                "total_bandwidth": 0, "today_bandwidth": 0,
            }

    # ─── Sudo Users ───────────────────────────────────────────────────────────

    async def add_sudo_user(self, user_id: str, added_by: str) -> bool:
        try:
            await self.sudo_users.update_one(
                {"user_id": user_id},
                {"$set": {"user_id": user_id, "added_by": added_by, "added_at": datetime.utcnow()}},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("add sudo user error: %s", e)
            return False

    async def remove_sudo_user(self, user_id: str) -> bool:
        try:
            result = await self.sudo_users.delete_one({"user_id": user_id})
            return result.deleted_count > 0
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
            cursor = self.sudo_users.find({})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error("get sudo users error: %s", e)
            return []

    async def get_user_count(self) -> int:
        try:
            return await self.users.count_documents({})
        except Exception as e:
            logger.error("get user count error: %s", e)
            return 0

    async def close(self):
        self.client.close()
