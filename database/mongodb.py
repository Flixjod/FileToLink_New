from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta, timezone
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
        self.db         = self.client[database_name]
        self.files      = self.db.files
        self.users      = self.db.users
        self.bandwidth  = self.db.bandwidth
        self.sudo_users = self.db.sudo_users
        self.config     = self.db.config

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

            # Ensure bandwidth cycle collection
            await self._ensure_bw_cycle()

            logger.info("✅ ᴅʙ ɪɴᴅᴇxᴇꜱ ʀᴇᴀᴅˏ ᴀʟʟ ɪɴꜱᴛᴀɴᴛ — ꜱᴄɪᴘᴘᴇᴅ ɴᴇᴡ ᴄʀᴇᴀᴛɪᴏɴ ᴏɴʟˏ")
            return True
        except Exception as e:
            logger.error("❌ ᴅʙ ɪɴɪᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    # ── Bandwidth Cycle System ──────────────────────────────────────────────
    # A 30-day rolling cycle. The cycle document is persistent in MongoDB
    # and is NEVER reset on restart/deploy — only auto-resets when the
    # 30-day window expires.

    async def _ensure_bw_cycle(self) -> dict:
        """
        Return the current bandwidth cycle document.
        Creates a new one if it doesn't exist, auto-resets if it expired.
        """
        try:
            doc = await self.config.find_one({"key": "BandwidthCycle"})
            now = datetime.utcnow()

            if doc is None:
                # First time: create a fresh cycle
                doc = {
                    "key":        "BandwidthCycle",
                    "cycle_start": now,
                    "cycle_end":   now + timedelta(days=30),
                    "bytes_used":  0,
                }
                await self.config.insert_one(doc)
                logger.info("📅 bandwidth cycle created: starts %s", now.date())
                return doc

            # Auto-reset if cycle has expired
            cycle_end = doc.get("cycle_end", now)
            if isinstance(cycle_end, str):
                cycle_end = datetime.fromisoformat(cycle_end)

            if now >= cycle_end:
                new_doc = {
                    "cycle_start": now,
                    "cycle_end":   now + timedelta(days=30),
                    "bytes_used":  0,
                }
                await self.config.update_one(
                    {"key": "BandwidthCycle"},
                    {"$set": new_doc},
                )
                doc.update(new_doc)
                logger.info("🔄 bandwidth cycle auto-reset: new start %s", now.date())

            return doc
        except Exception as exc:
            logger.error("_ensure_bw_cycle error: %s", exc)
            return {}

    async def get_bw_cycle(self) -> dict:
        """Return the current bandwidth cycle info."""
        return await self._ensure_bw_cycle()

    async def increment_bw_cycle(self, size: int) -> None:
        """Add *size* bytes to the current 30-day cycle counter."""
        try:
            await self._ensure_bw_cycle()   # auto-reset if expired
            await self.config.update_one(
                {"key": "BandwidthCycle"},
                {"$inc": {"bytes_used": size}},
            )
        except Exception as exc:
            logger.error("increment_bw_cycle error: %s", exc)

    # ── File operations ─────────────────────────────────────────────────────

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

    # ── Bandwidth operations ─────────────────────────────────────────────────

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
            # Also update the 30-day cycle
            await self.increment_bw_cycle(size)
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

    async def register_user_on_start(self, user_data: Dict) -> bool:
        try:
            existing = await self.users.find_one({"user_id": user_data["user_id"]})
            if existing:
                await self.users.update_one(
                    {"user_id": user_data["user_id"]},
                    {"$set": {"last_activity": datetime.utcnow()}},
                )
                return False

            await self.users.insert_one({
                "user_id":       user_data["user_id"],
                "username":      user_data.get("username", ""),
                "first_name":    user_data.get("first_name", ""),
                "last_name":     user_data.get("last_name", ""),
                "first_used":    datetime.utcnow(),
                "last_activity": datetime.utcnow(),
            })
            logger.info("👤 ɴᴇᴡ ᴜꜱᴇʀ ʀᴇɢɪꜱᴛᴇʀᴇᴅ: %s", user_data["user_id"])
            return True
        except Exception as e:
            logger.error("❌ ʀᴇɢɪꜱᴛᴇʀ_ᴜꜱᴇʀ_ᴏɴ_ꜱᴛᴀʀᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    async def get_user(self, user_id: str) -> Optional[Dict]:
        try:
            return await self.users.find_one({"user_id": user_id})
        except Exception as e:
            logger.error("get user error: %s", e)
            return None

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

    async def get_bandwidth_by_range(self, start_date: str, end_date: str) -> int:
        """
        Return total bytes consumed between *start_date* and *end_date* (inclusive).
        Dates should be ISO strings: 'YYYY-MM-DD'.
        """
        try:
            pipeline = [
                {"$match": {"date": {"$gte": start_date, "$lte": end_date}}},
                {"$group": {"_id": None, "total": {"$sum": "$total_bytes"}}},
            ]
            result = await self.bandwidth.aggregate(pipeline).to_list(length=1)
            return result[0]["total"] if result else 0
        except Exception as exc:
            logger.error("get_bandwidth_by_range error: %s", exc)
            return 0

    async def get_bandwidth_daily_series(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Return a list of {date, bytes} dicts for the given range, sorted ascending.
        Missing dates will not be included (front-end fills gaps if needed).
        """
        try:
            cursor = self.bandwidth.find(
                {"date": {"$gte": start_date, "$lte": end_date}},
                {"_id": 0, "date": 1, "total_bytes": 1},
            ).sort("date", 1)
            docs = await cursor.to_list(length=None)
            return [{"date": d["date"], "bytes": d.get("total_bytes", 0)} for d in docs]
        except Exception as exc:
            logger.error("get_bandwidth_daily_series error: %s", exc)
            return []

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
