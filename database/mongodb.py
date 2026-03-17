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

            logger.info("✅ ᴅʙ ɪɴᴅᴇxᴇꜱ ʀᴇᴀᴅˏ ᴀʟʟ ɪɴꜱᴛᴀɴᴛ — ꜱᴄɪᴘᴘᴇᴅ ɴᴇᴡ ᴄʀᴇᴀᴛɪᴏɴ ᴏɴʟˏ")
            return True
        except Exception as e:
            logger.error("❌ ᴅʙ ɪɴɪᴛ ᴇʀʀᴏʀ: %s", e)
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Bandwidth Cycle — persistent 30-day rolling window
    # Stored in the `config` collection under key="bandwidth_cycle".
    # This data SURVIVES server restarts / redeployments.
    # ─────────────────────────────────────────────────────────────────────────
    async def get_bandwidth_cycle(self) -> Dict:
        """Return the current bandwidth cycle document, auto-creating and
        auto-advancing it when the 30-day window has expired."""
        try:
            now = datetime.now(timezone.utc)
            doc = await self.config.find_one({"key": "bandwidth_cycle"})

            if not doc:
                # First-ever initialisation
                start      = now
                next_reset = start + timedelta(days=30)
                doc = {
                    "key":                    "bandwidth_cycle",
                    "bandwidth_cycle_start":  start,
                    "bandwidth_cycle_next_reset": next_reset,
                    "bandwidth_used":         0,
                }
                await self.config.insert_one(doc)
                logger.info("🔄 bandwidth cycle initialised — next reset: %s", next_reset.date())
                return doc

            # Check whether the window has expired and roll it forward
            next_reset = doc.get("bandwidth_cycle_next_reset")
            # Handle both timezone-aware and naive datetimes from Mongo
            if next_reset is not None:
                if next_reset.tzinfo is None:
                    next_reset = next_reset.replace(tzinfo=timezone.utc)

            if next_reset and now >= next_reset:
                # Advance the cycle: start = old next_reset, next = start + 30d
                new_start      = next_reset
                new_next_reset = new_start + timedelta(days=30)
                updates = {
                    "bandwidth_cycle_start":      new_start,
                    "bandwidth_cycle_next_reset": new_next_reset,
                    "bandwidth_used":             0,
                }
                await self.config.update_one(
                    {"key": "bandwidth_cycle"},
                    {"$set": updates},
                )
                doc.update(updates)
                logger.info(
                    "🔄 bandwidth cycle reset — new period %s → %s",
                    new_start.date(), new_next_reset.date(),
                )

            return doc
        except Exception as e:
            logger.error("get_bandwidth_cycle error: %s", e)
            return {}

    async def increment_cycle_bandwidth(self, size: int) -> bool:
        """Add *size* bytes to the current cycle's bandwidth_used counter.
        This is idempotent with respect to the cycle window — if the window
        has expired it will be auto-reset before incrementing."""
        try:
            # Ensure the cycle doc is current (may trigger auto-reset)
            await self.get_bandwidth_cycle()
            await self.config.update_one(
                {"key": "bandwidth_cycle"},
                {"$inc": {"bandwidth_used": size}},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("increment_cycle_bandwidth error: %s", e)
            return False

    async def get_cycle_bandwidth_used(self) -> int:
        """Return the bytes used in the current 30-day cycle."""
        try:
            doc = await self.get_bandwidth_cycle()
            return int(doc.get("bandwidth_used", 0))
        except Exception as e:
            logger.error("get_cycle_bandwidth_used error: %s", e)
            return 0

    async def get_bandwidth_by_range(
        self,
        from_date: Optional[datetime] = None,
        to_date:   Optional[datetime] = None,
    ) -> Dict:
        """Aggregate bandwidth usage for a date range.
        Uses indexed `date` field — no full-table scan.
        Both *from_date* and *to_date* are inclusive and UTC-date-only.
        """
        try:
            query: Dict = {}
            if from_date or to_date:
                date_filter: Dict = {}
                if from_date:
                    date_filter["$gte"] = from_date.strftime("%Y-%m-%d")
                if to_date:
                    date_filter["$lte"] = to_date.strftime("%Y-%m-%d")
                query["date"] = date_filter

            pipeline = [
                {"$match": query},
                {"$group": {"_id": None, "total": {"$sum": "$total_bytes"}}},
            ]
            result = await self.bandwidth.aggregate(pipeline).to_list(length=1)
            total  = result[0]["total"] if result else 0
            return {"total_bytes": total, "from_date": from_date, "to_date": to_date}
        except Exception as e:
            logger.error("get_bandwidth_by_range error: %s", e)
            return {"total_bytes": 0}

    async def get_bandwidth_last_n_days(self, days: int = 7) -> int:
        """Return bytes used in the last *days* calendar days (today included)."""
        try:
            now        = datetime.now(timezone.utc)
            from_date  = now - timedelta(days=days - 1)
            result     = await self.get_bandwidth_by_range(from_date=from_date, to_date=now)
            return result.get("total_bytes", 0)
        except Exception as e:
            logger.error("get_bandwidth_last_n_days error: %s", e)
            return 0

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
                "created_at":       datetime.now(timezone.utc),
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

    async def update_bandwidth(self, size: int) -> bool:
        try:
            today = datetime.now(timezone.utc).date().isoformat()
            await self.bandwidth.update_one(
                {"date": today},
                {
                    "$inc": {"total_bytes": size},
                    "$set": {"last_updated": datetime.now(timezone.utc)},
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
            # Update daily record AND 30-day cycle counter atomically
            await self.update_bandwidth(size)
            await self.increment_cycle_bandwidth(size)
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
                    {"$set": {"last_activity": datetime.now(timezone.utc)}},
                )
                return False  # not new

            await self.users.insert_one({
                "user_id":       user_data["user_id"],
                "username":      user_data.get("username", ""),
                "first_name":    user_data.get("first_name", ""),
                "last_name":     user_data.get("last_name", ""),
                "first_used":    datetime.now(timezone.utc),
                "last_activity": datetime.now(timezone.utc),
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
            today       = datetime.now(timezone.utc).date().isoformat()
            today_stats = await self.bandwidth.find_one({"date": today})
            cycle       = await self.get_bandwidth_cycle()
            return {
                "total_bandwidth":            total,
                "today_bandwidth":            today_stats.get("total_bytes", 0) if today_stats else 0,
                "cycle_bandwidth_used":       int(cycle.get("bandwidth_used", 0)),
                "bandwidth_cycle_start":      cycle.get("bandwidth_cycle_start"),
                "bandwidth_cycle_next_reset": cycle.get("bandwidth_cycle_next_reset"),
            }
        except Exception as e:
            logger.error("get bandwidth stats error: %s", e)
            return {
                "total_bandwidth":            0,
                "today_bandwidth":            0,
                "cycle_bandwidth_used":       0,
                "bandwidth_cycle_start":      None,
                "bandwidth_cycle_next_reset": None,
            }

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
                {"$set": {"user_id": user_id, "added_by": added_by, "added_at": datetime.now(timezone.utc)}},
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
