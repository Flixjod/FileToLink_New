from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# Bandwidth reset period in days (monthly = 30)
BANDWIDTH_RESET_DAYS = 30


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
        self.db              = self.client[database_name]
        self.files           = self.db.files
        self.users           = self.db.users
        self.bandwidth       = self.db.bandwidth
        self.user_bandwidth  = self.db.user_bandwidth
        self.sudo_users      = self.db.sudo_users
        self.config          = self.db.config
        self.bw_reset        = self.db.bw_reset   # tracks global reset timestamps

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

            ubw_idx = await _existing(self.user_bandwidth)
            if 'user_id' not in ubw_idx:
                await self.user_bandwidth.create_index('user_id', unique=True)
            if 'reset_at' not in ubw_idx:
                await self.user_bandwidth.create_index('reset_at')

            sudo_idx = await _existing(self.sudo_users)
            if 'user_id' not in sudo_idx:
                await self.sudo_users.create_index('user_id', unique=True)

            # Ensure bw_reset collection has an index
            bwr_idx = await _existing(self.bw_reset)
            if 'type' not in bwr_idx:
                await self.bw_reset.create_index('type', unique=True)

            # Seed global reset timestamp if not present
            existing_reset = await self.bw_reset.find_one({'type': 'global'})
            if not existing_reset:
                now = datetime.utcnow()
                await self.bw_reset.insert_one({
                    'type': 'global',
                    'last_reset': now,
                    'next_reset': now + timedelta(days=BANDWIDTH_RESET_DAYS),
                })
                logger.info("✅ ʙᴀɴᴅᴡɪᴅᴛʜ ʀᴇꜱᴇᴛ ᴛɪᴍᴇꜱᴛᴀᴍᴘ ɪɴɪᴛɪᴀʟɪᴢᴇᴅ")

            logger.info("✅ ᴅʙ ɪɴᴅᴇxᴇꜱ ʀᴇᴀᴅˏ ᴀʟʟ ɪɴꜱᴛᴀɴᴛ — ꜱᴄɪᴘᴘᴇᴅ ɴᴇᴡ ᴄʀᴇᴀᴛɪᴏɴ ᴏɴʟˏ")
            return True
        except Exception as e:
            logger.error("❌ ᴅʙ ɪɴɪᴛ ᴇʀʀᴏʀ: %s", e)
            return False

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

    async def track_bandwidth(self, message_id: str, size: int, user_id: str = None) -> bool:
        try:
            await self.files.update_one(
                {"message_id": message_id},
                {"$inc": {"bandwidth_used": size}},
            )
            await self.update_bandwidth(size)
            # Also track per-user bandwidth when user_id provided
            if user_id:
                await self.update_user_bandwidth(user_id, size)
            return True
        except Exception as e:
            logger.error("track bandwidth error: %s", e)
            return False

    async def reset_bandwidth(self) -> bool:
        try:
            now = datetime.utcnow()
            await self.bandwidth.delete_many({})
            await self.files.update_many({}, {"$set": {"bandwidth_used": 0}})
            # Update global reset timestamp
            await self.bw_reset.update_one(
                {"type": "global"},
                {"$set": {
                    "last_reset": now,
                    "next_reset": now + timedelta(days=BANDWIDTH_RESET_DAYS),
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("reset bandwidth error: %s", e)
            return False

    async def get_bandwidth_reset_info(self) -> Dict:
        """Return global bandwidth reset timestamps and countdown."""
        try:
            doc = await self.bw_reset.find_one({"type": "global"})
            now = datetime.utcnow()
            if not doc:
                next_reset = now + timedelta(days=BANDWIDTH_RESET_DAYS)
                last_reset = now
            else:
                last_reset = doc.get("last_reset", now)
                next_reset = doc.get("next_reset", now + timedelta(days=BANDWIDTH_RESET_DAYS))

            # If next_reset has already passed, compute next upcoming one
            if next_reset <= now:
                # Advance by multiples of BANDWIDTH_RESET_DAYS until future
                delta = (now - next_reset).total_seconds()
                cycles = int(delta / (BANDWIDTH_RESET_DAYS * 86400)) + 1
                next_reset = next_reset + timedelta(days=BANDWIDTH_RESET_DAYS * cycles)
                await self.bw_reset.update_one(
                    {"type": "global"},
                    {"$set": {"next_reset": next_reset}},
                    upsert=True,
                )

            remaining_seconds = max(0, (next_reset - now).total_seconds())
            days    = int(remaining_seconds // 86400)
            hours   = int((remaining_seconds % 86400) // 3600)
            minutes = int((remaining_seconds % 3600) // 60)

            if days > 0:
                reset_str = f"{days}d {hours}h {minutes}m"
            elif hours > 0:
                reset_str = f"{hours}h {minutes}m"
            else:
                reset_str = f"{minutes}m"

            return {
                "last_reset":       last_reset.isoformat(),
                "next_reset":       next_reset.isoformat(),
                "next_reset_epoch": int(next_reset.timestamp()),
                "remaining_seconds": int(remaining_seconds),
                "reset_in":         reset_str,
                "days":             days,
                "hours":            hours,
                "minutes":          minutes,
            }
        except Exception as e:
            logger.error("get_bandwidth_reset_info error: %s", e)
            return {
                "last_reset": "", "next_reset": "", "next_reset_epoch": 0,
                "remaining_seconds": 0, "reset_in": "N/A",
                "days": 0, "hours": 0, "minutes": 0,
            }

    # ── Per-user bandwidth ───────────────────────────────────────────────────

    async def update_user_bandwidth(self, user_id: str, size: int) -> bool:
        """Track bandwidth consumed by a specific user."""
        try:
            now = datetime.utcnow()
            doc = await self.user_bandwidth.find_one({"user_id": user_id})
            if not doc:
                await self.user_bandwidth.insert_one({
                    "user_id":    user_id,
                    "used":       size,
                    "last_reset": now,
                    "reset_at":   now + timedelta(days=BANDWIDTH_RESET_DAYS),
                    "updated_at": now,
                })
            else:
                # Auto-reset per-user bandwidth if period has passed
                reset_at = doc.get("reset_at", now)
                if reset_at and reset_at <= now:
                    await self.user_bandwidth.update_one(
                        {"user_id": user_id},
                        {"$set": {
                            "used":       size,
                            "last_reset": now,
                            "reset_at":   now + timedelta(days=BANDWIDTH_RESET_DAYS),
                            "updated_at": now,
                        }},
                    )
                else:
                    await self.user_bandwidth.update_one(
                        {"user_id": user_id},
                        {
                            "$inc": {"used": size},
                            "$set": {"updated_at": now},
                        },
                    )
            return True
        except Exception as e:
            logger.error("update_user_bandwidth error: %s", e)
            return False

    async def get_user_bandwidth(self, user_id: str) -> Dict:
        """Return per-user bandwidth usage + reset info."""
        try:
            doc = await self.user_bandwidth.find_one({"user_id": user_id})
            now = datetime.utcnow()
            if not doc:
                return {
                    "used": 0, "reset_in": "N/A", "next_reset_epoch": 0,
                    "remaining_seconds": 0, "days": 0, "hours": 0, "minutes": 0,
                }
            reset_at = doc.get("reset_at", now + timedelta(days=BANDWIDTH_RESET_DAYS))
            remaining_seconds = max(0, (reset_at - now).total_seconds())
            days    = int(remaining_seconds // 86400)
            hours   = int((remaining_seconds % 86400) // 3600)
            minutes = int((remaining_seconds % 3600) // 60)
            if days > 0:
                reset_str = f"{days}d {hours}h {minutes}m"
            elif hours > 0:
                reset_str = f"{hours}h {minutes}m"
            else:
                reset_str = f"{minutes}m"
            return {
                "used":             doc.get("used", 0),
                "reset_in":         reset_str,
                "next_reset_epoch": int(reset_at.timestamp()) if reset_at else 0,
                "remaining_seconds": int(remaining_seconds),
                "days":             days,
                "hours":            hours,
                "minutes":          minutes,
            }
        except Exception as e:
            logger.error("get_user_bandwidth error: %s", e)
            return {"used": 0, "reset_in": "N/A", "next_reset_epoch": 0,
                    "remaining_seconds": 0, "days": 0, "hours": 0, "minutes": 0}

    async def reset_user_bandwidth(self, user_id: str) -> bool:
        """Manually reset a specific user's bandwidth."""
        try:
            now = datetime.utcnow()
            await self.user_bandwidth.update_one(
                {"user_id": user_id},
                {"$set": {
                    "used":       0,
                    "last_reset": now,
                    "reset_at":   now + timedelta(days=BANDWIDTH_RESET_DAYS),
                    "updated_at": now,
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("reset_user_bandwidth error: %s", e)
            return False

    async def get_all_user_bandwidth(self, limit: int = 50) -> List[Dict]:
        """Return top bandwidth-consuming users."""
        try:
            cursor = self.user_bandwidth.find({}).sort("used", -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error("get_all_user_bandwidth error: %s", e)
            return []

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

    # ── User limits / warning system ─────────────────────────────────────────

    async def set_user_limit(self, user_id: str, max_bandwidth: int = 0,
                             max_files: int = 0) -> bool:
        """Set per-user resource limits. 0 = no limit."""
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "max_bandwidth": max_bandwidth,
                    "max_files":     max_files,
                    "limits_set_at": datetime.utcnow(),
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("set_user_limit error: %s", e)
            return False

    async def block_user(self, user_id: str, reason: str = "limit_exceeded") -> bool:
        """Block a user from streaming/downloading."""
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "blocked":    True,
                    "block_reason": reason,
                    "blocked_at": datetime.utcnow(),
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("block_user error: %s", e)
            return False

    async def unblock_user(self, user_id: str) -> bool:
        """Unblock a previously blocked user."""
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {
                    "blocked":      False,
                    "block_reason": "",
                    "unblocked_at": datetime.utcnow(),
                }},
            )
            return True
        except Exception as e:
            logger.error("unblock_user error: %s", e)
            return False

    async def is_user_blocked(self, user_id: str) -> bool:
        """Check if user is blocked."""
        try:
            doc = await self.users.find_one({"user_id": user_id})
            return bool(doc and doc.get("blocked", False))
        except Exception as e:
            logger.error("is_user_blocked error: %s", e)
            return False

    async def check_and_enforce_user_limits(self, user_id: str) -> Dict:
        """
        Check user limits and auto-block if exceeded.
        Returns {'allowed': bool, 'reason': str, 'warning': bool}
        """
        try:
            user = await self.users.find_one({"user_id": user_id})
            if not user:
                return {"allowed": True, "reason": "", "warning": False}

            if user.get("blocked", False):
                return {
                    "allowed": False,
                    "reason":  user.get("block_reason", "blocked"),
                    "warning": False,
                }

            max_bw    = user.get("max_bandwidth", 0)
            max_files = user.get("max_files", 0)

            # Check bandwidth limit
            if max_bw > 0:
                ubw = await self.get_user_bandwidth(user_id)
                used_bw = ubw.get("used", 0)
                if used_bw >= max_bw:
                    await self.block_user(user_id, "bandwidth_limit")
                    return {
                        "allowed": False,
                        "reason":  "bandwidth_limit",
                        "warning": False,
                        "used":    used_bw,
                        "limit":   max_bw,
                    }
                # Warning at 90%
                if used_bw >= max_bw * 0.9:
                    return {
                        "allowed": True,
                        "reason":  "",
                        "warning": True,
                        "warning_type": "bandwidth",
                        "used":    used_bw,
                        "limit":   max_bw,
                        "pct":     round(used_bw / max_bw * 100, 1),
                    }

            # Check file count limit
            if max_files > 0:
                file_count = await self.files.count_documents({"user_id": user_id})
                if file_count >= max_files:
                    await self.block_user(user_id, "file_limit")
                    return {
                        "allowed": False,
                        "reason":  "file_limit",
                        "warning": False,
                        "count":   file_count,
                        "limit":   max_files,
                    }
                if file_count >= max_files * 0.9:
                    return {
                        "allowed": True,
                        "reason":  "",
                        "warning": True,
                        "warning_type": "files",
                        "count":   file_count,
                        "limit":   max_files,
                        "pct":     round(file_count / max_files * 100, 1),
                    }

            return {"allowed": True, "reason": "", "warning": False}
        except Exception as e:
            logger.error("check_and_enforce_user_limits error: %s", e)
            return {"allowed": True, "reason": "", "warning": False}

    async def get_all_users(self, limit: int = 100, skip: int = 0) -> List[Dict]:
        """Return paginated list of all users."""
        try:
            cursor = self.users.find({}).sort("first_used", -1).skip(skip).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.error("get_all_users error: %s", e)
            return []

    async def close(self):
        self.client.close()
