import logging
from datetime import datetime, timedelta
from config import Config

logger = logging.getLogger(__name__)


async def check_bandwidth_limit(db) -> tuple:
    """
    Check the GLOBAL bot bandwidth cap.
    Returns (allowed: bool, stats: dict).
    """
    try:
        if not Config.get("bandwidth_mode", True):
            return True, {}
        stats  = await db.get_bandwidth_stats()
        max_bw = Config.get("max_bandwidth", 107374182400)
        if stats["total_bandwidth"] >= max_bw:
            return False, stats
        return True, stats
    except Exception as e:
        logger.error("bandwidth check error: %s", e)
        return True, {}


async def check_user_bandwidth(db, user_id: str) -> dict:
    """
    Check per-user bandwidth for free users.
    Skips check if user_bw_mode is disabled in config.

    Returns dict with keys:
      allowed, used, limit, remaining, pct, warning, window_end, skipped
    """
    try:
        if not Config.get("user_bw_mode", False):
            return {"allowed": True, "skipped": True}

        limit = Config.get("user_bw_limit", 10737418240)
        result = await db.check_user_bandwidth_limit(str(user_id), limit)
        result["skipped"] = False
        return result
    except Exception as e:
        logger.error("check_user_bandwidth error: %s", e)
        return {"allowed": True, "skipped": True}


def is_privileged_user(user_id: int) -> bool:
    """
    Returns True if user is owner or sudo — they bypass per-user bandwidth limits.
    Note: sudo check requires async DB call; this handles the Config.OWNER_ID part.
    """
    return user_id in Config.OWNER_ID


async def is_exempt_from_user_bw(db, user_id: int) -> bool:
    """
    Returns True if user should be exempt from per-user bandwidth limits:
    - Bot owner
    - Sudo users
    """
    if user_id in Config.OWNER_ID:
        return True
    return await db.is_sudo_user(str(user_id))
