import logging
from config import Config

logger = logging.getLogger(__name__)

# Warn users when they've used >= this fraction of their limit
USER_WARNING_THRESHOLD = 0.80   # 80%
# Warn owner when global bandwidth is >= this fraction
GLOBAL_WARNING_THRESHOLD = 0.85  # 85%


async def check_bandwidth_limit(db):
    """Check global bot-wide bandwidth limit.
    Returns (allowed: bool, stats: dict).
    Also auto-triggers 30-day cycle reset if expired.
    """
    try:
        stats  = await db.get_bandwidth_stats()
        max_bw = Config.get("max_bandwidth", 107374182400)
        if max_bw and stats["total_bandwidth"] >= max_bw:
            return False, stats
        return True, stats
    except Exception as e:
        logger.error("bandwidth check error: %s", e)
        return True, {}


async def check_user_bandwidth_limit(db, user_id: str) -> tuple:
    """Check per-user monthly bandwidth limit.

    Returns (allowed: bool, user_bw: dict).
    Only applies to free users (sudo/owner must be excluded by the caller).
    """
    try:
        if not Config.get("user_bandwidth_mode", False):
            return True, {}

        max_user_bw = Config.get("max_user_bandwidth", 10737418240)
        if not max_user_bw:
            return True, {}

        user_bw = await db.get_user_bandwidth(str(user_id))
        if user_bw["used"] >= max_user_bw:
            return False, user_bw
        return True, user_bw
    except Exception as e:
        logger.error("user bandwidth check error: %s", e)
        return True, {}


async def get_user_bandwidth_warning(db, user_id: str) -> dict | None:
    """Return warning info if user is approaching their limit, else None.

    Returns dict with keys: used, limit, pct, days_remaining, hours_remaining
    or None if no warning is needed.
    """
    try:
        if not Config.get("user_bandwidth_mode", False):
            return None

        max_user_bw = Config.get("max_user_bandwidth", 10737418240)
        if not max_user_bw:
            return None

        user_bw = await db.get_user_bandwidth(str(user_id))
        used    = user_bw.get("used", 0)
        pct     = used / max_user_bw if max_user_bw else 0

        if pct >= USER_WARNING_THRESHOLD and used < max_user_bw:
            return {
                "used":           used,
                "limit":          max_user_bw,
                "pct":            pct,
                "days_remaining":  user_bw.get("days_remaining",  30),
                "hours_remaining": user_bw.get("hours_remaining", 0),
            }
        return None
    except Exception as e:
        logger.error("get_user_bandwidth_warning error: %s", e)
        return None


async def get_global_bandwidth_warning(db) -> dict | None:
    """Return warning info if global bandwidth is approaching the limit, else None.

    Returns dict with keys: used, limit, pct, days_remaining, hours_remaining
    or None if no warning is needed.
    """
    try:
        if not Config.get("bandwidth_mode", True):
            return None

        max_bw = Config.get("max_bandwidth", 107374182400)
        if not max_bw:
            return None

        stats = await db.get_bandwidth_stats()
        used  = stats.get("total_bandwidth", 0)
        pct   = used / max_bw if max_bw else 0

        if pct >= GLOBAL_WARNING_THRESHOLD and used < max_bw:
            return {
                "used":           used,
                "limit":          max_bw,
                "pct":            pct,
                "days_remaining":  stats.get("days_remaining",  30),
                "hours_remaining": stats.get("hours_remaining", 0),
            }
        return None
    except Exception as e:
        logger.error("get_global_bandwidth_warning error: %s", e)
        return None
