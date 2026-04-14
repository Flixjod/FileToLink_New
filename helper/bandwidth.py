import logging
from config import Config

logger = logging.getLogger(__name__)


def _is_privileged(user_id: str) -> bool:
    """Return True if user_id belongs to an owner or sudo list (in-memory fast check)."""
    try:
        uid = int(user_id)
        if uid in Config.OWNER_ID:
            return True
    except (ValueError, TypeError):
        pass
    return False


async def is_privileged_user(db, user_id: str) -> bool:
    """
    Return True if user_id is an owner OR a sudo user.
    Owners are always exempt; sudo users are also exempt from per-user bandwidth.
    """
    if _is_privileged(user_id):
        return True
    try:
        return await db.is_sudo_user(str(user_id))
    except Exception:
        return False


async def check_bandwidth_limit(db):
    """
    Check global bot-wide bandwidth limit.
    Returns (allowed: bool, stats: dict).
    Uses the monthly cycle.
    """
    try:
        if not Config.get("bandwidth_mode", True):
            return True, {}

        # Primary check: monthly global cycle
        cycle_stats = await db.get_global_bw_cycle()
        max_bw      = Config.get("max_bandwidth", 107374182400)
        if max_bw > 0 and cycle_stats["used"] >= max_bw:
            return False, cycle_stats

        return True, cycle_stats
    except Exception as e:
        logger.error("bandwidth check error: %s", e)
        return True, {}


async def check_user_bandwidth_limit(db, user_id: str) -> tuple:
    """
    Check per-user monthly bandwidth limit.
    Sudo users and owners are always exempt.
    Returns (allowed: bool, stats: dict).
    """
    try:
        if not Config.get("user_bw_mode", True):
            return True, {}

        # Sudo / Owner exempt
        if await is_privileged_user(db, user_id):
            return True, {}

        max_ubw = Config.get("max_user_bandwidth", 10737418240)
        if max_ubw <= 0:
            return True, {}

        allowed, stats = await db.check_user_bw_limit(str(user_id))
        return allowed, stats
    except Exception as e:
        logger.error("check_user_bandwidth_limit error: %s", e)
        return True, {}


async def should_warn_global_bw(db) -> bool:
    """
    Return True if the global bandwidth warning threshold has been crossed
    AND the warning has NOT been sent yet this cycle (sends only once).
    """
    try:
        warn_pct = Config.get("bw_warn_pct", 80)
        if warn_pct <= 0:
            return False
        stats = await db.get_global_bw_cycle()
        if stats["pct"] < warn_pct:
            return False
        # One-shot: only returns True once per cycle
        return await db.should_send_global_bw_warn()
    except Exception as e:
        logger.error("should_warn_global_bw error: %s", e)
        return False


async def should_warn_global_bw_over(db) -> bool:
    """
    Return True if global BW is over limit and over-limit msg not sent yet.
    """
    try:
        return await db.should_send_global_bw_over()
    except Exception as e:
        logger.error("should_warn_global_bw_over error: %s", e)
        return False


async def should_warn_user_bw(db, user_id: str) -> bool:
    """
    Return True if the user's bandwidth warning threshold has been crossed
    AND the warning has NOT been sent yet this cycle.
    Privileged users are never warned.
    """
    try:
        warn_pct = Config.get("user_bw_warn_pct", 80)
        if warn_pct <= 0:
            return False
        if await is_privileged_user(db, user_id):
            return False
        stats = await db.get_user_bw(str(user_id))
        if stats["pct"] < warn_pct:
            return False
        # One-shot per cycle
        return await db.should_send_user_bw_warn(str(user_id))
    except Exception as e:
        logger.error("should_warn_user_bw error: %s", e)
        return False


async def should_warn_user_bw_over(db, user_id: str) -> bool:
    """
    Return True if the user is over their bandwidth limit and
    the over-limit notification has NOT been sent yet this cycle.
    """
    try:
        if await is_privileged_user(db, user_id):
            return False
        return await db.should_send_user_bw_over(str(user_id))
    except Exception as e:
        logger.error("should_warn_user_bw_over error: %s", e)
        return False


async def track_bandwidth_usage(db, message_id: str, size: int, user_id: str):
    """
    Unified bandwidth tracking:
    - legacy daily bandwidth collection
    - global monthly cycle
    - per-user monthly cycle (only for non-privileged users)
    """
    try:
        # Legacy per-file + daily tracking
        await db.track_bandwidth(message_id, size)
        # Monthly global cycle
        await db.record_global_bw(size)
        # Monthly per-user cycle — skip for sudo/owner
        if user_id:
            if not await is_privileged_user(db, user_id):
                await db.record_user_bw(str(user_id), size)
    except Exception as e:
        logger.error("track_bandwidth_usage error: %s", e)
