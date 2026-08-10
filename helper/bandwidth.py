import logging
from config import Config

logger = logging.getLogger(__name__)


def is_privileged_user(user_id: int) -> bool:
    """Return True if user is an owner or sudo — exempt from per-user bandwidth limits."""
    return user_id in Config.OWNER_ID


async def is_privileged_user_async(db, user_id: int) -> bool:
    """Return True if user is owner OR sudo — exempt from per-user bandwidth limits."""
    if user_id in Config.OWNER_ID:
        return True
    return await db.is_sudo_user(str(user_id))


async def check_bandwidth_limit(db):
    """
    Check global bot-wide bandwidth limit.
    Returns (allowed: bool, stats: dict).
    Uses the monthly cycle for primary enforcement.
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


async def check_user_bandwidth_limit(db, user_id: int) -> tuple:
    """
    Check per-user monthly bandwidth limit.
    SKIPS check for owners and sudo users.
    Returns (allowed: bool, stats: dict).
    """
    try:
        # Owners and sudo users are never limited
        if await is_privileged_user_async(db, user_id):
            return True, {}

        if not Config.get("user_bw_mode", True):
            return True, {}

        max_ubw = Config.get("max_user_bandwidth", 10737418240)
        if max_ubw <= 0:
            return True, {}

        allowed, stats = await db.check_user_bw_limit(str(user_id))
        return allowed, stats
    except Exception as e:
        logger.error("check_user_bandwidth_limit error: %s", e)
        return True, {}


async def should_warn_global_bw(db) -> tuple[bool, int]:
    """
    Return (should_warn, threshold_pct) for global bandwidth.
    Returns (False, 0) if warning already sent for this threshold.
    """
    try:
        warn_pct = Config.get("bw_warn_pct", 80)
        if warn_pct <= 0:
            return False, 0
        stats = await db.get_global_bw_cycle()
        current_pct = stats["pct"]
        if current_pct < warn_pct:
            return False, 0

        # Find which threshold bracket we're in (warn once per 10% bracket above threshold)
        # e.g. threshold=80: warn at 80%, 90%, 95%, 100%
        bracket = int(current_pct // 10) * 10  # round down to nearest 10
        bracket = max(bracket, warn_pct)

        already_sent = await db.has_sent_global_bw_warning(bracket)
        if already_sent:
            return False, 0

        return True, bracket
    except Exception as e:
        logger.error("should_warn_global_bw error: %s", e)
        return False, 0


async def should_warn_user_bw(db, user_id: int) -> tuple[bool, int]:
    """
    Return (should_warn, threshold_pct) for a user's bandwidth.
    Returns (False, 0) if already warned at this level.
    NEVER warns for privileged users.
    """
    try:
        # No warnings for owners/sudo
        if await is_privileged_user_async(db, user_id):
            return False, 0

        warn_pct = Config.get("user_bw_warn_pct", 80)
        if warn_pct <= 0:
            return False, 0

        stats = await db.get_user_bw(str(user_id))
        current_pct = stats["pct"]
        if current_pct < warn_pct:
            return False, 0

        # Bracket: warn at threshold, then again at 90%, 95%, 100%
        bracket = int(current_pct // 10) * 10
        bracket = max(bracket, warn_pct)

        already_sent = await db.has_sent_user_bw_warning(str(user_id), bracket)
        if already_sent:
            return False, 0

        return True, bracket
    except Exception as e:
        logger.error("should_warn_user_bw error: %s", e)
        return False, 0


async def track_bandwidth_usage(db, message_id: str, size: int, user_id):
    """
    Unified bandwidth tracking:
    - legacy daily bandwidth collection
    - global monthly cycle
    - per-user monthly cycle (only for non-privileged users)
    Tracking starts from first interaction/session automatically.
    """
    try:
        # Legacy per-file + daily tracking
        await db.track_bandwidth(message_id, size)
        # Monthly global cycle
        await db.record_global_bw(size)
        # Monthly per-user cycle — skip for owners/sudo
        if user_id:
            uid_int = int(user_id) if isinstance(user_id, str) else user_id
            if not await is_privileged_user_async(db, uid_int):
                await db.record_user_bw(str(user_id), size)
    except Exception as e:
        logger.error("track_bandwidth_usage error: %s", e)
