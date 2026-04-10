import logging
from config import Config

logger = logging.getLogger(__name__)


async def check_bandwidth_limit(db):
    """Check global bandwidth limit. Returns (allowed, stats)."""
    try:
        stats  = await db.get_bandwidth_stats()
        max_bw = Config.get("max_bandwidth", 107374182400)
        if stats["total_bandwidth"] >= max_bw:
            return False, stats
        return True, stats
    except Exception as e:
        logger.error("bandwidth check error: %s", e)
        return True, {}


async def check_user_access(db, user_id: str) -> dict:
    """
    Full user access check:
      - Checks if user is manually blocked
      - Checks global bandwidth limit
      - Checks per-user limits (bandwidth + file count)
      - Auto-blocks user if limit exceeded
      - Returns {'allowed': bool, 'reason': str, 'warning': bool, ...}
    """
    try:
        # 1. Check if globally blocked
        blocked = await db.is_user_blocked(user_id)
        if blocked:
            user = await db.get_user(user_id)
            reason = (user or {}).get("block_reason", "blocked") if user else "blocked"
            return {
                "allowed":      False,
                "reason":       reason,
                "reason_human": _human_reason(reason),
                "warning":      False,
            }

        # 2. Check global bandwidth (affects all users)
        if Config.get("bandwidth_mode", True):
            stats  = await db.get_bandwidth_stats()
            max_bw = Config.get("max_bandwidth", 107374182400)
            if stats["total_bandwidth"] >= max_bw:
                return {
                    "allowed":      False,
                    "reason":       "global_bandwidth",
                    "reason_human": "Service bandwidth limit reached. Please try again later.",
                    "warning":      False,
                }

        # 3. Check per-user limits (auto-enforce + return warning state)
        result = await db.check_and_enforce_user_limits(user_id)
        if not result.get("allowed", True):
            return {
                "allowed":      False,
                "reason":       result.get("reason", "limit_exceeded"),
                "reason_human": _human_reason(result.get("reason", "")),
                "warning":      False,
            }

        # Propagate 90% warning
        if result.get("warning"):
            return {
                "allowed":       True,
                "reason":        "",
                "reason_human":  "",
                "warning":       True,
                "warning_type":  result.get("warning_type", ""),
                "warning_pct":   result.get("pct", 0),
            }

        return {"allowed": True, "reason": "", "reason_human": "", "warning": False}

    except Exception as e:
        logger.error("check_user_access error: %s", e)
        return {"allowed": True, "reason": "", "reason_human": "", "warning": False}


def _human_reason(reason: str) -> str:
    """Convert a machine reason code into a user-friendly string."""
    reasons = {
        "bandwidth_limit":   "You have reached your bandwidth limit. Please contact the admin.",
        "file_limit":        "You have reached your file limit. Please contact the admin.",
        "blocked":           "Your account has been suspended. Please contact the admin.",
        "global_bandwidth":  "Service bandwidth limit reached. Please try again later.",
        "limit_exceeded":    "Your account limit has been reached. Please contact the admin.",
    }
    return reasons.get(reason, "Access denied. Please contact the admin.")
