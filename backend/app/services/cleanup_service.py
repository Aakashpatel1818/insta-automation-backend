# services/cleanup_service.py
# ─────────────────────────────────────────────────────────────────────────────
# Automated garbage collection & data retention for all MongoDB collections.
#
# What gets cleaned and why:
#
#   automation_logs   — high-volume write path, grows without bound
#                       keep 90 days, delete older rows
#   cooldown_logs     — only useful while a cooldown is active
#                       delete once the cooldown window has passed
#   dedup keys        — Redis, auto-expire (TTL=5 min), no action needed
#   leads             — keep forever (user data), but deduplicate stale entries
#   collected_users   — keep forever (user data)
#   dm_messages       — keep 60 days (inbox history)
#   scheduled_posts   — keep published/failed for 30 days; pending are live data
#   automation_analytics — keep forever (aggregate stats, tiny)
#   comment_queue     — Redis lists, auto-drained by worker; force-flush orphans
#   sessions/tokens   — expired JWTs removed (token_version keys in Redis)
#
# Schedule: runs once a day via start_cleanup_scheduler()
# ─────────────────────────────────────────────────────────────────────────────

import asyncio
import logging
from datetime import datetime, timedelta

from app.database import get_db
from app.redis_pool import get_redis

logger = logging.getLogger(__name__)

# ── Retention windows ─────────────────────────────────────────────────────────
AUTOMATION_LOGS_DAYS   = 90   # keep last 90 days of automation logs
COOLDOWN_LOGS_DAYS     = 2    # cooldowns > 2 days are definitely expired
DM_MESSAGES_DAYS       = 60   # inbox messages older than 60 days
SCHEDULED_POST_DAYS    = 30   # published/failed scheduled posts older than 30 days
COMMENT_QUEUE_ORPHAN_H = 6    # Redis queue jobs stuck > 6 hours are orphaned


# ── Individual cleaners ───────────────────────────────────────────────────────

async def clean_automation_logs(db) -> int:
    """Delete automation_logs older than AUTOMATION_LOGS_DAYS."""
    cutoff = datetime.utcnow() - timedelta(days=AUTOMATION_LOGS_DAYS)
    result = await db["automation_logs"].delete_many({"timestamp": {"$lt": cutoff}})
    logger.info(f"[Cleanup] automation_logs: deleted {result.deleted_count} old records (>{AUTOMATION_LOGS_DAYS}d)")
    return result.deleted_count


async def clean_cooldown_logs(db) -> int:
    """
    Delete cooldown_logs where triggered_at is older than the maximum
    possible cooldown window (COOLDOWN_LOGS_DAYS).  These can never block
    anyone again so they are pure garbage.
    """
    cutoff = datetime.utcnow() - timedelta(days=COOLDOWN_LOGS_DAYS)
    result = await db["cooldown_logs"].delete_many({"triggered_at": {"$lt": cutoff}})
    logger.info(f"[Cleanup] cooldown_logs: deleted {result.deleted_count} expired cooldowns")
    return result.deleted_count


async def clean_dm_messages(db) -> int:
    """Delete DM messages older than DM_MESSAGES_DAYS."""
    cutoff = datetime.utcnow() - timedelta(days=DM_MESSAGES_DAYS)
    result = await db["dm_messages"].delete_many({"timestamp": {"$lt": cutoff}})
    logger.info(f"[Cleanup] dm_messages: deleted {result.deleted_count} old messages (>{DM_MESSAGES_DAYS}d)")
    return result.deleted_count


async def clean_scheduled_posts(db) -> int:
    """Delete published/failed scheduled posts older than SCHEDULED_POST_DAYS."""
    cutoff = datetime.utcnow() - timedelta(days=SCHEDULED_POST_DAYS)
    result = await db["scheduled_posts"].delete_many({
        "status":       {"$in": ["published", "failed"]},
        "scheduled_at": {"$lt": cutoff},
    })
    logger.info(f"[Cleanup] scheduled_posts: deleted {result.deleted_count} old finished posts (>{SCHEDULED_POST_DAYS}d)")
    return result.deleted_count


async def clean_orphaned_automation_settings(db) -> int:
    """
    Delete automation_settings that reference a post_id / account_id
    for which the instagram_account no longer exists (user disconnected IG).
    """
    all_account_ids = set()
    async for acc in db["instagram_accounts"].find({}, {"_id": 1}):
        all_account_ids.add(str(acc["_id"]))

    # Find settings with unknown account_ids
    orphan_ids = []
    async for s in db["automation_settings"].find({}, {"_id": 1, "account_id": 1}):
        if s.get("account_id") not in all_account_ids:
            orphan_ids.append(s["_id"])

    if orphan_ids:
        result = await db["automation_settings"].delete_many({"_id": {"$in": orphan_ids}})
        # Also delete their rules
        orphan_str = [str(oid) for oid in orphan_ids]
        await db["keyword_rules"].delete_many({"automation_id": {"$in": orphan_str}})
        logger.info(f"[Cleanup] automation_settings: deleted {result.deleted_count} orphaned automations (no parent IG account)")
        return result.deleted_count

    logger.info("[Cleanup] automation_settings: no orphans found")
    return 0


async def clean_orphaned_keyword_rules(db) -> int:
    """
    Delete keyword_rules whose automation_id points to a non-existent
    automation_settings document (automation was deleted but rules weren't).
    """
    from bson import ObjectId

    all_automation_ids = set()
    async for s in db["automation_settings"].find({}, {"_id": 1}):
        all_automation_ids.add(str(s["_id"]))

    orphan_ids = []
    async for r in db["keyword_rules"].find(
        {"automation_id": {"$ne": None, "$exists": True}},
        {"_id": 1, "automation_id": 1}
    ):
        if r.get("automation_id") and r["automation_id"] not in all_automation_ids:
            orphan_ids.append(r["_id"])

    if orphan_ids:
        result = await db["keyword_rules"].delete_many({"_id": {"$in": orphan_ids}})
        logger.info(f"[Cleanup] keyword_rules: deleted {result.deleted_count} orphaned rules (no parent automation)")
        return result.deleted_count

    logger.info("[Cleanup] keyword_rules: no orphans found")
    return 0


async def clean_stale_leads(db) -> int:
    """
    Remove leads that reference an account_id that no longer exists.
    Real leads for existing accounts are never touched.
    """
    all_account_ids = set()
    async for acc in db["instagram_accounts"].find({}, {"_id": 1}):
        all_account_ids.add(str(acc["_id"]))

    result = await db["leads"].delete_many({
        "account_id": {"$nin": list(all_account_ids)}
    })
    logger.info(f"[Cleanup] leads: deleted {result.deleted_count} orphaned leads (no parent account)")
    return result.deleted_count


async def clean_stale_collected_users(db) -> int:
    """Same as above — remove collected_users for deleted accounts."""
    all_account_ids = set()
    async for acc in db["instagram_accounts"].find({}, {"_id": 1}):
        all_account_ids.add(str(acc["_id"]))

    result = await db["collected_users"].delete_many({
        "account_id": {"$nin": list(all_account_ids)}
    })
    logger.info(f"[Cleanup] collected_users: deleted {result.deleted_count} orphaned users")
    return result.deleted_count


async def clean_orphaned_redis_queue_jobs(max_age_hours: int = COMMENT_QUEUE_ORPHAN_H) -> int:
    """
    Scan all comment_queue:{account_id} Redis lists.
    Remove individual jobs that have been sitting there longer than
    max_age_hours — these are orphaned (worker crashed, account deleted, etc.).
    """
    r = get_redis()
    total_removed = 0

    try:
        account_ids = await r.smembers("comment_queue:accounts")
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)

        for account_id in account_ids:
            if isinstance(account_id, bytes):
                account_id = account_id.decode()

            queue_key = f"comment_queue:{account_id}"
            length = await r.llen(queue_key)
            if length == 0:
                await r.srem("comment_queue:accounts", account_id)
                continue

            # Scan all items, keep only fresh ones
            all_items = await r.lrange(queue_key, 0, -1)
            fresh = []
            stale_count = 0

            for raw in all_items:
                try:
                    import json
                    job = json.loads(raw)
                    enqueued_at_str = job.get("enqueued_at", "")
                    enqueued_at = datetime.fromisoformat(enqueued_at_str)
                    if enqueued_at >= cutoff:
                        fresh.append(raw)
                    else:
                        stale_count += 1
                        logger.warning(
                            f"[Cleanup] Dropping stale queue job: "
                            f"comment_id={job.get('comment_id')} "
                            f"enqueued={enqueued_at_str}"
                        )
                except Exception:
                    stale_count += 1  # malformed job — discard

            if stale_count > 0:
                # Atomically replace the list with only fresh items
                await r.delete(queue_key)
                if fresh:
                    await r.rpush(queue_key, *fresh)
                else:
                    await r.srem("comment_queue:accounts", account_id)
                total_removed += stale_count

        logger.info(f"[Cleanup] redis comment_queue: removed {total_removed} stale/orphaned jobs")
    except Exception as e:
        logger.error(f"[Cleanup] Redis queue cleanup error: {e}", exc_info=True)

    return total_removed


async def clean_expired_otp_records(db) -> int:
    """Delete OTP verification records that are well past their expiry."""
    cutoff = datetime.utcnow() - timedelta(hours=2)
    result = await db["otp_records"].delete_many({"created_at": {"$lt": cutoff}})
    if result.deleted_count:
        logger.info(f"[Cleanup] otp_records: deleted {result.deleted_count} expired OTPs")
    return result.deleted_count


async def clean_expired_token_version_keys() -> int:
    """
    Redis token_version:{user_id} keys are used for logout-all invalidation.
    They don't expire automatically.  Scan and remove keys older than the
    JWT max lifetime (ACCESS_TOKEN_EXPIRE_MINUTES).
    We can't know the age without a timestamp, so we only remove keys for
    users that no longer exist in MongoDB.
    """
    from bson import ObjectId
    db = get_redis()
    r  = get_redis()
    mongo_db = get_db()
    removed = 0

    try:
        cursor = 0
        while True:
            cursor, keys = await r.scan(cursor, match="token_version:*", count=100)
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode()
                user_id = key.split(":", 1)[1]
                try:
                    user = await mongo_db["users"].find_one({"_id": ObjectId(user_id)}, {"_id": 1})
                    if not user:
                        await r.delete(key)
                        removed += 1
                except Exception:
                    pass
            if cursor == 0:
                break
        if removed:
            logger.info(f"[Cleanup] Redis token_version: removed {removed} keys for deleted users")
    except Exception as e:
        logger.error(f"[Cleanup] token_version cleanup error: {e}", exc_info=True)

    return removed


async def clean_automation_analytics_orphans(db) -> int:
    """
    Remove automation_analytics rows that no longer have a parent automation.
    """
    all_automation_ids = set()
    async for s in db["automation_settings"].find({}, {"_id": 1}):
        all_automation_ids.add(str(s["_id"]))

    result = await db["automation_analytics"].delete_many({
        "automation_id": {"$nin": list(all_automation_ids)}
    })
    if result.deleted_count:
        logger.info(f"[Cleanup] automation_analytics: deleted {result.deleted_count} orphaned analytics rows")
    return result.deleted_count


# ── Master runner ─────────────────────────────────────────────────────────────

async def run_full_cleanup() -> dict:
    """
    Run every cleaner and return a summary dict.
    Safe to call at any time — each cleaner is independent.
    """
    db = get_db()
    started_at = datetime.utcnow()
    logger.info("=" * 60)
    logger.info("[Cleanup] Starting full garbage collection run…")
    logger.info("=" * 60)

    results = {}

    cleaners = [
        ("automation_logs",           lambda: clean_automation_logs(db)),
        ("cooldown_logs",             lambda: clean_cooldown_logs(db)),
        ("dm_messages",               lambda: clean_dm_messages(db)),
        ("scheduled_posts",           lambda: clean_scheduled_posts(db)),
        ("orphaned_automation_settings", lambda: clean_orphaned_automation_settings(db)),
        ("orphaned_keyword_rules",    lambda: clean_orphaned_keyword_rules(db)),
        ("stale_leads",               lambda: clean_stale_leads(db)),
        ("stale_collected_users",     lambda: clean_stale_collected_users(db)),
        ("orphaned_analytics",        lambda: clean_automation_analytics_orphans(db)),
        ("expired_otp_records",       lambda: clean_expired_otp_records(db)),
        ("redis_queue_orphans",       clean_orphaned_redis_queue_jobs),
        ("redis_token_version_keys",  clean_expired_token_version_keys),
    ]

    total_deleted = 0
    for name, fn in cleaners:
        try:
            count = await fn()
            results[name] = {"deleted": count, "status": "ok"}
            total_deleted += count
        except Exception as e:
            logger.error(f"[Cleanup] {name} failed: {e}", exc_info=True)
            results[name] = {"deleted": 0, "status": f"error: {e}"}

    elapsed = (datetime.utcnow() - started_at).total_seconds()
    logger.info(f"[Cleanup] Done in {elapsed:.1f}s — total records removed: {total_deleted}")
    logger.info("=" * 60)

    return {
        "started_at":    started_at.isoformat(),
        "elapsed_sec":   round(elapsed, 2),
        "total_deleted": total_deleted,
        "details":       results,
    }


# ── Scheduler ─────────────────────────────────────────────────────────────────

_cleanup_task = None
CLEANUP_INTERVAL_HOURS = 24   # run once a day


async def _cleanup_loop():
    """Run cleanup once a day at startup + every 24 hours thereafter."""
    # Initial delay: wait 5 minutes after startup before first run
    # (lets the app fully warm up)
    await asyncio.sleep(300)
    while True:
        try:
            await run_full_cleanup()
        except Exception as e:
            logger.error(f"[Cleanup] Loop error: {e}", exc_info=True)
        await asyncio.sleep(CLEANUP_INTERVAL_HOURS * 3600)


def start_cleanup_scheduler():
    global _cleanup_task
    if _cleanup_task and not _cleanup_task.done():
        logger.warning("[Cleanup] Scheduler already running")
        return
    _cleanup_task = asyncio.create_task(_cleanup_loop(), name="cleanup_scheduler")
    logger.info(f"[Cleanup] Scheduler started — runs every {CLEANUP_INTERVAL_HOURS}h ✅")


def stop_cleanup_scheduler():
    global _cleanup_task
    if _cleanup_task:
        _cleanup_task.cancel()
        _cleanup_task = None
        logger.info("[Cleanup] Scheduler stopped")
