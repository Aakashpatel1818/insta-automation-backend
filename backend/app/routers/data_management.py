# routers/data_management.py
# Admin endpoints for database health, stats, and manual cleanup.
# All routes require superadmin role.

import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from app.dependencies import get_current_user
from app.database import get_db
from app.redis_pool import get_redis
from app.services.cleanup_service import run_full_cleanup

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/data", tags=["Data Management"])


def require_admin(current_user: dict = Depends(get_current_user)):
    if not current_user.get("is_superadmin"):
        raise HTTPException(status_code=403, detail="Superadmin only")
    return current_user


# ── Collection stats ──────────────────────────────────────────────────────────

@router.get("/stats")
async def database_stats(_: dict = Depends(require_admin)):
    """
    Returns row counts, size estimates, and age distribution
    for every collection so you can see what's growing.
    """
    db = get_db()

    collections = [
        "users", "instagram_accounts",
        "automation_settings", "keyword_rules",
        "automation_logs", "automation_analytics",
        "cooldown_logs", "leads", "collected_users",
        "dm_messages", "scheduled_posts", "otp_records",
    ]

    stats = {}
    for col in collections:
        try:
            count = await db[col].count_documents({})
            # Estimate oldest + newest document age
            oldest = await db[col].find_one(
                {}, sort=[("_id", 1)], projection={"_id": 1}
            )
            newest = await db[col].find_one(
                {}, sort=[("_id", -1)], projection={"_id": 1}
            )
            stats[col] = {
                "count":   count,
                "oldest":  str(oldest["_id"].generation_time) if oldest else None,
                "newest":  str(newest["_id"].generation_time) if newest else None,
            }
        except Exception as e:
            stats[col] = {"count": -1, "error": str(e)}

    # Redis queue depth per account
    r = get_redis()
    try:
        account_ids = await r.smembers("comment_queue:accounts")
        queue_depths = {}
        for aid in account_ids:
            if isinstance(aid, bytes):
                aid = aid.decode()
            depth = await r.llen(f"comment_queue:{aid}")
            queue_depths[aid] = depth
        stats["_redis_comment_queues"] = queue_depths
    except Exception as e:
        stats["_redis_comment_queues"] = {"error": str(e)}

    return {"stats": stats, "generated_at": datetime.utcnow().isoformat()}


# ── Garbage preview (dry-run) ─────────────────────────────────────────────────

@router.get("/garbage-preview")
async def garbage_preview(_: dict = Depends(require_admin)):
    """
    Count how many records WOULD be deleted by cleanup — without deleting anything.
    Use this to understand the scale before triggering a real cleanup.
    """
    db  = get_db()
    now = datetime.utcnow()

    preview = {}

    # Automation logs > 90 days
    cutoff = now - timedelta(days=90)
    preview["automation_logs_>90d"] = await db["automation_logs"].count_documents(
        {"timestamp": {"$lt": cutoff}}
    )

    # Cooldown logs > 2 days
    cutoff = now - timedelta(days=2)
    preview["cooldown_logs_>2d"] = await db["cooldown_logs"].count_documents(
        {"triggered_at": {"$lt": cutoff}}
    )

    # DM messages > 60 days
    cutoff = now - timedelta(days=60)
    preview["dm_messages_>60d"] = await db["dm_messages"].count_documents(
        {"timestamp": {"$lt": cutoff}}
    )

    # Old finished scheduled posts > 30 days
    cutoff = now - timedelta(days=30)
    preview["scheduled_posts_finished_>30d"] = await db["scheduled_posts"].count_documents({
        "status":       {"$in": ["published", "failed"]},
        "scheduled_at": {"$lt": cutoff},
    })

    # Orphaned automation settings
    all_account_ids = [str(a["_id"]) async for a in db["instagram_accounts"].find({}, {"_id": 1})]
    preview["orphaned_automation_settings"] = await db["automation_settings"].count_documents(
        {"account_id": {"$nin": all_account_ids}}
    )

    # Orphaned keyword rules
    all_automation_ids = [str(s["_id"]) async for s in db["automation_settings"].find({}, {"_id": 1})]
    preview["orphaned_keyword_rules"] = await db["keyword_rules"].count_documents({
        "automation_id": {"$nin": all_automation_ids + [None]},
    })

    # Orphaned leads
    preview["orphaned_leads"] = await db["leads"].count_documents(
        {"account_id": {"$nin": all_account_ids}}
    )

    # Orphaned collected_users
    preview["orphaned_collected_users"] = await db["collected_users"].count_documents(
        {"account_id": {"$nin": all_account_ids}}
    )

    # Expired OTP records
    otp_cutoff = now - timedelta(hours=2)
    preview["expired_otp_records"] = await db["otp_records"].count_documents(
        {"created_at": {"$lt": otp_cutoff}}
    )

    total = sum(preview.values())
    return {
        "preview":      preview,
        "total_would_delete": total,
        "note":         "Call POST /admin/data/cleanup to execute",
        "generated_at": now.isoformat(),
    }


# ── Manual cleanup trigger ────────────────────────────────────────────────────

@router.post("/cleanup")
async def trigger_cleanup(_: dict = Depends(require_admin)):
    """
    Manually trigger a full garbage collection run.
    The scheduled cleanup runs automatically every 24h;
    use this endpoint to run it on-demand.
    """
    logger.info("[DataMgmt] Manual cleanup triggered via API")
    result = await run_full_cleanup()
    return result


# ── Per-collection targeted cleanup ──────────────────────────────────────────

@router.delete("/collection/{collection_name}/old")
async def delete_old_records(
    collection_name: str,
    days: int = 30,
    _: dict = Depends(require_admin),
):
    """
    Delete records older than `days` from a specific collection.
    Only works on collections with a timestamp/created_at field.
    Whitelisted collections only for safety.
    """
    ALLOWED = {
        "automation_logs": "timestamp",
        "cooldown_logs":   "triggered_at",
        "dm_messages":     "timestamp",
        "scheduled_posts": "scheduled_at",
        "otp_records":     "created_at",
    }
    if collection_name not in ALLOWED:
        raise HTTPException(
            status_code=400,
            detail=f"Collection '{collection_name}' not allowed. Choose from: {list(ALLOWED.keys())}"
        )
    db         = get_db()
    field      = ALLOWED[collection_name]
    cutoff     = datetime.utcnow() - timedelta(days=days)
    result     = await db[collection_name].delete_many({field: {"$lt": cutoff}})
    logger.info(f"[DataMgmt] Deleted {result.deleted_count} records from {collection_name} (>{days}d)")
    return {
        "collection": collection_name,
        "field":      field,
        "older_than": cutoff.isoformat(),
        "deleted":    result.deleted_count,
    }


# ── Redis queue management ─────────────────────────────────────────────────────

@router.get("/queues")
async def list_queues(_: dict = Depends(require_admin)):
    """Show all active comment queues and their depths."""
    r = get_redis()
    try:
        account_ids = await r.smembers("comment_queue:accounts")
        queues = []
        for aid in account_ids:
            if isinstance(aid, bytes):
                aid = aid.decode()
            depth    = await r.llen(f"comment_queue:{aid}")
            # Peek at the first job to show its age
            first_raw = await r.lindex(f"comment_queue:{aid}", 0)
            first_enqueued = None
            if first_raw:
                import json
                try:
                    first_enqueued = json.loads(first_raw).get("enqueued_at")
                except Exception:
                    pass
            queues.append({
                "account_id":       aid,
                "depth":            depth,
                "oldest_job_enqueued": first_enqueued,
            })
        return {"queues": queues, "total_accounts": len(queues)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/queues/{account_id}")
async def flush_account_queue(account_id: str, _: dict = Depends(require_admin)):
    """Flush (clear) the comment queue for a specific account."""
    r = get_redis()
    await r.delete(f"comment_queue:{account_id}")
    await r.srem("comment_queue:accounts", account_id)
    logger.info(f"[DataMgmt] Flushed comment queue for account={account_id}")
    return {"status": "flushed", "account_id": account_id}


@router.delete("/queues")
async def flush_all_queues(_: dict = Depends(require_admin)):
    """Flush ALL comment queues. Use in emergencies."""
    r = get_redis()
    account_ids = await r.smembers("comment_queue:accounts")
    flushed = 0
    for aid in account_ids:
        if isinstance(aid, bytes):
            aid = aid.decode()
        await r.delete(f"comment_queue:{aid}")
        flushed += 1
    await r.delete("comment_queue:accounts")
    logger.warning(f"[DataMgmt] Flushed ALL comment queues ({flushed} accounts)")
    return {"status": "all_flushed", "accounts_flushed": flushed}
