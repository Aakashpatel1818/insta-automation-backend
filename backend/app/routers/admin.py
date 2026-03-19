"""
Admin router — /admin/*
All endpoints require is_admin dependency (role: "admin" or "superadmin").
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from datetime import datetime, timedelta
from bson import ObjectId
import asyncio
import logging
import time

from app.database import get_db
from app.dependencies import get_current_user, invalidate_user_cache
from app.security import hash_password, create_access_token
from app.redis_pool import get_redis

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])


# ─────────────────────────────────────────────────────────────────────────────
# Admin guard dependency
# ─────────────────────────────────────────────────────────────────────────────

async def require_admin(current_user: dict = Depends(get_current_user)):
    role = current_user.get("role", "user")
    if role not in ("admin", "superadmin"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")
    return current_user


async def require_superadmin(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") != "superadmin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Superadmin access required.")
    return current_user


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard stats
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/stats")
async def admin_stats(_admin: dict = Depends(require_admin)):
    db = get_db()
    now         = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_ago    = now - timedelta(days=7)
    month_ago   = now - timedelta(days=30)

    (
        total_users, active_users, banned_users,
        new_today, new_this_week, new_this_month,
    ) = await asyncio.gather(
        db["users"].count_documents({}),
        db["users"].count_documents({"is_active": True}),
        db["users"].count_documents({"is_banned": True}),
        db["users"].count_documents({"created_at": {"$gte": today_start}}),
        db["users"].count_documents({"created_at": {"$gte": week_ago}}),
        db["users"].count_documents({"created_at": {"$gte": month_ago}}),
    )

    total_accounts, active_accounts = await asyncio.gather(
        db["instagram_accounts"].count_documents({}),
        db["instagram_accounts"].count_documents({"is_active": True}),
    )

    total_automations, active_automations = await asyncio.gather(
        db["keyword_rules"].count_documents({}),
        db["keyword_rules"].count_documents({"is_active": True}),
    )

    total_logs  = await db["automation_logs"].count_documents({})
    total_posts = await db["scheduled_posts"].count_documents({})

    # Log health breakdown
    success_logs = await db["automation_logs"].count_documents({"status": "success"})
    error_logs   = await db["automation_logs"].count_documents({"status": "error"})

    return {
        "users": {
            "total": total_users, "active": active_users, "banned": banned_users,
            "new_today": new_today, "new_this_week": new_this_week, "new_this_month": new_this_month,
        },
        "instagram_accounts": {"total": total_accounts, "active": active_accounts},
        "automations":        {"total": total_automations, "active": active_automations},
        "logs":  total_logs,
        "posts": total_posts,
        "log_health": {"success": success_logs, "error": error_logs},
    }


# ─────────────────────────────────────────────────────────────────────────────
# User management
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/users")
async def list_users(
    page:        int  = Query(default=1, ge=1),
    limit:       int  = Query(default=20, le=100),
    search:      str  = Query(default=""),
    role:        str  = Query(default=""),
    plan:        str  = Query(default=""),
    user_status: str  = Query(default="", alias="status"),
    _admin:      dict = Depends(require_admin),
):
    db = get_db()
    query: dict = {}
    if search:
        query["$or"] = [
            {"email":    {"$regex": search, "$options": "i"}},
            {"username": {"$regex": search, "$options": "i"}},
        ]
    if role:   query["role"] = role
    if plan:   query["plan"] = plan
    if user_status == "active":
        query["is_active"] = True
        query["is_banned"] = {"$ne": True}
    elif user_status == "banned":
        query["is_banned"] = True
    elif user_status == "inactive":
        query["is_active"] = False

    skip  = (page - 1) * limit
    total = await db["users"].count_documents(query)
    cursor = db["users"].find(query, {"hashed_password": 0}).sort("created_at", -1).skip(skip).limit(limit)

    users = []
    async for u in cursor:
        users.append({
            "id": str(u["_id"]), "username": u.get("username"), "email": u.get("email"),
            "role": u.get("role", "user"), "is_active": u.get("is_active", True),
            "is_banned": u.get("is_banned", False), "plan": u.get("plan", "free"),
            "coins": u.get("coins", 0), "email_verified": u.get("email_verified", False),
            "created_at": u.get("created_at"), "updated_at": u.get("updated_at"),
        })

    return {"users": users, "total": total, "page": page, "pages": (total + limit - 1) // limit}


@router.get("/users/{user_id}")
async def get_user_detail(user_id: str, _admin: dict = Depends(require_admin)):
    db = get_db()
    try:   oid = ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    user = await db["users"].find_one({"_id": oid}, {"hashed_password": 0})
    if not user: raise HTTPException(status_code=404, detail="User not found.")

    accounts = await db["instagram_accounts"].find({"user_id": user_id}).to_list(length=50)
    account_ids = [str(a["_id"]) for a in accounts]

    automation_count, log_count = await asyncio.gather(
        db["keyword_rules"].count_documents({"account_id": {"$in": account_ids}}),
        db["automation_logs"].count_documents({"account_id": {"$in": account_ids}}),
    )

    return {
        "id": str(user["_id"]), "username": user.get("username"), "email": user.get("email"),
        "role": user.get("role", "user"), "is_active": user.get("is_active", True),
        "is_banned": user.get("is_banned", False), "plan": user.get("plan", "free"),
        "coins": user.get("coins", 0), "email_verified": user.get("email_verified", False),
        "created_at": user.get("created_at"), "updated_at": user.get("updated_at"),
        "instagram_accounts": [
            {
                "id": str(a["_id"]), "username": a.get("username"),
                "instagram_user_id": a.get("instagram_user_id"),
                "is_active": a.get("is_active", False),
                "connected_at": a.get("connected_at"), "token_expires_at": a.get("token_expires_at"),
            }
            for a in accounts
        ],
        "stats": {"ig_accounts": len(accounts), "automations": automation_count, "automation_logs": log_count},
    }


@router.patch("/users/{user_id}")
async def update_user(user_id: str, body: dict, admin: dict = Depends(require_admin)):
    db = get_db()
    try:   oid = ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    allowed = {"role", "is_active", "is_banned", "plan", "coins", "password"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates: raise HTTPException(status_code=400, detail="No valid fields to update.")
    if "role" in updates and admin.get("role") != "superadmin":
        raise HTTPException(status_code=403, detail="Only superadmin can change roles.")
    if "password" in updates:
        updates["hashed_password"] = hash_password(updates.pop("password"))

    updates["updated_at"] = datetime.utcnow()
    result = await db["users"].update_one({"_id": oid}, {"$set": updates})
    if result.matched_count == 0: raise HTTPException(status_code=404, detail="User not found.")
    await invalidate_user_cache(user_id)
    logger.info(f"[ADMIN] User {user_id} updated by {admin['email']}: {list(updates.keys())}")
    return {"ok": True, "updated": list(updates.keys())}


@router.delete("/users/{user_id}")
async def delete_user(user_id: str, admin: dict = Depends(require_admin)):
    if admin.get("role") != "superadmin":
        raise HTTPException(status_code=403, detail="Only superadmin can delete users.")
    db = get_db()
    try:   oid = ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    user = await db["users"].find_one({"_id": oid}, {"email": 1})
    if not user: raise HTTPException(status_code=404, detail="User not found.")

    accounts    = await db["instagram_accounts"].find({"user_id": user_id}, {"_id": 1}).to_list(100)
    account_ids = [str(a["_id"]) for a in accounts]

    await asyncio.gather(
        db["instagram_accounts"].delete_many({"user_id": user_id}),
        db["automation_settings"].delete_many({"account_id": {"$in": account_ids}}),
        db["keyword_rules"].delete_many({"account_id": {"$in": account_ids}}),
        db["automation_logs"].delete_many({"account_id": {"$in": account_ids}}),
        db["scheduled_posts"].delete_many({"user_id": user_id}),
        db["referral_events"].delete_many({"$or": [{"referrer_id": user_id}, {"referred_id": user_id}]}),
        db["coin_transactions"].delete_many({"user_id": user_id}),
        db["admin_notifications"].delete_many({"user_id": user_id}),
        db["users"].delete_one({"_id": oid}),
    )

    await invalidate_user_cache(user_id)
    logger.warning(f"[ADMIN] User {user_id} ({user.get('email')}) DELETED by {admin['email']}")
    return {"ok": True, "deleted_user": user_id}


# ─────────────────────────────────────────────────────────────────────────────
# Force logout
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/users/{user_id}/force-logout")
async def force_logout_user(user_id: str, admin: dict = Depends(require_admin)):
    try:   ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    redis       = get_redis()
    version_key = f"token_version:{user_id}"
    await redis.setex(version_key, 60 * 60 * 24 * 30, str(time.time()))
    await invalidate_user_cache(user_id)
    logger.warning(f"[ADMIN] Force-logout user {user_id} by {admin['email']}")
    return {"ok": True, "message": "All sessions invalidated."}


# ─────────────────────────────────────────────────────────────────────────────
# Impersonate user (superadmin only)
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/users/{user_id}/impersonate")
async def impersonate_user(user_id: str, admin: dict = Depends(require_superadmin)):
    db = get_db()
    try:   oid = ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    target = await db["users"].find_one({"_id": oid}, {"hashed_password": 0})
    if not target: raise HTTPException(status_code=404, detail="User not found.")

    token = create_access_token(
        data={"sub": str(target["_id"]), "impersonated_by": str(admin["_id"])},
        expires_delta=timedelta(minutes=15),
    )
    await db["admin_audit_log"].insert_one({
        "action": "impersonate", "admin_id": str(admin["_id"]),
        "admin_email": admin.get("email"), "target_id": user_id,
        "target_email": target.get("email"), "created_at": datetime.utcnow(),
    })
    logger.warning(f"[ADMIN] Impersonation: {admin['email']} → @{target.get('username')} ({user_id})")
    return {
        "ok": True, "token": token,
        "user": {
            "id": str(target["_id"]), "username": target.get("username"),
            "email": target.get("email"), "plan": target.get("plan", "free"),
            "role": target.get("role", "user"),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Coins management
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/users/{user_id}/coins")
async def grant_coins(user_id: str, body: dict, admin: dict = Depends(require_admin)):
    db = get_db()
    amount = body.get("amount")
    if not isinstance(amount, int): raise HTTPException(status_code=400, detail="amount must be an integer.")
    try:   oid = ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    user = await db["users"].find_one({"_id": oid}, {"username": 1, "coins": 1})
    if not user: raise HTTPException(status_code=404, detail="User not found.")

    result = await db["users"].update_one(
        {"_id": oid},
        {"$inc": {"coins": amount}, "$set": {"updated_at": datetime.utcnow()}}
    )
    if result.matched_count == 0: raise HTTPException(status_code=404, detail="User not found.")

    await db["coin_transactions"].insert_one({
        "user_id": user_id, "amount": amount, "reason": body.get("reason", ""),
        "granted_by": str(admin["_id"]), "granted_by_email": admin.get("email"),
        "balance_before": user.get("coins", 0), "balance_after": user.get("coins", 0) + amount,
        "created_at": datetime.utcnow(),
    })
    await invalidate_user_cache(user_id)
    return {"ok": True, "coins_delta": amount}


@router.get("/users/{user_id}/coins/history")
async def coin_history(
    user_id: str,
    page:    int  = Query(default=1, ge=1),
    limit:   int  = Query(default=20, le=100),
    _admin:  dict = Depends(require_admin),
):
    db = get_db()
    try:   ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    query  = {"user_id": user_id}
    skip   = (page - 1) * limit
    total  = await db["coin_transactions"].count_documents(query)
    cursor = db["coin_transactions"].find(query).sort("created_at", -1).skip(skip).limit(limit)

    transactions = []
    async for t in cursor:
        transactions.append({
            "id": str(t["_id"]), "amount": t.get("amount"), "reason": t.get("reason", ""),
            "granted_by_email": t.get("granted_by_email"), "balance_before": t.get("balance_before"),
            "balance_after": t.get("balance_after"), "created_at": t.get("created_at"),
        })
    return {"transactions": transactions, "total": total, "page": page, "pages": (total + limit - 1) // limit}


# ─────────────────────────────────────────────────────────────────────────────
# User automations
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/users/{user_id}/automations")
async def user_automations(user_id: str, _admin: dict = Depends(require_admin)):
    db = get_db()
    try:   ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    accounts    = await db["instagram_accounts"].find({"user_id": user_id}, {"_id": 1, "username": 1}).to_list(50)
    account_map = {str(a["_id"]): a.get("username", "unknown") for a in accounts}
    account_ids = list(account_map.keys())

    cursor = db["keyword_rules"].find({"account_id": {"$in": account_ids}}).sort("created_at", -1)

    automations = []
    async for rule in cursor:
        automations.append({
            "id": str(rule["_id"]), "account_id": rule.get("account_id"),
            "account_username": account_map.get(rule.get("account_id", ""), "unknown"),
            "post_id": rule.get("post_id"), "trigger_words": rule.get("trigger_words", []),
            "is_active": rule.get("is_active", True),
            "auto_comment_reply": rule.get("reply_comment", False),
            "auto_dm": rule.get("send_dm", False), "created_at": rule.get("created_at"),
        })
    return {"automations": automations, "total": len(automations)}


@router.patch("/automations/{rule_id}/toggle")
async def toggle_automation(rule_id: str, body: dict, admin: dict = Depends(require_admin)):
    db = get_db()
    try:   oid = ObjectId(rule_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid rule ID.")

    is_active = body.get("is_active")
    if not isinstance(is_active, bool): raise HTTPException(status_code=400, detail="is_active must be boolean.")

    result = await db["keyword_rules"].update_one(
        {"_id": oid}, {"$set": {"is_active": is_active, "updated_at": datetime.utcnow()}}
    )
    if result.matched_count == 0: raise HTTPException(status_code=404, detail="Automation not found.")
    logger.info(f"[ADMIN] Automation {rule_id} set is_active={is_active} by {admin['email']}")
    return {"ok": True, "is_active": is_active}


# ─────────────────────────────────────────────────────────────────────────────
# User notifications
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/users/{user_id}/notify")
async def notify_user(user_id: str, body: dict, admin: dict = Depends(require_admin)):
    db = get_db()
    try:   ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    message = body.get("message", "").strip()
    if not message: raise HTTPException(status_code=400, detail="message is required.")

    await db["admin_notifications"].insert_one({
        "user_id": user_id, "message": message, "sent_by": str(admin["_id"]),
        "sent_by_email": admin.get("email"), "is_read": False, "created_at": datetime.utcnow(),
    })
    return {"ok": True}


@router.get("/users/{user_id}/notifications")
async def get_user_notifications(user_id: str, _admin: dict = Depends(require_admin)):
    db = get_db()
    cursor = db["admin_notifications"].find({"user_id": user_id}).sort("created_at", -1).limit(50)
    notifications = []
    async for n in cursor:
        notifications.append({
            "id": str(n["_id"]), "message": n.get("message"),
            "sent_by_email": n.get("sent_by_email"),
            "is_read": n.get("is_read", False), "created_at": n.get("created_at"),
        })
    return {"notifications": notifications}


# ─────────────────────────────────────────────────────────────────────────────
# User logs (upgraded — full fields, filters, stats)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/users/{user_id}/logs")
async def user_logs(
    user_id:    str,
    page:       int  = Query(default=1, ge=1),
    limit:      int  = Query(default=50, le=200),
    log_status: str  = Query(default="", alias="status"),
    action:     str  = Query(default=""),
    _admin:     dict = Depends(require_admin),
):
    """Full automation logs for a specific user with filters and summary stats."""
    db = get_db()
    try:   ObjectId(user_id)
    except Exception: raise HTTPException(status_code=400, detail="Invalid user ID.")

    accounts    = await db["instagram_accounts"].find({"user_id": user_id}, {"_id": 1, "username": 1}).to_list(50)
    account_map = {str(a["_id"]): a.get("username", "unknown") for a in accounts}
    account_ids = list(account_map.keys())

    # Base query
    query: dict = {"account_id": {"$in": account_ids}}
    if log_status: query["status"]  = log_status
    if action:     query["action"]  = {"$regex": action, "$options": "i"}

    skip  = (page - 1) * limit
    total = await db["automation_logs"].count_documents(query)
    cursor = db["automation_logs"].find(query).sort(
        [("timestamp", -1), ("created_at", -1)]
    ).skip(skip).limit(limit)

    # Summary stats (parallel)
    base_query = {"account_id": {"$in": account_ids}}
    success_count, error_count, dm_count, reply_count = await asyncio.gather(
        db["automation_logs"].count_documents({**base_query, "status": "success"}),
        db["automation_logs"].count_documents({**base_query, "status": "error"}),
        db["automation_logs"].count_documents({**base_query, "dm_sent": True}),
        db["automation_logs"].count_documents({**base_query, "reply_sent": True}),
    )

    logs = []
    async for log in cursor:
        # Resolve IG account username
        acc_username = account_map.get(log.get("account_id", ""), None)

        # Prefer timestamp field (set by webhook engine), fall back to created_at
        log_time = log.get("timestamp") or log.get("created_at")

        logs.append({
            "id":              str(log["_id"]),
            "account_id":      log.get("account_id"),
            "account_username": acc_username,
            "post_id":         log.get("post_id"),
            "automation_id":   log.get("automation_id"),
            "action":          log.get("action") or log.get("action_taken"),
            "status":          log.get("status"),
            "comment_text":    log.get("comment_text"),
            "commenter":       log.get("commenter_username") or log.get("commenter"),
            "reply_sent":      log.get("reply_sent", False),
            "dm_sent":         log.get("dm_sent", False),
            "error":           log.get("error"),
            "trigger_word":    log.get("trigger_word") or log.get("matched_keyword"),
            "timestamp":       log_time,
        })

    return {
        "logs":  logs,
        "total": total,
        "page":  page,
        "pages": (total + limit - 1) // limit,
        "stats": {
            "total":   await db["automation_logs"].count_documents(base_query),
            "success": success_count,
            "error":   error_count,
            "dms":     dm_count,
            "replies": reply_count,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bulk actions
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/users/bulk")
async def bulk_action(body: dict, admin: dict = Depends(require_admin)):
    db       = get_db()
    user_ids = body.get("user_ids", [])
    action   = body.get("action")
    value    = body.get("value")

    if not user_ids or not action:
        raise HTTPException(status_code=400, detail="user_ids and action are required.")

    oids = []
    for uid in user_ids:
        try:   oids.append(ObjectId(uid))
        except Exception: raise HTTPException(status_code=400, detail=f"Invalid user ID: {uid}")

    updates: dict = {"updated_at": datetime.utcnow()}
    if action == "ban":          updates["is_banned"] = True
    elif action == "unban":      updates["is_banned"] = False
    elif action == "set_plan":
        if value not in ("free", "pro", "enterprise"):
            raise HTTPException(status_code=400, detail="Invalid plan value.")
        updates["plan"] = value
    elif action == "deactivate": updates["is_active"] = False
    elif action == "activate":   updates["is_active"] = True
    else: raise HTTPException(status_code=400, detail=f"Unknown action: {action}")

    result = await db["users"].update_many({"_id": {"$in": oids}}, {"$set": updates})
    await asyncio.gather(*[invalidate_user_cache(uid) for uid in user_ids])
    logger.info(f"[ADMIN] Bulk action '{action}' on {result.modified_count} users by {admin['email']}")
    return {"ok": True, "modified": result.modified_count}


# ─────────────────────────────────────────────────────────────────────────────
# Instagram accounts overview
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/accounts")
async def list_all_accounts(
    page:   int  = Query(default=1, ge=1),
    limit:  int  = Query(default=20, le=100),
    search: str  = Query(default=""),
    _admin: dict = Depends(require_admin),
):
    db = get_db()
    query: dict = {}
    if search: query["username"] = {"$regex": search, "$options": "i"}

    skip  = (page - 1) * limit
    total = await db["instagram_accounts"].count_documents(query)
    cursor = db["instagram_accounts"].find(query).sort("connected_at", -1).skip(skip).limit(limit)

    accounts = []
    async for a in cursor:
        accounts.append({
            "id": str(a["_id"]), "user_id": a.get("user_id"), "username": a.get("username"),
            "instagram_user_id": a.get("instagram_user_id"), "is_active": a.get("is_active", False),
            "connected_at": a.get("connected_at"), "token_expires_at": a.get("token_expires_at"),
        })
    return {"accounts": accounts, "total": total, "page": page, "pages": (total + limit - 1) // limit}


# ─────────────────────────────────────────────────────────────────────────────
# Global Automation Logs — UPGRADED
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/logs")
async def list_logs(
    page:       int  = Query(default=1, ge=1),
    limit:      int  = Query(default=50, le=200),
    account_id: str  = Query(default=""),
    user_id:    str  = Query(default=""),
    log_status: str  = Query(default="", alias="status"),
    action:     str  = Query(default=""),
    dm_only:    bool = Query(default=False),
    error_only: bool = Query(default=False),
    _admin:     dict = Depends(require_admin),
):
    """
    Rich automation logs with full fields, filters, and summary stats.
    Filters: status, action keyword, dm_only, error_only, user_id, account_id.
    """
    db    = get_db()
    query: dict = {}

    if account_id:
        query["account_id"] = account_id
    elif user_id:
        accounts    = await db["instagram_accounts"].find({"user_id": user_id}, {"_id": 1}).to_list(50)
        account_ids = [str(a["_id"]) for a in accounts]
        query["account_id"] = {"$in": account_ids}

    if log_status: query["status"]   = log_status
    if action:     query["action"]   = {"$regex": action, "$options": "i"}
    if dm_only:    query["dm_sent"]  = True
    if error_only: query["status"]   = "error"

    skip  = (page - 1) * limit
    total = await db["automation_logs"].count_documents(query)
    cursor = db["automation_logs"].find(query).sort(
        [("timestamp", -1), ("created_at", -1)]
    ).skip(skip).limit(limit)

    # Resolve account_id → IG username (bulk)
    raw_logs = []
    account_ids_needed = set()
    async for log in cursor:
        raw_logs.append(log)
        if log.get("account_id"):
            account_ids_needed.add(log["account_id"])

    # Bulk fetch account usernames
    account_username_map: dict = {}
    if account_ids_needed:
        try:
            acc_oids = [ObjectId(x) for x in account_ids_needed if ObjectId.is_valid(x)]
            acc_cursor = db["instagram_accounts"].find(
                {"_id": {"$in": acc_oids}}, {"_id": 1, "username": 1}
            )
            async for a in acc_cursor:
                account_username_map[str(a["_id"])] = a.get("username")
        except Exception:
            pass

    logs = []
    for log in raw_logs:
        log_time = log.get("timestamp") or log.get("created_at")
        logs.append({
            "id":               str(log["_id"]),
            "account_id":       log.get("account_id"),
            "account_username": account_username_map.get(log.get("account_id", ""), None),
            "post_id":          log.get("post_id"),
            "automation_id":    log.get("automation_id"),
            "action":           log.get("action") or log.get("action_taken"),
            "status":           log.get("status"),
            "comment_text":     log.get("comment_text"),
            "commenter":        log.get("commenter_username") or log.get("commenter"),
            "reply_sent":       log.get("reply_sent", False),
            "dm_sent":          log.get("dm_sent", False),
            "error":            log.get("error"),
            "trigger_word":     log.get("trigger_word") or log.get("matched_keyword"),
            "timestamp":        log_time,
        })

    # Summary stats
    base: dict = {}
    if account_id:  base["account_id"] = account_id
    elif user_id:   base["account_id"] = query.get("account_id", {})

    success_count, error_count, dm_count, reply_count = await asyncio.gather(
        db["automation_logs"].count_documents({**base, "status": "success"}),
        db["automation_logs"].count_documents({**base, "status": "error"}),
        db["automation_logs"].count_documents({**base, "dm_sent": True}),
        db["automation_logs"].count_documents({**base, "reply_sent": True}),
    )

    return {
        "logs":  logs,
        "total": total,
        "page":  page,
        "pages": (total + limit - 1) // limit,
        "stats": {
            "success": success_count,
            "error":   error_count,
            "dms":     dm_count,
            "replies": reply_count,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap superadmin  (Bug #12 fix)
# ─────────────────────────────────────────────────────────────────────────────
# Old vulnerability: endpoint was live with only an "admins already exist" guard.
# On a fresh DB (admin_count == 0), ANY authenticated user who discovered the
# hidden URL could promote themselves to superadmin.
#
# Fix: require a BOOTSTRAP_SECRET value in the request body that must match
# the BOOTSTRAP_SECRET environment variable.  If the env var is not set the
# endpoint is unconditionally disabled (returns 403) so it can never be used
# accidentally in production.
# ─────────────────────────────────────────────────────────────────────────────

class BootstrapRequest(BaseModel):
    secret: str


@router.post("/bootstrap-superadmin", include_in_schema=False)
async def bootstrap_superadmin(
    body: BootstrapRequest,
    current_user: dict = Depends(get_current_user),
):
    from app.config import settings as _cfg

    # 1. Endpoint is disabled unless BOOTSTRAP_SECRET is explicitly configured.
    bootstrap_secret = getattr(_cfg, "BOOTSTRAP_SECRET", "").strip()
    if not bootstrap_secret:
        raise HTTPException(
            status_code=403,
            detail="Bootstrap endpoint is disabled. Set BOOTSTRAP_SECRET in .env to enable it.",
        )

    # 2. Caller must supply the correct secret.
    if body.secret != bootstrap_secret:
        logger.warning(
            f"[BOOTSTRAP] Failed attempt by {current_user.get('email')} "
            f"-- wrong secret"
        )
        raise HTTPException(status_code=403, detail="Invalid bootstrap secret.")

    db = get_db()
    admin_count = await db["users"].count_documents({"role": {"$in": ["admin", "superadmin"]}})
    if admin_count > 0:
        raise HTTPException(status_code=403, detail="Admins already exist.")

    await db["users"].update_one(
        {"_id": ObjectId(str(current_user["_id"]))},
        {"$set": {"role": "superadmin", "updated_at": datetime.utcnow()}}
    )
    await invalidate_user_cache(str(current_user["_id"]))
    logger.warning(f"[BOOTSTRAP] {current_user['email']} promoted to superadmin")
    return {"ok": True, "message": "You are now superadmin."}
