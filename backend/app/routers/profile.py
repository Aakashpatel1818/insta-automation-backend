# routers/profile.py
"""
User profile & settings endpoints.
- GET    /profile/me          → full profile
- PATCH  /profile/me         → update username
- POST   /profile/change-password
- POST   /profile/logout-all  → invalidate all sessions (Redis blocklist)
- DELETE /profile/me          → permanently delete account + all data
"""
from fastapi import APIRouter, Depends, HTTPException, status
from datetime import datetime
from pydantic import BaseModel
from bson import ObjectId
import logging

from app.database import get_db
from app.dependencies import get_current_user, invalidate_user_cache
from app.security import verify_password, hash_password, create_access_token
from app.redis_pool import get_redis
from app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/profile", tags=["Profile"])


# ── Schemas ───────────────────────────────────────────────

class UpdateProfileRequest(BaseModel):
    username: str | None = None

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


# ── GET /profile/me ───────────────────────────────────────

@router.get("/me")
async def get_profile(current_user: dict = Depends(get_current_user)):
    db  = get_db()
    uid = str(current_user["_id"])

    # Count linked IG accounts
    ig_count = await db["instagram_accounts"].count_documents({"user_id": uid})
    auto_count = await db["automation_settings"].count_documents({})

    return {
        "id":             uid,
        "username":       current_user.get("username"),
        "email":          current_user.get("email"),
        "role":           current_user.get("role", "user"),
        "plan":           current_user.get("plan", "free"),
        "coins":          current_user.get("coins", 0),
        "is_active":      current_user.get("is_active", True),
        "email_verified": current_user.get("email_verified", True),
        "created_at":     current_user.get("created_at"),
        "updated_at":     current_user.get("updated_at"),
        "stats": {
            "ig_accounts": ig_count,
        },
    }


# ── PATCH /profile/me ─────────────────────────────────────

@router.patch("/me")
async def update_profile(
    body: UpdateProfileRequest,
    current_user: dict = Depends(get_current_user),
):
    db  = get_db()
    uid = str(current_user["_id"])
    updates: dict = {}

    if body.username:
        username = body.username.strip()
        if len(username) < 3:
            raise HTTPException(status_code=400, detail="Username must be at least 3 characters.")
        # Check uniqueness
        existing = await db["users"].find_one(
            {"username": username, "_id": {"$ne": ObjectId(uid)}}
        )
        if existing:
            raise HTTPException(status_code=400, detail="Username already taken.")
        updates["username"] = username

    if not updates:
        raise HTTPException(status_code=400, detail="Nothing to update.")

    updates["updated_at"] = datetime.utcnow()
    await db["users"].update_one({"_id": ObjectId(uid)}, {"$set": updates})
    await invalidate_user_cache(uid)
    logger.info(f"[Profile] User {uid} updated: {list(updates.keys())}")
    return {"ok": True, "updated": list(updates.keys())}


# ── POST /profile/change-password ────────────────────────

@router.post("/change-password")
async def change_password(
    body: ChangePasswordRequest,
    current_user: dict = Depends(get_current_user),
):
    db  = get_db()
    uid = str(current_user["_id"])

    # Re-fetch from DB to get hashed_password (not in cache)
    user = await db["users"].find_one({"_id": ObjectId(uid)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    if not verify_password(body.current_password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect.")

    if len(body.new_password) < 6:
        raise HTTPException(status_code=400, detail="New password must be at least 6 characters.")

    if body.current_password == body.new_password:
        raise HTTPException(status_code=400, detail="New password must be different from current.")

    await db["users"].update_one(
        {"_id": ObjectId(uid)},
        {"$set": {"hashed_password": hash_password(body.new_password), "updated_at": datetime.utcnow()}}
    )
    await invalidate_user_cache(uid)

    # Increment token version to invalidate all existing JWTs
    await _bump_token_version(uid)

    logger.info(f"[Profile] Password changed for user {uid}")
    return {"ok": True, "message": "Password changed. Please log in again."}


# ── POST /profile/logout-all ──────────────────────────────

@router.post("/logout-all")
async def logout_all_devices(current_user: dict = Depends(get_current_user)):
    """Invalidates ALL tokens for this user by bumping their token version in Redis."""
    uid = str(current_user["_id"])
    await _bump_token_version(uid)
    await invalidate_user_cache(uid)
    logger.info(f"[Profile] All sessions invalidated for user {uid}")
    return {"ok": True, "message": "All sessions have been logged out."}


# ── DELETE /profile/me ───────────────────────────────────

class DeleteAccountRequest(BaseModel):
    confirmation: str   # must equal "DELETE"
    password: str       # must match current password


@router.delete("/me")
async def delete_account(
    body: DeleteAccountRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Permanently delete the authenticated user's account and ALL associated data.
    Requires typing 'DELETE' and providing current password as confirmation.
    """
    if body.confirmation != "DELETE":
        raise HTTPException(status_code=400, detail="Type DELETE to confirm.")

    db  = get_db()
    uid = str(current_user["_id"])

    # Re-fetch from DB to verify password (not stored in cache)
    user = await db["users"].find_one({"_id": ObjectId(uid)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    if not verify_password(body.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect password.")

    logger.info(f"[DeleteAccount] Starting cascade delete for user={uid}")

    # ── 1. Flush all Redis comment queues for this user's accounts ────────────
    try:
        r = get_redis()
        accounts = await db["instagram_accounts"].find(
            {"user_id": uid}, {"_id": 1}
        ).to_list(length=50)
        for acc in accounts:
            acc_id = str(acc["_id"])
            await r.delete(f"comment_queue:{acc_id}")
            await r.srem("comment_queue:accounts", acc_id)
        logger.info(f"[DeleteAccount] Flushed {len(accounts)} Redis queues")
    except Exception as e:
        logger.warning(f"[DeleteAccount] Redis flush failed (non-fatal): {e}")

    # ── 2. Collect all account_ids for bulk deletes ───────────────────────────
    account_ids = [str(a["_id"]) for a in accounts]

    # ── 3. Delete all automation data ────────────────────────────────────────
    # Get automation_ids first so we can delete rules by automation_id
    auto_docs = await db["automation_settings"].find(
        {"user_id": uid}, {"_id": 1}
    ).to_list(length=500)
    auto_ids = [str(a["_id"]) for a in auto_docs]

    if auto_ids:
        await db["keyword_rules"].delete_many({"automation_id": {"$in": auto_ids}})
    await db["keyword_rules"].delete_many({"user_id": uid})
    await db["automation_settings"].delete_many({"user_id": uid})
    await db["automation_analytics"].delete_many({"account_id": {"$in": account_ids}})
    await db["automation_logs"].delete_many({"account_id": {"$in": account_ids}})
    await db["cooldown_logs"].delete_many({"account_id": {"$in": account_ids}})
    logger.info(f"[DeleteAccount] Automation data deleted")

    # ── 4. Delete leads & collected users ─────────────────────────────────────
    await db["leads"].delete_many({"user_id": uid})
    await db["collected_users"].delete_many({"account_id": {"$in": account_ids}})
    logger.info(f"[DeleteAccount] Leads & collected users deleted")

    # ── 5. Delete DMs, scheduled posts ───────────────────────────────────────
    await db["dm_messages"].delete_many({"account_id": {"$in": account_ids}})
    await db["scheduled_posts"].delete_many({"user_id": uid})
    logger.info(f"[DeleteAccount] DMs & scheduled posts deleted")

    # ── 6. Delete referral data ───────────────────────────────────────────────
    await db["referrals"].delete_many(
        {"$or": [{"referrer_id": uid}, {"referee_id": uid}]}
    )
    logger.info(f"[DeleteAccount] Referral data deleted")

    # ── 7. Delete Instagram accounts ─────────────────────────────────────────
    await db["instagram_accounts"].delete_many({"user_id": uid})
    logger.info(f"[DeleteAccount] Instagram accounts deleted")

    # ── 8. Invalidate all Redis session tokens + user cache ───────────────────
    try:
        r = get_redis()
        await r.delete(f"user:{uid}")
        await r.delete(f"token_version:{uid}")
    except Exception as e:
        logger.warning(f"[DeleteAccount] Redis cleanup failed (non-fatal): {e}")

    # ── 9. Delete the user document itself ───────────────────────────────────
    await db["users"].delete_one({"_id": ObjectId(uid)})
    logger.info(f"[DeleteAccount] User {uid} permanently deleted ✅")

    return {"ok": True, "message": "Account permanently deleted."}


# ── Helper ────────────────────────────────────────────────

async def _bump_token_version(user_id: str):
    """Store a 'token invalidated at' timestamp in Redis.
    get_current_user checks this and rejects tokens issued before this time."""
    redis = get_redis()
    key   = f"token_version:{user_id}"
    await redis.set(key, datetime.utcnow().timestamp())
