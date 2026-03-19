from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from app.security import decode_access_token
from app.database import get_db
from app.redis_pool import get_redis
from app.config import settings
from bson import ObjectId
import logging
import json

logger = logging.getLogger(__name__)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def _serialize_user(user: dict) -> dict:
    """Convert MongoDB user doc to JSON-safe dict (ObjectId → str)."""
    return {**user, "_id": str(user["_id"])}


def _deserialize_user(user: dict) -> dict:
    """Restore user dict from Redis cache — _id stays as str, which is fine."""
    return user


async def invalidate_user_cache(user_id: str) -> None:
    """
    Call this whenever you deactivate/update a user so the cache
    doesn't serve stale data for up to USER_CACHE_TTL seconds.
    Usage: await invalidate_user_cache(str(user["_id"]))
    """
    try:
        redis = get_redis()
        await redis.delete(f"user:{user_id}")
        logger.debug(f"User cache invalidated for {user_id}")
    except Exception as e:
        logger.warning(f"Failed to invalidate user cache: {e}")


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    payload = decode_access_token(token)
    if payload is None:
        raise credentials_exception

    user_id: str = payload.get("sub")
    if user_id is None:
        raise credentials_exception

    # ── ✅ FIX 1: Check token version (logout-all invalidation) ──────────────
    try:
        redis = get_redis()
        version_key = f"token_version:{user_id}"
        token_invalidated_at = await redis.get(version_key)
        if token_invalidated_at:
            # JWT payload has 'iat' (issued-at) in seconds
            token_iat = payload.get("iat", 0)
            if token_iat < float(token_invalidated_at):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Session has been invalidated. Please log in again.",
                    headers={"WWW-Authenticate": "Bearer"},
                )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Token version check failed (non-fatal): {e}")

    # ── ✅ FIX 2: Try Redis cache first ──────────────────────────────────────
    try:
        redis = get_redis()
        cache_key = f"user:{user_id}"
        cached = await redis.get(cache_key)
        if cached:
            user = json.loads(cached)
            # Still enforce is_active from cache — cache is invalidated on deactivation
            if not user.get("is_active", True):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Account is inactive"
                )
            logger.debug(f"User cache HIT for {user_id}")
            return user
    except HTTPException:
        raise
    except Exception as e:
        # Redis failure is non-fatal — fall through to MongoDB
        logger.warning(f"Redis cache read failed, falling back to DB: {e}")

    # ── Cache miss → hit MongoDB ──────────────────────────────────────────────
    db = get_db()
    try:
        user = await db["users"].find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise credentials_exception

    if user is None:
        raise credentials_exception

    if not user.get("is_active", True):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is inactive"
        )

    # ── Store in Redis for USER_CACHE_TTL seconds ────────────────────────────
    try:
        redis = get_redis()
        serialized = _serialize_user(user)
        await redis.setex(
            f"user:{user_id}",
            settings.USER_CACHE_TTL,
            json.dumps(serialized, default=str)
        )
        logger.debug(f"User cache SET for {user_id}")
    except Exception as e:
        logger.warning(f"Redis cache write failed (non-fatal): {e}")

    return user


async def get_ig_account(current_user: dict, account_id: str | None = None, db=None):
    """
    Shared helper: load a specific or active Instagram account for the current user.
    Used by both routers/analytics.py and analytics/router.py to avoid duplication.
    Pass db explicitly if already in scope, otherwise it fetches one.
    """
    if db is None:
        db = get_db()
    user_id = str(current_user["_id"])

    if account_id:
        try:
            account = await db["instagram_accounts"].find_one({
                "_id": ObjectId(account_id), "user_id": user_id
            })
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid account ID")
    else:
        account = await db["instagram_accounts"].find_one(
            {"user_id": user_id, "is_active": True}
        )
        if not account:
            account = await db["instagram_accounts"].find_one({"user_id": user_id})

    if not account:
        raise HTTPException(status_code=404, detail="No Instagram account connected.")
    return account
