import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_user
from app.database import get_db
from app.analytics.pg_database import get_pg_session
from app.analytics.service import get_account_analytics, get_post_analytics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics/v2", tags=["Analytics V2 (PostgreSQL)"])


async def _get_ig_account(current_user: dict, account_id: str | None, mongo_db):
    """Load active or specific IG account from MongoDB."""
    from bson import ObjectId
    user_id = str(current_user["_id"])

    if account_id:
        try:
            account = await mongo_db["instagram_accounts"].find_one({
                "_id": ObjectId(account_id), "user_id": user_id
            })
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid account ID")
    else:
        account = await mongo_db["instagram_accounts"].find_one(
            {"user_id": user_id, "is_active": True}
        )
        if not account:
            account = await mongo_db["instagram_accounts"].find_one({"user_id": user_id})

    if not account:
        raise HTTPException(status_code=404, detail="No Instagram account connected.")
    return account


# ── Account Analytics ─────────────────────────────────────
@router.get("/account")
async def account_analytics(
    period: str = Query(default="month", enum=["day", "week", "month", "year"]),
    account_id: str = Query(default=None),
    current_user: dict = Depends(get_current_user),
    pg: AsyncSession = Depends(get_pg_session),
):
    """
    Account-level analytics.
    - day/week/month → 1 Instagram API call
    - year           → DB aggregation (no 12x API calls)
    - Rate limited   → returns cached data
    """
    mongo_db = get_db()
    account = await _get_ig_account(current_user, account_id, mongo_db)

    try:
        result = await get_account_analytics(
            session=pg,
            account_id=str(account["_id"]),
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
            period=period,
        )
        return {
            "username": account.get("username"),
            "account_id": str(account["_id"]),
            **result,
        }
    except Exception as e:
        logger.error(f"Account analytics failed: {e}")
        raise HTTPException(status_code=502, detail=f"Analytics error: {str(e)}")


# ── Post Analytics ────────────────────────────────────────
@router.get("/post/{post_id}")
async def post_analytics(
    post_id: str,
    period: str = Query(default="lifetime", enum=["day", "week", "month", "lifetime"]),
    account_id: str = Query(default=None),
    current_user: dict = Depends(get_current_user),
    pg: AsyncSession = Depends(get_pg_session),
):
    """
    Post-level analytics.
    - lifetime → 24h cache (max 1 API call/day)
    - day/week/month → API call with rate limit check
    """
    mongo_db = get_db()
    account = await _get_ig_account(current_user, account_id, mongo_db)

    try:
        result = await get_post_analytics(
            session=pg,
            post_id=post_id,
            account_id=str(account["_id"]),
            access_token=account["access_token"],
            period=period,
        )
        return result
    except Exception as e:
        logger.error(f"Post analytics failed: {e}")
        raise HTTPException(status_code=502, detail=f"Analytics error: {str(e)}")
