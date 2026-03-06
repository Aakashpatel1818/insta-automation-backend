import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from bson import ObjectId

from app.dependencies import get_current_user
from app.database import get_db
from app.schemas.analytics import AccountInsights, PostInsights, PostSummary
from app.services.analytics_service import (
    get_account_insights,
    get_post_insights,
    get_all_posts_insights,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


async def get_ig_account(current_user: dict, account_id: str = None):
    """Load specific or active IG account."""
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


# ── Account insights ──────────────────────────────────────
@router.get("/account", response_model=AccountInsights)
async def account_insights(
    period: str = Query(default="month", enum=["day", "week", "month"]),
    account_id: str = Query(default=None, description="Account ID. Uses active account if not specified."),
    current_user: dict = Depends(get_current_user),
):
    account = await get_ig_account(current_user, account_id)
    try:
        data = await get_account_insights(
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
            period=period,
        )
    except Exception as e:
        logger.error(f"Account insights failed: {e}")
        raise HTTPException(status_code=502, detail=str(e))

    return AccountInsights(**data)


# ── All posts ─────────────────────────────────────────────
@router.get("/posts", response_model=List[PostSummary])
async def all_posts_insights(
    account_id: str = Query(default=None),
    current_user: dict = Depends(get_current_user),
):
    account = await get_ig_account(current_user, account_id)
    try:
        posts = await get_all_posts_insights(
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
        )
    except Exception as e:
        logger.error(f"All posts failed: {e}")
        raise HTTPException(status_code=502, detail=str(e))

    return [PostSummary(**p) for p in posts]


# ── Single post insights ──────────────────────────────────
@router.get("/post/{post_id}", response_model=PostInsights)
async def single_post_insights(
    post_id: str,
    period: str = Query(default="lifetime", enum=["day", "week", "month", "lifetime"]),
    account_id: str = Query(default=None),
    current_user: dict = Depends(get_current_user),
):
    account = await get_ig_account(current_user, account_id)
    try:
        data = await get_post_insights(
            post_id=post_id,
            access_token=account["access_token"],
            period=period,
        )
    except Exception as e:
        logger.error(f"Post insights failed: {e}")
        raise HTTPException(status_code=502, detail=str(e))

    return PostInsights(**data)
