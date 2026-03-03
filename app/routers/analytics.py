import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
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


async def get_ig_account(current_user: dict):
    db = get_db()
    account = await db["instagram_accounts"].find_one(
        {"user_id": str(current_user["_id"])}
    )
    if not account:
        raise HTTPException(
            status_code=404,
            detail="No Instagram account connected."
        )
    return account


# ── Account insights ──────────────────────────────────────
@router.get("/account", response_model=AccountInsights)
async def account_insights(
    period: str = Query(default="month", enum=["day", "week", "month"]),
    current_user: dict = Depends(get_current_user),
):
    """
    Account-level stats.
    ?period=day | week | month
    """
    account = await get_ig_account(current_user)
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
    current_user: dict = Depends(get_current_user),
):
    """All posts with basic metrics."""
    account = await get_ig_account(current_user)
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
    current_user: dict = Depends(get_current_user),
):
    """Deep insights for one post. period: lifetime | day | week | month"""
    account = await get_ig_account(current_user)
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
