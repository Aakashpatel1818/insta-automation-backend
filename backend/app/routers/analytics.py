import logging
import csv
import io
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from app.dependencies import get_current_user, get_ig_account
from app.database import get_db
from app.plans import get_plan_limits
from app.schemas.analytics import AccountInsights, PostInsights, PostSummary
from app.services.analytics_service import (
    get_account_insights,
    get_post_insights,
    get_all_posts_insights,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["Analytics"])

PERIOD_DAYS = {"day": 1, "week": 7, "month": 30, "year": 365}


# ── Account insights ──────────────────────────────────────
@router.get("/account", response_model=AccountInsights)
async def account_insights(
    period: str = Query(default="week", enum=["day", "week", "month", "year"]),
    account_id: str = Query(default=None, description="Account ID. Uses active account if not specified."),
    current_user: dict = Depends(get_current_user),
):
    # Enforce analytics_days plan limit
    plan         = current_user.get("plan", "free")
    limits       = get_plan_limits(plan)
    allowed_days = limits["analytics_days"]   # 7, 30, or 365

    requested_days = PERIOD_DAYS.get(period, 7)
    if requested_days > allowed_days:
        raise HTTPException(
            status_code=403,
            detail=f"Your {plan} plan supports up to {allowed_days} days of analytics history. "
                   f"Upgrade to access the '{period}' period.",
        )

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


# ── Analytics export (Enterprise only) ───────────────────
@router.get("/export")
async def export_analytics(
    account_id: str = Query(default=None),
    current_user: dict = Depends(get_current_user),
):
    """Export full post analytics as CSV. Enterprise plan only."""
    plan   = current_user.get("plan", "free")
    limits = get_plan_limits(plan)
    if not limits["analytics_export"]:
        raise HTTPException(
            status_code=403,
            detail=f"Analytics export is not available on the {plan} plan. Upgrade to Enterprise.",
        )

    account = await get_ig_account(current_user, account_id)
    try:
        posts = await get_all_posts_insights(
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "post_id", "caption", "media_type",
        "like_count", "comments_count", "share_count",
        "reach", "impressions", "timestamp",
    ])
    for p in posts:
        writer.writerow([
            p.get("post_id", ""),
            (p.get("caption") or "").replace("\n", " ")[:200],
            p.get("media_type", ""),
            p.get("like_count", 0),
            p.get("comments_count", 0),
            p.get("share_count", 0),
            p.get("reach", ""),
            p.get("impressions", ""),
            p.get("timestamp", ""),
        ])

    output.seek(0)
    filename = f"analytics_{account['username']}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


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
