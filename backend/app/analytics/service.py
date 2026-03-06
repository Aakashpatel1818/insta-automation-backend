import logging
import httpx
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.analytics.models import AccountMonthlyInsights, PostInsightsCache, ApiUsageLog

logger = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.instagram.com/v19.0"
API_RATE_LIMIT = 150   # max calls per hour per account
CACHE_TTL_HOURS = 24   # post cache TTL


# ═══════════════════════════════════════════════════════════
# RATE LIMIT
# ═══════════════════════════════════════════════════════════

async def check_rate_limit(session: AsyncSession, account_id: str) -> bool:
    """
    Returns True if rate limit exceeded.
    Max 150 API calls per hour per account.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    result = await session.execute(
        select(func.count(ApiUsageLog.id)).where(
            ApiUsageLog.account_id == account_id,
            ApiUsageLog.timestamp >= cutoff,
        )
    )
    count = result.scalar_one()
    if count >= API_RATE_LIMIT:
        logger.warning(f"Rate limit hit for account {account_id}: {count} calls/hr")
        return True
    return False


async def log_api_call(session: AsyncSession, account_id: str, endpoint: str):
    """Log an Instagram API call."""
    session.add(ApiUsageLog(account_id=account_id, endpoint=endpoint))
    await session.commit()


# ═══════════════════════════════════════════════════════════
# INSTAGRAM API HELPERS
# ═══════════════════════════════════════════════════════════

async def _fetch_account_insights_api(
    ig_user_id: str,
    access_token: str,
    period: str,
) -> dict:
    """Single Instagram API call for account insights."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        profile_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}",
            params={
                "fields":       "id,username,followers_count,media_count",
                "access_token": access_token,
            }
        )
        profile = profile_resp.json()

        insights_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}/insights",
            params={
                "metric":       "reach,impressions,profile_views,follower_count",
                "period":       period,
                "access_token": access_token,
            }
        )
        insights = insights_resp.json()

    metrics = {}
    for item in insights.get("data", []):
        values = item.get("values", [])
        metrics[item["name"]] = values[-1].get("value", 0) if values else 0

    return {
        "username":       profile.get("username", ""),
        "followers":      profile.get("followers_count", 0),
        "media_count":    profile.get("media_count", 0),
        "reach":          metrics.get("reach", 0),
        "impressions":    metrics.get("impressions", 0),
        "profile_views":  metrics.get("profile_views", 0),
        "follower_count": metrics.get("follower_count", 0),
    }


async def _fetch_post_insights_api(
    post_id: str,
    access_token: str,
    period: str,
) -> dict:
    """Single Instagram API call for post insights."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        post_resp = await client.get(
            f"{GRAPH_BASE}/{post_id}",
            params={
                "fields":       "id,like_count,comments_count,media_type",
                "access_token": access_token,
            }
        )
        post_data = post_resp.json()

        media_type = post_data.get("media_type", "IMAGE")
        if media_type == "REELS":
            metrics_list = "plays,reach,likes,comments,shares,saved,total_interactions"
        else:
            metrics_list = "impressions,reach,likes,comments,shares,saved,total_interactions"

        insights_resp = await client.get(
            f"{GRAPH_BASE}/{post_id}/insights",
            params={
                "metric":       metrics_list,
                "period":       period,
                "access_token": access_token,
            }
        )
        insights = insights_resp.json()

    metrics = {}
    for item in insights.get("data", []):
        val = item.get("values", [{}])[0].get("value", 0) if item.get("values") else item.get("value", 0)
        metrics[item.get("name")] = val

    likes    = metrics.get("likes", post_data.get("like_count", 0))
    comments = metrics.get("comments", post_data.get("comments_count", 0))
    saves    = metrics.get("saved", 0)
    reach    = metrics.get("reach", 0)

    # Compute engagement rate locally
    engagement_rate = round((likes + comments + saves) / reach * 100, 2) if reach > 0 else 0.0

    return {
        "reach":           reach,
        "impressions":     metrics.get("impressions", 0),
        "likes":           likes,
        "comments":        comments,
        "saves":           saves,
        "shares":          metrics.get("shares", 0),
        "plays":           metrics.get("plays", 0),
        "engagement_rate": engagement_rate,
    }


# ═══════════════════════════════════════════════════════════
# ACCOUNT ANALYTICS
# ═══════════════════════════════════════════════════════════

async def get_account_analytics(
    session: AsyncSession,
    account_id: str,
    ig_user_id: str,
    access_token: str,
    period: str,
) -> dict:
    """
    period=day|week|month → 1 API call
    period=year           → check DB first, fetch only missing months, sum all
    """
    if period == "year":
        return await _get_year_analytics(session, account_id, ig_user_id, access_token)

    # day | week | month → direct API call with rate limit check
    if await check_rate_limit(session, account_id):
        # Return last cached month data instead
        logger.warning(f"Rate limit — returning cached data for {account_id}")
        return await _get_cached_account_fallback(session, account_id)

    try:
        data = await _fetch_account_insights_api(ig_user_id, access_token, period)
        await log_api_call(session, account_id, f"account_insights_{period}")
        return {**data, "period": period, "source": "api"}
    except Exception as e:
        logger.error(f"Account insights API error: {e}")
        return await _get_cached_account_fallback(session, account_id)


async def _get_year_analytics(
    session: AsyncSession,
    account_id: str,
    ig_user_id: str,
    access_token: str,
) -> dict:
    """
    Year aggregation:
    1. Check DB for all 12 months of current year
    2. Fetch only missing months from API
    3. Save missing months to DB
    4. Sum all 12 from DB
    """
    now = datetime.now(timezone.utc)
    current_year = now.year

    # Step 1: Get existing months from DB
    result = await session.execute(
        select(AccountMonthlyInsights).where(
            AccountMonthlyInsights.account_id == account_id,
            AccountMonthlyInsights.year == current_year,
        )
    )
    existing = {row.month: row for row in result.scalars().all()}
    existing_months = set(existing.keys())

    # Step 2: Determine missing months (only up to current month)
    all_months = set(range(1, now.month + 1))
    missing_months = all_months - existing_months

    logger.info(f"Year analytics: {len(existing_months)} cached, {len(missing_months)} to fetch")

    # Step 3: Fetch missing months
    for month in missing_months:
        if await check_rate_limit(session, account_id):
            logger.warning(f"Rate limit — skipping month {month}")
            break

        try:
            # Calculate since/until for that month
            since_dt = datetime(current_year, month, 1, tzinfo=timezone.utc)
            if month == 12:
                until_dt = datetime(current_year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                until_dt = datetime(current_year, month + 1, 1, tzinfo=timezone.utc)

            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    f"{GRAPH_BASE}/{ig_user_id}/insights",
                    params={
                        "metric":       "reach,impressions,profile_views,follower_count",
                        "period":       "month",
                        "since":        int(since_dt.timestamp()),
                        "until":        int(until_dt.timestamp()),
                        "access_token": access_token,
                    }
                )
            data = resp.json()
            await log_api_call(session, account_id, f"account_insights_month_{current_year}_{month}")

            metrics = {}
            for item in data.get("data", []):
                values = item.get("values", [])
                metrics[item["name"]] = values[0].get("value", 0) if values else 0

            # Get followers from profile
            async with httpx.AsyncClient(timeout=30.0) as client:
                profile_resp = await client.get(
                    f"{GRAPH_BASE}/{ig_user_id}",
                    params={"fields": "followers_count", "access_token": access_token}
                )
            followers = profile_resp.json().get("followers_count", 0)

            # Upsert into DB
            stmt = pg_insert(AccountMonthlyInsights).values(
                account_id=account_id,
                year=current_year,
                month=month,
                reach=metrics.get("reach", 0),
                impressions=metrics.get("impressions", 0),
                profile_views=metrics.get("profile_views", 0),
                followers=followers,
                updated_at=datetime.now(timezone.utc),
            ).on_conflict_do_update(
                constraint="uq_account_month",
                set_=dict(
                    reach=metrics.get("reach", 0),
                    impressions=metrics.get("impressions", 0),
                    profile_views=metrics.get("profile_views", 0),
                    followers=followers,
                    updated_at=datetime.now(timezone.utc),
                )
            )
            await session.execute(stmt)
            await session.commit()

        except Exception as e:
            logger.error(f"Failed to fetch month {month}: {e}")
            continue

    # Step 4: Sum all available months from DB
    result = await session.execute(
        select(AccountMonthlyInsights).where(
            AccountMonthlyInsights.account_id == account_id,
            AccountMonthlyInsights.year == current_year,
        )
    )
    rows = result.scalars().all()

    total_reach = sum(r.reach or 0 for r in rows)
    total_impressions = sum(r.impressions or 0 for r in rows)
    total_profile_views = sum(r.profile_views or 0 for r in rows)
    latest_followers = max((r.followers or 0 for r in rows), default=0)

    return {
        "period":        "year",
        "year":          current_year,
        "months_cached": len(rows),
        "reach":         total_reach,
        "impressions":   total_impressions,
        "profile_views": total_profile_views,
        "followers":     latest_followers,
        "source":        "db_aggregated",
    }


async def _get_cached_account_fallback(session: AsyncSession, account_id: str) -> dict:
    """Return last available monthly data when rate limited."""
    result = await session.execute(
        select(AccountMonthlyInsights)
        .where(AccountMonthlyInsights.account_id == account_id)
        .order_by(AccountMonthlyInsights.updated_at.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row:
        return {
            "period":       "cached",
            "reach":        row.reach,
            "impressions":  row.impressions,
            "profile_views": row.profile_views,
            "followers":    row.followers,
            "source":       "cache_fallback",
        }
    return {"period": "none", "reach": 0, "impressions": 0, "profile_views": 0, "followers": 0, "source": "empty"}


# ═══════════════════════════════════════════════════════════
# POST ANALYTICS
# ═══════════════════════════════════════════════════════════

async def get_post_analytics(
    session: AsyncSession,
    post_id: str,
    account_id: str,
    access_token: str,
    period: str,
) -> dict:
    """
    period=day|week|month → API call with rate limit check
    period=lifetime       → check 24h cache first, then API
    """
    if period == "lifetime":
        return await _get_lifetime_post_analytics(session, post_id, account_id, access_token)

    # day | week | month
    if await check_rate_limit(session, account_id):
        cached = await _get_post_cache(session, post_id, period)
        if cached:
            return {**cached, "source": "cache_rate_limited"}
        return _empty_post_response(post_id, period)

    try:
        data = await _fetch_post_insights_api(post_id, access_token, period)
        await log_api_call(session, account_id, f"post_insights_{period}")
        await _upsert_post_cache(session, post_id, account_id, period, data)
        return {**data, "post_id": post_id, "period": period, "source": "api"}
    except Exception as e:
        logger.error(f"Post insights API error for {post_id}: {e}")
        cached = await _get_post_cache(session, post_id, period)
        if cached:
            return {**cached, "source": "cache_error_fallback"}
        return _empty_post_response(post_id, period)


async def _get_lifetime_post_analytics(
    session: AsyncSession,
    post_id: str,
    account_id: str,
    access_token: str,
) -> dict:
    """
    Lifetime: check cache first.
    If cache exists and < 24h old → return cache.
    Else → call API, update cache.
    """
    cached = await _get_post_cache(session, post_id, "lifetime")
    if cached:
        cache_age = datetime.now(timezone.utc) - cached["updated_at"].replace(tzinfo=timezone.utc)
        if cache_age < timedelta(hours=CACHE_TTL_HOURS):
            logger.info(f"Post {post_id} lifetime — cache hit ({cache_age.seconds//3600}h old)")
            return {**cached, "source": "cache"}

    if await check_rate_limit(session, account_id):
        if cached:
            return {**cached, "source": "cache_rate_limited"}
        return _empty_post_response(post_id, "lifetime")

    try:
        data = await _fetch_post_insights_api(post_id, access_token, "lifetime")
        await log_api_call(session, account_id, "post_insights_lifetime")
        await _upsert_post_cache(session, post_id, account_id, "lifetime", data)
        return {**data, "post_id": post_id, "period": "lifetime", "source": "api"}
    except Exception as e:
        logger.error(f"Lifetime post insights error for {post_id}: {e}")
        if cached:
            return {**cached, "source": "cache_error_fallback"}
        return _empty_post_response(post_id, "lifetime")


async def _get_post_cache(session: AsyncSession, post_id: str, period: str) -> dict | None:
    result = await session.execute(
        select(PostInsightsCache).where(
            PostInsightsCache.post_id == post_id,
            PostInsightsCache.period == period,
        )
    )
    row = result.scalar_one_or_none()
    if not row:
        return None
    return {
        "post_id":        post_id,
        "period":         period,
        "reach":          row.reach,
        "impressions":    row.impressions,
        "likes":          row.likes,
        "comments":       row.comments,
        "saves":          row.saves,
        "shares":         row.shares,
        "plays":          row.plays,
        "engagement_rate": row.engagement_rate,
        "updated_at":     row.updated_at,
    }


async def _upsert_post_cache(
    session: AsyncSession,
    post_id: str,
    account_id: str,
    period: str,
    data: dict,
):
    stmt = pg_insert(PostInsightsCache).values(
        post_id=post_id,
        account_id=account_id,
        period=period,
        reach=data.get("reach", 0),
        impressions=data.get("impressions", 0),
        likes=data.get("likes", 0),
        comments=data.get("comments", 0),
        saves=data.get("saves", 0),
        shares=data.get("shares", 0),
        plays=data.get("plays", 0),
        engagement_rate=data.get("engagement_rate", 0.0),
        updated_at=datetime.now(timezone.utc),
    ).on_conflict_do_update(
        constraint="uq_post_period",
        set_=dict(
            reach=data.get("reach", 0),
            impressions=data.get("impressions", 0),
            likes=data.get("likes", 0),
            comments=data.get("comments", 0),
            saves=data.get("saves", 0),
            shares=data.get("shares", 0),
            plays=data.get("plays", 0),
            engagement_rate=data.get("engagement_rate", 0.0),
            updated_at=datetime.now(timezone.utc),
        )
    )
    await session.execute(stmt)
    await session.commit()


def _empty_post_response(post_id: str, period: str) -> dict:
    return {
        "post_id": post_id, "period": period,
        "reach": 0, "impressions": 0, "likes": 0,
        "comments": 0, "saves": 0, "shares": 0,
        "plays": 0, "engagement_rate": 0.0, "source": "empty",
    }
