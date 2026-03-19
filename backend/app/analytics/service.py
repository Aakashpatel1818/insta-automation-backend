import logging
import httpx
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

import json
from app.analytics.models import AccountMonthlyInsights, PostInsightsCache, ApiUsageLog, PostListCache

logger = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.instagram.com/v19.0"
API_RATE_LIMIT = 150   # max calls per hour per account
CACHE_TTL_HOURS = 24   # post insights cache TTL
POST_LIST_CACHE_HOURS = 6  # posts list cache TTL


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
    """Single Instagram API call for account insights with since/until for precise date ranges."""
    now = datetime.now(timezone.utc)

    # Instagram insights API rules:
    # - period=day:   since/until range, returns one value per day
    # - period=week:  since/until range, each value covers 7 days
    # - period=month: since/until range, each value covers 28 days
    # Minimum range must be >= 1 full period bucket
    # We always fetch with period=day and sum buckets for flexibility
    period_days = {"day": 2, "week": 7, "month": 28}  # day needs at least 2 for API
    days_back = period_days.get(period, 28)

    # Align since to start of day to get clean daily buckets
    since_dt = (now - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
    until_dt = now

    since_ts = int(since_dt.timestamp())
    until_ts = int(until_dt.timestamp())

    async with httpx.AsyncClient(timeout=30.0) as client:
        profile_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}",
            params={
                "fields":       "id,username,followers_count,media_count",
                "access_token": access_token,
            }
        )
        profile = profile_resp.json()
        logger.info(f"Profile response: {profile.get('username')} followers={profile.get('followers_count')}")

        # ── NOTE: `impressions` was deprecated by Instagram in API v18+. ────────
        # Including it in ANY metric list causes the ENTIRE API call to fail.
        # We use `total_interactions` + `reach` as substitutes instead.
        # `profile_views` / `accounts_engaged` / `follower_count` all require
        # instagram_manage_insights scope — fetched separately so a failure
        # there does NOT zero out the reach metric.

        # Step 1: reach + total_interactions — work without manage_insights
        insights_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}/insights",
            params={
                "metric":       "reach,total_interactions",
                "period":       "day",
                "since":        since_ts,
                "until":        until_ts,
                "access_token": access_token,
            }
        )
        insights = insights_resp.json()
        logger.info(f"Insights response period={period}: data_count={len(insights.get('data', []))}, error={insights.get('error')}")
        logger.info(f"[DEBUG] RAW insights keys: {list(insights.keys())}")
        for i, item in enumerate(insights.get('data', [])):
            logger.info(f"[DEBUG] data[{i}]: name={item.get('name')} | value={item.get('value')} | values={item.get('values')}")

        if "error" in insights:
            logger.error(f"Primary insights call failed: {insights['error'].get('message')}")
            insights = {"data": []}

        # Step 2: accounts_engaged + profile_views + follower_count — all require
        # instagram_manage_insights scope. Isolated so failures don't zero out reach.
        managed_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}/insights",
            params={
                "metric":       "accounts_engaged,profile_views,follower_count",
                "period":       "day",
                "since":        since_ts,
                "until":        until_ts,
                "access_token": access_token,
            }
        )
        managed_data = managed_resp.json()
        if "error" in managed_data:
            logger.warning(f"Managed insights unavailable (needs instagram_manage_insights): {managed_data['error'].get('message')}")
            managed_data = {"data": []}
        else:
            logger.info(f"Managed insights returned {len(managed_data.get('data', []))} metrics")

        # Merge all data
        insights = {"data": insights.get("data", []) + managed_data.get("data", [])}

    def _parse_metrics(data_list: list) -> dict:
        result = {}
        for item in data_list:
            name = item["name"]
            if "value" in item:
                total = item["value"]
                logger.info(f"  metric={name} value={total} (flat)")
            else:
                values = item.get("values", [])
                total = sum(v.get("value", 0) for v in values)
                logger.info(f"  metric={name} buckets={len(values)} total={total} (values[])")
            result[name] = total
        return result

    metrics = {}
    if "data" in insights:
        metrics.update(_parse_metrics(insights["data"]))
    elif "error" in insights:
        logger.error(f"Insights API error: {insights['error']}")

    logger.info(f"Final parsed metrics: {metrics}")

    # Use total_interactions as the impressions substitute (accounts_engaged
    # requires manage_insights and is often 0 without it)
    total_interactions = metrics.get("total_interactions", 0) or 0
    accounts_engaged   = metrics.get("accounts_engaged",   0) or 0
    impressions_val    = total_interactions if total_interactions > 0 else accounts_engaged

    return {
        "username":           profile.get("username", ""),
        "followers":          profile.get("followers_count", 0),
        "media_count":        profile.get("media_count", 0),
        "reach":              metrics.get("reach", 0) or 0,
        "impressions":        impressions_val,
        "profile_views":      metrics.get("profile_views", 0) or 0,
        "follower_count":     metrics.get("follower_count", 0) or 0,
        "accounts_engaged":   accounts_engaged,
        "total_interactions": total_interactions,
        "api_error":          insights.get("error", {}).get("message") if "error" in insights else None,
    }


async def _fetch_post_insights_api(
    post_id: str,
    access_token: str,
    period: str,
) -> dict:
    """Single Instagram API call for post insights.

    BUG NOTES (fixed here):
    1. `impressions` was deprecated in v18+ and breaks the entire call when
       included alongside other metrics — replaced with `total_interactions`.
    2. REELS use a different metric set; `plays` replaces `impressions`.
    3. The original code fetched both post fields and insights in one client
       context but only opened ONE async client — they need to be separate
       calls, or the second call would fail silently if the first errored.
    4. `saved` is the correct API field name, NOT `saves` — kept as-is but
       mapped explicitly to avoid silent zero if Instagram ever renames it.
    5. For non-lifetime periods the `values` array may be empty for very
       recent posts — we now fall back to 0 gracefully instead of crashing.
    """
    # ── Step 1: fetch post type so we can build the correct metric list ──
    async with httpx.AsyncClient(timeout=30.0) as client:
        post_resp = await client.get(
            f"{GRAPH_BASE}/{post_id}",
            params={
                "fields":       "id,like_count,comments_count,media_type",
                "access_token": access_token,
            }
        )
    post_data  = post_resp.json()
    media_type = post_data.get("media_type", "IMAGE").upper()

    # BUG FIX 1: `impressions` causes the ENTIRE insights call to fail in v18+
    # for non-REELS posts.  Use `total_interactions` instead, and only add
    # `plays` for REELS/VIDEO.  `video_views` is also deprecated.
    base_metrics = "reach,likes,comments,shares,saved,total_interactions"
    if media_type in ("REELS", "VIDEO"):
        metrics_list = f"plays,{base_metrics}"
    else:
        metrics_list = base_metrics

    # ── Step 2: fetch insights ────────────────────────────────────────────
    async with httpx.AsyncClient(timeout=30.0) as client:
        insights_resp = await client.get(
            f"{GRAPH_BASE}/{post_id}/insights",
            params={
                "metric":       metrics_list,
                "period":       period,
                "access_token": access_token,
            }
        )
    insights = insights_resp.json()

    if "error" in insights:
        logger.error(f"Post insights API error for {post_id}: {insights['error'].get('message')}")

    # ── Step 3: parse metric values ───────────────────────────────────────
    metrics: dict = {}
    for item in insights.get("data", []):
        name = item.get("name")
        if name is None:
            continue
        # Lifetime → flat "value" key
        # day/week/month → nested "values" array
        if "value" in item and item["value"] is not None:
            val = item["value"]
        else:
            values = item.get("values") or []
            # BUG FIX 5: values may be empty for brand-new posts
            val = values[0].get("value", 0) if values else 0
        metrics[name] = val

    # BUG FIX 4: use post-level like_count/comments_count as authoritative
    # fallback — the insights endpoint sometimes omits them for old posts
    likes    = metrics.get("likes")    if metrics.get("likes")    is not None else post_data.get("like_count",     0)
    comments = metrics.get("comments") if metrics.get("comments") is not None else post_data.get("comments_count", 0)
    saves    = metrics.get("saved",  0)  or 0
    reach    = metrics.get("reach",  0)  or 0
    shares   = metrics.get("shares", 0)  or 0
    plays    = metrics.get("plays",  0)  or 0
    # total_interactions from API (likes+comments+shares+saves — most reliable)
    total_interactions = metrics.get("total_interactions", 0) or 0

    # BUG FIX 2: `impressions` was always 0 because the field was either
    # deprecated (non-REELS) or not requested (REELS).  We now expose
    # `total_interactions` as the impressions substitute so the frontend
    # has a non-zero value when the account has the required permissions.
    impressions = total_interactions  # best available substitute

    logger.info(
        f"Post {post_id} ({media_type}) metrics: "
        f"likes={likes} comments={comments} reach={reach} "
        f"shares={shares} saves={saves} plays={plays} "
        f"total_interactions={total_interactions}"
    )

    # Engagement rate as a plain percentage (e.g. 12.5 means 12.5%)
    # Only compute when reach is available — avoids the 0.0% misleading display
    engagement_rate = (
        round((likes + comments + saves + shares) / reach * 100, 2)
        if reach > 0 else 0.0
    )

    return {
        "reach":              reach,
        "impressions":        impressions,   # = total_interactions (best available)
        "likes":              likes,
        "comments":           comments,
        "saves":              saves,
        "shares":             shares,
        "plays":              plays,
        "total_interactions": total_interactions,
        "engagement_rate":    engagement_rate,  # e.g. 12.5 = 12.5%
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

    # Map frontend period to Instagram API period
    # Instagram only accepts: day, week, month
    ig_period_map = {
        "day":   "day",
        "week":  "week",
        "month": "month",
    }
    ig_period = ig_period_map.get(period, "month")

    # day | week | month → direct API call with rate limit check
    if await check_rate_limit(session, account_id):
        # Return last cached month data instead
        logger.warning(f"Rate limit — returning cached data for {account_id}")
        return await _get_cached_account_fallback(session, account_id)

    try:
        data = await _fetch_account_insights_api(ig_user_id, access_token, ig_period)
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
    Year view:
    - Instagram API only allows ~30-day lookback, so per-month historical
      calls always fail for older months.
    - Instead: fetch reach for the last 30 days (best we can get),
      get followers + media_count from profile API,
      and let the router compute interactions from the posts cache.
    """
    now = datetime.now(timezone.utc)

    # ── 1. Profile: followers + media_count (always reliable) ──────────
    async with httpx.AsyncClient(timeout=30.0) as client:
        profile_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}",
            params={"fields": "followers_count,media_count,username", "access_token": access_token}
        )
    profile = profile_resp.json()
    followers   = profile.get("followers_count", 0)
    media_count = profile.get("media_count", 0)
    username    = profile.get("username", "")
    logger.info(f"Year profile: {username} followers={followers} posts={media_count}")

    # ── 2. Reach: last 30 days (max reliable window from Instagram API) ──
    year_reach = 0
    try:
        since_dt = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        async with httpx.AsyncClient(timeout=30.0) as client:
            reach_resp = await client.get(
                f"{GRAPH_BASE}/{ig_user_id}/insights",
                params={
                    "metric":       "reach",
                    "period":       "day",
                    "since":        int(since_dt.timestamp()),
                    "until":        int(now.timestamp()),
                    "access_token": access_token,
                }
            )
        reach_data = reach_resp.json()
        await log_api_call(session, account_id, "year_reach_30d")
        for item in reach_data.get("data", []):
            if item.get("name") == "reach":
                year_reach = sum(v.get("value", 0) for v in item.get("values", []))
        logger.info(f"Year reach (last 30d): {year_reach}")
    except Exception as e:
        logger.error(f"Year reach fetch failed: {e}")

    return {
        "period":       "year",
        "username":     username,
        "followers":    followers,    # live from profile API
        "media_count":  media_count,  # live from profile API
        "reach":        year_reach,   # last 30 days (API limitation)
        "impressions":  0,            # router fills from posts cache
        "profile_views": 0,           # needs manage_insights scope
        "source":       "year",
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
    If cache exists and < 24h old AND has non-zero reach/impressions → return cache.
    If cache is all zeros (stale from before bug fix) → force refresh from API.
    """
    cached = await _get_post_cache(session, post_id, "lifetime")
    if cached:
        cached_at = cached["updated_at"]
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        cache_age = datetime.now(timezone.utc) - cached_at.astimezone(timezone.utc)

        # KEY FIX: don't serve a cache row that has reach=0 AND impressions=0
        # AND likes=0 — that's a broken row from the old buggy `impressions`
        # field. Force a fresh API call to overwrite it.
        cache_looks_broken = (
            cached.get("reach", 0) == 0
            and cached.get("impressions", 0) == 0
            and cached.get("likes", 0) == 0
            and cached.get("comments", 0) == 0
        )
        if cache_looks_broken:
            logger.info(f"Post {post_id} cache looks empty/broken — forcing API refresh")
        elif cache_age < timedelta(hours=CACHE_TTL_HOURS):
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


# ═══════════════════════════════════════════════════════════
# POSTS LIST (with 6h cache)
# ═══════════════════════════════════════════════════════════

async def get_posts_list(
    session: AsyncSession,
    account_id: str,
    ig_user_id: str,
    access_token: str,
    force_refresh: bool = False,
) -> list:
    """
    Returns all posts for an account.
    - Checks DB cache first (6h TTL)
    - Only calls Instagram API if cache is stale or missing
    - Saves result back to DB
    """
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    # 1. Check cache
    if not force_refresh:
        result = await session.execute(
            select(PostListCache).where(PostListCache.account_id == account_id)
        )
        row = result.scalar_one_or_none()
        if row:
            age = datetime.now(timezone.utc) - row.updated_at.replace(tzinfo=timezone.utc)
            if age < timedelta(hours=POST_LIST_CACHE_HOURS):
                logger.info(f"Posts list cache hit for {account_id} ({int(age.total_seconds()//3600)}h old)")
                return json.loads(row.posts_json)

    # 2. Rate limit check
    if await check_rate_limit(session, account_id):
        logger.warning(f"Rate limited — serving stale posts list for {account_id}")
        result = await session.execute(
            select(PostListCache).where(PostListCache.account_id == account_id)
        )
        row = result.scalar_one_or_none()
        return json.loads(row.posts_json) if row else []

    # 3. Fetch from Instagram
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{GRAPH_BASE}/{ig_user_id}/media",
                params={
                    "fields": "id,caption,media_type,timestamp,like_count,comments_count,media_url,thumbnail_url,permalink",
                    "access_token": access_token,
                    "limit": 100,
                }
            )
        data = resp.json()
        await log_api_call(session, account_id, "posts_list")

        posts = [
            {
                "post_id":        p.get("id"),
                "caption":        (p.get("caption") or "")[:200],
                "media_type":     p.get("media_type", ""),
                "media_url":      p.get("media_url", ""),
                "thumbnail_url":  p.get("thumbnail_url", ""),
                "permalink":      p.get("permalink", ""),
                "timestamp":      p.get("timestamp", ""),
                "like_count":     p.get("like_count", 0),
                "comments_count": p.get("comments_count", 0),
                "share_count":    0,  # not in basic media endpoint
            }
            for p in data.get("data", [])
        ]

        # 4. Save to cache (upsert)
        stmt = pg_insert(PostListCache).values(
            account_id=account_id,
            posts_json=json.dumps(posts),
            updated_at=datetime.now(timezone.utc),
        ).on_conflict_do_update(
            index_elements=["account_id"],
            set_=dict(
                posts_json=json.dumps(posts),
                updated_at=datetime.now(timezone.utc),
            )
        )
        await session.execute(stmt)
        await session.commit()

        logger.info(f"Posts list fetched and cached: {len(posts)} posts for {account_id}")
        return posts

    except Exception as e:
        logger.error(f"Posts list fetch failed: {e}")
        # Return stale cache if available
        result = await session.execute(
            select(PostListCache).where(PostListCache.account_id == account_id)
        )
        row = result.scalar_one_or_none()
        return json.loads(row.posts_json) if row else []


def _empty_post_response(post_id: str, period: str) -> dict:
    return {
        "post_id": post_id, "period": period,
        "reach": 0, "impressions": 0, "likes": 0,
        "comments": 0, "saves": 0, "shares": 0,
        "plays": 0, "engagement_rate": 0.0, "source": "empty",
    }
