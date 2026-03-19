import logging
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_user, get_ig_account
from app.database import get_db
from app.analytics.pg_database import get_pg_session
from app.analytics.service import get_account_analytics, get_post_analytics, get_posts_list

# Maps frontend period → how many days back to look when filtering posts
PERIOD_DAYS = {
    "day":   1,
    "week":  7,
    "month": 30,
    "year":  365,
}

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics/v2", tags=["Analytics V2 (PostgreSQL)"])


# Account Analytics
@router.get("/account")
async def account_analytics(
    period: str = Query(default="month", enum=["day", "week", "month", "year"]),
    account_id: str = Query(default=None),
    current_user: dict = Depends(get_current_user),
    pg: AsyncSession = Depends(get_pg_session),
):
    """
    Account-level analytics.
    - day/week/month: 1 Instagram API call
    - year: DB aggregation
    - Rate limited: returns cached data
    """
    mongo_db = get_db()
    account = await get_ig_account(current_user, account_id, mongo_db)

    try:
        result = await get_account_analytics(
            session=pg,
            account_id=str(account["_id"]),
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
            period=period,
        )

        # ── Fallback: compute interactions + followers from cached posts ───────
        # accounts_engaged / follower_count both need instagram_manage_insights.
        # We instead derive them from the post list cache filtered by period.
        try:
            all_posts = await get_posts_list(
                session=pg,
                account_id=str(account["_id"]),
                ig_user_id=account["instagram_user_id"],
                access_token=account["access_token"],
                force_refresh=False,
            )

            # ── Interactions: likes + comments + shares filtered by period ─────
            # Instagram API requires instagram_manage_insights scope for windowed
            # engagement metrics. Without it, accounts_engaged is always 0.
            # Backend is the single source of truth — we filter posts by the
            # requested period here so the frontend does NOT recompute this.
            if result.get("impressions", 0) == 0:
                cutoff = datetime.now(timezone.utc) - timedelta(days=PERIOD_DAYS.get(period, 30))
                period_posts = [
                    p for p in all_posts
                    if p.get("timestamp") and datetime.fromisoformat(
                        p["timestamp"].replace("Z", "+00:00")
                    ) > cutoff
                ]
                computed = sum(
                    (p.get("like_count", 0) or 0)
                    + (p.get("comments_count", 0) or 0)
                    + (p.get("share_count", 0) or 0)
                    for p in period_posts
                )
                result["impressions"] = computed
                logger.info(f"Interactions (period={period}, {len(period_posts)}/{len(all_posts)} posts): {computed}")

            # ── Followers: estimate new followers as posts' like growth ──
            # True follower gain requires manage_insights. Best proxy:
            # show current total for all periods (it IS correct — it's a
            # running total, not a windowed metric). Override with period
            # post count as a rough engagement proxy only if 0.
            # Keep result["followers"] as-is (current count from profile API).
            # Just log so it's clear what's happening.
            logger.info(f"Followers (current total from profile): {result.get('followers', 0)}")

        except Exception as ie:
            logger.warning(f"Could not compute period-filtered metrics from posts: {ie}")

        return {
            "username": account.get("username"),
            "account_id": str(account["_id"]),
            **result,
            "api_error": result.get("api_error"),
        }
    except Exception as e:
        logger.error(f"Account analytics failed: {e}")
        raise HTTPException(status_code=502, detail=f"Analytics error: {str(e)}")


# Posts List
@router.get("/posts")
async def posts_list(
    account_id: str = Query(default=None),
    refresh: bool = Query(default=False),
    current_user: dict = Depends(get_current_user),
    pg: AsyncSession = Depends(get_pg_session),
):
    """All posts — cached 6h, ?refresh=true to force fetch."""
    mongo_db = get_db()
    account = await get_ig_account(current_user, account_id, mongo_db)

    try:
        posts = await get_posts_list(
            session=pg,
            account_id=str(account["_id"]),
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
            force_refresh=refresh,
        )
        return posts
    except Exception as e:
        logger.error(f"Posts list failed: {e}")
        raise HTTPException(status_code=502, detail=f"Posts list error: {str(e)}")


# Debug: raw Instagram insights response
@router.get("/debug/insights")
async def debug_insights(
    account_id: str = Query(default=None),
    current_user: dict = Depends(get_current_user),
):
    """
    Comprehensive debug endpoint — tests every metric combination against the
    Instagram Graph API and returns raw responses so you can see exactly what
    the API accepts/rejects and what values it returns.

    Hit: GET /analytics/v2/debug/insights
    """
    import httpx
    from datetime import datetime, timedelta, timezone
    mongo_db = get_db()
    account = await get_ig_account(current_user, account_id, mongo_db)
    ig_user_id   = account["instagram_user_id"]
    access_token = account["access_token"]

    now      = datetime.now(timezone.utc)
    since_dt = (now - timedelta(days=28)).replace(hour=0, minute=0, second=0, microsecond=0)
    since_ts = int(since_dt.timestamp())
    until_ts = int(now.timestamp())
    base     = "https://graph.instagram.com/v22.0"

    results = {}

    async with httpx.AsyncClient(timeout=30.0) as client:

        # ── 1. Profile (always works) ──────────────────────────────────────────
        r = await client.get(f"{base}/{ig_user_id}",
            params={"fields": "id,username,account_type,followers_count,media_count",
                    "access_token": access_token})
        results["profile"] = r.json()

        # ── 2. Token debug — shows scopes & expiry ─────────────────────────────
        r = await client.get("https://graph.instagram.com/v22.0/me",
            params={"fields": "id,username", "access_token": access_token})
        results["token_me"] = r.json()

        # ── 3. reach + total_interactions (v18+ approved metrics, no manage_insights) ──
        r = await client.get(f"{base}/{ig_user_id}/insights",
            params={"metric": "reach,total_interactions", "period": "day",
                    "since": since_ts, "until": until_ts, "access_token": access_token})
        results["reach_total_interactions_day"] = r.json()

        # ── 4. impressions alone (deprecated in v18 — should error) ───────────
        r = await client.get(f"{base}/{ig_user_id}/insights",
            params={"metric": "impressions", "period": "day",
                    "since": since_ts, "until": until_ts, "access_token": access_token})
        results["impressions_deprecated_test"] = r.json()

        # ── 5. accounts_engaged (requires manage_insights) ────────────────────
        r = await client.get(f"{base}/{ig_user_id}/insights",
            params={"metric": "accounts_engaged", "period": "day",
                    "since": since_ts, "until": until_ts, "access_token": access_token})
        results["accounts_engaged"] = r.json()

        # ── 6. profile_views (requires manage_insights) ───────────────────────
        r = await client.get(f"{base}/{ig_user_id}/insights",
            params={"metric": "profile_views", "period": "day",
                    "since": since_ts, "until": until_ts, "access_token": access_token})
        results["profile_views"] = r.json()

        # ── 7. follower_count (requires manage_insights) ──────────────────────
        r = await client.get(f"{base}/{ig_user_id}/insights",
            params={"metric": "follower_count", "period": "day",
                    "since": since_ts, "until": until_ts, "access_token": access_token})
        results["follower_count"] = r.json()

        # ── 8. Post-level test: pick the most recent post ─────────────────────
        media_r = await client.get(f"{base}/{ig_user_id}/media",
            params={"fields": "id,media_type,like_count,comments_count,timestamp",
                    "limit": 1, "access_token": access_token})
        media_data = media_r.json()
        results["latest_post_fields"] = media_data

        posts = media_data.get("data", [])
        if posts:
            pid        = posts[0]["id"]
            media_type = posts[0].get("media_type", "IMAGE").upper()

            # 8a. post insights: reach + total_interactions (no impressions)
            base_m = "reach,likes,comments,shares,saved,total_interactions"
            if media_type in ("REELS", "VIDEO"):
                m_list = f"plays,{base_m}"
            else:
                m_list = base_m

            r = await client.get(f"{base}/{pid}/insights",
                params={"metric": m_list, "period": "lifetime",
                        "access_token": access_token})
            results["post_insights_lifetime"] = {"post_id": pid, "media_type": media_type, "response": r.json()}

            # 8b. post insights: impressions alone (should error on v18+)
            r = await client.get(f"{base}/{pid}/insights",
                params={"metric": "impressions", "period": "lifetime",
                        "access_token": access_token})
            results["post_impressions_deprecated_test"] = r.json()

    # ── Summary: parse what actually worked ───────────────────────────────────
    def _extract_values(api_result: dict) -> dict:
        """Pull out metric name → total value from a insights response."""
        out = {}
        for item in api_result.get("data", []):
            name = item.get("name", "?")
            if "value" in item:
                out[name] = item["value"]
            else:
                out[name] = sum(v.get("value", 0) for v in item.get("values", []))
        if "error" in api_result:
            out["__error"] = api_result["error"].get("message", "unknown error")
            out["__error_code"] = api_result["error"].get("code")
        return out

    summary = {
        "account_type":                   results["profile"].get("account_type", "UNKNOWN"),
        "followers":                      results["profile"].get("followers_count"),
        "reach_total_interactions_day":   _extract_values(results["reach_total_interactions_day"]),
        "accounts_engaged":               _extract_values(results["accounts_engaged"]),
        "profile_views":                  _extract_values(results["profile_views"]),
        "impressions_deprecated_errors":  _extract_values(results["impressions_deprecated_test"]),
    }
    if "post_insights_lifetime" in results:
        summary["post_insights"] = _extract_values(results["post_insights_lifetime"]["response"])

    return {
        "summary":     summary,
        "raw":         results,
        "ig_user_id":  ig_user_id,
        "token_prefix": access_token[:25] + "...",
    }


# Cache management
@router.delete("/cache/posts")
async def clear_post_insights_cache(
    account_id: str = Query(default=None),
    only_zeros: bool = Query(default=True, description="If true, only delete rows where reach=0 AND impressions=0 AND likes=0"),
    current_user: dict = Depends(get_current_user),
    pg: AsyncSession = Depends(get_pg_session),
):
    """
    Clears the post insights cache so every post fetches fresh data from Instagram.
    - only_zeros=true (default): only deletes rows that are clearly broken (all zeros)
    - only_zeros=false: clears ALL cached rows for this account (nuclear option)
    """
    from sqlalchemy import delete as sa_delete
    from app.analytics.models import PostInsightsCache

    mongo_db = get_db()
    account  = await get_ig_account(current_user, account_id, mongo_db)
    acc_id   = str(account["_id"])

    if only_zeros:
        stmt = sa_delete(PostInsightsCache).where(
            PostInsightsCache.account_id == acc_id,
            PostInsightsCache.reach       == 0,
            PostInsightsCache.impressions == 0,
            PostInsightsCache.likes       == 0,
        )
        label = "zero-value rows"
    else:
        stmt = sa_delete(PostInsightsCache).where(
            PostInsightsCache.account_id == acc_id,
        )
        label = "all rows"

    result = await pg.execute(stmt)
    await pg.commit()
    deleted = result.rowcount
    logger.info(f"Cache cleared: {deleted} {label} deleted for account {acc_id}")
    return {"deleted": deleted, "label": label, "account_id": acc_id}


# Post Analytics
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
    - lifetime: 24h cache
    - day/week/month: API call with rate limit check
    """
    mongo_db = get_db()
    account = await get_ig_account(current_user, account_id, mongo_db)

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
