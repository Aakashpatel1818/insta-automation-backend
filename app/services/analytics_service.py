import httpx
import logging

logger = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.instagram.com/v19.0"

# period options: day, week, month, year (lifetime not supported by IG insights)
VALID_PERIODS = ["day", "week", "month"]


async def get_account_insights(ig_user_id: str, access_token: str, period: str = "month") -> dict:
    """
    Fetch account-level insights.
    period: day | week | month | year
    """
    if period not in VALID_PERIODS:
        period = "month"


    async with httpx.AsyncClient(timeout=30.0) as client:

        # Basic profile
        profile_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}",
            params={
                "fields": "id,username,followers_count,media_count,profile_picture_url,website,biography",
                "access_token": access_token,
            }
        )
        profile = profile_resp.json()

        # Account insights
        insights_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}/insights",
            params={
                "metric":       "reach,impressions,profile_views,follower_count",
                "period":       period,
                "access_token": access_token,
            }
        )
        insights = insights_resp.json()
        logger.debug(f"Account insights ({period}): {insights}")

    metrics = {}
    for item in insights.get("data", []):
        name = item.get("name")
        values = item.get("values", [])
        if values:
            metrics[name] = values[-1].get("value", 0)

    return {
        "username":        profile.get("username"),
        "followers_count": profile.get("followers_count", 0),
        "media_count":     profile.get("media_count", 0),
        "biography":       profile.get("biography", ""),
        "website":         profile.get("website", ""),
        "reach":           metrics.get("reach", 0),
        "impressions":     metrics.get("impressions", 0),
        "profile_views":   metrics.get("profile_views", 0),
        "follower_count":  metrics.get("follower_count", 0),
        "period":          period,
    }


async def get_year_insights(ig_user_id: str, access_token: str) -> dict:
    """
    Simulate year data by fetching 12 monthly snapshots and summing them.
    """
    import asyncio
    from datetime import datetime, timedelta

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Basic profile (same regardless of period)
        profile_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}",
            params={
                "fields": "id,username,followers_count,media_count,website,biography",
                "access_token": access_token,
            }
        )
        profile = profile_resp.json()

        # Fetch last 12 months of data
        total_reach = 0
        total_impressions = 0
        total_profile_views = 0

        for i in range(12):
            since = int((datetime.utcnow() - timedelta(days=30 * (i + 1))).timestamp())
            until = int((datetime.utcnow() - timedelta(days=30 * i)).timestamp())

            try:
                insights_resp = await client.get(
                    f"{GRAPH_BASE}/{ig_user_id}/insights",
                    params={
                        "metric":       "reach,impressions,profile_views",
                        "period":       "month",
                        "since":        since,
                        "until":        until,
                        "access_token": access_token,
                    }
                )
                insights = insights_resp.json()
                for item in insights.get("data", []):
                    name = item.get("name")
                    values = item.get("values", [])
                    val = values[0].get("value", 0) if values else 0
                    if name == "reach":
                        total_reach += val
                    elif name == "impressions":
                        total_impressions += val
                    elif name == "profile_views":
                        total_profile_views += val
            except Exception as e:
                logger.warning(f"Month {i+1} insights failed: {e}")
                continue

    return {
        "username":        profile.get("username"),
        "followers_count": profile.get("followers_count", 0),
        "media_count":     profile.get("media_count", 0),
        "biography":       profile.get("biography", ""),
        "website":         profile.get("website", ""),
        "reach":           total_reach,
        "impressions":     total_impressions,
        "profile_views":   total_profile_views,
        "follower_count":  profile.get("followers_count", 0),
        "period":          "year",
    }


async def get_post_insights(post_id: str, access_token: str, period: str = "lifetime") -> dict:
    """Fetch ALL data for a single post or reel. period: lifetime | day | week | month"""
    async with httpx.AsyncClient(timeout=30.0) as client:

        # Step 1 — Full post/reel info
        post_resp = await client.get(
            f"{GRAPH_BASE}/{post_id}",
            params={
                "fields": (
                    "id,caption,media_type,media_url,thumbnail_url,"
                    "timestamp,like_count,comments_count,"
                    "permalink,username,is_shared_to_feed"
                ),
                "access_token": access_token,
            }
        )
        post_data = post_resp.json()
        logger.debug(f"Post data: {post_data}")

        media_type = post_data.get("media_type", "IMAGE")

        # Step 2 — Metrics differ for REELS vs IMAGE/VIDEO
        if media_type == "REELS":
            metrics_list = "plays,reach,likes,comments,shares,saved,total_interactions,ig_reels_avg_watch_time,ig_reels_video_view_total_time"
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
        logger.debug(f"Post insights: {insights}")

        # Step 3 — Fetch comments list
        comments_resp = await client.get(
            f"{GRAPH_BASE}/{post_id}/comments",
            params={
                "fields":       "id,text,username,timestamp,like_count,replies{{text,username,timestamp}}",
                "access_token": access_token,
            }
        )
        comments_data = comments_resp.json()

    # Parse metrics
    metrics = {}
    for item in insights.get("data", []):
        val = item.get("values", [{}])[0].get("value", 0) if item.get("values") else item.get("value", 0)
        metrics[item.get("name")] = val

    # Parse comments
    comments = []
    for c in comments_data.get("data", []):
        replies = [
            {
                "text":      r.get("text", ""),
                "username":  r.get("username", ""),
                "timestamp": r.get("timestamp", ""),
            }
            for r in c.get("replies", {}).get("data", [])
        ]
        comments.append({
            "id":         c.get("id"),
            "text":       c.get("text", ""),
            "username":   c.get("username", ""),
            "timestamp":  c.get("timestamp", ""),
            "like_count": c.get("like_count", 0),
            "replies":    replies,
        })

    return {
        # ── Post info ──────────────────────────
        "post_id":          post_data.get("id"),
        "caption":          post_data.get("caption", ""),
        "media_type":       media_type,
        "media_url":        post_data.get("media_url", ""),
        "thumbnail_url":    post_data.get("thumbnail_url", ""),
        "permalink":        post_data.get("permalink", ""),
        "username":         post_data.get("username", ""),
        "timestamp":        post_data.get("timestamp", ""),
        "period":           period,
        # ── Engagement ─────────────────────────
        "like_count":         post_data.get("like_count", 0),
        "comments_count":     post_data.get("comments_count", 0),
        "impressions":        metrics.get("impressions", 0),
        "reach":              metrics.get("reach", 0),
        "shares":             metrics.get("shares", 0),
        "saves":              metrics.get("saved", 0),
        "total_interactions": metrics.get("total_interactions", 0),
        # ── Reels only ─────────────────────────
        "plays":                        metrics.get("plays", 0),
        "avg_watch_time":               metrics.get("ig_reels_avg_watch_time", 0),
        "total_video_view_time":         metrics.get("ig_reels_video_view_total_time", 0),
        # ── Comments list ──────────────────────
        "comments": comments,
    }


async def get_all_posts_insights(ig_user_id: str, access_token: str) -> list:
    """Fetch all media posts with basic metrics."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}/media",
            params={
                "fields":       "id,caption,media_type,timestamp,like_count,comments_count,media_url",
                "access_token": access_token,
            }
        )
    data = resp.json()

    posts = []
    for post in data.get("data", []):
        posts.append({
            "post_id":        post.get("id"),
            "caption":        post.get("caption", "")[:100],
            "media_type":     post.get("media_type", ""),
            "media_url":      post.get("media_url", ""),
            "timestamp":      post.get("timestamp", ""),
            "like_count":     post.get("like_count", 0),
            "comments_count": post.get("comments_count", 0),
        })

    return posts
