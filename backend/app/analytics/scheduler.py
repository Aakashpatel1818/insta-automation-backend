import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.analytics.pg_database import AsyncSessionLocal
from app.analytics.service import (
    _fetch_account_insights_api,
    _upsert_post_cache,
    _fetch_post_insights_api,
    check_rate_limit,
    log_api_call,
)
from app.analytics.models import AccountMonthlyInsights
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)

_scheduler = AsyncIOScheduler(timezone="UTC")


def start_analytics_scheduler():
    """Register and start analytics background jobs."""
    _scheduler.add_job(
        refresh_monthly_account_insights,
        trigger=CronTrigger(hour=2, minute=0),   # 2 AM UTC daily
        id="refresh_monthly_insights",
        replace_existing=True,
    )
    _scheduler.add_job(
        refresh_active_post_caches,
        trigger=CronTrigger(hour=2, minute=30),  # 2:30 AM UTC daily
        id="refresh_post_caches",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("Analytics scheduler started (runs at 2AM and 2:30AM UTC daily).")


def stop_analytics_scheduler():
    if _scheduler.running:
        _scheduler.shutdown()
        logger.info("Analytics scheduler stopped.")


# ── Job 1: Refresh current month for all accounts ─────────
async def refresh_monthly_account_insights():
    """
    Daily at 2 AM:
    For each connected account → fetch current month → update DB.
    """
    logger.info("Scheduler: starting monthly account insights refresh")

    from app.database import get_db
    mongo_db = get_db()

    accounts = await mongo_db["instagram_accounts"].find({}).to_list(length=500)
    now = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        for account in accounts:
            account_id = str(account["_id"])
            try:
                if await check_rate_limit(session, account_id):
                    logger.warning(f"Rate limit — skipping account {account_id}")
                    continue

                data = await _fetch_account_insights_api(
                    ig_user_id=account["instagram_user_id"],
                    access_token=account["access_token"],
                    period="month",
                )
                await log_api_call(session, account_id, "scheduler_monthly_refresh")

                stmt = pg_insert(AccountMonthlyInsights).values(
                    account_id=account_id,
                    year=now.year,
                    month=now.month,
                    reach=data.get("reach", 0),
                    impressions=data.get("impressions", 0),
                    profile_views=data.get("profile_views", 0),
                    followers=data.get("followers", 0),
                    updated_at=now,
                ).on_conflict_do_update(
                    constraint="uq_account_month",
                    set_=dict(
                        reach=data.get("reach", 0),
                        impressions=data.get("impressions", 0),
                        profile_views=data.get("profile_views", 0),
                        followers=data.get("followers", 0),
                        updated_at=now,
                    )
                )
                await session.execute(stmt)
                await session.commit()
                logger.info(f"Monthly data refreshed for account {account.get('username')}")

            except Exception as e:
                logger.error(f"Scheduler monthly refresh failed for {account_id}: {e}")
                continue

    logger.info("Scheduler: monthly account insights refresh complete")


# ── Job 2: Refresh lifetime cache for active posts ────────
async def refresh_active_post_caches():
    """
    Daily at 2:30 AM:
    For each recently published post → refresh lifetime cache.
    """
    logger.info("Scheduler: starting post cache refresh")

    from app.database import get_db
    mongo_db = get_db()

    # Fetch posts from last 30 days
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    posts = await mongo_db["posts"].find({
        "published_at": {"$gte": cutoff}
    }).to_list(length=500)

    async with AsyncSessionLocal() as session:
        for post in posts:
            account_id = str(post.get("account_id", ""))
            post_id    = post.get("post_id", "")

            if not account_id or not post_id:
                continue

            try:
                account = await mongo_db["instagram_accounts"].find_one(
                    {"_id": __import__("bson").ObjectId(account_id)}
                )
                if not account:
                    continue

                if await check_rate_limit(session, account_id):
                    logger.warning(f"Rate limit — skipping post {post_id}")
                    continue

                data = await _fetch_post_insights_api(
                    post_id=post_id,
                    access_token=account["access_token"],
                    period="lifetime",
                )
                await log_api_call(session, account_id, "scheduler_post_cache_refresh")
                await _upsert_post_cache(session, post_id, account_id, "lifetime", data)
                logger.info(f"Post cache refreshed: {post_id}")

            except Exception as e:
                logger.error(f"Scheduler post cache failed for {post_id}: {e}")
                continue

    logger.info("Scheduler: post cache refresh complete")
