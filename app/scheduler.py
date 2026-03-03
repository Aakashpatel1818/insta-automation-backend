import asyncio
import logging
from datetime import datetime, timezone

from app.database import get_db
from app.services.instagram_service import create_instagram_post

logger = logging.getLogger(__name__)

_scheduler_task = None


async def process_scheduled_posts():
    """
    Runs every 60 seconds.
    Finds all pending posts where scheduled_at <= now → publishes them.
    """
    while True:
        try:
            db = get_db()
            now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC

            # Find all due pending posts
            due_posts = await db["scheduled_posts"].find({
                "status": "pending",
                "scheduled_at": {"$lte": now}
            }).to_list(length=100)

            if due_posts:
                logger.info(f"Scheduler: found {len(due_posts)} post(s) to publish")

            for post in due_posts:
                post_id = post["_id"]
                logger.info(f"Publishing scheduled post {post_id}")

                # Mark as processing to avoid double-publish
                await db["scheduled_posts"].update_one(
                    {"_id": post_id},
                    {"$set": {"status": "processing"}}
                )

                try:
                    # Get fresh IG account token
                    ig_account = await db["instagram_accounts"].find_one(
                        {"user_id": post["user_id"]}
                    )

                    if not ig_account:
                        raise Exception("Instagram account not connected")

                    result = await create_instagram_post(
                        ig_user_id=ig_account["instagram_user_id"],
                        access_token=ig_account["access_token"],
                        image_url=post["image_url"],
                        caption=post["caption"],
                    )

                    # Mark as published
                    await db["scheduled_posts"].update_one(
                        {"_id": post_id},
                        {"$set": {
                            "status":       "published",
                            "published_at": datetime.utcnow(),
                            "post_id":      result["post_id"],
                            "creation_id":  result["creation_id"],
                            "error":        None,
                        }}
                    )
                    logger.info(f"Scheduled post {post_id} published successfully → IG post {result['post_id']}")

                except Exception as e:
                    logger.error(f"Scheduled post {post_id} failed: {e}")
                    # Mark as failed with error message
                    await db["scheduled_posts"].update_one(
                        {"_id": post_id},
                        {"$set": {
                            "status": "failed",
                            "error":  str(e),
                        }}
                    )

        except Exception as e:
            logger.error(f"Scheduler loop error: {e}")

        # Wait 60 seconds before next check
        await asyncio.sleep(60)


def start_scheduler():
    """Start the background scheduler task."""
    global _scheduler_task
    _scheduler_task = asyncio.create_task(process_scheduled_posts())
    logger.info("Post scheduler started — checking every 60 seconds.")


def stop_scheduler():
    """Stop the background scheduler task."""
    global _scheduler_task
    if _scheduler_task:
        _scheduler_task.cancel()
        logger.info("Post scheduler stopped.")
