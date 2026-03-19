from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings
import logging

logger = logging.getLogger(__name__)

client: AsyncIOMotorClient = None
db = None


async def connect_db():
    global client, db
    try:
        client = AsyncIOMotorClient(
            settings.MONGODB_URL,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
            maxPoolSize=100,          # max connections in pool
            minPoolSize=10,           # keep 10 warm connections ready
            maxIdleTimeMS=30000,      # close idle connections after 30s
            waitQueueTimeoutMS=5000,  # fail fast if pool exhausted
        )
        await client.admin.command("ping")
        db = client[settings.DATABASE_NAME]
        logger.info(f"Connected to MongoDB: {settings.DATABASE_NAME}")
        await _ensure_indexes()
        logger.info("All MongoDB indexes ensured.")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


async def _ensure_indexes():
    """Create all indexes needed for performance and correctness.
    Safe to run on every startup — create_index is idempotent for existing indexes.
    """

    # users
    await db["users"].create_index("email",         unique=True, name="unique_email")
    await db["users"].create_index("referral_code", unique=True, sparse=True, name="unique_referral_code")

    # instagram_accounts
    await db["instagram_accounts"].create_index("user_id",           name="idx_accounts_user_id")
    await db["instagram_accounts"].create_index("instagram_user_id", name="idx_accounts_ig_user_id")
    await db["instagram_accounts"].create_index("is_active",         name="idx_accounts_active")

    # automation_settings
    await db["automation_settings"].create_index("account_id", name="idx_auto_settings_account")
    await db["automation_settings"].create_index("post_id",    name="idx_auto_settings_post")
    await db["automation_settings"].create_index(
        [("account_id", 1), ("post_id", 1)],
        unique=True, name="unique_auto_settings_account_post"
    )

    # keyword_rules
    await db["keyword_rules"].create_index("account_id", name="idx_rules_account")
    await db["keyword_rules"].create_index("post_id",    name="idx_rules_post")
    await db["keyword_rules"].create_index("is_active",  name="idx_rules_active")
    await db["keyword_rules"].create_index(
        [("account_id", 1), ("post_id", 1)],
        name="idx_rules_account_post"
    )

    # scheduled_posts
    await db["scheduled_posts"].create_index("user_id",    name="idx_sched_user")
    await db["scheduled_posts"].create_index("account_id", name="idx_sched_account")
    await db["scheduled_posts"].create_index(
        [("status", 1), ("scheduled_at", 1)],
        name="idx_sched_status_time"  # used by scheduler query every 60s
    )

    # automation_logs
    # MongoDB only allows one index per key field. The TTL index on created_at
    # also serves as a sort index, so we drop any plain idx_logs_created that
    # may already exist before creating the TTL version.
    await db["automation_logs"].create_index("account_id", name="idx_logs_account")
    await db["automation_logs"].create_index("post_id",    name="idx_logs_post")
    try:
        await db["automation_logs"].drop_index("idx_logs_created")
        logger.info("Dropped old idx_logs_created — replacing with TTL index.")
    except Exception:
        pass  # index didn't exist — that's fine
    await db["automation_logs"].create_index(
        "created_at",
        expireAfterSeconds=60 * 60 * 24 * 90,  # auto-delete logs after 90 days
        name="ttl_logs_90d"
    )

    # referral_events
    await db["referral_events"].create_index("referrer_id", name="idx_ref_referrer")
    await db["referral_events"].create_index("referred_id", name="idx_ref_referred")
    await db["referral_events"].create_index("milestone",   name="idx_ref_milestone")

    # dm_cooldowns
    await db["dm_cooldowns"].create_index(
        [("account_id", 1), ("commenter_id", 1)],
        name="idx_cooldown_account_user"
    )
    await db["dm_cooldowns"].create_index(
        "expires_at",
        expireAfterSeconds=0,  # MongoDB TTL: auto-delete when expires_at passes
        name="ttl_cooldowns"
    )


async def disconnect_db():
    global client
    if client:
        client.close()
        logger.info("MongoDB connection closed.")


def get_db():
    return db


def get_client():
    return client
