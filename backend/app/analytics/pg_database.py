from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings
import logging

logger = logging.getLogger(__name__)

engine = create_async_engine(
    settings.POSTGRES_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def init_pg_db():
    """Create all tables on startup and migrate timestamp columns to TIMESTAMPTZ."""
    from app.analytics.models import AccountMonthlyInsights, PostInsightsCache, ApiUsageLog  # noqa
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # ── Migrate existing TIMESTAMP columns to TIMESTAMPTZ ──
        # Safe to run repeatedly — ALTER TYPE is idempotent on already-tz columns.
        migrations = [
            "ALTER TABLE api_usage_logs ALTER COLUMN timestamp TYPE TIMESTAMPTZ USING timestamp AT TIME ZONE 'UTC'",
            "ALTER TABLE account_monthly_insights ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC'",
            "ALTER TABLE post_insights_cache ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC'",
        ]
        for sql in migrations:
            try:
                await conn.execute(__import__('sqlalchemy').text(sql))
                logger.info(f"Migration OK: {sql[:60]}...")
            except Exception as e:
                # Column already TIMESTAMPTZ or table doesn't exist yet — both fine
                logger.debug(f"Migration skipped ({e}): {sql[:60]}...")

    logger.info("PostgreSQL tables created/verified.")


async def get_pg_session() -> AsyncSession:
    """Dependency — yields a PostgreSQL session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
