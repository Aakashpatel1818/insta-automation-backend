# app/redis_pool.py
# Shared async Redis connection pool — import `get_redis` anywhere you need Redis.
# One pool is created at startup and reused across all requests instead of
# opening a new connection on every call.
import redis.asyncio as aioredis
import logging
from app.config import settings

logger = logging.getLogger(__name__)

_pool: aioredis.Redis | None = None


async def init_redis_pool() -> None:
    """Create the shared Redis pool. Called once at app startup."""
    global _pool
    _pool = aioredis.from_url(
        settings.REDIS_URL,
        max_connections=settings.REDIS_POOL_SIZE,
        socket_connect_timeout=settings.REDIS_POOL_TIMEOUT,
        socket_timeout=settings.REDIS_POOL_TIMEOUT,
        decode_responses=True,
        health_check_interval=30,   # auto-ping to keep connections alive
    )
    # Verify connection
    await _pool.ping()
    logger.info(f"Redis pool ready (max={settings.REDIS_POOL_SIZE}) → {settings.REDIS_URL}")


async def close_redis_pool() -> None:
    """Close the pool on shutdown."""
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None
        logger.info("Redis pool closed.")


def get_redis() -> aioredis.Redis:
    """Return the shared Redis client. Use as a FastAPI dependency or call directly."""
    if _pool is None:
        raise RuntimeError("Redis pool not initialised — call init_redis_pool() at startup")
    return _pool
