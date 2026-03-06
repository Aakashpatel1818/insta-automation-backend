import ssl
import redis.asyncio as redis
import logging
from app.config import settings

logger = logging.getLogger(__name__)

# Build SSL context for Redis Cloud (TLS required)
def _make_ssl_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

def _make_client():
    url = settings.REDIS_URL
    if url.startswith("rediss://"):
        return redis.from_url(
            url,
            socket_connect_timeout=10,
            socket_timeout=10,
            decode_responses=True,
            ssl_certfile=None,
            ssl_keyfile=None,
            ssl_ca_certs=None,
            ssl_cert_reqs="none",
        )
    return redis.from_url(
        url,
        socket_connect_timeout=10,
        socket_timeout=10,
        decode_responses=True,
    )

redis_client = _make_client()


async def ping_redis() -> bool:
    """Returns True if Redis is reachable, False otherwise. Never raises."""
    tmp = None
    try:
        tmp = _make_client()
        result = await tmp.ping()
        return result is True
    except Exception as e:
        logger.debug(f"Redis unreachable: {e}")
        return False
    finally:
        if tmp:
            await tmp.aclose()
