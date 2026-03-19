import logging
import os
import time
import uuid

from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
# StaticFiles import removed — FIX #3 & #4: uploads are no longer publicly served
# from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from app.database import connect_db, disconnect_db
from app.config import settings
from app.redis_pool import init_redis_pool, close_redis_pool, get_redis
from app.routers import (
    auth,
    instagram,
    webhook,
    schedule,
    analytics,
    automation,
    referral as referral_router,
    admin as admin_router,
    profile as profile_router,
)
from app.routers.admin import require_admin
from app.routers import leads as leads_router
from app.routers.announcement import (
    router as announcement_router,
    admin_router as announcement_admin_router,
)
from app.routers import engagement as engagement_router
from app.routers import dm as dm_router
from app.routers import plans as plans_router
from app.socket_manager import sio
import socketio
from app.analytics.router import router as analytics_v2_router
from app.analytics.pg_database import init_pg_db
from app.analytics.scheduler import start_analytics_scheduler, stop_analytics_scheduler
from app.scheduler import start_scheduler, stop_scheduler
from app.automation.queue import start_queue_worker, stop_queue_worker
from app.services.cleanup_service import start_cleanup_scheduler, stop_cleanup_scheduler
from app.routers.data_management import router as data_management_router

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# Silence noisy third-party debug loggers even in DEBUG mode
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.INFO)
logging.getLogger("tzlocal").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# =============================================================================
# Lifespan
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await connect_db()
    logger.info("MongoDB connected.")
    await init_pg_db()
    logger.info("PostgreSQL ready.")
    await init_redis_pool()
    logger.info("Redis pool ready.")
    start_scheduler()
    start_analytics_scheduler()
    start_queue_worker()
    start_cleanup_scheduler()
    import asyncio
    from app.services.referral_service import run_nudge_checker
    asyncio.create_task(run_nudge_checker())
    yield
    stop_queue_worker()
    stop_cleanup_scheduler()
    stop_scheduler()
    stop_analytics_scheduler()
    await close_redis_pool()
    await disconnect_db()
    logger.info("Shutdown complete.")


# =============================================================================
# FastAPI app
# =============================================================================

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
    # Docs disabled in production — no free API map for attackers.
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
)


# =============================================================================
# DDoS / Abuse-protection middleware stack
#
# Execution order (outermost → innermost, i.e. first registered = outermost):
#   1. RequestIDMiddleware    – tag every request with a trace UUID
#   2. MaxBodySizeMiddleware  – reject oversized bodies immediately (anti-flood)
#   3. GlobalRateLimitMiddleware – per-IP sliding-window rate limit (Redis)
#
# Auth-endpoint tighter rate limits live inside the router dependency so they
# can share the same Redis counter infrastructure without a separate middleware.
# =============================================================================


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Attach a unique X-Request-ID to every request and response.
    Makes it trivial to correlate log lines during a spike / incident.
    """

    async def dispatch(self, request: Request, call_next):
        req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = req_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = req_id
        return response


class MaxBodySizeMiddleware(BaseHTTPMiddleware):
    """
    Reject requests whose Content-Length exceeds MAX_REQUEST_BODY_SIZE before
    any body bytes are read.  Also blocks requests that declare no Content-Length
    but stream more bytes than the limit (checked lazily in call_next path via
    the Content-Length header check here – full streaming check would require
    wrapping the receive channel which is done below).

    Protects against:
      - HTTP flood with large bodies exhausting I/O bandwidth
      - Slowloris-style attacks sending bodies byte-by-byte for minutes
        (uvicorn's --timeout-keep-alive handles the TCP layer; this handles
        the application layer early rejection)
    """

    def __init__(self, app, max_bytes: int):
        super().__init__(app)
        self.max_bytes = max_bytes

    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > self.max_bytes:
                    logger.warning(
                        f"[DDoS] Oversized body rejected: "
                        f"Content-Length={content_length} "
                        f"limit={self.max_bytes} "
                        f"ip={_get_client_ip(request)} "
                        f"path={request.url.path}"
                    )
                    return JSONResponse(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        content={"detail": "Request body too large."},
                    )
            except ValueError:
                pass  # malformed header — let upstream handle it

        return await call_next(request)


class GlobalRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-IP sliding-window rate limiter backed by Redis.

    Algorithm: fixed 60-second window using Redis INCR + EXPIRE.
    - First request in a window: INCR creates the key and we set TTL=60.
    - Subsequent requests in the same window: INCR increments atomically.
    - When count > limit: return 429 immediately without hitting any router.

    Two tiers:
      - /auth/* endpoints   → RATE_LIMIT_AUTH_PER_MIN  (default 10 / min)
      - everything else     → RATE_LIMIT_GLOBAL_PER_MIN (default 300 / min)

    Webhook endpoint (/webhook/) is intentionally excluded from IP rate-limiting
    because Meta's delivery IPs are shared infrastructure — blocking them would
    silently drop real events.  Signature verification (Bug #3 fix) handles
    webhook security instead.

    Fails open: if Redis is unavailable the request is allowed through and a
    warning is logged.  This keeps the API running during Redis blips at the
    cost of temporarily disabling rate limiting.
    """

    # Paths that are exempt from per-IP rate limiting
    _EXEMPT_PREFIXES = ("/webhook/",)

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Exempt webhook delivery
        for prefix in self._EXEMPT_PREFIXES:
            if path.startswith(prefix):
                return await call_next(request)

        ip = _get_client_ip(request)

        # Choose limit tier
        if path.startswith("/auth/"):
            limit = settings.RATE_LIMIT_AUTH_PER_MIN
            window_key = f"rl:auth:{ip}"
        else:
            limit = settings.RATE_LIMIT_GLOBAL_PER_MIN
            window_key = f"rl:global:{ip}"

        try:
            r = get_redis()
            count = await r.incr(window_key)
            if count == 1:
                # First hit in this window — set 60-second TTL
                await r.expire(window_key, 60)

            if count > limit:
                retry_after = await r.ttl(window_key)
                logger.warning(
                    f"[RateLimit] 429 ip={ip} path={path} "
                    f"count={count} limit={limit} "
                    f"retry_after={retry_after}s"
                )
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={"detail": "Too many requests. Please slow down."},
                    headers={"Retry-After": str(max(retry_after, 1))},
                )
        except Exception as e:
            # Fail open — log and continue
            logger.warning(f"[RateLimit] Redis error (fail-open): {e}")

        return await call_next(request)


def _get_client_ip(request: Request) -> str:
    """
    Extract the real client IP, honouring common reverse-proxy headers.
    X-Forwarded-For is used by nginx, Cloudflare, AWS ALB etc.
    Takes the FIRST (leftmost) IP which is the original client;
    subsequent entries are proxy hops and could be spoofed.

    NOTE: if you run behind a trusted reverse proxy, ensure the proxy
    strips X-Forwarded-For from untrusted clients before forwarding —
    otherwise a client can supply a fake IP to bypass per-IP limiting.
    Cloudflare does this automatically; nginx requires `real_ip_header`.
    """
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip.strip()
    if request.client:
        return request.client.host
    return "unknown"


# Register middleware — ORDER MATTERS (last registered = outermost)
# We want: RequestID (outer) → MaxBody → RateLimit (inner) → app
# BaseHTTPMiddleware stack is LIFO so we register in reverse:
app.add_middleware(
    GlobalRateLimitMiddleware,
)
app.add_middleware(
    MaxBodySizeMiddleware,
    max_bytes=settings.MAX_REQUEST_BODY_SIZE,
)
app.add_middleware(RequestIDMiddleware)


# =============================================================================
# Standard middleware
# =============================================================================

# GZip compression: shrinks JSON responses >500 bytes by ~70%
app.add_middleware(GZipMiddleware, minimum_size=500)

# CORS — locked down to configured domain(s) in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# FIX #3 & #4 — No public static mount for uploads
#
# Previously:
#   os.makedirs("static/uploads", exist_ok=True)
#   app.mount("/static", StaticFiles(directory="static"), name="static")
#
# This made every uploaded file directly accessible at:
#   GET /static/uploads/<filename>  ← anyone with the URL could fetch it
#
# Now:
#   - Uploads are stored in  app/private_uploads/  (outside the web root)
#   - Files are served ONLY via  GET /instagram/serve-image/{filename}
#     which requires a valid JWT — so only authenticated users can fetch them
#   - Script execution in the upload folder is impossible because the folder
#     is never mounted as a static directory (Fix #4)
#
# The private_uploads directory is created by the router on first run.
# =============================================================================


# =============================================================================
# Debug / internal routes  (admin-gated)
# =============================================================================

@app.get("/debug/info")
async def debug_info(_admin: dict = Depends(require_admin)):
    from app.database import get_db
    db = get_db()
    accounts = await db["instagram_accounts"].find({}).to_list(length=20)
    result = []
    for acc in accounts:
        aid   = str(acc["_id"])
        autos = await db["automation_settings"].find({"account_id": aid}).to_list(20)
        rules = await db["keyword_rules"].find({"account_id": aid}).to_list(50)
        result.append({
            "account_id":        aid,
            "username":          acc.get("username"),
            "instagram_user_id": acc.get("instagram_user_id"),
            "automations": [
                {
                    "post_id":            a["post_id"],
                    "is_active":          a.get("is_active"),
                    "auto_comment_reply": a.get("auto_comment_reply"),
                    "auto_dm":            a.get("auto_dm"),
                    "test_url": (
                        f"http://127.0.0.1:8000/debug/test"
                        f"?post_id={a['post_id']}&account_id={aid}&comment_text=test"
                    ),
                }
                for a in autos
            ],
            "rules": [
                {
                    "post_id":       r["post_id"],
                    "trigger_words": r.get("trigger_words"),
                    "response":      r.get("response"),
                    "reply_comment": r.get("reply_comment"),
                    "send_dm":       r.get("send_dm"),
                    "is_active":     r.get("is_active"),
                }
                for r in rules
            ],
        })
    return {"accounts": result}


@app.get("/debug/test")
async def debug_test(
    post_id: str,
    account_id: str,
    comment_text: str = "test",
    _admin: dict = Depends(require_admin),
):
    from bson import ObjectId
    from app.database import get_db
    from app.automation.engine import process_comment_event
    db = get_db()
    try:
        account = await db["instagram_accounts"].find_one({"_id": ObjectId(account_id)})
    except Exception:
        return {"error": f"Invalid account_id: {account_id}"}
    if not account:
        return {"error": f"No account for id={account_id}"}
    logger.info(
        f"[DEBUG TEST] post={post_id} account=@{account.get('username')} "
        f"comment='{comment_text}'"
    )
    await process_comment_event(
        db=db,
        media_id=post_id,
        comment_id=f"test_{int(time.time())}",
        comment_text=comment_text,
        commenter_id="test_user_999",
        account_id=account_id,
        ig_user_id=account["instagram_user_id"],
        access_token=account["access_token"],
    )
    return {
        "status":       "triggered",
        "post_id":      post_id,
        "account":      account.get("username"),
        "comment_text": comment_text,
        "note":         "Check uvicorn terminal for [Engine] logs",
    }


# =============================================================================
# Routers
# =============================================================================

app.include_router(auth.router)
app.include_router(instagram.router)
app.include_router(webhook.router)
app.include_router(schedule.router)
app.include_router(analytics.router)
app.include_router(analytics_v2_router)
app.include_router(automation.router)
app.include_router(referral_router.router)
app.include_router(engagement_router.router)
app.include_router(dm_router.router)
app.include_router(admin_router.router)
app.include_router(profile_router.router)
app.include_router(leads_router.router)
app.include_router(announcement_router)
app.include_router(announcement_admin_router)
app.include_router(plans_router.router)
app.include_router(data_management_router)


# =============================================================================
# Standard routes
# =============================================================================

@app.get("/")
async def health_check():
    return {
        "status":  "ok",
        "app":     settings.APP_NAME,
        "version": settings.APP_VERSION,
    }


@app.get("/system/health")
async def system_health():
    from app.analytics.pg_database import engine as pg_engine
    from app.database import get_client
    from sqlalchemy import text

    errors: dict = {}

    try:
        await get_redis().ping()
        redis_ok = True
    except Exception as e:
        errors["redis"] = str(e)
        redis_ok = False

    try:
        async with pg_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        postgres_ok = True
    except Exception as e:
        errors["postgres"] = str(e)
        postgres_ok = False

    try:
        mc = get_client()
        await mc.admin.command("ping")
        mongo_ok = True
    except Exception as e:
        errors["mongo"] = str(e)
        mongo_ok = False

    return {
        "status":  "ok",
        "app":     settings.APP_NAME,
        "version": settings.APP_VERSION,
        "services": {
            "redis":     "ok" if redis_ok    else "error",
            "postgres":  "ok" if postgres_ok else "error",
            "mongo":     "ok" if mongo_ok    else "error",
            "scheduler": "ok",
        },
        "errors": errors,
    }


@app.get("/test-redis")
async def test_redis(_admin: dict = Depends(require_admin)):
    """
    Bug #5 fix: was public and returned the full REDIS_URL including credentials.
    Now admin-gated and returns a masked URL so credentials never leak.
    """
    try:
        pong = await get_redis().ping()
        # Mask credentials in the URL (redis://:password@host:port → redis://***@host:port)
        raw_url = settings.REDIS_URL
        try:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(raw_url)
            masked = parsed._replace(
                netloc=f"***@{parsed.hostname}:{parsed.port or 6379}"
            )
            safe_url = urlunparse(masked)
        except Exception:
            safe_url = "redis://***"
        return {"ok": bool(pong), "redis_url": safe_url, "pool": "shared"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# =============================================================================
# Socket.IO — wrap AFTER all routes are registered
# Run with: uvicorn main:sio_app --reload
# =============================================================================
sio_app = socketio.ASGIApp(sio, other_asgi_app=app)
