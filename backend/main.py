from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import logging, os, time

from app.database import connect_db, disconnect_db
from app.config import settings
from app.routers import auth, instagram, webhook, schedule, analytics, automation
from app.analytics.router import router as analytics_v2_router
from app.analytics.pg_database import init_pg_db
from app.analytics.scheduler import start_analytics_scheduler, stop_analytics_scheduler
from app.scheduler import start_scheduler, stop_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await connect_db()
    logger.info("MongoDB connected.")
    await init_pg_db()
    logger.info("PostgreSQL ready.")
    start_scheduler()
    start_analytics_scheduler()
    yield
    stop_scheduler()
    stop_analytics_scheduler()
    await disconnect_db()
    logger.info("Shutdown complete.")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs("static/uploads", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ═══════════════════════════════════════════════════════════
# DEBUG ROUTES - no auth, registered BEFORE all routers
# ═══════════════════════════════════════════════════════════

@app.get("/debug/info")
async def debug_info():
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
                    "test_url":           f"http://127.0.0.1:8000/debug/test?post_id={a['post_id']}&account_id={aid}&comment_text=test",
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
async def debug_test(post_id: str, account_id: str, comment_text: str = "test"):
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
    logger.info(f"[DEBUG TEST] post={post_id} account=@{account.get('username')} comment='{comment_text}'")
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
        "status": "triggered",
        "post_id": post_id,
        "account": account.get("username"),
        "comment_text": comment_text,
        "note": "Check uvicorn terminal for [Engine] logs",
    }


# ═══════════════════════════════════════════════════════════
# ROUTERS
# ═══════════════════════════════════════════════════════════

app.include_router(auth.router)
app.include_router(instagram.router)
app.include_router(webhook.router)
app.include_router(schedule.router)
app.include_router(analytics.router)
app.include_router(analytics_v2_router)
app.include_router(automation.router)


# ═══════════════════════════════════════════════════════════
# STANDARD ROUTES
# ═══════════════════════════════════════════════════════════

@app.get("/")
async def health_check():
    return {"status": "ok", "app": settings.APP_NAME, "version": settings.APP_VERSION}


@app.get("/system/health")
async def system_health():
    import redis.asyncio as aioredis
    from app.analytics.pg_database import engine as pg_engine
    from app.database import get_client
    from sqlalchemy import text
    errors = {}
    try:
        r = aioredis.from_url(settings.REDIS_URL, socket_connect_timeout=2, socket_timeout=2)
        pong = await r.ping()
        await r.aclose()
        redis_ok = bool(pong)
    except Exception as e:
        errors["redis"] = str(e); redis_ok = False
    try:
        async with pg_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        postgres_ok = True
    except Exception as e:
        errors["postgres"] = str(e); postgres_ok = False
    try:
        mc = get_client()
        await mc.admin.command("ping")
        mongo_ok = True
    except Exception as e:
        errors["mongo"] = str(e); mongo_ok = False
    return {
        "status": "ok", "app": settings.APP_NAME, "version": settings.APP_VERSION,
        "services": {
            "redis":     "ok" if redis_ok    else "error",
            "postgres":  "ok" if postgres_ok else "error",
            "mongo":     "ok" if mongo_ok    else "error",
            "scheduler": "ok",
        },
        "errors": errors,
    }


@app.get("/test-redis")
async def test_redis():
    import redis.asyncio as aioredis
    try:
        r = aioredis.from_url(settings.REDIS_URL, socket_connect_timeout=2, socket_timeout=2)
        pong = await r.ping()
        await r.aclose()
        return {"ok": bool(pong), "redis_url": settings.REDIS_URL}
    except Exception as e:
        return {"ok": False, "error": str(e)}
