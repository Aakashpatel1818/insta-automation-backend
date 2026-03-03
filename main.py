from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.database import connect_db, disconnect_db
from app.config import settings
from app.routers import auth, instagram, webhook, schedule
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
    logger.info("MongoDB connected successfully.")
    start_scheduler()
    yield
    stop_scheduler()
    logger.info("Shutting down...")
    await disconnect_db()
    logger.info("MongoDB disconnected.")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(instagram.router)
app.include_router(webhook.router)
app.include_router(schedule.router)


# ── Health ────────────────────────────────────────────────
@app.get("/")
async def health_check():
    return {
        "status": "ok",
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "message": "Server is running. MongoDB is connected."
    }
