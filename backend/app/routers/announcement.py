# routers/announcement.py
"""
Admin announcement banner — show/hide a dismissable notice to all users.
POST /admin/announcement   → set/update announcement (admin only)
DELETE /admin/announcement → clear announcement (admin only)
GET  /announcement         → public read (any logged-in user)
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import json
import logging

from app.redis_pool import get_redis
from app.dependencies import get_current_user

logger = logging.getLogger(__name__)

router      = APIRouter(tags=["Announcement"])
admin_router = APIRouter(prefix="/admin", tags=["Admin"])

_KEY = "platform:announcement"

class AnnouncementBody(BaseModel):
    message: str
    type: str = "info"   # info | warning | success | error


# ── Admin: set announcement ───────────────────────────────

async def require_admin(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") not in ("admin", "superadmin"):
        raise HTTPException(status_code=403, detail="Admin access required.")
    return current_user

@admin_router.post("/announcement")
async def set_announcement(
    body: AnnouncementBody,
    admin: dict = Depends(require_admin),
):
    redis = get_redis()
    payload = {
        "message":    body.message,
        "type":       body.type,
        "created_at": datetime.utcnow().isoformat(),
        "created_by": admin.get("username", "admin"),
    }
    await redis.set(_KEY, json.dumps(payload))
    logger.info(f"[Announcement] Set by {admin['email']}: {body.message[:60]}")
    return {"ok": True, "announcement": payload}


@admin_router.delete("/announcement")
async def clear_announcement(admin: dict = Depends(require_admin)):
    redis = get_redis()
    await redis.delete(_KEY)
    logger.info(f"[Announcement] Cleared by {admin['email']}")
    return {"ok": True}


# ── Public: read announcement ─────────────────────────────

@router.get("/announcement")
async def get_announcement(_: dict = Depends(get_current_user)):
    redis = get_redis()
    raw = await redis.get(_KEY)
    if not raw:
        return {"announcement": None}
    return {"announcement": json.loads(raw)}
