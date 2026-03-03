import logging
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from bson import ObjectId

from app.schemas.schedule import SchedulePostRequest, ScheduledPostPublic
from app.dependencies import get_current_user
from app.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instagram/scheduled", tags=["Scheduled Posts"])


# ── Schedule a post ───────────────────────────────────────
@router.post("/", response_model=ScheduledPostPublic, status_code=status.HTTP_201_CREATED)
async def schedule_post(
    body: SchedulePostRequest,
    current_user: dict = Depends(get_current_user),
):
    """Schedule a post to be published at a future date/time (UTC)."""
    db = get_db()

    # Must have IG connected
    ig_account = await db["instagram_accounts"].find_one(
        {"user_id": str(current_user["_id"])}
    )
    if not ig_account:
        raise HTTPException(
            status_code=404,
            detail="No Instagram account connected. Go to /instagram/connect first."
        )

    # Must be in the future
    if body.scheduled_at <= datetime.utcnow():
        raise HTTPException(
            status_code=400,
            detail="scheduled_at must be a future UTC datetime."
        )

    now = datetime.utcnow()
    doc = {
        "user_id":           str(current_user["_id"]),
        "instagram_user_id": ig_account["instagram_user_id"],
        "username":          ig_account["username"],
        "image_url":         body.image_url,
        "caption":           body.caption,
        "scheduled_at":      body.scheduled_at,
        "status":            "pending",
        "created_at":        now,
        "published_at":      None,
        "post_id":           None,
        "creation_id":       None,
        "error":             None,
    }

    result = await db["scheduled_posts"].insert_one(doc)
    logger.info(f"Post scheduled for {body.scheduled_at} by @{ig_account['username']}")

    return ScheduledPostPublic(
        id=str(result.inserted_id),
        image_url=doc["image_url"],
        caption=doc["caption"],
        scheduled_at=doc["scheduled_at"],
        status=doc["status"],
        instagram_username=doc["username"],
        created_at=doc["created_at"],
    )


# ── List scheduled posts ──────────────────────────────────
@router.get("/", response_model=List[ScheduledPostPublic])
async def list_scheduled_posts(
    current_user: dict = Depends(get_current_user),
):
    """List all scheduled posts for the current user."""
    db = get_db()

    posts = await db["scheduled_posts"].find(
        {"user_id": str(current_user["_id"])}
    ).sort("scheduled_at", 1).to_list(length=100)

    return [
        ScheduledPostPublic(
            id=str(p["_id"]),
            image_url=p["image_url"],
            caption=p["caption"],
            scheduled_at=p["scheduled_at"],
            status=p["status"],
            instagram_username=p["username"],
            created_at=p["created_at"],
            published_at=p.get("published_at"),
            error=p.get("error"),
        )
        for p in posts
    ]


# ── Cancel a scheduled post ───────────────────────────────
@router.delete("/{post_id}", status_code=status.HTTP_200_OK)
async def cancel_scheduled_post(
    post_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Cancel a pending scheduled post."""
    db = get_db()

    try:
        oid = ObjectId(post_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid post ID")

    post = await db["scheduled_posts"].find_one({
        "_id":     oid,
        "user_id": str(current_user["_id"])
    })

    if not post:
        raise HTTPException(status_code=404, detail="Scheduled post not found")

    if post["status"] != "pending":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel a post with status '{post['status']}'"
        )

    await db["scheduled_posts"].delete_one({"_id": oid})
    logger.info(f"Scheduled post {post_id} cancelled")

    return {"status": "cancelled", "message": "Scheduled post removed."}
