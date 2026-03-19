import logging
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from app.dependencies import get_current_user
from app.database import get_db
from bson import ObjectId

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/engagement", tags=["Engagement"])

GRAPH_BASE = "https://graph.instagram.com/v19.0"


async def get_account(db, user_id: str, account_id: str):
    try:
        account = await db["instagram_accounts"].find_one({
            "_id": ObjectId(account_id), "user_id": user_id
        })
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid account ID")
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


# ── Get comments ───────────────────────────────────────────
@router.get("/comments")
async def get_comments(
    account_id: str = Query(...),
    limit: int = Query(50, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    account = await get_account(db, str(current_user["_id"]), account_id)
    ig_user_id   = account["instagram_user_id"]
    access_token = account["access_token"]

    # Step 1: Get recent media
    async with httpx.AsyncClient(timeout=30.0) as client:
        media_resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}/media",
            params={
                "fields": "id,caption,permalink,thumbnail_url,media_url,timestamp",
                "access_token": access_token,
                "limit": 10,  # fetch comments from last 10 posts
            }
        )
    media_data = media_resp.json()
    if "error" in media_data:
        raise HTTPException(status_code=502, detail=media_data["error"].get("message", "API error"))

    posts = media_data.get("data", [])
    all_comments = []

    # Step 2: For each post, fetch comments
    async with httpx.AsyncClient(timeout=30.0) as client:
        for post in posts:
            post_id = post["id"]
            comments_resp = await client.get(
                f"{GRAPH_BASE}/{post_id}/comments",
                params={
                    "fields": "id,text,username,timestamp,like_count,hidden",
                    "access_token": access_token,
                    "limit": limit,
                }
            )
            comments_data = comments_resp.json()
            if "error" in comments_data:
                logger.warning(f"[comments] Error fetching comments for post {post_id}: {comments_data['error']}")
                continue

            for comment in comments_data.get("data", []):
                all_comments.append({
                    **comment,
                    "post_id":        post_id,
                    "post_caption":   post.get("caption", ""),
                    "post_permalink": post.get("permalink", ""),
                    "post_thumbnail": post.get("thumbnail_url") or post.get("media_url", ""),
                })

    # Sort by most recent first
    all_comments.sort(key=lambda c: c.get("timestamp", ""), reverse=True)

    return {"comments": all_comments[:limit], "total": len(all_comments)}


# ── Get mentions ───────────────────────────────────────────
@router.get("/mentions")
async def get_mentions(
    account_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    account = await get_account(db, str(current_user["_id"]), account_id)
    ig_user_id   = account["instagram_user_id"]
    access_token = account["access_token"]

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{GRAPH_BASE}/{ig_user_id}/tags",
            params={
                "fields": "id,caption,media_type,timestamp,media_url,thumbnail_url,permalink",
                "access_token": access_token,
                "limit": 50,
            }
        )
    data = resp.json()
    if "error" in data:
        raise HTTPException(status_code=502, detail=data["error"].get("message", "API error"))
    return {"mentions": data.get("data", []), "total": len(data.get("data", []))}
