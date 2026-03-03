import secrets
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from bson import ObjectId

from app.schemas.instagram import InstagramConnectURL, InstagramAccountPublic
from app.schemas.post import PostRequest, PostResponse
from app.dependencies import get_current_user
from app.database import get_db
from app.instagram_oauth import (
    build_auth_url,
    exchange_code_for_short_token,
    exchange_for_long_lived_token,
    fetch_ig_profile,
)
from app.services.instagram_service import create_instagram_post

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instagram", tags=["Instagram"])


# ── Step 1: Get the OAuth URL ─────────────────────────────
@router.get("/connect", response_model=InstagramConnectURL)
async def get_connect_url(current_user: dict = Depends(get_current_user)):
    """
    Returns the Instagram OAuth URL.
    Frontend opens this URL so the user can authorize.
    State = user_id (so we know who's connecting on callback).
    """
    state = str(current_user["_id"])
    url = build_auth_url(state=state)
    return InstagramConnectURL(auth_url=url)


# ── Step 2: OAuth Callback ────────────────────────────────
@router.get("/callback")
async def instagram_callback(
    code: str = Query(...),
    state: str = Query(...),
    error: str = Query(default=None),
    error_reason: str = Query(default=None),
):
    """
    Instagram redirects here after user authorizes (or denies).
    state = user_id from Step 1.
    """
    if error:
        logger.warning(f"Instagram OAuth denied: {error_reason}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Instagram authorization denied: {error_reason or error}"
        )

    db = get_db()

    try:
        user = await db["users"].find_one({"_id": ObjectId(state)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid state parameter")

    if not user:
        raise HTTPException(status_code=400, detail="User not found for given state")

    try:
        short_data = await exchange_code_for_short_token(code)
    except Exception as e:
        logger.error(f"Short token exchange failed: {e}")
        raise HTTPException(status_code=502, detail=f"Token exchange failed: {str(e)}")

    short_token = short_data.get("access_token")
    if not short_token:
        raise HTTPException(status_code=502, detail="No access_token in Instagram response")

    try:
        long_data = await exchange_for_long_lived_token(short_token)
    except Exception as e:
        logger.error(f"Long token exchange failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to get long-lived token from Instagram")

    long_token = long_data.get("access_token")
    expires_in = long_data.get("expires_in", 0)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in) if expires_in else None

    try:
        profile = await fetch_ig_profile(long_token)
    except Exception as e:
        logger.error(f"IG profile fetch failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch Instagram profile")

    ig_user_id  = profile.get("id")
    ig_username = profile.get("username")

    if not ig_user_id:
        raise HTTPException(status_code=502, detail="Could not retrieve Instagram user ID")

    now = datetime.utcnow()
    await db["instagram_accounts"].update_one(
        {"instagram_user_id": ig_user_id},
        {"$set": {
            "user_id":           str(user["_id"]),
            "instagram_user_id": ig_user_id,
            "username":          ig_username,
            "access_token":      long_token,
            "token_expires_at":  expires_at,
            "updated_at":        now,
        },
         "$setOnInsert": {"connected_at": now}
        },
        upsert=True,
    )

    logger.info(f"Instagram account @{ig_username} connected for user {user['email']}")

    return {
        "status": "connected",
        "instagram_username": ig_username,
        "instagram_user_id":  ig_user_id,
        "token_expires_at":   expires_at,
        "message": "Instagram account successfully connected and token saved."
    }


# ── View connected IG account ─────────────────────────────
@router.get("/me", response_model=InstagramAccountPublic)
async def get_my_instagram(current_user: dict = Depends(get_current_user)):
    db = get_db()
    account = await db["instagram_accounts"].find_one(
        {"user_id": str(current_user["_id"])}
    )
    if not account:
        raise HTTPException(status_code=404, detail="No Instagram account connected")

    return InstagramAccountPublic(
        id=str(account["_id"]),
        instagram_user_id=account["instagram_user_id"],
        username=account["username"],
        token_expires_at=account.get("token_expires_at"),
        connected_at=account["connected_at"],
    )


# ── Disconnect IG account ─────────────────────────────────
@router.delete("/disconnect", status_code=status.HTTP_200_OK)
async def disconnect_instagram(current_user: dict = Depends(get_current_user)):
    db = get_db()
    result = await db["instagram_accounts"].delete_one(
        {"user_id": str(current_user["_id"])}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="No Instagram account connected")

    logger.info(f"Instagram disconnected for user {current_user['email']}")
    return {"status": "disconnected", "message": "Instagram account removed."}


# ── POST to Instagram ─────────────────────────────────────
@router.post("/post", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
async def post_to_instagram(
    body: PostRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Publish a photo post to the connected Instagram account.
    image_url must be a publicly accessible HTTPS URL.
    """
    db = get_db()

    # 1. Load connected IG account
    account = await db["instagram_accounts"].find_one(
        {"user_id": str(current_user["_id"])}
    )
    if not account:
        raise HTTPException(
            status_code=404,
            detail="No Instagram account connected. Go to /instagram/connect first."
        )

    # 2. Call the 2-step service
    try:
        result = await create_instagram_post(
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
            image_url=body.image_url,
            caption=body.caption,
        )
    except Exception as e:
        logger.error(f"Post failed for user {current_user['email']}: {e}")
        raise HTTPException(status_code=502, detail=str(e))

    # 3. Save post record to DB (post history)
    post_doc = {
        "user_id":           str(current_user["_id"]),
        "instagram_user_id": account["instagram_user_id"],
        "username":          account["username"],
        "post_id":           result["post_id"],
        "creation_id":       result["creation_id"],
        "image_url":         body.image_url,
        "caption":           body.caption,
        "published_at":      result["published_at"],
        "created_at":        datetime.utcnow(),
    }
    await db["posts"].insert_one(post_doc)
    logger.info(f"Post {result['post_id']} saved for @{account['username']}")

    return PostResponse(
        status="published",
        post_id=result["post_id"],
        creation_id=result["creation_id"],
        instagram_username=account["username"],
        published_at=result["published_at"],
    )
