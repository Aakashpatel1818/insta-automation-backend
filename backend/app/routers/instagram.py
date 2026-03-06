import logging
from datetime import datetime, timedelta

import uuid, os, aiofiles, io
from fastapi import APIRouter, Depends, HTTPException, Query, status, UploadFile, File
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from bson import ObjectId

from app.schemas.instagram import InstagramConnectURL, InstagramAccountPublic, InstagramAccountList
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
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instagram", tags=["Instagram"])


# ── Helper: get specific or active account ────────────────
async def get_account(db, user_id: str, account_id: str = None) -> dict:
    """
    If account_id given → return that specific account.
    Otherwise → return the active account.
    Raises 404 if not found.
    """
    if account_id:
        try:
            account = await db["instagram_accounts"].find_one({
                "_id":     ObjectId(account_id),
                "user_id": user_id,
            })
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid account ID")
        if not account:
            raise HTTPException(status_code=404, detail="Instagram account not found")
        return account

    # No account_id → use active account
    account = await db["instagram_accounts"].find_one({
        "user_id":   user_id,
        "is_active": True,
    })
    if not account:
        # Fallback: any account
        account = await db["instagram_accounts"].find_one({"user_id": user_id})
    if not account:
        raise HTTPException(
            status_code=404,
            detail="No Instagram account connected. Go to /instagram/connect first."
        )
    return account


# ── Step 1: Get OAuth URL ─────────────────────────────────
@router.get("/connect", response_model=InstagramConnectURL)
async def get_connect_url(current_user: dict = Depends(get_current_user)):
    """Returns Instagram OAuth URL. Supports connecting multiple accounts."""
    state = str(current_user["_id"])
    url = build_auth_url(state=state)
    return InstagramConnectURL(auth_url=url)


# ── Step 2: OAuth Callback ────────────────────────────────
@router.get("/callback")
async def instagram_callback(
    code:         str = Query(default=None),
    state:        str = Query(default=None),
    error:        str = Query(default=None),
    error_reason: str = Query(default=None),
):
    frontend_accounts = f"{settings.FRONTEND_URL}/accounts"

    # ── OAuth denied by user ──────────────────────────────
    if error:
        reason = error_reason or error
        logger.warning(f"Instagram OAuth denied: {reason}")
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg={reason}",
            status_code=302,
        )

    if not code or not state:
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=missing_params",
            status_code=302,
        )

    db = get_db()

    # ── Validate state → find user ────────────────────────
    try:
        user = await db["users"].find_one({"_id": ObjectId(state)})
    except Exception:
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=invalid_state",
            status_code=302,
        )

    if not user:
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=user_not_found",
            status_code=302,
        )

    # ── Exchange code → short-lived token ─────────────────
    try:
        short_data = await exchange_code_for_short_token(code)
    except Exception as e:
        logger.error(f"Short token exchange failed: {e}")
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=token_exchange_failed",
            status_code=302,
        )

    short_token = short_data.get("access_token")
    if not short_token:
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=no_access_token",
            status_code=302,
        )

    # ── Upgrade to long-lived token (60 days) ─────────────
    try:
        long_data = await exchange_for_long_lived_token(short_token)
    except Exception as e:
        logger.error(f"Long token exchange failed: {e}")
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=long_token_failed",
            status_code=302,
        )

    long_token = long_data.get("access_token")
    expires_in = long_data.get("expires_in", 0)
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in) if expires_in else None

    # ── Fetch IG profile ──────────────────────────────────
    try:
        profile = await fetch_ig_profile(long_token)
    except Exception as e:
        logger.error(f"IG profile fetch failed: {e}")
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=profile_fetch_failed",
            status_code=302,
        )

    ig_user_id  = profile.get("id")
    ig_username = profile.get("username")

    if not ig_user_id:
        return RedirectResponse(
            url=f"{frontend_accounts}?instagram=error&msg=no_ig_user_id",
            status_code=302,
        )

    # ── Upsert account in DB ──────────────────────────────
    existing_count = await db["instagram_accounts"].count_documents(
        {"user_id": str(user["_id"])}
    )
    is_first = existing_count == 0
    now = datetime.utcnow()

    await db["instagram_accounts"].update_one(
        {"instagram_user_id": ig_user_id, "user_id": str(user["_id"])},
        {"$set": {
            "user_id":           str(user["_id"]),
            "instagram_user_id": ig_user_id,
            "username":          ig_username,
            "access_token":      long_token,
            "token_expires_at":  expires_at,
            "is_active":         is_first,
            "updated_at":        now,
        },
         "$setOnInsert": {"connected_at": now}
        },
        upsert=True,
    )

    logger.info(f"Instagram @{ig_username} connected for user {user['email']} (active={is_first})")

    # ── Redirect back to frontend with success ────────────
    return RedirectResponse(
        url=f"{frontend_accounts}?instagram=connected&username={ig_username}",
        status_code=302,
    )


# ── List all connected accounts ───────────────────────────
@router.get("/accounts", response_model=InstagramAccountList)
async def list_accounts(current_user: dict = Depends(get_current_user)):
    """List all Instagram accounts connected to this user."""
    db = get_db()
    accounts = await db["instagram_accounts"].find(
        {"user_id": str(current_user["_id"])}
    ).to_list(length=50)

    return InstagramAccountList(
        accounts=[
            InstagramAccountPublic(
                id=str(a["_id"]),
                instagram_user_id=a["instagram_user_id"],
                username=a["username"],
                token_expires_at=a.get("token_expires_at"),
                connected_at=a["connected_at"],
                is_active=a.get("is_active", False),
            )
            for a in accounts
        ],
        total=len(accounts),
    )


# ── Get active account ────────────────────────────────────
@router.get("/me", response_model=InstagramAccountPublic)
async def get_my_instagram(current_user: dict = Depends(get_current_user)):
    """Returns the currently active Instagram account."""
    db = get_db()
    account = await get_account(db, str(current_user["_id"]))
    return InstagramAccountPublic(
        id=str(account["_id"]),
        instagram_user_id=account["instagram_user_id"],
        username=account["username"],
        token_expires_at=account.get("token_expires_at"),
        connected_at=account["connected_at"],
        is_active=account.get("is_active", False),
    )


# ── Switch active account ─────────────────────────────────
@router.post("/accounts/{account_id}/activate", status_code=status.HTTP_200_OK)
async def activate_account(
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Switch the active Instagram account."""
    db = get_db()
    user_id = str(current_user["_id"])

    try:
        oid = ObjectId(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid account ID")

    account = await db["instagram_accounts"].find_one({
        "_id": oid, "user_id": user_id
    })
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    # Deactivate all → activate selected
    await db["instagram_accounts"].update_many(
        {"user_id": user_id},
        {"$set": {"is_active": False}}
    )
    await db["instagram_accounts"].update_one(
        {"_id": oid},
        {"$set": {"is_active": True}}
    )

    logger.info(f"Switched active account to @{account['username']} for user {current_user['email']}")
    return {
        "status":   "activated",
        "username": account["username"],
        "message":  f"@{account['username']} is now the active account."
    }


# ── Disconnect one account ────────────────────────────────
@router.delete("/accounts/{account_id}", status_code=status.HTTP_200_OK)
async def disconnect_account(
    account_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Disconnect a specific Instagram account."""
    db = get_db()
    user_id = str(current_user["_id"])

    try:
        oid = ObjectId(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid account ID")

    account = await db["instagram_accounts"].find_one({
        "_id": oid, "user_id": user_id
    })
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    was_active = account.get("is_active", False)
    await db["instagram_accounts"].delete_one({"_id": oid})

    # If deleted account was active → activate the next available one
    if was_active:
        next_account = await db["instagram_accounts"].find_one({"user_id": user_id})
        if next_account:
            await db["instagram_accounts"].update_one(
                {"_id": next_account["_id"]},
                {"$set": {"is_active": True}}
            )

    logger.info(f"Instagram @{account['username']} disconnected")
    return {"status": "disconnected", "message": f"@{account['username']} removed."}


# ── Fetch media list (posts/reels/stories) ──────────────
@router.get("/media")
async def get_media(
    account_id: str = Query(..., description="Instagram account ID"),
    media_type: str = Query(default="post", description="post | story"),
    current_user: dict = Depends(get_current_user),
):
    """Fetch Instagram media with thumbnails for the given account."""
    import httpx
    db = get_db()
    account = await get_account(db, str(current_user["_id"]), account_id)

    ig_user_id  = account["instagram_user_id"]
    access_token = account["access_token"]

    fields = "id,caption,media_type,media_url,thumbnail_url,timestamp,permalink"

    if media_type == "story":
        url = f"https://graph.instagram.com/v19.0/{ig_user_id}/stories"
    else:
        url = f"https://graph.instagram.com/v19.0/{ig_user_id}/media"

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params={"fields": fields, "access_token": access_token, "limit": 30})

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Instagram API error: {resp.text}")

    data = resp.json().get("data", [])

    media = []
    for item in data:
        media.append({
            "id":        item.get("id"),
            "caption":   (item.get("caption") or "")[:80],
            "type":      item.get("media_type", "IMAGE"),
            "thumbnail": item.get("thumbnail_url") or item.get("media_url"),
            "url":       item.get("media_url"),
            "permalink": item.get("permalink"),
            "timestamp": item.get("timestamp"),
        })

    return {"media": media, "total": len(media)}


# ── Upload image from PC → returns public URL ───────────
UPLOAD_DIR = "static/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@router.post("/upload-image")
async def upload_image(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    """Upload a local image, auto-convert to JPEG, return a publicly accessible URL."""
    from PIL import Image as PILImage

    allowed = {"image/jpeg", "image/png", "image/webp", "image/jpg"}
    if file.content_type not in allowed:
        raise HTTPException(status_code=400, detail="Only JPEG, PNG or WebP images allowed")

    contents = await file.read()
    if len(contents) > 20 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 20 MB)")

    # Always convert to JPEG — Instagram requires JPEG/PNG but JPEG is most reliable
    try:
        img = PILImage.open(io.BytesIO(contents))
        # Convert RGBA/P (transparent PNG) → RGB before saving as JPEG
        if img.mode in ("RGBA", "P", "LA"):
            background = PILImage.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
            img = background
        elif img.mode != "RGB":
            img = img.convert("RGB")

        output = io.BytesIO()
        img.save(output, format="JPEG", quality=95, optimize=True)
        jpeg_bytes = output.getvalue()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Image processing failed: {e}")

    filename = f"{uuid.uuid4().hex}.jpg"
    filepath = os.path.join(UPLOAD_DIR, filename)

    async with aiofiles.open(filepath, "wb") as f:
        await f.write(jpeg_bytes)

    base = settings.BASE_URL.rstrip("/")
    if "localhost" in base or "127.0.0.1" in base:
        logger.warning("BASE_URL is localhost — Instagram cannot fetch this image! Set BASE_URL to ngrok URL in .env")

    public_url = f"{base}/static/uploads/{filename}"
    logger.info(f"Image uploaded & converted to JPEG: {public_url}")
    return {"url": public_url, "filename": filename}


# ── Post to Instagram ─────────────────────────────────────
@router.post("/post", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
async def post_to_instagram(
    body: PostRequest,
    account_id: str = Query(default=None, description="Target account ID. Uses active account if not specified."),
    current_user: dict = Depends(get_current_user),
):
    """
    Publish a post. Uses active account by default.
    Pass ?account_id=... to post to a specific account.
    """
    db = get_db()
    account = await get_account(db, str(current_user["_id"]), account_id)

    try:
        result = await create_instagram_post(
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
            image_url=body.image_url,
            caption=body.caption,
        )
    except Exception as e:
        logger.error(f"Post failed for @{account['username']}: {e}")
        raise HTTPException(status_code=502, detail=str(e))

    post_doc = {
        "user_id":           str(current_user["_id"]),
        "account_id":        str(account["_id"]),
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
    logger.info(f"Post {result['post_id']} published for @{account['username']}")

    return PostResponse(
        status="published",
        post_id=result["post_id"],
        creation_id=result["creation_id"],
        instagram_username=account["username"],
        published_at=result["published_at"],
    )
