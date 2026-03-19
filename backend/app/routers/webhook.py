import hmac as hmac_module   # aliased to avoid name collision with local vars
import hashlib
import logging
import asyncio
import json
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Request, Query, HTTPException, Header
from fastapi.responses import PlainTextResponse
from app.database import get_db
from app.automation.engine import process_comment_event
from app.automation.queue import enqueue_comment_event
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhook", tags=["Webhook"])


# -----------------------------------------------------------------------------
# Admin guard — imported here to protect utility endpoints (Bug #4)
# Imported lazily inside the dependency to avoid circular import at module load.
# -----------------------------------------------------------------------------

async def _require_admin_dep(request: Request):
    """
    Thin wrapper that defers the import of require_admin until request-time,
    avoiding any circular-import risk between webhook.py and admin.py.
    """
    from app.routers.admin import require_admin
    from fastapi import Depends as _Depends
    # Call the actual dependency manually via FastAPI's dependency injection.
    # Because we can't use Depends() at the function-definition level here
    # (circular import), we invoke get_current_user directly then role-check.
    from app.dependencies import get_current_user
    from app.security import decode_access_token
    from fastapi.security import OAuth2PasswordBearer
    from fastapi import HTTPException as _HTTPException, status as _status

    # Extract Bearer token the same way OAuth2PasswordBearer does
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=_status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = auth_header[len("Bearer "):]

    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=_status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=_status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # Fetch user (reuses the same Redis-cached path as get_current_user)
    from bson import ObjectId
    from app.database import get_db as _get_db
    db = _get_db()
    try:
        user = await db["users"].find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise HTTPException(status_code=_status.HTTP_401_UNAUTHORIZED, detail="Invalid user")

    if not user:
        raise HTTPException(status_code=_status.HTTP_401_UNAUTHORIZED, detail="User not found")

    role = user.get("role", "user")
    if role not in ("admin", "superadmin"):
        raise HTTPException(
            status_code=_status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )
    return user


# -----------------------------------------------------------------------------
# Cached account lookup
# -----------------------------------------------------------------------------

async def get_account_by_ig_id(db, ig_account_id: str) -> Optional[dict]:
    """
    Look up an Instagram account by its IG user/page ID.
    Checks Redis first -- saves 2-3 DB queries on every webhook event.
    Cache is invalidated when a token is refreshed via invalidate_account_cache().
    """
    from app.redis_pool import get_redis

    redis = get_redis()
    cache_key = f"ig_account:{ig_account_id}"

    try:
        cached = await redis.get(cache_key)
        if cached:
            logger.debug(f"[AccountCache] HIT for ig_account_id={ig_account_id}")
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"[AccountCache] Redis read failed (non-fatal): {e}")

    account = await db["instagram_accounts"].find_one({"instagram_user_id": ig_account_id})
    if not account:
        account = await db["instagram_accounts"].find_one({"page_id": ig_account_id})
    if not account:
        account = await db["instagram_accounts"].find_one({"is_active": True})
        if account:
            logger.warning(
                f"[AccountCache] ig_account_id='{ig_account_id}' not matched -- "
                f"falling back to active account @{account.get('username')}."
            )

    if not account:
        return None

    try:
        serialized = {**account, "_id": str(account["_id"])}
        await redis.setex(
            cache_key,
            settings.ACCOUNT_CACHE_TTL,
            json.dumps(serialized, default=str)
        )
        logger.debug(f"[AccountCache] SET for ig_account_id={ig_account_id}")
    except Exception as e:
        logger.warning(f"[AccountCache] Redis write failed (non-fatal): {e}")

    return account


async def invalidate_account_cache(ig_account_id: str) -> None:
    """Call this whenever an account's access_token is refreshed."""
    try:
        from app.redis_pool import get_redis
        redis = get_redis()
        await redis.delete(f"ig_account:{ig_account_id}")
        logger.debug(f"[AccountCache] Invalidated for ig_account_id={ig_account_id}")
    except Exception as e:
        logger.warning(f"[AccountCache] Invalidation failed (non-fatal): {e}")


# -----------------------------------------------------------------------------
# Signature verification helper
# -----------------------------------------------------------------------------

def verify_signature(payload: bytes, signature: str, app_secret: str) -> bool:
    expected = "sha256=" + hmac_module.new(
        app_secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac_module.compare_digest(expected, signature)


# -----------------------------------------------------------------------------
# GET /webhook/ -- Meta webhook verification handshake
# -----------------------------------------------------------------------------

@router.get("/", response_class=PlainTextResponse)
async def verify_webhook(
    hub_mode: Optional[str] = Query(alias="hub.mode", default=None),
    hub_verify_token: Optional[str] = Query(alias="hub.verify_token", default=None),
    hub_challenge: Optional[str] = Query(alias="hub.challenge", default=None),
):
    logger.info(
        f"Webhook verification attempt: mode={hub_mode} "
        f"token={hub_verify_token} challenge={hub_challenge}"
    )

    if hub_mode == "subscribe" and hub_verify_token == settings.WEBHOOK_VERIFY_TOKEN:
        logger.info("Webhook verified successfully.")
        return hub_challenge

    logger.warning(
        f"Webhook verification FAILED. "
        f"Expected token='{settings.WEBHOOK_VERIFY_TOKEN}', "
        f"got='{hub_verify_token}'"
    )
    raise HTTPException(status_code=403, detail="Webhook verification failed")


# -----------------------------------------------------------------------------
# POST /webhook/ -- Receive Instagram events
# -----------------------------------------------------------------------------

@router.post("/")
async def receive_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature_256: Optional[str] = Header(default=None),
):
    # 1. Read raw body once
    payload = await request.body()

    # 2. Pretty-print to console for debugging (only in DEBUG mode)
    if settings.DEBUG:
        try:
            pretty = json.dumps(json.loads(payload), indent=2)
            print("\n" + "=" * 60)
            print("INCOMING WEBHOOK PAYLOAD")
            print("=" * 60)
            print(pretty)
            print("=" * 60 + "\n")
        except Exception:
            print(f"INCOMING WEBHOOK (raw): {payload[:500]}")

    # -------------------------------------------------------------------------
    # 3. Signature verification  (FIX for Bug #3)
    #
    # Old (vulnerable) logic:
    #   if x_hub_signature_256 and settings.INSTAGRAM_APP_SECRET:   <-- skips if header missing
    #       ...verify...
    #   else:
    #       logger.debug("Signature check skipped")                  <-- attacker omits header = free pass
    #
    # New logic:
    #   - INSTAGRAM_APP_SECRET not configured + production = hard reject (misconfigured).
    #   - INSTAGRAM_APP_SECRET configured + header missing + production = reject.
    #   - INSTAGRAM_APP_SECRET configured + header missing + DEBUG = warn only (dev convenience).
    #   - INSTAGRAM_APP_SECRET configured + header present = verify, reject on mismatch.
    # -------------------------------------------------------------------------
    if settings.INSTAGRAM_APP_SECRET:
        if not x_hub_signature_256:
            if settings.DEBUG:
                logger.warning("[Webhook] X-Hub-Signature-256 header missing -- allowed in DEBUG mode only")
            else:
                logger.warning("[Webhook] Rejected: X-Hub-Signature-256 header missing in production")
                return {"status": "missing_signature"}
        elif not verify_signature(payload, x_hub_signature_256, settings.INSTAGRAM_APP_SECRET):
            logger.warning("[Webhook] Rejected: invalid HMAC signature")
            return {"status": "invalid_signature"}
        else:
            logger.debug("[Webhook] Signature verified OK")
    else:
        # No app secret at all
        if not settings.DEBUG:
            logger.error(
                "[Webhook] INSTAGRAM_APP_SECRET is not configured. "
                "All webhook events are rejected in production until it is set."
            )
            return {"status": "misconfigured"}
        logger.warning("[Webhook] Signature check skipped -- INSTAGRAM_APP_SECRET not set (DEBUG mode only)")

    # 4. Parse JSON
    try:
        body = json.loads(payload)
    except Exception:
        logger.warning("Webhook payload is not valid JSON")
        return {"status": "ok"}

    logger.info(
        f"Webhook received: object={body.get('object')} "
        f"entries={len(body.get('entry', []))}"
    )

    if body.get("object") != "instagram":
        logger.info(f"Ignoring non-instagram object: {body.get('object')}")
        return {"status": "ok"}

    db = get_db()

    for entry in body.get("entry", []):
        ig_account_id = entry.get("id")

        # Handle DMs
        for msg_event in entry.get("messaging", []):
            if "read" in msg_event and "message" not in msg_event:
                continue  # messaging_seen
            if any(k in msg_event for k in ("postback", "referral", "pass_thread_control", "optin")):
                continue

            sender_id    = (msg_event.get("sender") or {}).get("id")
            recipient_id = (msg_event.get("recipient") or {}).get("id")
            message      = msg_event.get("message", {})
            msg_text     = message.get("text", "").strip()
            msg_id       = message.get("mid", "")

            if message.get("is_echo"):
                logger.info(f"[DM] Skipping echo message mid={msg_id}")
                continue

            if sender_id == ig_account_id or sender_id == recipient_id:
                logger.info(f"[DM] Skipping self-sent DM")
                continue

            attachments = message.get("attachments", [])

            reply_to   = message.get("reply_to", {})
            story_info = reply_to.get("story", {})
            story_id   = story_info.get("id") if story_info else None

            if not story_id and attachments:
                for att in attachments:
                    if att.get("type") in ("story_mention", "story_reply"):
                        story_id = att.get("payload", {}).get("id")
                        break

            if story_id:
                effective_text = msg_text if msg_text else "*"
                logger.info(
                    f"Story reply -- from={sender_id} story_id={story_id} "
                    f"mid={msg_id} text='{effective_text}'"
                )
                background_tasks.add_task(
                    handle_story_reply_event,
                    db,
                    ig_account_id,
                    sender_id,
                    effective_text,
                    msg_id,
                    story_id,
                )
                continue

            if not msg_text:
                if attachments:
                    att_types = [a.get("type", "unknown") for a in attachments]
                    logger.info(
                        f"[DM] Attachment-only message mid={msg_id} "
                        f"types={att_types} -- using '*' for keyword matching"
                    )
                    msg_text = "*"
                else:
                    logger.info(f"[DM] Skipping empty message (no text, no attachments) mid={msg_id}")
                    continue

            logger.info(
                f"DM event -- from={sender_id} to={recipient_id} "
                f"mid={msg_id} text='{msg_text}'"
            )

            background_tasks.add_task(
                handle_dm_event,
                db,
                ig_account_id,
                sender_id,
                msg_text,
                msg_id,
            )

        # Handle comments
        for change in entry.get("changes", []):
            field = change.get("field")
            value = change.get("value", {})

            logger.info(f"Webhook change: field={field} ig_account={ig_account_id}")
            logger.debug(f"Change value: {value}")

            if field == "mentions":
                media_id     = value.get("media_id") or value.get("id")
                comment_id   = value.get("comment_id")
                sender_id_mn = value.get("from", {}).get("id") or entry.get("id")
                caption      = value.get("caption", "").strip()

                logger.info(
                    f"Story mention -- from={sender_id_mn} "
                    f"media_id={media_id} caption='{caption}'"
                )

                if sender_id_mn and sender_id_mn != ig_account_id:
                    background_tasks.add_task(
                        handle_story_mention_event,
                        db,
                        ig_account_id,
                        sender_id_mn,
                        caption,
                        comment_id or f"mention_{media_id}",
                        media_id,
                    )

            if field == "comments":
                comment_id   = value.get("id")
                comment_text = value.get("text", "").strip()
                media_id     = (value.get("media") or {}).get("id") or value.get("media_id")
                from_block   = value.get("from") or {}
                commenter_id = from_block.get("id") or value.get("from_id")

                logger.info(
                    f"Comment event -- comment_id={comment_id} | "
                    f"media_id={media_id} | commenter={commenter_id} | "
                    f"text='{comment_text}'"
                )

                background_tasks.add_task(
                    handle_comment_event,
                    db,
                    ig_account_id,
                    value,
                    use_queue=True,
                )

    return {"status": "ok"}


# -----------------------------------------------------------------------------
# Background handlers
# -----------------------------------------------------------------------------

async def handle_comment_event(db, ig_account_id: str, value: dict, use_queue: bool = True):
    try:
        media_block  = value.get("media") or {}
        media_id     = media_block.get("id") or value.get("media_id") or value.get("post_id")
        comment_id   = value.get("id")
        comment_text = value.get("text", "").strip()
        from_block   = value.get("from") or {}
        commenter_id = from_block.get("id") or value.get("from_id")

        logger.info(f"[BG] Raw comment value: {value}")

        missing = [k for k, v in {
            "media_id":     media_id,
            "comment_id":   comment_id,
            "commenter_id": commenter_id,
        }.items() if not v]

        if missing:
            logger.warning(f"[BG] Missing fields {missing} in comment event: {value}")
            return

        if not comment_text:
            logger.warning(f"[BG] Empty comment text -- skipping: {value}")
            return

        if commenter_id == ig_account_id:
            logger.info(f"[BG] Skipping own account comment (self on own post)")
            return
        if value.get("from", {}).get("self_ig_scoped_id"):
            logger.info(f"[BG] Skipping own scoped comment")
            return

        account = await get_account_by_ig_id(db, ig_account_id)

        if account:
            try:
                from app.socket_manager import emit_inbox_event
                _user_id = str(account.get("user_id", ""))
                await emit_inbox_event(_user_id, "comment", {
                    "id":        comment_id,
                    "text":      comment_text,
                    "username":  from_block.get("username", "unknown"),
                    "timestamp": value.get("timestamp", ""),
                    "media_id":  media_id,
                })
            except Exception as _se:
                logger.debug(f"[BG] Socket emit failed (non-critical): {_se}")

        if not account:
            logger.warning(
                f"[BG] No DB account found for instagram_user_id='{ig_account_id}'. "
                f"Fix: open /webhook/fix-account-id?page_id={ig_account_id} to update DB."
            )
            return

        account_id = str(account["_id"])
        logger.info(f"[BG] Matched account @{account.get('username')} ({account_id})")

        commenter_username = from_block.get("username", "")
        if use_queue:
            depth = await enqueue_comment_event(
                account_id=account_id,
                media_id=media_id,
                comment_id=comment_id,
                comment_text=comment_text,
                commenter_id=commenter_id,
                ig_user_id=account["instagram_user_id"],
                access_token=account["access_token"],
                commenter_username=commenter_username,
            )
            logger.info(f"[BG] Comment queued (depth={depth}) -- worker will process with human-like delay")
        else:
            await process_comment_event(
                db=db,
                media_id=media_id,
                comment_id=comment_id,
                comment_text=comment_text,
                commenter_id=commenter_id,
                account_id=account_id,
                ig_user_id=account["instagram_user_id"],
                access_token=account["access_token"],
                commenter_username=commenter_username,
            )

    except Exception as e:
        logger.error(f"[BG] Comment handler error: {e}", exc_info=True)


async def handle_story_reply_event(
    db,
    ig_account_id: str,
    sender_id: str,
    msg_text: str,
    msg_id: str,
    story_id: str,
):
    try:
        from app.automation.engine import process_story_event

        account = await get_account_by_ig_id(db, ig_account_id)
        if not account:
            logger.warning(f"[Story BG] No account found for ig_account_id={ig_account_id}")
            return

        logger.info(
            f"[Story BG] story_id={story_id} from={sender_id} "
            f"text='{msg_text}' account=@{account.get('username')}"
        )

        await process_story_event(
            db=db,
            sender_id=sender_id,
            msg_text=msg_text,
            msg_id=msg_id,
            story_id=story_id,
            account_id=str(account["_id"]),
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
        )
    except Exception as e:
        logger.error(f"[Story BG] Error: {e}", exc_info=True)


async def handle_story_mention_event(
    db,
    ig_account_id: str,
    sender_id: str,
    caption: str,
    mention_id: str,
    media_id: str,
):
    """Called when someone mentions your account in their story."""
    try:
        from app.automation.engine import process_story_event

        account = await get_account_by_ig_id(db, ig_account_id)
        if not account:
            logger.warning(f"[Mention BG] No account found for ig_account_id={ig_account_id}")
            return

        logger.info(
            f"[Mention BG] story mention: from={sender_id} "
            f"media_id={media_id} caption='{caption}' account=@{account.get('username')}"
        )

        await process_story_event(
            db=db,
            sender_id=sender_id,
            msg_text=caption or "*",
            msg_id=mention_id,
            story_id=media_id,
            account_id=str(account["_id"]),
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
        )
    except Exception as e:
        logger.error(f"[Mention BG] Error: {e}", exc_info=True)


async def handle_dm_event(
    db,
    ig_account_id: str,
    sender_id: str,
    msg_text: str,
    msg_id: str,
):
    try:
        from app.automation.engine import process_dm_event

        account = await get_account_by_ig_id(db, ig_account_id)
        if not account:
            logger.warning(f"[DM BG] No account found for ig_account_id={ig_account_id}")
            return

        logger.info(f"[DM BG] from={sender_id} text='{msg_text}' account=@{account.get('username')}")

        try:
            account_oid = str(account["_id"])
            existing = await db["dm_messages"].find_one({"msg_id": msg_id})
            if not existing:
                await db["dm_messages"].insert_one({
                    "account_id": account_oid,
                    "sender_id":  sender_id,
                    "username":   sender_id,
                    "text":       msg_text,
                    "direction":  "in",
                    "msg_id":     msg_id,
                    "timestamp":  datetime.utcnow(),
                    "read":       False,
                })
        except Exception as _dbe:
            logger.debug(f"[DM BG] DB save failed (non-critical): {_dbe}")

        try:
            from app.socket_manager import emit_inbox_event
            _user_id    = str(account.get("user_id", ""))
            account_oid = str(account["_id"])
            await emit_inbox_event(_user_id, "dm", {
                "id":         msg_id,
                "text":       msg_text,
                "username":   sender_id,
                "timestamp":  datetime.utcnow().isoformat(),
                "sender_id":  sender_id,
                "account_id": account_oid,
                "direction":  "in",
            })
        except Exception as _se:
            logger.debug(f"[DM BG] Socket emit failed (non-critical): {_se}")

        await process_dm_event(
            db=db,
            sender_id=sender_id,
            msg_text=msg_text,
            msg_id=msg_id,
            account_id=str(account["_id"]),
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
        )
    except Exception as e:
        logger.error(f"[DM BG] Error: {e}", exc_info=True)


# -----------------------------------------------------------------------------
# Utility / debug routes  (FIX for Bug #4 -- all now require admin auth)
# -----------------------------------------------------------------------------

@router.get("/fix-account-id")
async def fix_account_id(
    page_id: str = Query(...),
    account_id: str = Query(default=None),
    _admin: dict = Depends(_require_admin_dep),
):
    """Admin only: remap a page_id onto an Instagram account record."""
    from bson import ObjectId
    db = get_db()

    if account_id:
        filt = {"_id": ObjectId(account_id)}
    else:
        filt = {"username": {"$exists": True}}

    account = await db["instagram_accounts"].find_one(filt)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    await db["instagram_accounts"].update_one(
        {"_id": account["_id"]},
        {"$set": {"page_id": page_id}}
    )

    await invalidate_account_cache(page_id)
    await invalidate_account_cache(account.get("instagram_user_id", ""))

    logger.info(f"[FIX] Stored page_id={page_id} on account @{account.get('username')} by admin {_admin.get('email')}")
    return {
        "status":   "updated",
        "username": account.get("username"),
        "page_id":  page_id,
        "note":     "Webhook account lookup will now work correctly",
    }


@router.get("/test-story")
async def test_story_get(
    story_id: str = Query(...),
    account_id: str = Query(...),
    msg_text: str = Query(default="*"),
    _admin: dict = Depends(_require_admin_dep),
):
    """Admin only: simulate a story reply to trigger story automation."""
    from bson import ObjectId

    db = get_db()
    try:
        account = await db["instagram_accounts"].find_one({"_id": ObjectId(account_id)})
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid account_id: {account_id}")

    if not account:
        raise HTTPException(status_code=404, detail=f"No account found for id={account_id}")

    from app.automation.engine import process_story_event
    fake_msg_id = f"test_story_{int(time.time())}"
    logger.info(
        f"[TEST-STORY] story_id={story_id} account=@{account.get('username')} "
        f"msg_id={fake_msg_id} text='{msg_text}' by admin {_admin.get('email')}"
    )

    await process_story_event(
        db=db,
        sender_id="test_story_user_999",
        msg_text=msg_text,
        msg_id=fake_msg_id,
        story_id=story_id,
        account_id=account_id,
        ig_user_id=account["instagram_user_id"],
        access_token=account["access_token"],
    )

    return {
        "status":   "triggered",
        "story_id": story_id,
        "account":  account.get("username"),
        "msg_id":   fake_msg_id,
        "msg_text": msg_text,
        "note":     "Check uvicorn terminal for [Story Engine] logs",
    }


@router.get("/clear-cooldown")
async def clear_cooldown(
    account_id: str = Query(...),
    post_id: str = Query(...),
    _admin: dict = Depends(_require_admin_dep),
):
    """Admin only: clear cooldown records for a post so automations can re-trigger."""
    db = get_db()
    result = await db["cooldown_logs"].delete_many({"post_id": post_id})
    logger.info(
        f"[COOLDOWN] Cleared {result.deleted_count} cooldown records for "
        f"post={post_id} by admin {_admin.get('email')}"
    )
    return {
        "status":  "cleared",
        "deleted": result.deleted_count,
        "post_id": post_id,
        "note":    "You can now re-trigger the automation for any user on this post",
    }


@router.get("/test-comment")
async def test_comment_get(
    post_id: str = Query(...),
    account_id: str = Query(...),
    comment_text: str = Query(default="hi"),
    _admin: dict = Depends(_require_admin_dep),
):
    """Admin only: simulate a comment to trigger comment automation."""
    from bson import ObjectId

    db = get_db()
    try:
        account = await db["instagram_accounts"].find_one({"_id": ObjectId(account_id)})
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid account_id: {account_id}")

    if not account:
        raise HTTPException(status_code=404, detail=f"No account found for id={account_id}")

    fake_comment_id = f"test_{int(time.time())}"
    logger.info(
        f"[TEST] post={post_id} account=@{account.get('username')} "
        f"comment_id={fake_comment_id} text='{comment_text}' by admin {_admin.get('email')}"
    )

    await process_comment_event(
        db=db,
        media_id=post_id,
        comment_id=fake_comment_id,
        comment_text=comment_text,
        commenter_id="test_user_999",
        account_id=account_id,
        ig_user_id=account["instagram_user_id"],
        access_token=account["access_token"],
        commenter_username="testuser",
    )

    return {
        "status":       "triggered",
        "post_id":      post_id,
        "account":      account.get("username"),
        "comment_id":   fake_comment_id,
        "comment_text": comment_text,
        "note":         "Check uvicorn terminal for [Engine] logs",
    }
