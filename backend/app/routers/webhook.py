import hmac as hmac_module   # ← aliased to avoid name collision with local vars
import hashlib
import logging
import asyncio
import json
import time
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Request, Query, HTTPException, Header
from fastapi.responses import PlainTextResponse
from app.database import get_db
from app.automation.engine import process_comment_event
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhook", tags=["Webhook"])


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def verify_signature(payload: bytes, signature: str, app_secret: str) -> bool:
    """
    Verify X-Hub-Signature-256 from Instagram.
    BUG FIX: original code used parameter named 'hmac' which shadowed the
             imported module — renamed import to hmac_module above.
    """
    expected = "sha256=" + hmac_module.new(
        app_secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac_module.compare_digest(expected, signature)


# ─────────────────────────────────────────────────────────────────────────────
# GET /webhook/  — Meta webhook verification handshake
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/", response_class=PlainTextResponse)
async def verify_webhook(
    hub_mode: Optional[str] = Query(alias="hub.mode", default=None),
    hub_verify_token: Optional[str] = Query(alias="hub.verify_token", default=None),
    hub_challenge: Optional[str] = Query(alias="hub.challenge", default=None),
):
    """
    Meta calls this endpoint to confirm ownership of the webhook URL.

    BUG FIX 1: Must return hub_challenge as plain text (not JSON int).
               Meta performs a string equality check on the response body.
    BUG FIX 2: VERIFY_TOKEN now comes from settings (config + .env) so it
               never drifts out of sync between code and Meta dashboard.
    """
    logger.info(
        f"Webhook verification attempt: mode={hub_mode} "
        f"token={hub_verify_token} challenge={hub_challenge}"
    )

    # ── Debug print both sides of the token comparison ───────────────────────
    print(f"[WEBHOOK DEBUG] hub_verify_token  = {repr(hub_verify_token)}")
    print(f"[WEBHOOK DEBUG] WEBHOOK_VERIFY_TOKEN = {repr(settings.WEBHOOK_VERIFY_TOKEN)}")
    print(f"[WEBHOOK DEBUG] match = {hub_verify_token == settings.WEBHOOK_VERIFY_TOKEN}")

    if hub_mode == "subscribe" and hub_verify_token == settings.WEBHOOK_VERIFY_TOKEN:
        logger.info("✅ Webhook verified successfully.")
        # Return challenge as plain text — Meta expects exactly this string back
        return hub_challenge  # PlainTextResponse wraps it correctly

    logger.warning(
        f"❌ Webhook verification FAILED. "
        f"Expected token='{settings.WEBHOOK_VERIFY_TOKEN}', "
        f"got='{hub_verify_token}'"
    )
    raise HTTPException(status_code=403, detail="Webhook verification failed")


# ─────────────────────────────────────────────────────────────────────────────
# POST /webhook/  — Receive Instagram events
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/")
async def receive_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature_256: Optional[str] = Header(default=None),
):
    """
    Receives all Instagram webhook events.

    Rules:
    • Always return HTTP 200 immediately — Instagram retries on anything else.
    • Parse and validate the payload, then hand off to BackgroundTasks so the
      response is sent before the automation engine runs.

    BUG FIX 3: Replaced asyncio.get_event_loop().create_task() with FastAPI
               BackgroundTasks — the old approach raises DeprecationWarning in
               Python 3.10+ and can fail if no running loop exists at call time.
    BUG FIX 4: Body is read once into `payload` bytes, then decoded manually.
               Calling request.json() after request.body() returns empty data
               because the stream is already exhausted.
    """
    # ── 1. Read raw body once ─────────────────────────────────────────────────
    payload = await request.body()

    # ── 2. Pretty-print to console for debugging ──────────────────────────────
    try:
        pretty = json.dumps(json.loads(payload), indent=2)
        print("\n" + "═" * 60)
        print("📩 INCOMING WEBHOOK PAYLOAD")
        print("═" * 60)
        print(pretty)
        print("═" * 60 + "\n")
    except Exception:
        print(f"📩 INCOMING WEBHOOK (raw): {payload[:500]}")

    # ── 3. Verify signature (optional but recommended) ────────────────────────
    if x_hub_signature_256 and settings.INSTAGRAM_APP_SECRET:
        if not verify_signature(payload, x_hub_signature_256, settings.INSTAGRAM_APP_SECRET):
            logger.warning("⚠️  Invalid webhook signature — ignoring event")
            # Still return 200 to avoid Instagram disabling the webhook
            return {"status": "invalid_signature"}
    else:
        logger.debug("Signature check skipped (no secret configured or header missing)")

    # ── 4. Parse JSON ─────────────────────────────────────────────────────────
    try:
        body = json.loads(payload)
    except Exception:
        logger.warning("Webhook payload is not valid JSON")
        return {"status": "ok"}

    logger.info(
        f"Webhook received: object={body.get('object')} "
        f"entries={len(body.get('entry', []))}"
    )

    # ── 5. Only process Instagram events ──────────────────────────────────────
    if body.get("object") != "instagram":
        logger.info(f"Ignoring non-instagram object: {body.get('object')}")
        return {"status": "ok"}

    # ── 6. Dispatch comment events to background ──────────────────────────────
    db = get_db()

    for entry in body.get("entry", []):
        ig_account_id = entry.get("id")  # Numeric IG business account ID

        for change in entry.get("changes", []):
            field = change.get("field")
            value = change.get("value", {})

            logger.info(f"Webhook change: field={field} ig_account={ig_account_id}")
            logger.debug(f"Change value: {value}")

            if field == "comments":
                # Extract key fields right here so they appear in logs immediately
                comment_id   = value.get("id")
                comment_text = value.get("text", "").strip()
                media_id     = (value.get("media") or {}).get("id") or value.get("media_id")
                from_block   = value.get("from") or {}
                commenter_id = from_block.get("id") or value.get("from_id")

                logger.info(
                    f"📝 Comment event extracted → "
                    f"comment_id={comment_id} | "
                    f"media_id={media_id} | "
                    f"commenter={commenter_id} | "
                    f"text='{comment_text}'"
                )

                # Hand off to background — response returns 200 immediately
                background_tasks.add_task(
                    handle_comment_event,
                    db,
                    ig_account_id,
                    value,
                )

    # ── 7. Respond quickly ────────────────────────────────────────────────────
    return {"status": "ok"}


# ─────────────────────────────────────────────────────────────────────────────
# Background handler
# ─────────────────────────────────────────────────────────────────────────────

async def handle_comment_event(db, ig_account_id: str, value: dict):
    """
    Process one comment webhook event.

    Instagram webhook value structure (comments field):
    {
      "id":   "<comment_id>",
      "text": "<comment text>",
      "media": { "id": "<media_id>", "media_product_type": "POST" },
      "from": { "id": "<commenter_ig_id>", "username": "<username>" }
    }
    """
    try:
        # ── Extract fields ────────────────────────────────────────────────────
        media_block  = value.get("media") or {}
        media_id     = media_block.get("id") or value.get("media_id") or value.get("post_id")
        comment_id   = value.get("id")
        comment_text = value.get("text", "").strip()
        from_block   = value.get("from") or {}
        commenter_id = from_block.get("id") or value.get("from_id")

        logger.info(f"[BG] Raw comment value: {value}")

        # ── Validate ──────────────────────────────────────────────────────────
        missing = [k for k, v in {
            "media_id":    media_id,
            "comment_id":  comment_id,
            "commenter_id": commenter_id,
        }.items() if not v]

        if missing:
            logger.warning(f"[BG] Missing fields {missing} in comment event: {value}")
            return

        if not comment_text:
            logger.warning(f"[BG] Empty comment text — skipping: {value}")
            return

        # ── Skip any comment made by our own account ──────────────────────────
        # Instagram fires a webhook for our own replies too — skip all of them.
        if commenter_id == ig_account_id:
            logger.info(f"[BG] Skipping own account comment (self on own post)")
            return
        # Also skip if self_ig_scoped_id is present (another way Meta identifies self)
        if value.get("from", {}).get("self_ig_scoped_id"):
            logger.info(f"[BG] Skipping own scoped comment")
            return

        # ── Look up account by Instagram user ID OR page/business ID ──────────
        account = await db["instagram_accounts"].find_one(
            {"instagram_user_id": ig_account_id}
        )
        if not account:
            # Fallback: Meta sometimes sends the Facebook Page ID in entry.id
            # instead of the Instagram User ID — try matching by page_id field
            account = await db["instagram_accounts"].find_one(
                {"page_id": ig_account_id}
            )
        if not account:
            # Last resort: if only one account exists, use it directly
            account = await db["instagram_accounts"].find_one({"is_active": True})
            if account:
                logger.warning(
                    f"[BG] ig_account_id='{ig_account_id}' not matched — "
                    f"falling back to active account @{account.get('username')}. "
                    f"Fix: store page_id='{ig_account_id}' on this account in DB."
                )
        if not account:
            logger.warning(
                f"[BG] No DB account found for instagram_user_id='{ig_account_id}'. "
                f"Fix: open /debug/fix-account-id?page_id={ig_account_id} to update DB."
            )
            return

        account_id = str(account["_id"])
        logger.info(f"[BG] Matched account @{account.get('username')} ({account_id})")

        # ── Run automation engine ─────────────────────────────────────────────
        await process_comment_event(
            db=db,
            media_id=media_id,
            comment_id=comment_id,
            comment_text=comment_text,
            commenter_id=commenter_id,
            account_id=account_id,
            ig_user_id=account["instagram_user_id"],
            access_token=account["access_token"],
        )

    except Exception as e:
        logger.error(f"[BG] Comment handler error: {e}", exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# GET /webhook/test-comment — Manual trigger for local testing
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/fix-account-id")
async def fix_account_id(
    page_id: str = Query(...),
    account_id: str = Query(default=None),
):
    """
    Store the Facebook Page ID on the account so webhook lookups work.
    Usage: /debug/fix-account-id?page_id=17841474771906366
    """
    from bson import ObjectId
    db = get_db()

    if account_id:
        filt = {"_id": ObjectId(account_id)}
    else:
        # Update all accounts with same instagram_user_id pattern
        filt = {"username": {"$exists": True}}

    # Just update the one active account if no account_id given
    account = await db["instagram_accounts"].find_one(filt)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    await db["instagram_accounts"].update_one(
        {"_id": account["_id"]},
        {"$set": {"page_id": page_id}}
    )
    logger.info(f"[FIX] Stored page_id={page_id} on account @{account.get('username')}")
    return {
        "status":   "updated",
        "username": account.get("username"),
        "page_id":  page_id,
        "note":     "Webhook account lookup will now work correctly",
    }


@router.get("/clear-cooldown")
async def clear_cooldown(
    account_id: str = Query(...),
    post_id: str = Query(...),
):
    """
    Delete all cooldown records for a post — use during testing so
    the engine doesn't skip events due to the 24h cooldown window.
    """
    db = get_db()
    result = await db["cooldown_logs"].delete_many({
        "post_id": post_id,
    })
    logger.info(f"[COOLDOWN] Cleared {result.deleted_count} cooldown records for post {post_id}")
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
):
    """
    Fire the automation engine directly — no real Instagram webhook needed.
    Usage: GET /webhook/test-comment?post_id=...&account_id=...&comment_text=hi
    """
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
        f"comment_id={fake_comment_id} text='{comment_text}'"
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
    )

    return {
        "status":       "triggered",
        "post_id":      post_id,
        "account":      account.get("username"),
        "comment_id":   fake_comment_id,
        "comment_text": comment_text,
        "note":         "Check uvicorn terminal for [Engine] logs",
    }
