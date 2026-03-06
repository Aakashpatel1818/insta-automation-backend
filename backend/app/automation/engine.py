import asyncio
import random
import logging
import httpx
from datetime import datetime, timedelta

from app.config import settings as app_settings

logger = logging.getLogger(__name__)

GRAPH_BASE    = "https://graph.instagram.com/v19.0"
DM_RATE_LIMIT = app_settings.DM_RATE_LIMIT  # max DMs per hour (set in .env)

# In-memory dedup set — prevents the same comment_id from being
# processed twice if two automations exist on the same post or
# if Instagram sends the webhook twice (it does sometimes).
_processing: set = set()


# ── Keyword Matching ──────────────────────────────────────
def match_keyword(comment_text: str, trigger_words: list) -> str | None:
    """
    Exact match (case-insensitive, trimmed).
    '*' wildcard matches any comment.
    """
    if "*" in trigger_words:
        logger.info("Wildcard '*' matched — triggering on any comment")
        return "*"

    text_lower = comment_text.strip().lower()
    for word in trigger_words:
        if word.strip().lower() == text_lower:
            logger.info(f"Exact keyword matched: '{word}' == '{comment_text}'")
            return word

    return None


# ── Rate Limit ────────────────────────────────────────────
async def check_rate_limit(db, account_id: str) -> bool:
    cutoff = datetime.utcnow() - timedelta(hours=1)
    count  = await db["automation_logs"].count_documents({
        "account_id": account_id,
        "dm_sent":    True,
        "timestamp":  {"$gte": cutoff},
    })
    return count >= app_settings.DM_RATE_LIMIT


# ── Delay ─────────────────────────────────────────────────
async def apply_delay():
    delay = random.uniform(5, 15)
    logger.info(f"Applying delay of {delay:.1f}s")
    await asyncio.sleep(delay)


# ── Comment Reply ─────────────────────────────────────────
async def send_comment_reply(comment_id: str, message: str, access_token: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{GRAPH_BASE}/{comment_id}/replies",
                data={"message": message, "access_token": access_token},
            )
        data = resp.json()
        logger.info(f"Comment reply response: {data}")
        return "id" in data
    except Exception as e:
        logger.error(f"Comment reply error: {e}")
        return False


# ── Send DM ───────────────────────────────────────────────
async def send_dm(ig_user_id: str, recipient_id: str, message: str, access_token: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{GRAPH_BASE}/{ig_user_id}/messages",
                json={"recipient": {"id": recipient_id}, "message": {"text": message}},
                params={"access_token": access_token},
            )
        data = resp.json()
        logger.info(f"DM response: {data}")

        if "message_id" in data or "id" in data:
            return True

        # Log the exact error from Instagram for visibility
        err = data.get("error", {})
        logger.warning(
            f"DM failed — code={err.get('code')} subcode={err.get('error_subcode')} "
            f"msg='{err.get('message')}'"
        )
        return False

    except Exception as e:
        logger.error(f"DM error: {e}")
        return False


# ── Log ───────────────────────────────────────────────────
async def log_action(db, log_data: dict):
    log_data["timestamp"] = datetime.utcnow()
    await db["automation_logs"].insert_one(log_data)


async def update_analytics_by_id(db, automation_id: str, post_id: str, account_id: str, field: str):
    await db["automation_analytics"].update_one(
        {"automation_id": automation_id},
        {"$setOnInsert": {
            "automation_id":      automation_id,
            "post_id":            post_id,
            "account_id":         account_id,
            "trigger_count":      0,
            "reply_sent_count":   0,
            "dm_sent_count":      0,
            "cooldown_blocked":   0,
            "rate_limit_blocked": 0,
            "last_updated":       datetime.utcnow(),
        }},
        upsert=True,
    )
    await db["automation_analytics"].update_one(
        {"automation_id": automation_id},
        {"$inc": {field: 1}, "$set": {"last_updated": datetime.utcnow()}},
    )


# ── Per-automation pipeline ───────────────────────────────
async def _run_automation(
    db, settings: dict, log_base: dict,
    media_id: str, comment_id: str, comment_text: str,
    commenter_id: str, account_id: str,
    ig_user_id: str, access_token: str,
    already_replied: bool = False,
) -> bool:
    automation_id = str(settings["_id"])
    log_entry     = {**log_base, "automation_id": automation_id}

    if not settings.get("auto_comment_reply") and not settings.get("auto_dm"):
        logger.warning(f"[Engine] Automation {automation_id}: both disabled, skipping")
        return

    # Fetch rules for this automation
    rules = await db["keyword_rules"].find({
        "automation_id": automation_id,
        "is_active":     True,
    }).to_list(length=50)

    # Legacy fallback — rules saved with null/missing automation_id
    if not rules:
        rules = await db["keyword_rules"].find({
            "post_id":    media_id,
            "account_id": account_id,
            "is_active":  True,
            "$or": [{"automation_id": None}, {"automation_id": {"$exists": False}}],
        }).to_list(length=50)

    if not rules:
        logger.warning(f"[Engine] Automation {automation_id}: no active rules, skipping")
        return

    # Exact keyword match
    matched_rule    = None
    matched_keyword = None
    for rule in rules:
        kw = match_keyword(comment_text, rule["trigger_words"])
        if kw:
            matched_rule    = rule
            matched_keyword = kw
            break

    if not matched_rule:
        logger.info(f"[Engine] No exact match for '{comment_text}'")
        return already_replied

    logger.info(f"[Engine] Matched '{matched_keyword}'")
    await update_analytics_by_id(db, automation_id, media_id, account_id, "trigger_count")

    # Cooldown (per commenter + automation)
    cutoff   = datetime.utcnow() - timedelta(hours=app_settings.COOLDOWN_HOURS)
    cooldown = await db["cooldown_logs"].find_one({
        "commenter_id":  commenter_id,
        "automation_id": automation_id,
        "triggered_at":  {"$gte": cutoff},
    })
    if cooldown:
        logger.info(f"[Engine] {commenter_id} in cooldown, skipping")
        await update_analytics_by_id(db, automation_id, media_id, account_id, "cooldown_blocked")
        await log_action(db, {**log_entry, "keyword_triggered": matched_keyword,
                               "action_taken": "none", "reply_sent": False,
                               "dm_sent": False, "success": True, "error": "cooldown"})
        return already_replied

    # Rate limit
    if matched_rule.get("send_dm") and settings.get("auto_dm"):
        if await check_rate_limit(db, account_id):
            logger.warning(f"[Engine] Rate limit exceeded")
            await update_analytics_by_id(db, automation_id, media_id, account_id, "rate_limit_blocked")
            await log_action(db, {**log_entry, "keyword_triggered": matched_keyword,
                                   "action_taken": "none", "reply_sent": False,
                                   "dm_sent": False, "success": True, "error": "rate_limit"})
            return already_replied

    # Delay
    if settings.get("delay_enabled"):
        await apply_delay()

    # ── Send actions ──────────────────────────────────────
    reply_sent   = False
    dm_sent      = False
    action_taken = []

    # 1. Comment reply — skip if another automation already replied to this comment
    if matched_rule.get("reply_comment") and settings.get("auto_comment_reply"):
        if already_replied:
            logger.info(f"[Engine] Skipping comment reply — already sent for comment {comment_id}")
        else:
            reply_sent = await send_comment_reply(
                comment_id=comment_id,
                message=matched_rule["response"],
                access_token=access_token,
            )
            if reply_sent:
                already_replied = True
                action_taken.append("reply")
                await update_analytics_by_id(db, automation_id, media_id, account_id, "reply_sent_count")

    # 2. DM sequence: opening message → follow DM → dm_actions
    if matched_rule.get("send_dm") and settings.get("auto_dm"):
        dm_text = matched_rule.get("opening_message") or matched_rule["response"]
        dm_sent = await send_dm(
            ig_user_id=ig_user_id,
            recipient_id=commenter_id,
            message=dm_text,
            access_token=access_token,
        )
        if dm_sent:
            action_taken.append("dm")
            await update_analytics_by_id(db, automation_id, media_id, account_id, "dm_sent_count")

            # 2b. Follow DM — separate message after opening
            follow_msg = matched_rule.get("follow_dm_message", "").strip()
            if follow_msg:
                await asyncio.sleep(1)  # small gap between messages
                await send_dm(
                    ig_user_id=ig_user_id,
                    recipient_id=commenter_id,
                    message=follow_msg,
                    access_token=access_token,
                )
                action_taken.append("follow_dm")

            # 2c. DM Actions — send as plain text (label: url)
            dm_actions = matched_rule.get("dm_actions", [])
            if dm_actions:
                lines = []
                for btn in dm_actions:
                    label = btn.get("label", "").strip()
                    action = btn.get("action", "").strip()
                    if label and action:
                        lines.append(f"{label}: {action}")
                    elif label:
                        lines.append(label)
                if lines:
                    await asyncio.sleep(1)
                    await send_dm(
                        ig_user_id=ig_user_id,
                        recipient_id=commenter_id,
                        message="\n".join(lines),
                        access_token=access_token,
                    )
                    action_taken.append("dm_actions")
        else:
            action_taken.append("dm_failed")
            logger.warning(
                f"[Engine] DM to {commenter_id} failed — "
                f"Instagram 24h messaging window may be closed."
            )

    # Update cooldown
    await db["cooldown_logs"].update_one(
        {"commenter_id": commenter_id, "automation_id": automation_id},
        {"$set": {
            "commenter_id":  commenter_id,
            "automation_id": automation_id,
            "post_id":       media_id,
            "account_id":    account_id,
            "triggered_at":  datetime.utcnow(),
        }},
        upsert=True,
    )

    await log_action(db, {
        **log_entry,
        "keyword_triggered": matched_keyword,
        "action_taken":      "+".join(action_taken) if action_taken else "none",
        "reply_sent":        reply_sent,
        "dm_sent":           dm_sent,
        "success":           True,
        "error":             None,
    })
    logger.info(f"[Engine] Done: actions={action_taken}")
    return already_replied


# ── Main Entry Point ──────────────────────────────────────
async def process_comment_event(
    db,
    media_id: str,
    comment_id: str,
    comment_text: str,
    commenter_id: str,
    account_id: str,
    ig_user_id: str,
    access_token: str,
):
    # ── Global dedup: skip if this comment_id is already being processed ──
    if comment_id in _processing:
        logger.info(f"[Engine] Duplicate comment_id={comment_id} — skipping")
        return
    _processing.add(comment_id)

    log_base = {
        "account_id":   account_id,
        "post_id":      media_id,
        "comment_id":   comment_id,
        "commenter_id": commenter_id,
        "comment_text": comment_text,
    }

    try:
        all_settings = await db["automation_settings"].find({
            "post_id":    media_id,
            "account_id": account_id,
            "is_active":  True,
        }).to_list(length=20)

        if not all_settings:
            logger.warning(f"[Engine] No active automations for post {media_id}")
            return

        # Track whether a comment reply was already sent for this comment_id
        # across all automations — prevents double-reply when multiple
        # automations exist on the same post.
        already_replied = False

        for settings in all_settings:
            already_replied = await _run_automation(
                db, settings, log_base, media_id, comment_id,
                comment_text, commenter_id, account_id,
                ig_user_id, access_token,
                already_replied=already_replied,
            )

    except Exception as e:
        logger.error(f"[Engine] Error: {e}", exc_info=True)
        await log_action(db, {
            **log_base,
            "keyword_triggered": "",
            "action_taken":      "error",
            "reply_sent":        False,
            "dm_sent":           False,
            "success":           False,
            "error":             str(e),
        })
    finally:
        # Always release the lock
        _processing.discard(comment_id)
