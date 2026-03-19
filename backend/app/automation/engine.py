import asyncio
import random
import logging
import httpx
import re
from datetime import datetime, timedelta

from app.config import settings as app_settings
from app.redis_pool import get_redis  # Fix #3: use shared pool (sync getter, returns Redis client)

logger = logging.getLogger(__name__)

GRAPH_BASE    = "https://graph.instagram.com/v19.0"
DM_RATE_LIMIT = app_settings.DM_RATE_LIMIT  # max DMs per hour (set in .env)

# Redis dedup (Fix #1: replaces in-memory _processing set)
# Key = dedup:{comment_id_or_msg_id}   TTL = 5 minutes
DEDUP_PREFIX = "dedup:"
DEDUP_TTL    = 300   # 5 minutes

async def _is_duplicate(event_id: str) -> bool:
    """Returns True if already processing; atomically sets the key if not."""
    try:
        r = get_redis()
        key = f"{DEDUP_PREFIX}{event_id}"
        result = await r.set(key, "1", nx=True, ex=DEDUP_TTL)
        return result is None   # None means key already existed -> duplicate
    except Exception as e:
        logger.warning(f"[Dedup] Redis error, allowing event: {e}")
        return False            # fail-open: process the event

async def _clear_dedup(event_id: str):
    try:
        r = get_redis()
        await r.delete(f"{DEDUP_PREFIX}{event_id}")
    except Exception:
        pass


EMAIL_AWAIT_TTL    = 3600            # 1 hour for user to reply with their email
EMAIL_AWAIT_PREFIX = "email_await:"  # Redis key = email_await:{account_id}:{sender_id}

# Round-robin counter prefix -- key = rr:{rule_id}  value = int index
RR_PREFIX = "rr:"

# Strict email validation regex
_EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

def is_valid_email(text: str) -> str | None:
    candidate = text.strip().lower()
    loose_match = re.search(r"[\w.%+\-]+@[\w.\-]+\.[a-zA-Z]{2,}", candidate)
    if loose_match:
        candidate = loose_match.group(0)
    if _EMAIL_REGEX.match(candidate):
        return candidate
    return None


# Fix #4: Shared plan-lookup helper (replaces 5x copy-pasted blocks)
async def get_account_plan(db, account_id: str) -> str:
    """Resolve account_id -> user -> plan string. Returns 'free' on any error."""
    try:
        from bson import ObjectId as _OID
        acct = await db["instagram_accounts"].find_one({"_id": _OID(account_id)})
        uid  = str(acct.get("user_id", "")) if acct else ""
        user = await db["users"].find_one({"_id": _OID(uid)}) if uid else None
        return user.get("plan", "free") if user else "free"
    except Exception as e:
        logger.debug(f"[Plan] get_account_plan failed for {account_id}: {e}")
        return "free"


# Round-Robin Picker
async def pick_round_robin(rule_id: str, messages: list, username: str = "") -> str:
    """
    Pick the next message from the list in a round-robin sequence.
    Uses Redis INCR to maintain a per-rule counter.
    Falls back to random if Redis fails.
    Replaces {username} placeholder.
    """
    if not messages:
        return ""
    try:
        r = get_redis()
        key = f"{RR_PREFIX}{rule_id}"
        idx = await r.incr(key)
        if idx == 1:
            await r.expire(key, 86400 * 30)  # 30 days TTL
        text = messages[(idx - 1) % len(messages)]
    except Exception as e:
        logger.warning(f"[RR] Redis failed, falling back to random: {e}")
        text = random.choice(messages)

    if username and "{username}" in text:
        text = text.replace("{username}", f"@{username}")
    return text.strip()


# -- Keyword Matching (Bug #9 fix: substring/contains mode added) -------------
def match_keyword(comment_text: str, trigger_words: list) -> str | None:
    """
    Keyword matching with two modes (Bug #9 fix).

    Old (broken): strict full-string equality only.
    "info" would NOT match "Info!", "send me the link" would NOT match
    "Can you send me the link please?" -- the vast majority of real comments
    silently fell through with no automation triggered.

    New behaviour (default: contains/substring):
      1. '*' wildcard    -- matches any text, always checked first.
      2. Prefix '='      -- forces exact-only mode, e.g. '=yes' only matches "yes".
      3. Exact match     -- full comment equals trigger word (trimmed, lowercase).
      4. Contains match  -- comment contains the trigger word as a substring.
                           Catches natural language ("Can you send me the link?").
    """
    if "*" in trigger_words:
        logger.info("Wildcard '*' matched -- triggering on any comment")
        return "*"

    text_lower = comment_text.strip().lower()

    for word in trigger_words:
        raw = word.strip()
        if not raw:
            continue

        # Exact-only mode: trigger word prefixed with '='
        if raw.startswith("="):
            kw = raw[1:].lower()
            if text_lower == kw:
                logger.info(f"Exact keyword matched (strict): '{raw}' == '{comment_text}'")
                return word
            continue

        kw = raw.lower()

        # Exact full-string match
        if text_lower == kw:
            logger.info(f"Exact keyword matched: '{word}' == '{comment_text}'")
            return word

        # Contains/substring match (catches natural language comments)
        if kw in text_lower:
            logger.info(f"Substring keyword matched: '{word}' in '{comment_text}'")
            return word

    return None


# Rate Limit
async def check_rate_limit(db, account_id: str) -> bool:
    cutoff = datetime.utcnow() - timedelta(hours=1)
    count  = await db["automation_logs"].count_documents({
        "account_id": account_id,
        "dm_sent":    True,
        "timestamp":  {"$gte": cutoff},
    })
    return count >= app_settings.DM_RATE_LIMIT


# Delay
async def apply_delay():
    delay = random.uniform(10, 30)
    logger.info(f"Applying delay of {delay:.1f}s")
    await asyncio.sleep(delay)


# Daily DM Cap
async def check_daily_cap(db, account_id: str) -> bool:
    """Returns True if today's per-plan DM cap has been reached."""
    from app.plans import get_plan_limits
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    count = await db["automation_logs"].count_documents({
        "account_id": account_id,
        "dm_sent":    True,
        "timestamp":  {"$gte": today_start},
    })
    plan   = await get_account_plan(db, account_id)   # Fix #4
    limits = get_plan_limits(plan)
    cap    = limits["dm_per_day"] if limits["dm_per_day"] is not None else app_settings.DAILY_DM_CAP
    return count >= cap


# Reply Picker (random -- used for comment replies / fallback)
def pick_reply(rule: dict, field: str, username: str = "") -> str:
    arr  = rule.get(field + "s") or []
    text = random.choice(arr) if arr else (rule.get(field) or "")
    if username and "{username}" in text:
        text = text.replace("{username}", f"@{username}")
    return text.strip()


# Build Follow DM message (text + profile link)
def build_follow_message(raw_msg: str, account_username: str, sender_username: str = "") -> str:
    msg = raw_msg.strip()
    if sender_username and "{username}" in msg:
        msg = msg.replace("{username}", f"@{sender_username}")
    if account_username:
        profile_url = f"instagram.com/{account_username}"
        if profile_url not in msg:
            msg = f"{msg}\n\U0001f449 {profile_url}"
    return msg


# Fix #2: Check if user follows the account
async def check_user_follows(ig_user_id: str, commenter_id: str, access_token: str) -> bool:
    """
    Check if commenter_id follows ig_user_id via the ig_follows_business field.
    Returns False on any error (fail-open: send the follow DM).
    """
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{GRAPH_BASE}/{commenter_id}",
                params={
                    "fields":       "ig_follows_business",
                    "access_token": access_token,
                },
            )
        data = resp.json()
        logger.debug(f"[FollowCheck] API response for {commenter_id}: {data}")

        if "error" in data:
            logger.warning(
                f"[FollowCheck] API error: {data['error'].get('message')} -- defaulting to False"
            )
            return False

        follows = bool(data.get("ig_follows_business", False))
        logger.info(f"[FollowCheck] commenter={commenter_id} follows={follows}")
        return follows

    except Exception as e:
        logger.warning(f"[FollowCheck] Request failed (defaulting to False -- send DM): {e}")
        return False


# Comment Reply
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


# Send DM
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

        err = data.get("error", {})
        logger.warning(
            f"DM failed -- code={err.get('code')} subcode={err.get('error_subcode')} "
            f"msg='{err.get('message')}'"
        )
        return False

    except Exception as e:
        logger.error(f"DM error: {e}")
        return False


# Log
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


# Lead Capture Helper
async def _fetch_ig_username(commenter_id: str, access_token: str) -> str:
    """
    Fetch the Instagram username for a given commenter_id via Graph API.
    Returns empty string on failure so lead capture is never blocked.
    Result is cached in Redis for 24h to avoid repeated API calls.

    Bug #10 fix: this function is now the single source of truth for username
    lookups. The DM and Story engines previously made their own uncached inline
    httpx calls on every event. All three engines now call this helper, so each
    unique sender_id costs at most one Graph API call per 24 hours.
    """
    cache_key = f"ig_username:{commenter_id}"
    try:
        r = get_redis()
        cached = await r.get(cache_key)
        if cached:
            logger.debug(f"[Username] Cache HIT for {commenter_id} -> {cached}")
            return cached
    except Exception:
        pass

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{GRAPH_BASE}/{commenter_id}",
                params={"fields": "username", "access_token": access_token},
            )
        data = resp.json()
        username = data.get("username", "")
        if username:
            logger.info(f"[Username] Fetched @{username} for commenter_id={commenter_id}")
            try:
                r = get_redis()
                await r.setex(cache_key, 86400, username)  # cache 24h
            except Exception:
                pass
        return username
    except Exception as e:
        logger.warning(f"[Username] Graph API call failed for {commenter_id}: {e}")
        return ""


async def _capture_lead(
    db,
    account_id: str,
    commenter_id: str,
    user_id: str,
    comment_text: str,
    keyword: str,
    post_id: str,
    automation_id: str,
    source: str,
    dm_sent: bool,
    reply_sent: bool,
    commenter_username: str = "",
    access_token: str = "",
):
    now = datetime.utcnow()

    # Resolve username if not already known
    if not commenter_username and access_token:
        commenter_username = await _fetch_ig_username(commenter_id, access_token)

    await db["leads"].update_one(
        {"commenter_id": commenter_id, "account_id": account_id},
        {"$set": {
            "commenter_id":       commenter_id,
            "commenter_username": commenter_username,
            "account_id":         account_id,
            "user_id":            user_id,
            "comment_text":       comment_text,
            "keyword":            keyword,
            "post_id":            post_id,
            "automation_id":      automation_id,
            "source":             source,
            "dm_sent":            dm_sent,
            "reply_sent":         reply_sent,
            "captured_at":        now,
        }},
        upsert=True,
    )

    await db["collected_users"].update_one(
        {"ig_user_id": commenter_id, "account_id": account_id},
        {
            "$set": {
                "ig_user_id":    commenter_id,
                "username":      commenter_username,
                "account_id":    account_id,
                "automation_id": automation_id,
                "source":        source,
                "updated_at":    now,
            },
            "$setOnInsert": {
                "email":      "",
                "created_at": now,
            },
        },
        upsert=True,
    )
    logger.info(f"[Lead] Captured: source={source} commenter={commenter_id} username=@{commenter_username}")


# Email Collection: send prompt & mark Redis
async def _send_email_prompt(
    db,
    account_id: str,
    automation_id: str,
    sender_id: str,
    ig_user_id: str,
    access_token: str,
    rule: dict,
):
    """Send the email-collection DM and set the Redis await key."""
    email_prompt = (
        rule.get("email_prompt") or
        "What's your email address? We'll send you more details \U0001f4e9"
    )
    await asyncio.sleep(1)
    sent = await send_dm(
        ig_user_id=ig_user_id,
        recipient_id=sender_id,
        message=email_prompt,
        access_token=access_token,
    )
    if sent:
        r = get_redis()
        redis_key = f"{EMAIL_AWAIT_PREFIX}{account_id}:{sender_id}"
        await r.setex(redis_key, EMAIL_AWAIT_TTL, automation_id)
        logger.info(f"[Email] Prompt sent -> awaiting email reply from {sender_id}")


# Email Reply Handler
async def process_email_reply(
    db,
    sender_id: str,
    msg_text: str,
    account_id: str,
    ig_user_id: str,
    access_token: str,
) -> bool:
    try:
        r = get_redis()
        redis_key = f"{EMAIL_AWAIT_PREFIX}{account_id}:{sender_id}"
        automation_id = await r.get(redis_key)

        if not automation_id:
            return False

        email = is_valid_email(msg_text)

        if not email:
            # -- Bug #8 fix: cap re-prompts to avoid infinite DM spam ----------
            # Before this fix every invalid reply caused an unlimited chain of
            # re-prompt DMs until the 1-hour TTL expired, risking IG spam flags.
            reprompt_key   = f"email_reprompt:{account_id}:{sender_id}"
            reprompt_count = await r.incr(reprompt_key)
            if reprompt_count == 1:
                # Set TTL on first increment so the counter expires with session
                await r.expire(reprompt_key, EMAIL_AWAIT_TTL)

            MAX_REPROMPTS = 3
            if reprompt_count > MAX_REPROMPTS:
                logger.info(
                    f"[Email] Max re-prompts ({MAX_REPROMPTS}) reached for "
                    f"{sender_id} -- aborting email collection"
                )
                await r.delete(redis_key, reprompt_key)
                await send_dm(
                    ig_user_id=ig_user_id,
                    recipient_id=sender_id,
                    message="No worries! Feel free to reach out anytime. \U0001f44b",
                    access_token=access_token,
                )
                return True

            logger.info(
                f"[Email] Invalid email '{msg_text}' from {sender_id} -- "
                f"re-prompt {reprompt_count}/{MAX_REPROMPTS}"
            )
            await send_dm(
                ig_user_id=ig_user_id,
                recipient_id=sender_id,
                message=(
                    "Hmm, that doesn't look like a valid email address. "
                    "Please reply with a valid email (e.g. name@example.com) \U0001f4e7"
                ),
                access_token=access_token,
            )
            return True

        logger.info(f"[Email] Valid email='{email}' captured from {sender_id}")
        now = datetime.utcnow()

        await db["collected_users"].update_one(
            {"ig_user_id": sender_id, "account_id": account_id},
            {"$set": {
                "ig_user_id":        sender_id,
                "account_id":        account_id,
                "email":             email,
                "email_captured_at": now,
                "updated_at":        now,
            },
            "$setOnInsert": {
                "username":   "",
                "created_at": now,
            }},
            upsert=True,
        )

        await db["leads"].update_one(
            {"commenter_id": sender_id, "account_id": account_id},
            {"$set": {
                "email":             email,
                "email_captured_at": now,
            }},
            upsert=True,
        )

        await r.delete(redis_key)

        await send_dm(
            ig_user_id=ig_user_id,
            recipient_id=sender_id,
            message="Got it, thanks! \U0001f389 We'll be in touch at that email address soon.",
            access_token=access_token,
        )
        return True

    except Exception as e:
        logger.error(f"[Email] process_email_reply error: {e}", exc_info=True)
        return False


# Per-automation pipeline
async def _run_automation(
    db, settings: dict, log_base: dict,
    media_id: str, comment_id: str, comment_text: str,
    commenter_id: str, account_id: str,
    ig_user_id: str, access_token: str,
    already_replied: bool = False,
    commenter_username: str = "",
) -> bool:
    automation_id = str(settings["_id"])
    log_entry     = {**log_base, "automation_id": automation_id}

    if not settings.get("auto_comment_reply") and not settings.get("auto_dm"):
        logger.warning(f"[Engine] Automation {automation_id}: both disabled, skipping")
        return already_replied

    # Fetch rules
    rules = await db["keyword_rules"].find({
        "automation_id": automation_id,
        "is_active":     True,
    }).to_list(length=50)

    if not rules:
        rules = await db["keyword_rules"].find({
            "post_id":    media_id,
            "account_id": account_id,
            "is_active":  True,
            "$or": [{"automation_id": None}, {"automation_id": {"$exists": False}}],
        }).to_list(length=50)

    if not rules:
        logger.warning(f"[Engine] Automation {automation_id}: no active rules, skipping")
        return already_replied

    # Keyword match
    matched_rule    = None
    matched_keyword = None
    for rule in rules:
        kw = match_keyword(comment_text, rule["trigger_words"])
        if kw:
            matched_rule    = rule
            matched_keyword = kw
            break

    if not matched_rule:
        logger.info(f"[Engine] No match for '{comment_text}'")
        return already_replied

    logger.info(f"[Engine] Matched '{matched_keyword}'")
    await update_analytics_by_id(db, automation_id, media_id, account_id, "trigger_count")

    # Fix #5: use per-automation cooldown_hours if set
    cooldown_hours = settings.get("cooldown_hours") or app_settings.COOLDOWN_HOURS
    cutoff   = datetime.utcnow() - timedelta(hours=cooldown_hours)
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

    # Rate limit check (DM path only)
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

    # -------------------------------------------------------------------------
    # ACTION SEQUENCE
    # -------------------------------------------------------------------------
    reply_sent   = False
    dm_sent      = False
    action_taken = []

    will_send_dm = matched_rule.get("send_dm") and settings.get("auto_dm")

    if will_send_dm:
        if await check_daily_cap(db, account_id):
            logger.warning(f"[Engine] Daily DM cap reached for account={account_id}")
            await update_analytics_by_id(db, automation_id, media_id, account_id, "rate_limit_blocked")
            await log_action(db, {**log_entry, "keyword_triggered": matched_keyword,
                                   "action_taken": "none", "reply_sent": False,
                                   "dm_sent": False, "success": True, "error": "daily_cap"})
            return already_replied

        # STEP 1: Opening Message (round-robin)
        opening_msgs = matched_rule.get("opening_messages") or []
        if not opening_msgs:
            single = matched_rule.get("opening_message", "").strip()
            if single:
                opening_msgs = [single]

        if opening_msgs:
            rule_id      = str(matched_rule.get("_id", automation_id))
            opening_text = await pick_round_robin(rule_id, opening_msgs, commenter_username)
            if opening_text:
                dm_sent = await send_dm(
                    ig_user_id=ig_user_id,
                    recipient_id=commenter_id,
                    message=opening_text,
                    access_token=access_token,
                )
                if dm_sent:
                    action_taken.append("opening_dm")
                    await update_analytics_by_id(db, automation_id, media_id, account_id, "dm_sent_count")
                    logger.info(f"[Engine] Opening DM sent (round-robin) to {commenter_id}")
                else:
                    action_taken.append("opening_dm_failed")
                    logger.warning(f"[Engine] Opening DM failed -- Instagram 24h window may be closed")

        # STEP 2: Ask to Follow
        follow_msg = matched_rule.get("follow_dm_message", "").strip()
        if follow_msg:
            already_follows = await check_user_follows(ig_user_id, commenter_id, access_token)
            if already_follows:
                logger.info(f"[Engine] {commenter_id} already follows -- skipping Ask-to-Follow DM")
                action_taken.append("follow_skip")
            else:
                await asyncio.sleep(1)
                follow_sent = await send_dm(
                    ig_user_id=ig_user_id,
                    recipient_id=commenter_id,
                    message=follow_msg,
                    access_token=access_token,
                )
                if follow_sent:
                    dm_sent = True
                    action_taken.append("follow_dm")
                    logger.info(f"[Engine] Ask-to-Follow DM sent to {commenter_id}")

        # DM Actions (buttons / links)
        dm_actions = matched_rule.get("dm_actions", [])
        if dm_actions:
            lines = []
            for btn in dm_actions:
                label  = btn.get("label", "").strip()
                action = btn.get("action", "").strip()
                if label and action:
                    lines.append(f"{label}: {action}")
                elif label:
                    lines.append(label)
            if lines:
                await asyncio.sleep(1)
                acts_sent = await send_dm(
                    ig_user_id=ig_user_id,
                    recipient_id=commenter_id,
                    message="\n".join(lines),
                    access_token=access_token,
                )
                if acts_sent:
                    dm_sent = True
                    action_taken.append("dm_actions")

    # STEP 3: Triggered Reply (comment reply)
    if matched_rule.get("reply_comment") and settings.get("auto_comment_reply"):
        if already_replied:
            logger.info(f"[Engine] Skipping comment reply -- already sent for comment {comment_id}")
        else:
            reply_text = pick_reply(matched_rule, "response", commenter_username)
            reply_sent = await send_comment_reply(
                comment_id=comment_id,
                message=reply_text,
                access_token=access_token,
            )
            if reply_sent:
                already_replied = True
                action_taken.append("comment_reply")
                await update_analytics_by_id(db, automation_id, media_id, account_id, "reply_sent_count")

    # Lead capture
    if reply_sent or dm_sent:
        try:
            from bson import ObjectId as _OID_lead
            acct     = await db["instagram_accounts"].find_one({"_id": _OID_lead(account_id)})
            _user_id = str(acct.get("user_id", "")) if acct else ""
            await _capture_lead(
                db=db,
                account_id=account_id,
                commenter_id=commenter_id,
                user_id=_user_id,
                comment_text=comment_text,
                keyword=matched_keyword,
                post_id=media_id,
                automation_id=automation_id,
                source="comment",
                dm_sent=dm_sent,
                reply_sent=reply_sent,
                commenter_username=commenter_username,
                access_token=access_token,
            )

            # STEP 4: Email Collection
            if dm_sent and matched_rule.get("collect_email"):
                try:
                    from app.plans import check_feature as _cfe
                    plan = await get_account_plan(db, account_id)   # Fix #4
                    if _cfe(plan, "email_collection"):
                        await _send_email_prompt(
                            db=db,
                            account_id=account_id,
                            automation_id=automation_id,
                            sender_id=commenter_id,
                            ig_user_id=ig_user_id,
                            access_token=access_token,
                            rule=matched_rule,
                        )
                    else:
                        logger.info(f"[Engine] email_collection blocked for plan={plan}")
                except Exception as _ee:
                    logger.debug(f"[Engine] email plan check failed: {_ee}")

        except Exception as _le:
            logger.debug(f"[Engine] Lead capture failed (non-critical): {_le}")

    # Cooldown update
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


# Main Entry Point
async def process_comment_event(
    db,
    media_id: str,
    comment_id: str,
    comment_text: str,
    commenter_id: str,
    account_id: str,
    ig_user_id: str,
    access_token: str,
    commenter_username: str = "",
):
    # Fix #1: Redis-backed dedup instead of in-memory set
    if await _is_duplicate(comment_id):
        logger.info(f"[Engine] Duplicate comment_id={comment_id} -- skipping")
        return

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

        already_replied = False
        for settings in all_settings:
            already_replied = await _run_automation(
                db, settings, log_base, media_id, comment_id,
                comment_text, commenter_id, account_id,
                ig_user_id, access_token,
                already_replied=already_replied,
                commenter_username=commenter_username,
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
        await _clear_dedup(comment_id)


# Story Reply Entry Point
async def process_story_event(
    db,
    sender_id: str,
    msg_text: str,
    msg_id: str,
    story_id: str,
    account_id: str,
    ig_user_id: str,
    access_token: str,
):
    # Fix #1: Redis-backed dedup
    if await _is_duplicate(msg_id):
        logger.info(f"[Story Engine] Duplicate msg_id={msg_id} -- skipping")
        return

    try:
        # Fix #4: use shared plan helper
        plan = await get_account_plan(db, account_id)
        try:
            from app.plans import get_plan_limits as _gpl
            if not _gpl(plan).get("story_reply", False):
                logger.info(f"[Story Engine] story_reply not enabled for plan={plan} -- skipping")
                return
        except Exception as _se:
            logger.debug(f"[Story Engine] plan check error: {_se}")

        story_settings = await db["automation_settings"].find({
            "post_id":    story_id,
            "account_id": account_id,
            "is_active":  True,
        }).to_list(10)

        if not story_settings:
            logger.info(f"[Story Engine] No automation for story_id={story_id} -- no reply sent")
            return

        for settings in story_settings:
            automation_id = str(settings["_id"])

            rules = await db["keyword_rules"].find({
                "automation_id": automation_id,
                "is_active":     True,
            }).to_list(50)
            if not rules:
                rules = await db["keyword_rules"].find({
                    "post_id":    story_id,
                    "account_id": account_id,
                    "is_active":  True,
                }).to_list(50)

            if not rules:
                logger.info(f"[Story Engine] No rules for automation={automation_id}")
                continue

            matched_rule = None
            matched_kw   = None
            for rule in rules:
                kw = match_keyword(msg_text or "", rule.get("trigger_words", []))
                if kw:
                    matched_rule = rule
                    matched_kw   = kw
                    break

            if not matched_rule:
                logger.info(f"[Story Engine] No keyword match for '{msg_text}'")
                continue

            # Fix #5: per-automation cooldown_hours
            cooldown_hours = settings.get("cooldown_hours") or app_settings.COOLDOWN_HOURS
            cutoff   = datetime.utcnow() - timedelta(hours=cooldown_hours)
            cooldown = await db["cooldown_logs"].find_one({
                "commenter_id":  sender_id,
                "automation_id": automation_id,
                "triggered_at":  {"$gte": cutoff},
            })
            if cooldown:
                logger.info(f"[Story Engine] {sender_id} in cooldown")
                continue

            await update_analytics_by_id(db, automation_id, story_id, account_id, "trigger_count")

            if await check_rate_limit(db, account_id):
                logger.warning(f"[Story Engine] Rate limit hit")
                await update_analytics_by_id(db, automation_id, story_id, account_id, "rate_limit_blocked")
                continue

            if await check_daily_cap(db, account_id):
                logger.warning(f"[Story Engine] Daily DM cap reached for account={account_id}")
                await update_analytics_by_id(db, automation_id, story_id, account_id, "rate_limit_blocked")
                continue

            action_taken = []
            dm_sent      = False

            # STEP 1: Opening Message (round-robin)
            opening_msgs = matched_rule.get("opening_messages") or []
            if not opening_msgs:
                single = matched_rule.get("opening_message", "").strip()
                if single:
                    opening_msgs = [single]
            if not opening_msgs:
                resp_msgs = matched_rule.get("responses") or []
                if not resp_msgs and matched_rule.get("response"):
                    resp_msgs = [matched_rule["response"]]
                opening_msgs = resp_msgs

            if not opening_msgs:
                logger.warning(f"[Story Engine] No reply text in rule")
                continue

            # Bug #10 fix: use cached _fetch_ig_username instead of inline httpx
            sender_username = await _fetch_ig_username(sender_id, access_token)

            rule_id      = str(matched_rule.get("_id", automation_id))
            opening_text = await pick_round_robin(rule_id, opening_msgs, sender_username)
            if opening_text:
                dm_sent = await send_dm(
                    ig_user_id=ig_user_id,
                    recipient_id=sender_id,
                    message=opening_text,
                    access_token=access_token,
                )
                if dm_sent:
                    action_taken.append("opening_dm")
                    await update_analytics_by_id(db, automation_id, story_id, account_id, "dm_sent_count")

            # STEP 2: Ask to Follow
            _follow_raw = matched_rule.get("follow_dm_message", "").strip()
            follow_msg  = _follow_raw.replace("{username}", f"@{sender_username}") if sender_username else _follow_raw
            if follow_msg:
                already_follows = await check_user_follows(ig_user_id, sender_id, access_token)
                if already_follows:
                    logger.info(f"[Story Engine] {sender_id} already follows -- skipping Ask-to-Follow")
                    action_taken.append("follow_skip")
                else:
                    await asyncio.sleep(1)
                    follow_sent = await send_dm(
                        ig_user_id=ig_user_id,
                        recipient_id=sender_id,
                        message=follow_msg,
                        access_token=access_token,
                    )
                    if follow_sent:
                        dm_sent = True
                        action_taken.append("follow_dm")

            # DM Actions
            dm_actions = matched_rule.get("dm_actions", [])
            if dm_actions:
                lines = []
                for btn in dm_actions:
                    label  = btn.get("label", "").strip()
                    act    = btn.get("action", "").strip()
                    if label and act: lines.append(f"{label}: {act}")
                    elif label:       lines.append(label)
                if lines:
                    await asyncio.sleep(1)
                    acts_sent = await send_dm(ig_user_id=ig_user_id, recipient_id=sender_id,
                                              message="\n".join(lines), access_token=access_token)
                    if acts_sent:
                        dm_sent = True
                        action_taken.append("dm_actions")

            if not dm_sent:
                action_taken.append("dm_failed")

            await db["cooldown_logs"].update_one(
                {"commenter_id": sender_id, "automation_id": automation_id},
                {"$set": {"commenter_id": sender_id, "automation_id": automation_id,
                          "post_id": story_id, "account_id": account_id,
                          "triggered_at": datetime.utcnow()}},
                upsert=True,
            )
            await log_action(db, {
                "account_id":        account_id,
                "post_id":           story_id,
                "comment_id":        msg_id,
                "commenter_id":      sender_id,
                "comment_text":      msg_text,
                "automation_id":     automation_id,
                "keyword_triggered": matched_kw,
                "action_taken":      "+".join(action_taken) if action_taken else "none",
                "reply_sent":        False,
                "dm_sent":           dm_sent,
                "success":           dm_sent,
                "error":             None if dm_sent else "dm_failed",
            })

            # Lead capture + Email Collection
            if dm_sent:
                try:
                    from bson import ObjectId as _OID_s
                    acct     = await db["instagram_accounts"].find_one({"_id": _OID_s(account_id)})
                    _user_id = str(acct.get("user_id", "")) if acct else ""
                    await _capture_lead(
                        db=db,
                        account_id=account_id,
                        commenter_id=sender_id,
                        user_id=_user_id,
                        comment_text=msg_text,
                        keyword=matched_kw,
                        post_id=story_id,
                        automation_id=automation_id,
                        source="story",
                        dm_sent=True,
                        reply_sent=False,
                    )

                    # STEP 4: Email Collection
                    if matched_rule.get("collect_email"):
                        try:
                            from app.plans import check_feature as _cfs
                            plan_s = await get_account_plan(db, account_id)   # Fix #4
                            if _cfs(plan_s, "email_collection"):
                                await _send_email_prompt(
                                    db=db,
                                    account_id=account_id,
                                    automation_id=automation_id,
                                    sender_id=sender_id,
                                    ig_user_id=ig_user_id,
                                    access_token=access_token,
                                    rule=matched_rule,
                                )
                            else:
                                logger.info(f"[Story Engine] email_collection blocked for plan={plan_s}")
                        except Exception as _ees:
                            logger.debug(f"[Story Engine] email plan check failed: {_ees}")
                except Exception as _le:
                    logger.debug(f"[Story Engine] Lead capture failed (non-critical): {_le}")

    except Exception as e:
        logger.error(f"[Story Engine] Error: {e}", exc_info=True)
    finally:
        await _clear_dedup(msg_id)


# DM Entry Point
async def process_dm_event(
    db,
    sender_id: str,
    msg_text: str,
    msg_id: str,
    account_id: str,
    ig_user_id: str,
    access_token: str,
):
    # Fix #1: Redis-backed dedup
    if await _is_duplicate(msg_id):
        logger.info(f"[DM Engine] Duplicate msg_id={msg_id} -- skipping")
        return

    try:
        # 1. Email reply interception (highest priority)
        consumed = await process_email_reply(
            db=db,
            sender_id=sender_id,
            msg_text=msg_text,
            account_id=account_id,
            ig_user_id=ig_user_id,
            access_token=access_token,
        )
        if consumed:
            logger.info(f"[DM Engine] Message from {sender_id} consumed by email handler")
            return

        # 2. Keyword matching
        dm_rules = await db["keyword_rules"].find({
            "account_id": account_id,
            "is_active":  True,
            "send_dm":    True,
        }).to_list(50)

        if not dm_rules:
            logger.info(f"[DM Engine] No DM-enabled rules for account={account_id} -- no reply sent")
            return

        matched_rule = None
        matched_kw   = None
        for rule in dm_rules:
            kw = match_keyword(msg_text, rule.get("trigger_words", []))
            if kw:
                matched_rule = rule
                matched_kw   = kw
                break

        if not matched_rule:
            logger.info(f"[DM Engine] No keyword match for DM text='{msg_text}'")
            return

        automation_id = str(matched_rule.get("automation_id", "dm"))
        logger.info(f"[DM Engine] Matched '{matched_kw}' -- will reply to {sender_id}")

        # Fix #5: per-automation cooldown (DM rules fall back to global)
        cutoff   = datetime.utcnow() - timedelta(hours=app_settings.COOLDOWN_HOURS)
        cooldown = await db["cooldown_logs"].find_one({
            "commenter_id":  sender_id,
            "automation_id": automation_id,
            "triggered_at":  {"$gte": cutoff},
        })
        if cooldown:
            logger.info(f"[DM Engine] {sender_id} in cooldown -- skipping")
            return

        if await check_rate_limit(db, account_id):
            logger.warning(f"[DM Engine] Rate limit hit for account={account_id}")
            return

        if await check_daily_cap(db, account_id):
            logger.warning(f"[DM Engine] Daily DM cap reached for account={account_id}")
            return

        action_taken = []
        dm_sent      = False

        # Bug #10 fix: use cached _fetch_ig_username instead of inline httpx call
        sender_username = await _fetch_ig_username(sender_id, access_token)

        # STEP 1: Opening Message (round-robin)
        opening_msgs = matched_rule.get("opening_messages") or []
        if not opening_msgs:
            single = matched_rule.get("opening_message", "").strip()
            if single:
                opening_msgs = [single]
        if not opening_msgs:
            resp_msgs = matched_rule.get("responses") or []
            if not resp_msgs and matched_rule.get("response"):
                resp_msgs = [matched_rule["response"]]
            opening_msgs = resp_msgs

        if not opening_msgs:
            logger.warning(f"[DM Engine] Matched rule has no reply text -- skipping")
            return

        rule_id      = str(matched_rule.get("_id", automation_id))
        opening_text = await pick_round_robin(rule_id, opening_msgs, sender_username)
        if opening_text:
            dm_sent = await send_dm(
                ig_user_id=ig_user_id,
                recipient_id=sender_id,
                message=opening_text,
                access_token=access_token,
            )
            if dm_sent:
                action_taken.append("opening_dm")

        # STEP 2: Ask to Follow
        _follow_raw = matched_rule.get("follow_dm_message", "").strip()
        follow_msg  = _follow_raw.replace("{username}", f"@{sender_username}") if sender_username else _follow_raw
        if follow_msg:
            already_follows = await check_user_follows(ig_user_id, sender_id, access_token)
            if already_follows:
                logger.info(f"[DM Engine] {sender_id} already follows -- skipping Ask-to-Follow")
                action_taken.append("follow_skip")
            else:
                await asyncio.sleep(1)
                follow_sent = await send_dm(
                    ig_user_id=ig_user_id,
                    recipient_id=sender_id,
                    message=follow_msg,
                    access_token=access_token,
                )
                if follow_sent:
                    dm_sent = True
                    action_taken.append("follow_dm")

        # DM Actions
        dm_actions = matched_rule.get("dm_actions", [])
        if dm_actions:
            lines = []
            for btn in dm_actions:
                label  = btn.get("label", "").strip()
                act    = btn.get("action", "").strip()
                if label and act:
                    lines.append(f"{label}: {act}")
                elif label:
                    lines.append(label)
            if lines:
                await asyncio.sleep(1)
                acts_sent = await send_dm(ig_user_id=ig_user_id, recipient_id=sender_id,
                                          message="\n".join(lines), access_token=access_token)
                if acts_sent:
                    dm_sent = True
                    action_taken.append("dm_actions")

        await db["cooldown_logs"].update_one(
            {"commenter_id": sender_id, "automation_id": automation_id},
            {"$set": {"commenter_id": sender_id, "automation_id": automation_id,
                      "post_id": "dm", "account_id": account_id,
                      "triggered_at": datetime.utcnow()}},
            upsert=True,
        )

        await log_action(db, {
            "account_id":        account_id,
            "post_id":           "dm",
            "comment_id":        msg_id,
            "commenter_id":      sender_id,
            "comment_text":      msg_text,
            "automation_id":     automation_id,
            "keyword_triggered": matched_kw,
            "action_taken":      "+".join(action_taken) if action_taken else "none",
            "reply_sent":        False,
            "dm_sent":           dm_sent,
            "success":           dm_sent,
            "error":             None if dm_sent else "dm_failed",
        })

        # Lead capture + Email Collection
        if dm_sent:
            try:
                from bson import ObjectId as _OID_dm
                acct     = await db["instagram_accounts"].find_one({"_id": _OID_dm(account_id)})
                _user_id = str(acct.get("user_id", "")) if acct else ""
                await _capture_lead(
                    db=db,
                    account_id=account_id,
                    commenter_id=sender_id,
                    user_id=_user_id,
                    comment_text=msg_text,
                    keyword=matched_kw,
                    post_id="dm",
                    automation_id=automation_id,
                    source="dm",
                    dm_sent=True,
                    reply_sent=False,
                )

                # STEP 4: Email Collection
                if matched_rule.get("collect_email"):
                    try:
                        from app.plans import check_feature as _cfd
                        plan_d = await get_account_plan(db, account_id)   # Fix #4
                        if _cfd(plan_d, "email_collection"):
                            await _send_email_prompt(
                                db=db,
                                account_id=account_id,
                                automation_id=automation_id,
                                sender_id=sender_id,
                                ig_user_id=ig_user_id,
                                access_token=access_token,
                                rule=matched_rule,
                            )
                        else:
                            logger.info(f"[DM Engine] email_collection blocked for plan={plan_d}")
                    except Exception as _eed:
                        logger.debug(f"[DM Engine] email plan check failed: {_eed}")
            except Exception as _le:
                logger.debug(f"[DM Engine] Lead capture failed (non-critical): {_le}")

    except Exception as e:
        logger.error(f"[DM Engine] Error: {e}", exc_info=True)
    finally:
        await _clear_dedup(msg_id)
