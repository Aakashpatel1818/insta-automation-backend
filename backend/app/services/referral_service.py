"""
referral_service.py
Core logic for the entire referral system:
  - Code generation
  - Milestone rewards (register / first_automation / pro_purchase)
  - Level-2 chain rewards
  - Bonus milestones (every 5th referral)
  - Nudge scheduler (day 3 / 6 / 7)
  - Coin wallet helpers
"""

import asyncio
import logging
import random
import string
from datetime import datetime, timedelta
from typing import Optional

from app.database import get_db
from app.config import settings

logger = logging.getLogger(__name__)

# ─── Milestone coin config (change anytime here) ─────────────────────────────
MILESTONE_CONFIG = {
    "register":         {"referrer": 25,  "referee": 25},
    "first_automation": {"referrer": 75,  "referee": 50},
    "pro_purchase":     {"referrer": 200, "referee": 100},
}

# Level-2 (indirect) chain coins
CHAIN_CONFIG = {
    "register":         {"referrer": 10},
    "first_automation": {"referrer": 25},
    "pro_purchase":     {"referrer": 50},
}

# Bonus coins every Nth referral
BONUS_MILESTONES = {5: 250, 10: 600, 25: 2000, 50: 5000}

REFERRAL_EXPIRY_DAYS = 7


# ─── Utility ─────────────────────────────────────────────────────────────────

def generate_referral_code(username: str) -> str:
    # 4 chars from username + 4 random = 8 char code  e.g. AAKA3X9M
    # 36^4 = 1.6 million combinations per username prefix
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    base = username[:4].upper() if len(username) >= 4 else username.upper().ljust(4, 'X')
    return f"{base}{suffix}"


def mask_email(email: str) -> str:
    parts = email.split("@")
    if len(parts) != 2:
        return "***"
    name, domain = parts
    return f"{name[:2]}***@{domain}"


# ─── Wallet helpers ───────────────────────────────────────────────────────────

async def get_or_create_wallet(db, user_id: str) -> dict:
    wallet = await db["coin_wallets"].find_one({"user_id": user_id})
    if not wallet:
        doc = {"user_id": user_id, "balance": 0, "total_earned": 0, "total_spent": 0}
        await db["coin_wallets"].insert_one(doc)
        return doc
    return wallet


async def credit_coins(db, user_id: str, amount: int, source: str, ref_id=None, level: int = 1):
    from app.plans import get_plan_limits
    # Apply coins_multiplier from user's plan (except for bonus_milestone sources
    # to avoid double-multiplying already generous bonuses)
    try:
        user   = await db["users"].find_one({"_id": __oid(user_id)})
        plan   = user.get("plan", "free") if user else "free"
        mult   = get_plan_limits(plan).get("coins_multiplier", 1)
        # Don't multiply bonus_milestone rewards (they're already generous)
        if mult > 1 and not source.startswith("bonus_milestone"):
            amount = amount * mult
    except Exception:
        pass  # fall back to original amount on any error

    await get_or_create_wallet(db, user_id)
    await db["coin_wallets"].update_one(
        {"user_id": user_id},
        {"$inc": {"balance": amount, "total_earned": amount}},
    )
    await db["coin_transactions"].insert_one({
        "user_id":    user_id,
        "type":       "earn",
        "source":     source,
        "amount":     amount,
        "ref_id":     str(ref_id) if ref_id else None,
        "level":      level,
        "created_at": datetime.utcnow(),
    })
    logger.info(f"[Coins] +{amount} → user {user_id}  source={source} level={level}")

    # Real-time socket emit
    try:
        from app.socket_manager import emit_coin_update
        wallet = await db["coin_wallets"].find_one({"user_id": user_id})
        balance = wallet["balance"] if wallet else 0
        await emit_coin_update(user_id, amount, source, balance)
    except Exception as e:
        logger.warning(f"[Socket] emit_coin_update failed: {e}")


async def get_wallet_balance(db, user_id: str) -> int:
    wallet = await db["coin_wallets"].find_one({"user_id": user_id})
    return wallet["balance"] if wallet else 0


# ─── Referral code init (called on register) ─────────────────────────────────

async def init_user_referral(db, user_id: str, username: str, referred_by_code: Optional[str] = None):
    """
    Called right after user is created.
    1. Creates unique referral code for the new user.
    2. Links referred_by_level1 / level2 if a code was provided.
    3. Triggers the 'register' milestone.
    """
    # Generate unique code
    code = generate_referral_code(username)
    while await db["users"].find_one({"referral_code": code}):
        code = generate_referral_code(username)

    referrer_l1_id = None
    referrer_l2_id = None

    if referred_by_code:
        referrer = await db["users"].find_one({"referral_code": referred_by_code.upper()})
        if referrer:
            # Prevent self-referral by user ID
            if str(referrer["_id"]) == user_id:
                logger.warning(f"[Referral] Self-referral blocked for user {user_id}")
            # Prevent self-referral by email (same person, different account attempt)
            elif referrer.get("email", "").lower() == (await db["users"].find_one({"_id": __oid(user_id)}) or {}).get("email", "").lower():
                logger.warning(f"[Referral] Same-email referral blocked for user {user_id}")
            else:
                referrer_l1_id = str(referrer["_id"])
                # Level 2 — referrer's referrer
                if referrer.get("referred_by_level1"):
                    referrer_l2_id = referrer["referred_by_level1"]

    # Update new user doc
    await db["users"].update_one(
        {"_id": __oid(user_id)},
        {"$set": {
            "referral_code":      code,
            "referred_by_level1": referrer_l1_id,
            "referred_by_level2": referrer_l2_id,
        }},
    )

    # Create wallet for new user
    await get_or_create_wallet(db, user_id)

    # If there's a referrer → create referral document + trigger milestone
    if referrer_l1_id:
        referral_doc = {
            "referrer_id":      referrer_l1_id,
            "referee_id":       user_id,
            "status":           "pending",
            "coins_given":      0,
            "created_at":       datetime.utcnow(),
            "rewarded_at":      None,
            "expires_at":       datetime.utcnow() + timedelta(days=REFERRAL_EXPIRY_DAYS),
            "nudge_day3_sent":  False,
            "nudge_day6_sent":  False,
            "nudge_day7_sent":  False,
            "milestones_done":  [],
        }
        result = await db["referrals"].insert_one(referral_doc)
        referral_id = result.inserted_id

        await trigger_milestone(db, user_id, "register", referral_id=referral_id)


async def trigger_milestone(db, referee_id: str, milestone: str, referral_id=None):
    """
    Central function called when a user hits a milestone.
    Rewards referrer (L1 + L2) and referee.
    Also checks bonus milestone.
    """
    if milestone not in MILESTONE_CONFIG:
        return

    # Find referral doc if not provided
    if referral_id is None:
        referral = await db["referrals"].find_one({"referee_id": referee_id, "status": {"$ne": "invalid"}})
    else:
        from bson import ObjectId
        referral = await db["referrals"].find_one({"_id": referral_id})

    if not referral:
        return

    # Avoid double rewarding same milestone
    if milestone in referral.get("milestones_done", []):
        return

    referrer_l1_id = referral["referrer_id"]
    cfg = MILESTONE_CONFIG[milestone]
    chain_cfg = CHAIN_CONFIG[milestone]

    # Get referee user for L2 lookup
    referee_user = await db["users"].find_one({"_id": __oid(referee_id)})
    referrer_l2_id = referee_user.get("referred_by_level2") if referee_user else None

    ref_oid = referral["_id"]

    # Credit L1 referrer
    await credit_coins(db, referrer_l1_id, cfg["referrer"], f"referral_{milestone}", ref_id=ref_oid, level=1)

    # Credit L2 referrer (chain)
    if referrer_l2_id:
        await credit_coins(db, referrer_l2_id, chain_cfg["referrer"], f"referral_chain_{milestone}", ref_id=ref_oid, level=2)

    # Credit referee
    await credit_coins(db, referee_id, cfg["referee"], f"referral_welcome_{milestone}", ref_id=ref_oid, level=0)

    # Update referral doc
    await db["referrals"].update_one(
        {"_id": ref_oid},
        {"$push": {"milestones_done": milestone},
         "$inc":  {"coins_given": cfg["referrer"]},
         "$set":  {"status": "rewarded", "rewarded_at": datetime.utcnow()}},
    )

    # Log milestone
    await db["referral_milestones"].insert_one({
        "referral_id":    str(ref_oid),
        "milestone_type": milestone,
        "referrer_coins": cfg["referrer"],
        "referee_coins":  cfg["referee"],
        "chain_coins":    chain_cfg["referrer"] if referrer_l2_id else 0,
        "status":         "rewarded",
        "rewarded_at":    datetime.utcnow(),
    })

    # Check bonus milestone for L1 referrer
    await check_bonus_milestone(db, referrer_l1_id)

    logger.info(f"[Referral] milestone={milestone} referee={referee_id} referrer={referrer_l1_id}")


async def check_bonus_milestone(db, referrer_id: str):
    """Check if referrer just hit a bonus count (5, 10, 25, 50)."""
    count = await db["referrals"].count_documents({
        "referrer_id": referrer_id,
        "status":      {"$ne": "invalid"},
    })
    bonus = BONUS_MILESTONES.get(count)
    if bonus:
        # Avoid double bonus
        already = await db["coin_transactions"].find_one({
            "user_id": referrer_id,
            "source":  f"bonus_milestone_{count}",
        })
        if not already:
            await credit_coins(db, referrer_id, bonus, f"bonus_milestone_{count}")
            logger.info(f"[Bonus] {referrer_id} hit {count} referrals → +{bonus} coins")


# ─── Referral stats (dashboard API) ──────────────────────────────────────────

async def get_referral_stats(db, user_id: str) -> dict:
    user = await db["users"].find_one({"_id": __oid(user_id)})

    # Auto-generate referral code for existing users who don't have one
    if not user.get("referral_code"):
        code = generate_referral_code(user["username"])
        while await db["users"].find_one({"referral_code": code}):
            code = generate_referral_code(user["username"])
        await db["users"].update_one(
            {"_id": __oid(user_id)},
            {"$set": {"referral_code": code}},
        )
        user["referral_code"] = code
        logger.info(f"[Referral] Auto-generated code {code} for existing user {user_id}")

    # Direct referrals
    direct_refs = await db["referrals"].find(
        {"referrer_id": user_id, "status": {"$ne": "invalid"}}
    ).sort("created_at", -1).to_list(length=100)

    # Indirect (chain) referrals
    indirect_count = await db["referrals"].count_documents({
        "referrer_id": {"$in": [r["referee_id"] for r in direct_refs]},
        "status":      {"$ne": "invalid"},
    })

    # Coins from referrals
    pipeline = [
        {"$match": {"user_id": user_id, "type": "earn", "source": {"$regex": "referral"}}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
    ]
    agg = await db["coin_transactions"].aggregate(pipeline).to_list(1)
    coins_earned = agg[0]["total"] if agg else 0

    pending_count = sum(1 for r in direct_refs if r["status"] == "pending")

    # Next bonus milestone
    total_count = len(direct_refs)
    next_bonus = None
    next_bonus_coins = None
    for threshold in sorted(BONUS_MILESTONES.keys()):
        if total_count < threshold:
            next_bonus = threshold
            next_bonus_coins = BONUS_MILESTONES[threshold]
            break

    # Recent referrals (last 10, masked)
    recent = []
    for r in direct_refs[:10]:
        referee = await db["users"].find_one({"_id": __oid(r["referee_id"])})
        recent.append({
            "masked_email":     mask_email(referee["email"]) if referee else "***",
            "status":           r["status"],
            "milestones_done":  r.get("milestones_done", []),
            "coins":            r.get("coins_given", 0),
            "date":             r["created_at"].strftime("%Y-%m-%d"),
            "expires_at":       r["expires_at"].strftime("%Y-%m-%d") if r.get("expires_at") else None,
        })

    wallet = await get_or_create_wallet(db, user_id)

    # Include onboarding inline so frontend doesn't need a separate request
    onboarding = await get_new_user_onboarding(db, user_id)

    return {
        "referral_code":    user["referral_code"],
        "referral_link":    f"{settings.FRONTEND_URL}/register?ref={user['referral_code']}",
        "total_referred":   total_count,
        "indirect_referred": indirect_count,
        "coins_earned":     coins_earned,
        "pending_count":    pending_count,
        "wallet_balance":   wallet["balance"],
        "next_bonus":       next_bonus,
        "next_bonus_coins": next_bonus_coins,
        "recent_referrals": recent,
        "onboarding":       onboarding,
    }


async def get_new_user_onboarding(db, user_id: str) -> dict | None:
    """
    Returns milestone progress for new users who registered via referral.
    Returns None if user was not referred.
    """
    user = await db["users"].find_one({"_id": __oid(user_id)})
    if not user or not user.get("referred_by_level1"):
        return None

    referral = await db["referrals"].find_one({"referee_id": user_id})
    if not referral:
        return None

    referrer = await db["users"].find_one({"_id": __oid(user["referred_by_level1"])})
    done = referral.get("milestones_done", [])

    steps = []
    total_possible = 0
    total_earned = 0
    for key, cfg in MILESTONE_CONFIG.items():
        completed = key in done
        steps.append({
            "milestone": key,
            "label":     key.replace("_", " ").title(),
            "coins":     cfg["referee"],
            "completed": completed,
        })
        total_possible += cfg["referee"]
        if completed:
            total_earned += cfg["referee"]

    progress_pct = int((total_earned / total_possible) * 100) if total_possible else 0

    return {
        "referrer_username": referrer["username"] if referrer else "Someone",
        "steps":             steps,
        "total_earned":      total_earned,
        "total_possible":    total_possible,
        "progress_pct":      progress_pct,
        "all_done":          len(done) == len(MILESTONE_CONFIG),
    }


# ─── Nudge scheduler ─────────────────────────────────────────────────────────

async def run_nudge_checker():
    """
    Runs every 24 hours.
    Sends in-app notifications to referrers whose referee hasn't completed milestones.
    """
    while True:
        try:
            db = get_db()
            now = datetime.utcnow()

            pending_referrals = await db["referrals"].find({"status": "pending"}).to_list(length=500)

            for ref in pending_referrals:
                days_old = (now - ref["created_at"]).days
                referrer_id = ref["referrer_id"]
                ref_id = ref["_id"]

                referee = await db["users"].find_one({"_id": __oid(ref["referee_id"])})
                if not referee:
                    continue
                masked = mask_email(referee["email"])
                done = ref.get("milestones_done", [])

                # Day 3 — hasn't done first_automation
                if days_old >= 3 and not ref.get("nudge_day3_sent") and "first_automation" not in done:
                    await _send_nudge(db, referrer_id, ref_id, "day3", masked)

                # Day 6 — hasn't done pro_purchase
                if days_old >= 6 and not ref.get("nudge_day6_sent") and "pro_purchase" not in done:
                    await _send_nudge(db, referrer_id, ref_id, "day6", masked)

                # Day 7 — expiry warning
                if days_old >= 7 and not ref.get("nudge_day7_sent"):
                    await _send_nudge(db, referrer_id, ref_id, "day7", masked)
                    # Expire the referral
                    await db["referrals"].update_one({"_id": ref_id}, {"$set": {"status": "expired"}})

        except Exception as e:
            logger.error(f"[NudgeChecker] error: {e}")

        await asyncio.sleep(60 * 60 * 24)  # 24 hours


async def _send_nudge(db, referrer_id: str, ref_id, nudge_type: str, masked_email: str):
    MESSAGES = {
        "day3": {
            "title":   "Reminder: Your friend hasn't tried automation yet!",
            "body":    f"⏰ {masked_email} hasn't run their first automation. Remind them — you'll earn +75 coins when they do!",
            "action":  "remind_automation",
        },
        "day6": {
            "title":   "Your friend is still on the Free plan!",
            "body":    f"💎 {masked_email} is still on Free. Share why Pro is worth it — you earn +200 coins if they upgrade!",
            "action":  "remind_pro",
        },
        "day7": {
            "title":   "⚠️ Referral expiring in 24 hours!",
            "body":    f"Your referral for {masked_email} expires soon. They need to take action now!",
            "action":  "expiry_warning",
        },
    }
    msg = MESSAGES.get(nudge_type, {})
    await db["notifications"].insert_one({
        "user_id":    referrer_id,
        "type":       "referral_nudge",
        "nudge_type": nudge_type,
        "title":      msg.get("title", ""),
        "body":       msg.get("body", ""),
        "action":     msg.get("action", ""),
        "ref_id":     str(ref_id),
        "read":       False,
        "created_at": datetime.utcnow(),
    })
    field = f"nudge_{nudge_type}_sent"
    await db["referrals"].update_one({"_id": ref_id}, {"$set": {field: True}})
    logger.info(f"[Nudge] {nudge_type} sent to {referrer_id} for {masked_email}")


# ─── Notifications (for nudges + milestones) ──────────────────────────────────

async def get_notifications(db, user_id: str, limit: int = 20) -> list:
    notifs = await db["notifications"].find(
        {"user_id": user_id}
    ).sort("created_at", -1).to_list(limit)
    return [
        {
            "id":         str(n["_id"]),
            "type":       n.get("type"),
            "title":      n.get("title"),
            "body":       n.get("body"),
            "action":     n.get("action"),
            "read":       n.get("read", False),
            "created_at": n["created_at"].isoformat(),
        }
        for n in notifs
    ]


async def mark_notifications_read(db, user_id: str):
    await db["notifications"].update_many(
        {"user_id": user_id, "read": False},
        {"$set": {"read": True}},
    )


# ─── Helper ───────────────────────────────────────────────────────────────────

def __oid(id_str):
    from bson import ObjectId
    try:
        return ObjectId(id_str)
    except Exception:
        return id_str
