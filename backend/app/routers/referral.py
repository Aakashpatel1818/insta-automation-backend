from fastapi import APIRouter, Depends, HTTPException
from typing import List
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel

from app.dependencies import get_current_user
from app.database import get_db
from app.schemas.referral import (
    ReferralStatsOut, OnboardingOut, NotificationOut
)
from app.services.referral_service import (
    get_referral_stats, get_new_user_onboarding,
    get_notifications, mark_notifications_read,
    trigger_milestone, get_or_create_wallet,
)
from bson import ObjectId

router = APIRouter(prefix="/referral", tags=["Referral"])

def __oid(id_str):
    try: return ObjectId(id_str)
    except: return id_str


# ── Dashboard stats ───────────────────────────────────────
@router.get("/stats", response_model=ReferralStatsOut)
async def referral_stats(current_user: dict = Depends(get_current_user)):
    db = get_db()
    return await get_referral_stats(db, str(current_user["_id"]))


# ── New user onboarding milestone progress ────────────────
@router.get("/onboarding", response_model=OnboardingOut)
async def referral_onboarding(current_user: dict = Depends(get_current_user)):
    db = get_db()
    data = await get_new_user_onboarding(db, str(current_user["_id"]))
    if not data:
        raise HTTPException(status_code=404, detail="No referral onboarding for this user")
    return data


# ── Notifications list ────────────────────────────────────
@router.get("/notifications", response_model=List[NotificationOut])
async def referral_notifications(current_user: dict = Depends(get_current_user)):
    db = get_db()
    return await get_notifications(db, str(current_user["_id"]))


# ── Mark all notifications read ───────────────────────────
@router.post("/notifications/read")
async def read_notifications(current_user: dict = Depends(get_current_user)):
    db = get_db()
    await mark_notifications_read(db, str(current_user["_id"]))
    return {"ok": True}


# ── Validate referral code ───────────────────────────────
@router.get("/validate/{code}")
async def validate_referral_code(code: str):
    db = get_db()
    referrer = await db["users"].find_one(
        {"referral_code": code.upper()},
        {"username": 1}
    )
    if not referrer:
        raise HTTPException(status_code=404, detail="Invalid referral code")
    return {"valid": True, "username": referrer["username"]}


# ── Card data for shareable image ────────────────────────
@router.get("/card")
async def referral_card(current_user: dict = Depends(get_current_user)):
    db = get_db()
    stats = await get_referral_stats(db, str(current_user["_id"]))
    return {
        "referral_code":  stats["referral_code"],
        "referral_link":  stats["referral_link"],
        "coins_earned":   stats["coins_earned"],
        "total_referred": stats["total_referred"],
        "username":       current_user["username"],
    }


# ── Wallet balance ────────────────────────────────────────
@router.get("/wallet")
async def wallet_balance(current_user: dict = Depends(get_current_user)):
    db = get_db()
    wallet = await get_or_create_wallet(db, str(current_user["_id"]))
    return {
        "balance":      wallet["balance"],
        "total_earned": wallet["total_earned"],
        "total_spent":  wallet["total_spent"],
    }


# ── ✅ SPEND COINS — saves deduction to DB so it survives refresh ─────────────
class SpendRequest(BaseModel):
    item_id:    str
    item_title: str
    coins:      int

@router.post("/spend")
async def spend_coins(
    body: SpendRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Deduct coins from wallet and record the purchase in the database.
    This is the ONLY correct way to spend coins — frontend must call this.
    The balance returned here is the source of truth; frontend should sync to it.
    """
    db = get_db()
    user_id = str(current_user["_id"])

    if body.coins <= 0:
        raise HTTPException(status_code=400, detail="Coins must be positive")

    # ── Atomic check-and-deduct using MongoDB findOneAndUpdate ────────────────
    # Using $inc with a check prevents race conditions (double-spend).
    result = await db["coin_wallets"].find_one_and_update(
        {
            "user_id": user_id,
            "balance": {"$gte": body.coins},   # only succeeds if enough balance
        },
        {
            "$inc": {
                "balance":     -body.coins,
                "total_spent":  body.coins,
            }
        },
        return_document=True,  # return updated doc
    )

    if result is None:
        # Either wallet doesn't exist or insufficient balance
        wallet = await get_or_create_wallet(db, user_id)
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient coins. You have {wallet['balance']} but need {body.coins}."
        )

    new_balance = result["balance"]

    # ── Record the spend transaction ──────────────────────────────────────────
    await db["coin_transactions"].insert_one({
        "user_id":    user_id,
        "type":       "spend",
        "source":     f"shop_{body.item_id}",
        "amount":     body.coins,
        "item_id":    body.item_id,
        "item_title": body.item_title,
        "ref_id":     None,
        "level":      0,
        "created_at": datetime.utcnow(),
    })

    # ── Record purchase in coin_purchases for history ──────────────────────────
    await db["coin_purchases"].insert_one({
        "user_id":      user_id,
        "item_id":      body.item_id,
        "item_title":   body.item_title,
        "coins_spent":  body.coins,
        "purchased_at": datetime.utcnow(),
    })

    # ── Invalidate user cache so wallet reflects immediately ──────────────────
    try:
        from app.dependencies import invalidate_user_cache
        await invalidate_user_cache(user_id)
    except Exception:
        pass

    return {
        "ok":          True,
        "new_balance": new_balance,
        "spent":       body.coins,
        "item_id":     body.item_id,
    }


# ── Purchase history from DB ──────────────────────────────
@router.get("/purchases")
async def purchase_history(current_user: dict = Depends(get_current_user)):
    """Returns coin shop purchase history stored in the database."""
    db = get_db()
    purchases = await db["coin_purchases"].find(
        {"user_id": str(current_user["_id"])}
    ).sort("purchased_at", -1).to_list(100)
    return [
        {
            "item_id":      p["item_id"],
            "item_title":   p["item_title"],
            "coins_spent":  p["coins_spent"],
            "purchased_at": p["purchased_at"].isoformat(),
        }
        for p in purchases
    ]


# ── Leaderboard ─────────────────────────────────────────
@router.get("/leaderboard")
async def referral_leaderboard(current_user: dict = Depends(get_current_user)):
    db = get_db()
    pipeline = [
        {"$match": {"type": "earn", "source": {"$regex": "^referral|^bonus"}}},
        {"$group": {"_id": "$user_id", "coins": {"$sum": "$amount"}}},
        {"$sort": {"coins": -1}},
        {"$limit": 10},
    ]
    rows = await db["coin_transactions"].aggregate(pipeline).to_list(10)
    my_id = str(current_user["_id"])

    result = []
    for rank, row in enumerate(rows, 1):
        uid = row["_id"]
        user = await db["users"].find_one({"_id": __oid(uid)}, {"username": 1})
        ref_count = await db["referrals"].count_documents({"referrer_id": uid})
        result.append({
            "rank":      rank,
            "username":  user["username"] if user else "Unknown",
            "coins":     row["coins"],
            "referrals": ref_count,
            "is_me":     uid == my_id,
        })
    return result


# ── Daily share bonus ───────────────────────────────────────────────────────
@router.post("/daily-share")
async def daily_share_bonus(current_user: dict = Depends(get_current_user)):
    """
    Awards +5 coins once per calendar day (resets at midnight UTC).
    Returns ok=True + new_balance on success, or already_claimed=True if already done today.
    """
    db = get_db()
    user_id = str(current_user["_id"])

    # Today's date in IST (UTC+5:30) — resets at 12:00 AM IST
    IST = timezone(timedelta(hours=5, minutes=30))
    today = datetime.now(IST).strftime("%Y-%m-%d")

    # Check if already claimed today
    existing = await db["daily_share_claims"].find_one({"user_id": user_id, "date": today})
    if existing:
        return {"ok": False, "already_claimed": True}

    # Award +5 coins
    BONUS = 5
    wallet = await get_or_create_wallet(db, user_id)
    await db["coin_wallets"].update_one(
        {"user_id": user_id},
        {"$inc": {"balance": BONUS, "total_earned": BONUS}}
    )
    await db["coin_transactions"].insert_one({
        "user_id":    user_id,
        "type":       "earn",
        "source":     "daily_share_bonus",
        "amount":     BONUS,
        "ref_id":     None,
        "level":      0,
        "created_at": datetime.utcnow(),
    })

    # Record the claim so it can't be done again today
    await db["daily_share_claims"].insert_one({"user_id": user_id, "date": today})

    updated_wallet = await get_or_create_wallet(db, user_id)
    return {"ok": True, "already_claimed": False, "new_balance": updated_wallet["balance"]}


# ── Check daily share status ─────────────────────────────────────────────────
@router.get("/daily-share/status")
async def daily_share_status(current_user: dict = Depends(get_current_user)):
    """Returns whether the user has already claimed their daily share bonus today."""
    db = get_db()
    user_id = str(current_user["_id"])
    IST = timezone(timedelta(hours=5, minutes=30))
    today = datetime.now(IST).strftime("%Y-%m-%d")
    existing = await db["daily_share_claims"].find_one({"user_id": user_id, "date": today})
    return {"already_claimed": bool(existing)}


# ── Recent coin transactions ──────────────────────────────
@router.get("/transactions")
async def coin_transactions(current_user: dict = Depends(get_current_user)):
    db = get_db()
    txns = await db["coin_transactions"].find(
        {"user_id": str(current_user["_id"])}
    ).sort("created_at", -1).to_list(50)
    return [
        {
            "type":       t["type"],
            "source":     t["source"],
            "amount":     t["amount"],
            "level":      t.get("level", 1),
            "created_at": t["created_at"].isoformat(),
        }
        for t in txns
    ]
