# app/routers/plans.py
"""
Plans router — returns plan config + current user's plan to the frontend.
GET /plans/        → all plans (public, for pricing page)
GET /plans/me      → current user's plan + usage summary
PATCH /plans/me    → upgrade/downgrade (admin sets plan; real billing handled externally)
"""
from fastapi import APIRouter, Depends, HTTPException
from app.dependencies import get_current_user
from app.database import get_db
from app.plans import PLAN_LIMITS, PLAN_PRICING, get_plan_limits
from bson import ObjectId
from datetime import datetime

router = APIRouter(prefix="/plans", tags=["Plans"])


@router.get("/", response_model=None)
async def list_plans():
    """Return all plans with limits and pricing — used by the pricing UI."""
    from fastapi.responses import JSONResponse
    result = []
    for plan_key in ["free", "pro", "enterprise"]:
        pricing = PLAN_PRICING[plan_key]
        limits  = PLAN_LIMITS[plan_key]
        result.append({
            "id":       plan_key,
            "label":    pricing["label"],
            "price":    pricing["price"],
            "currency": pricing["currency"],
            "interval": pricing["interval"],
            "limits":   limits,
        })
    return JSONResponse(
        content={"plans": result},
        headers={"Cache-Control": "no-store"},
    )


@router.get("/me")
async def my_plan(current_user: dict = Depends(get_current_user)):
    """Return the current user's plan + live usage counters."""
    db      = get_db()
    user_id = str(current_user["_id"])
    plan    = current_user.get("plan", "free")
    limits  = get_plan_limits(plan)

    # Live usage
    account_count = await db["instagram_accounts"].count_documents({"user_id": user_id})

    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    dm_today = await db["automation_logs"].count_documents({
        "user_id": user_id,
        "dm_sent": True,
        "timestamp": {"$gte": today_start},
    })

    automation_count = await db["automation_settings"].count_documents({"user_id": user_id})

    scheduled_count = await db["scheduled_posts"].count_documents({
        "user_id": user_id,
        "status":  "pending",
    })

    pricing = PLAN_PRICING[plan]

    return {
        "plan":    plan,
        "label":   pricing["label"],
        "price":   pricing["price"],
        "limits":  limits,
        "usage": {
            "accounts":       account_count,
            "dm_today":       dm_today,
            "automations":    automation_count,
            "scheduled_posts": scheduled_count,
        },
    }


@router.patch("/me")
async def update_my_plan(
    body: dict,
    current_user: dict = Depends(get_current_user),
):
    """
    Admin-only: set a user's plan directly.
    Body: { "plan": "pro" }
    """
    if current_user.get("role") not in ("admin", "superadmin"):
        raise HTTPException(status_code=403, detail="Admin only.")

    new_plan = body.get("plan")
    if new_plan not in PLAN_LIMITS:
        raise HTTPException(status_code=400, detail=f"Invalid plan: {new_plan}")

    db = get_db()
    target_user_id = body.get("user_id", str(current_user["_id"]))

    await db["users"].update_one(
        {"_id": ObjectId(target_user_id)},
        {"$set": {"plan": new_plan, "updated_at": datetime.utcnow()}},
    )

    # Invalidate cache
    try:
        from app.dependencies import invalidate_user_cache
        await invalidate_user_cache(target_user_id)
    except Exception:
        pass

    return {"ok": True, "plan": new_plan}
