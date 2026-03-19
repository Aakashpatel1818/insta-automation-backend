# routers/dm.py
import logging
import httpx
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from bson import ObjectId
from app.dependencies import get_current_user
from app.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dm", tags=["DM"])

GRAPH_BASE = "https://graph.instagram.com/v19.0"


async def _get_account(db, user_id: str, account_id: str):
    try:
        account = await db["instagram_accounts"].find_one({
            "_id": ObjectId(account_id), "user_id": user_id
        })
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid account ID")
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


def _fmt_ts(ts):
    if ts is None:
        return ""
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


# ── List conversations ─────────────────────────────────────
@router.get("/conversations")
async def list_conversations(
    account_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    """Return one entry per unique sender — most recent first."""
    db = get_db()
    await _get_account(db, str(current_user["_id"]), account_id)

    pipeline = [
        {"$match": {"account_id": account_id}},
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id":            "$sender_id",
            "username":       {"$first": "$username"},
            "last_message":   {"$first": "$text"},
            "last_timestamp": {"$first": "$timestamp"},
            "last_direction": {"$first": "$direction"},
            "unread_count":   {"$sum": {"$cond": [
                {"$and": [
                    {"$eq": ["$read", False]},
                    {"$eq": ["$direction", "in"]},
                ]}, 1, 0
            ]}},
        }},
        {"$sort": {"last_timestamp": -1}},
        {"$limit": 100},
    ]
    convos = await db["dm_messages"].aggregate(pipeline).to_list(100)

    return {
        "conversations": [
            {
                "sender_id":      c["_id"],
                "username":       c.get("username") or c["_id"],
                "last_message":   c.get("last_message", ""),
                "last_timestamp": _fmt_ts(c.get("last_timestamp")),
                "last_direction": c.get("last_direction", "in"),
                "unread_count":   c.get("unread_count", 0),
            }
            for c in convos
        ]
    }


# ── Get messages in a conversation ────────────────────────
@router.get("/conversations/{sender_id}/messages")
async def get_messages(
    sender_id: str,
    account_id: str = Query(...),
    limit: int = Query(default=100, le=200),
    current_user: dict = Depends(get_current_user),
):
    """Return all messages between the account and this sender, oldest first."""
    db = get_db()
    await _get_account(db, str(current_user["_id"]), account_id)

    msgs = await db["dm_messages"].find(
        {"account_id": account_id, "sender_id": sender_id}
    ).sort("timestamp", 1).limit(limit).to_list(limit)

    # Mark incoming messages as read
    await db["dm_messages"].update_many(
        {"account_id": account_id, "sender_id": sender_id,
         "direction": "in", "read": False},
        {"$set": {"read": True}},
    )

    return {
        "messages": [
            {
                "id":        str(m["_id"]),
                "msg_id":    m.get("msg_id", ""),
                "sender_id": m["sender_id"],
                "username":  m.get("username") or sender_id,
                "text":      m.get("text", ""),
                "direction": m.get("direction", "in"),
                "timestamp": _fmt_ts(m.get("timestamp")),
                "read":      m.get("read", True),
            }
            for m in msgs
        ]
    }


# ── Send a manual DM reply ─────────────────────────────────
@router.post("/conversations/{sender_id}/reply")
async def reply_dm(
    sender_id: str,
    account_id: str = Query(...),
    message: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    """Manually send a DM to a conversation. Saves the outgoing message to DB."""
    db = get_db()
    account = await _get_account(db, str(current_user["_id"]), account_id)

    ig_user_id   = account["instagram_user_id"]
    access_token = account["access_token"]

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(
            f"{GRAPH_BASE}/{ig_user_id}/messages",
            json={"recipient": {"id": sender_id}, "message": {"text": message}},
            params={"access_token": access_token},
        )

    data = resp.json()
    if "error" in data:
        err = data["error"]
        raise HTTPException(status_code=502, detail=err.get("message", "Instagram API error"))

    now = datetime.utcnow()
    await db["dm_messages"].insert_one({
        "account_id": account_id,
        "sender_id":  sender_id,
        "username":   sender_id,
        "text":       message,
        "direction":  "out",
        "msg_id":     data.get("message_id", ""),
        "timestamp":  now,
        "read":       True,
    })

    logger.info(f"[DM] Manual reply sent to {sender_id} from account {account_id}")
    return {
        "status":     "sent",
        "message_id": data.get("message_id", ""),
        "timestamp":  now.isoformat(),
    }


# ── Mark conversation as read ──────────────────────────────
@router.post("/conversations/{sender_id}/read")
async def mark_read(
    sender_id: str,
    account_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    await _get_account(db, str(current_user["_id"]), account_id)
    result = await db["dm_messages"].update_many(
        {"account_id": account_id, "sender_id": sender_id,
         "direction": "in", "read": False},
        {"$set": {"read": True}},
    )
    return {"status": "ok", "updated": result.modified_count}


# ── Total unread count ─────────────────────────────────────
@router.get("/unread-count")
async def unread_count(
    account_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    db = get_db()
    await _get_account(db, str(current_user["_id"]), account_id)
    count = await db["dm_messages"].count_documents({
        "account_id": account_id,
        "direction":  "in",
        "read":       False,
    })
    return {"unread": count}
