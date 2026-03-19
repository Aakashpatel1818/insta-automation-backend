"""
socket_manager.py
Central Socket.io setup for InstaAuto.
Handles real-time coin/referral events per user room.
"""
import socketio
import logging
from app.security import decode_token

logger = logging.getLogger(__name__)

# Async Socket.io server — CORS allows frontend dev server
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)


# ── Connection lifecycle ──────────────────────────────────

@sio.event
async def connect(sid, environ, auth):
    """
    Client connects with { token: "JWT..." } in auth.
    We decode it and put the socket in a personal room: user:<id>
    """
    try:
        token = (auth or {}).get("token", "")
        if not token:
            logger.warning(f"[Socket] No token — rejecting {sid}")
            return False  # reject connection

        user_id = decode_token(token)
        if not user_id:
            return False

        await sio.enter_room(sid, f"user:{user_id}")
        logger.info(f"[Socket] {sid} connected → room user:{user_id}")
    except Exception as e:
        logger.error(f"[Socket] connect error: {e}")
        return False


@sio.event
async def disconnect(sid):
    logger.info(f"[Socket] {sid} disconnected")


# ── Emit helpers (called from referral_service) ──────────

async def emit_coin_update(user_id: str, amount: int, source: str, balance: int):
    """Emit coin credit event to a specific user's room."""
    await sio.emit(
        "coins:updated",
        {
            "amount":  amount,
            "source":  source,
            "balance": balance,
        },
        room=f"user:{user_id}",
    )
    logger.info(f"[Socket] coins:updated → user:{user_id} +{amount}")


async def emit_referral_milestone(user_id: str, milestone: str, coins: int, balance: int, masked_email: str):
    """Emit referral milestone reward event."""
    LABELS = {
        "register":         "Your friend joined!",
        "first_automation": "Your friend ran their first automation!",
        "pro_purchase":     "Your friend upgraded to Pro!",
    }
    await sio.emit(
        "referral:milestone",
        {
            "milestone":    milestone,
            "coins":        coins,
            "balance":      balance,
            "user":         masked_email,
            "message":      LABELS.get(milestone, "Referral milestone reached!"),
        },
        room=f"user:{user_id}",
    )


async def emit_bonus_milestone(user_id: str, count: int, coins: int, balance: int):
    """Emit bonus milestone event."""
    await sio.emit(
        "referral:bonus",
        {
            "count":   count,
            "coins":   coins,
            "balance": balance,
            "message": f"🎉 {count} referrals! Bonus +{coins} coins!",
        },
        room=f"user:{user_id}",
    )


async def emit_inbox_event(user_id: str, item_type: str, data: dict):
    """
    Emit a real-time inbox event to the user's frontend.
    item_type: 'comment' | 'dm' | 'mention'
    data: the item payload
    """
    await sio.emit(
        "inbox:new_item",
        {
            "type": item_type,
            "data": data,
        },
        room=f"user:{user_id}",
    )
    logger.info(f"[Socket] inbox:new_item ({item_type}) → user:{user_id}")


async def emit_automation_fired(user_id: str, data: dict):
    """
    Emit a real-time automation fired event.
    data: { post_id, comment_id, comment_text, commenter_id, rule_matched, actions }
    """
    await sio.emit(
        "automation:fired",
        data,
        room=f"user:{user_id}",
    )
    logger.info(f"[Socket] automation:fired → user:{user_id}")
