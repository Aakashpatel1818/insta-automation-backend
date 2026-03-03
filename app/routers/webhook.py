from fastapi import APIRouter, Request, Query, HTTPException
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhook", tags=["Webhook"])


@router.get("/")
async def verify_webhook(
    hub_mode: str = Query(alias="hub.mode", default=None),
    hub_verify_token: str = Query(alias="hub.verify_token", default=None),
    hub_challenge: str = Query(alias="hub.challenge", default=None),
):
    """Meta webhook verification endpoint."""
    VERIFY_TOKEN = "myverifytoken123"

    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        logger.info("Webhook verified successfully.")
        return int(hub_challenge)

    raise HTTPException(status_code=403, detail="Verification failed")


@router.post("/")
async def receive_webhook(request: Request):
    """Receive webhook events from Meta."""
    body = await request.json()
    logger.info(f"Webhook received: {body}")
    return {"status": "ok"}
