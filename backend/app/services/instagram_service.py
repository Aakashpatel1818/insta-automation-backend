import httpx
import logging
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

# Instagram Business Login tokens (IGAAp...) use graph.instagram.com
GRAPH_API_VERSION = "v19.0"
BASE_URL = f"https://graph.instagram.com/{GRAPH_API_VERSION}"


async def create_media_container(
    ig_user_id: str,
    access_token: str,
    image_url: str,
    caption: str,
) -> str:
    url = f"{BASE_URL}/{ig_user_id}/media"
    payload = {
        "image_url":    image_url,
        "caption":      caption,
        "access_token": access_token,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, params=payload)

    data = resp.json()
    logger.info(f"Media container response: {data}")

    if resp.status_code != 200 or "id" not in data:
        error_msg = data.get("error", {}).get("message", str(data))
        raise Exception(f"Media container creation failed: {error_msg}")

    return data["id"]


async def publish_media_container(
    ig_user_id: str,
    access_token: str,
    creation_id: str,
) -> dict:
    url = f"{BASE_URL}/{ig_user_id}/media_publish"
    payload = {
        "creation_id":  creation_id,
        "access_token": access_token,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, params=payload)

    data = resp.json()
    logger.info(f"Publish response: {data}")

    if resp.status_code != 200 or "id" not in data:
        error_msg = data.get("error", {}).get("message", str(data))
        raise Exception(f"Publishing failed: {error_msg}")

    return data


async def wait_for_container_ready(
    ig_user_id: str,
    creation_id: str,
    access_token: str,
    max_wait: int = 30,
) -> None:
    """Poll container status until FINISHED before publishing."""
    url = f"https://graph.instagram.com/{GRAPH_API_VERSION}/{creation_id}"
    params = {
        "fields": "status_code,status",
        "access_token": access_token,
    }
    for attempt in range(max_wait // 3):
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
        data = resp.json()
        status_code = data.get("status_code", "")
        logger.info(f"Container {creation_id} status: {status_code} (attempt {attempt+1})")
        if status_code == "FINISHED":
            return
        if status_code in ("ERROR", "EXPIRED"):
            raise Exception(f"Media container failed with status: {status_code} — {data.get('status', '')}")
        await asyncio.sleep(3)
    raise Exception("Media container did not finish processing in time (30s timeout)")


async def create_instagram_post(
    ig_user_id: str,
    access_token: str,
    image_url: str,
    caption: str,
) -> dict:
    logger.info(f"Creating post for IG user {ig_user_id}")

    creation_id = await create_media_container(
        ig_user_id=ig_user_id,
        access_token=access_token,
        image_url=image_url,
        caption=caption,
    )
    logger.info(f"Media container created: {creation_id}")

    # BUG FIX 3: Instagram requires waiting for media container to finish processing
    # before publishing. Poll status until FINISHED (up to 30 seconds).
    await wait_for_container_ready(ig_user_id, creation_id, access_token)

    publish_data = await publish_media_container(
        ig_user_id=ig_user_id,
        access_token=access_token,
        creation_id=creation_id,
    )
    logger.info(f"Post published: {publish_data['id']}")

    return {
        "creation_id":  creation_id,
        "post_id":      publish_data["id"],
        "published_at": datetime.utcnow(),
    }
