import httpx
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

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
        resp = await client.post(url, data=payload)

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
        resp = await client.post(url, data=payload)

    data = resp.json()
    logger.info(f"Publish response: {data}")

    if resp.status_code != 200 or "id" not in data:
        error_msg = data.get("error", {}).get("message", str(data))
        raise Exception(f"Publishing failed: {error_msg}")

    return data


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
