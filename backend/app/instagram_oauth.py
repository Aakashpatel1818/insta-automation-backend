import httpx
import logging
from datetime import datetime, timedelta

from app.config import settings

logger = logging.getLogger(__name__)

# Instagram Business Login - correct endpoints
AUTH_BASE  = "https://www.instagram.com/oauth/authorize"
TOKEN_URL  = "https://api.instagram.com/oauth/access_token"
GRAPH_BASE = "https://graph.instagram.com"


def build_auth_url(state: str) -> str:
    """Build Instagram Business OAuth URL."""
    params = (
        f"?force_reauth=true"
        f"&client_id={settings.INSTAGRAM_APP_ID}"
        f"&redirect_uri={settings.INSTAGRAM_REDIRECT_URI}"
        f"&response_type=code"
        f"&scope=instagram_business_basic,instagram_business_manage_messages,instagram_business_manage_comments,instagram_business_content_publish,instagram_business_manage_insights,instagram_manage_insights"
        f"&state={state}"
    )
    return AUTH_BASE + params


async def exchange_code_for_short_token(code: str) -> dict:
    """Exchange auth code for access token."""
    clean_code = code.split("#")[0]

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(TOKEN_URL, data={
            "client_id":     settings.INSTAGRAM_APP_ID,
            "client_secret": settings.INSTAGRAM_APP_SECRET,
            "grant_type":    "authorization_code",
            "redirect_uri":  settings.INSTAGRAM_REDIRECT_URI,
            "code":          clean_code,
        })

    data = resp.json()
    logger.debug(f"Short token response: {data}")

    if "access_token" not in data:
        error = data.get("error_message") or data.get("error", {}).get("message", str(data))
        raise Exception(f"Token exchange failed: {error}")

    return data  # {access_token, user_id}


async def exchange_for_long_lived_token(short_token: str) -> dict:
    """Upgrade short-lived token to long-lived (60 days)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{GRAPH_BASE}/access_token", params={
            "grant_type":    "ig_exchange_token",
            "client_secret": settings.INSTAGRAM_APP_SECRET,
            "access_token":  short_token,
        })

    data = resp.json()
    logger.debug(f"Long token response: {data}")

    if "access_token" not in data:
        error = data.get("error", {}).get("message", str(data))
        raise Exception(f"Long token exchange failed: {error}")

    return data  # {access_token, token_type, expires_in}


async def fetch_ig_profile(access_token: str) -> dict:
    """Fetch IG user id and username."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{GRAPH_BASE}/me", params={
            "fields":       "id,username,name",
            "access_token": access_token,
        })

    data = resp.json()
    logger.debug(f"IG profile response: {data}")

    if "id" not in data:
        error = data.get("error", {}).get("message", str(data))
        raise Exception(f"Profile fetch failed: {error}")

    return data  # {id, username, name}
