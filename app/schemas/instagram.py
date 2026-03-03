from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class InstagramConnectURL(BaseModel):
    """Returned to the frontend so user can click Connect."""
    auth_url: str


class InstagramAccountPublic(BaseModel):
    """Safe public view of a connected IG account."""
    id: str
    instagram_user_id: str
    username: str
    token_expires_at: Optional[datetime]
    connected_at: datetime
