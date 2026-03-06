from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class InstagramConnectURL(BaseModel):
    auth_url: str


class InstagramAccountPublic(BaseModel):
    id: str
    instagram_user_id: str
    username: str
    token_expires_at: Optional[datetime]
    connected_at: datetime
    is_active: bool = False  # currently selected account


class InstagramAccountList(BaseModel):
    accounts: List[InstagramAccountPublic]
    total: int
