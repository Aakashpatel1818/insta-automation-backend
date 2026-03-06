from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class InstagramAccountModel(BaseModel):
    """Stored in MongoDB collection: instagram_accounts"""
    id: Optional[str] = Field(default=None, alias="_id")
    user_id: str                        # references users._id
    instagram_user_id: str              # IG numeric user ID
    username: str                       # IG @handle
    access_token: str                   # long-lived token
    token_expires_at: Optional[datetime] = None
    connected_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
