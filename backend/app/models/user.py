from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field
from bson import ObjectId


class UserModel(BaseModel):
    """Internal DB model — what gets stored in MongoDB."""
    id: Optional[str] = Field(default=None, alias="_id")
    username: str
    email: str
    hashed_password: str
    is_active: bool = True
    is_banned: bool = False
    role: str = "user"           # user | admin | superadmin
    plan: str = "free"           # free | pro | enterprise
    coins: int = 0
    email_verified: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
