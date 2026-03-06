from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class SchedulePostRequest(BaseModel):
    image_url: str
    caption: str
    scheduled_at: datetime  # UTC datetime when to publish


class ScheduledPostPublic(BaseModel):
    id: str
    image_url: str
    caption: str
    scheduled_at: datetime
    status: str             # pending | published | failed
    instagram_username: str
    created_at: datetime
    published_at: Optional[datetime] = None
    error: Optional[str] = None
