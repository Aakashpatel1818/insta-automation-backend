from pydantic import BaseModel, HttpUrl
from typing import Optional
from datetime import datetime


class PostRequest(BaseModel):
    image_url: str      # must be publicly accessible URL
    caption: str


class PostResponse(BaseModel):
    status: str
    post_id: str
    creation_id: str
    instagram_username: str
    published_at: datetime
