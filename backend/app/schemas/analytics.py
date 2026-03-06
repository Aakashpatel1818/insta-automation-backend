from pydantic import BaseModel
from typing import Optional, List


class AccountInsights(BaseModel):
    username: str
    followers_count: int
    media_count: int
    biography: Optional[str] = ""
    website: Optional[str] = ""
    reach: int = 0
    impressions: int = 0
    profile_views: int = 0
    follower_count: int = 0
    period: str = "month"


class CommentReply(BaseModel):
    text: str
    username: str
    timestamp: str


class Comment(BaseModel):
    id: str
    text: str
    username: str
    timestamp: str
    like_count: int = 0
    replies: List[CommentReply] = []


class PostInsights(BaseModel):
    # ── Post info ──────────────────────────────
    post_id: str
    caption: Optional[str] = ""
    media_type: Optional[str] = ""
    media_url: Optional[str] = ""
    thumbnail_url: Optional[str] = ""
    permalink: Optional[str] = ""
    username: Optional[str] = ""
    timestamp: Optional[str] = ""
    period: Optional[str] = "lifetime"
    # ── Engagement ─────────────────────────────
    like_count: int = 0
    comments_count: int = 0
    impressions: int = 0
    reach: int = 0
    shares: int = 0
    saves: int = 0
    total_interactions: int = 0
    # ── Reels only ─────────────────────────────
    plays: int = 0
    avg_watch_time: int = 0
    total_video_view_time: int = 0
    # ── Comments list ──────────────────────────
    comments: List[Comment] = []


class PostSummary(BaseModel):
    post_id: str
    caption: Optional[str] = ""
    media_type: Optional[str] = ""
    media_url: Optional[str] = ""
    timestamp: Optional[str] = ""
    like_count: int = 0
    comments_count: int = 0
