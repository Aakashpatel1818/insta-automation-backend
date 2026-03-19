from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


# ── Automation Settings ───────────────────────────────────
class AutomationSettings(BaseModel):
    post_id: str
    account_id: str
    auto_comment_reply: bool = False
    auto_dm: bool = False
    delay_enabled: bool = True
    is_active: bool = True
    cooldown_hours: float = 24.0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AutomationSettingsRequest(BaseModel):
    post_id: str
    account_id: str
    auto_comment_reply: bool = False
    auto_dm: bool = False
    delay_enabled: bool = True
    is_active: bool = True
    cooldown_hours: float = 24.0


# ── Keyword Rules ─────────────────────────────────────────
class KeywordRule(BaseModel):
    id: Optional[str] = None
    post_id: str
    account_id: str
    trigger_words: List[str]
    response: str
    reply_comment: bool = True
    send_dm: bool = False
    is_active: bool = True
    created_at: Optional[datetime] = None


class KeywordRuleRequest(BaseModel):
    post_id: str
    account_id: str
    automation_id: Optional[str] = None
    trigger_words: List[str]
    response: str
    responses: List[str] = []             # multiple reply variants — engine picks random
    reply_comment: bool = True
    send_dm: bool = False
    is_active: bool = True
    # Opening Message
    opening_message: str = ""
    opening_messages: List[str] = []      # multiple opening message variants
    opening_message_btn: str = ""
    opening_message_btn_url: str = ""
    # Follow DM
    follow_dm_message: str = ""
    # DM Actions
    dm_actions: list = []
    # ── Email Collection ──────────────────────────────────
    collect_email: bool = False          # toggle — if True, ask for email after DM
    email_prompt: str = ""               # custom text sent asking for email


# ── Collected User (email capture) ───────────────────────
class CollectedUser(BaseModel):
    ig_user_id: str                      # Instagram scoped user ID (commenter_id)
    account_id: str                      # which IG account triggered this
    automation_id: Optional[str] = None
    username: Optional[str] = None       # IG username if available
    email: Optional[str] = None
    email_captured_at: Optional[datetime] = None
    source: str = "comment"              # comment | story | dm
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# ── DM / Action Log ───────────────────────────────────────
class AutomationLog(BaseModel):
    id: Optional[str] = None
    user_id: str
    account_id: str
    post_id: str
    comment_id: str
    commenter_id: str
    comment_text: str
    keyword_triggered: str
    action_taken: str
    reply_sent: bool = False
    dm_sent: bool = False
    success: bool = True
    error: Optional[str] = None
    timestamp: Optional[datetime] = None


# ── Analytics Counter ─────────────────────────────────────
class AutomationAnalytics(BaseModel):
    post_id: str
    account_id: str
    trigger_count: int = 0
    reply_sent_count: int = 0
    dm_sent_count: int = 0
    cooldown_blocked: int = 0
    rate_limit_blocked: int = 0
    last_updated: Optional[datetime] = None
