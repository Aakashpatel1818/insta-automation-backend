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
    cooldown_hours: float = 24.0   # 0 = disabled, any positive = hours between triggers per user
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AutomationSettingsRequest(BaseModel):
    post_id: str
    account_id: str
    auto_comment_reply: bool = False
    auto_dm: bool = False
    delay_enabled: bool = True
    is_active: bool = True
    cooldown_hours: float = 24.0   # 0 = disabled, any positive = hours between triggers per user


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
    automation_id: Optional[str] = None      # links rule to specific automation
    trigger_words: List[str]
    response: str
    reply_comment: bool = True
    send_dm: bool = False
    is_active: bool = True
    # Opening Message (DM sent immediately when keyword is triggered)
    opening_message: str = ""
    opening_message_btn: str = ""
    opening_message_btn_url: str = ""
    # Follow DM (separate DM sent after opening message)
    follow_dm_message: str = ""
    # DM Actions (buttons sent as plain text in DM)
    dm_actions: list = []



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
    action_taken: str        # reply | dm | reply+dm | none
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
