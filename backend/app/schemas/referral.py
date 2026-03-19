from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class ReferralStepOut(BaseModel):
    milestone: str
    label: str
    coins: int
    completed: bool


class OnboardingOut(BaseModel):
    referrer_username: str
    steps: List[ReferralStepOut]
    total_earned: int
    total_possible: int
    progress_pct: int
    all_done: bool


class RecentReferralOut(BaseModel):
    masked_email: str
    status: str
    milestones_done: List[str]
    coins: int
    date: str
    expires_at: Optional[str]


class ReferralStatsOut(BaseModel):
    referral_code: str
    referral_link: str
    total_referred: int
    indirect_referred: int
    coins_earned: int
    pending_count: int
    wallet_balance: int
    next_bonus: Optional[int]
    next_bonus_coins: Optional[int]
    recent_referrals: List[RecentReferralOut]
    onboarding: Optional[dict] = None


class NotificationOut(BaseModel):
    id: str
    type: Optional[str]
    title: Optional[str]
    body: Optional[str]
    action: Optional[str]
    read: bool
    created_at: str


