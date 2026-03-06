from sqlalchemy import Column, String, Integer, Float, DateTime, UniqueConstraint, Index
from sqlalchemy.sql import func
from app.analytics.pg_database import Base

# All DateTime columns use timezone=True so PostgreSQL stores
# TIMESTAMP WITH TIME ZONE — compatible with datetime.now(timezone.utc)


class AccountMonthlyInsights(Base):
    """
    Stores monthly account-level Instagram insights.
    Used for year aggregation without 12 API calls.
    """
    __tablename__ = "account_monthly_insights"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    account_id   = Column(String, nullable=False, index=True)
    year         = Column(Integer, nullable=False)
    month        = Column(Integer, nullable=False)   # 1–12
    reach        = Column(Integer, default=0)
    impressions  = Column(Integer, default=0)
    profile_views = Column(Integer, default=0)
    followers    = Column(Integer, default=0)
    updated_at   = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("account_id", "year", "month", name="uq_account_month"),
        Index("ix_account_year", "account_id", "year"),
    )

    def __repr__(self):
        return f"<AccountMonthly {self.account_id} {self.year}-{self.month:02d}>"


class PostInsightsCache(Base):
    """
    Cache for post-level Instagram insights.
    Max 1 API call per post per day.
    """
    __tablename__ = "post_insights_cache"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    post_id      = Column(String, nullable=False, index=True)
    account_id   = Column(String, nullable=False, index=True)
    period       = Column(String, nullable=False, default="lifetime")
    reach        = Column(Integer, default=0)
    impressions  = Column(Integer, default=0)
    likes        = Column(Integer, default=0)
    comments     = Column(Integer, default=0)
    saves        = Column(Integer, default=0)
    shares       = Column(Integer, default=0)
    plays        = Column(Integer, default=0)
    engagement_rate = Column(Float, default=0.0)   # computed locally
    updated_at   = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("post_id", "period", name="uq_post_period"),
        Index("ix_post_account", "post_id", "account_id"),
    )

    def __repr__(self):
        return f"<PostCache {self.post_id} {self.period}>"


class ApiUsageLog(Base):
    """
    Tracks every Instagram API call per account.
    Used for rate-limit protection (max 150/hour).
    """
    __tablename__ = "api_usage_logs"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String, nullable=False, index=True)
    endpoint   = Column(String, nullable=False)
    timestamp  = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    __table_args__ = (
        Index("ix_account_timestamp", "account_id", "timestamp"),
    )

    def __repr__(self):
        return f"<ApiLog {self.account_id} {self.endpoint}>"
