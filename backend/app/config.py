from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    APP_NAME: str = "Insta App"
    APP_VERSION: str = "1.0.0"

    # MongoDB
    MONGODB_URL: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "insta_db"

    # PostgreSQL (Analytics)
    POSTGRES_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/insta_analytics"

    # JWT
    SECRET_KEY: str = "change-this-secret-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

    # Automation
    COOLDOWN_HOURS: float = 24
    DM_RATE_LIMIT: int = 50

    # Webhook
    WEBHOOK_VERIFY_TOKEN: str = "myverifytoken123"

    # Instagram OAuth
    INSTAGRAM_APP_ID: str = ""
    INSTAGRAM_APP_SECRET: str = ""
    INSTAGRAM_REDIRECT_URI: str = "http://localhost:8000/instagram/callback"
    FRONTEND_URL: str = "http://localhost:5173"
    BASE_URL: str = "http://localhost:8000"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
