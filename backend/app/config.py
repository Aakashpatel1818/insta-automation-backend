from pydantic_settings import BaseSettings
from functools import lru_cache
import sys
import secrets

# ── Insecure default sentinels — detected at startup ─────────────────────────
_INSECURE_SECRET_KEY      = "change-this-secret-in-production"
_INSECURE_WEBHOOK_TOKEN   = "myverifytoken123"


class Settings(BaseSettings):
    APP_NAME: str = "Insta App"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # MongoDB
    MONGODB_URL: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "insta_db"

    # PostgreSQL (Analytics)
    POSTGRES_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/insta_analytics"
    PG_POOL_SIZE: int = 20
    PG_MAX_OVERFLOW: int = 10

    # JWT
    # ⚠ MUST be overridden in .env — startup guard below will abort if not set.
    SECRET_KEY: str = _INSECURE_SECRET_KEY
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24

    # Redis
    REDIS_URL: str = "redis://localhost:6379"
    REDIS_POOL_SIZE: int = 50
    REDIS_POOL_TIMEOUT: int = 5

    # Cache TTLs (seconds)
    USER_CACHE_TTL: int = 300
    ACCOUNT_CACHE_TTL: int = 600

    # Rate limiting
    RATE_LIMIT_PER_MIN: int = 120
    RATE_LIMIT_AUTH_PER_MIN: int = 10

    # DDoS / abuse protection
    # Global per-IP cap across all endpoints (requests per 60 s window).
    # Set higher than RATE_LIMIT_PER_MIN so legitimate power users are unaffected
    # while bot floods are blocked before they hit any business logic.
    RATE_LIMIT_GLOBAL_PER_MIN: int = 300
    # Maximum allowed request body size (bytes). 1 MB default.
    # Blocks HTTP flood attacks that send large bodies to exhaust I/O.
    MAX_REQUEST_BODY_SIZE: int = 1_048_576  # 1 MB

    # Automation
    COOLDOWN_HOURS: float = 24
    DM_RATE_LIMIT: int = 50
    DAILY_DM_CAP: int = 150

    # Webhook
    # ⚠ MUST be overridden in .env — startup guard below will abort if not set.
    WEBHOOK_VERIFY_TOKEN: str = _INSECURE_WEBHOOK_TOKEN

    # Bootstrap superadmin (Bug #12)
    # Set to a strong random string in .env to enable the bootstrap endpoint.
    # Leave empty (default) to keep the endpoint permanently disabled.
    BOOTSTRAP_SECRET: str = ""

    # Instagram OAuth
    INSTAGRAM_APP_ID: str = ""
    INSTAGRAM_APP_SECRET: str = ""
    INSTAGRAM_REDIRECT_URI: str = "http://localhost:8000/instagram/callback"
    FRONTEND_URL: str = "http://localhost:5173"
    BASE_URL: str = "http://localhost:8000"

    # CORS
    # ⚠ In production set this to your exact frontend domain(s), e.g.:
    #   ALLOWED_ORIGINS=["https://app.yourdomain.com"]
    # Leaving it as ["*"] is only acceptable during local development.
    ALLOWED_ORIGINS: list[str] = ["*"]

    # ── Email / OTP ────────────────────────────────────────────────────────────
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_FROM_NAME: str = "InstaAuto"
    SMTP_FROM_EMAIL: str = ""

    OTP_EXPIRE_MINUTES: int = 10
    OTP_MAX_ATTEMPTS: int = 5
    OTP_RESEND_COOLDOWN_SECONDS: int = 60

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()


# ── Production startup guard ──────────────────────────────────────────────────
# Abort immediately if critical secrets are still set to their insecure defaults.
# This prevents accidental deployment with placeholder credentials.
def _validate_production_secrets() -> None:
    if settings.DEBUG:
        # Allow insecure defaults in local dev — just warn loudly.
        if settings.SECRET_KEY == _INSECURE_SECRET_KEY:
            print(
                "\033[93m[CONFIG WARNING] SECRET_KEY is set to the insecure default. "
                "Set a real value in .env before deploying to production.\033[0m",
                file=sys.stderr,
            )
        if settings.WEBHOOK_VERIFY_TOKEN == _INSECURE_WEBHOOK_TOKEN:
            print(
                "\033[93m[CONFIG WARNING] WEBHOOK_VERIFY_TOKEN is set to the insecure default. "
                "Set a real value in .env before deploying to production.\033[0m",
                file=sys.stderr,
            )
        if settings.ALLOWED_ORIGINS == ["*"]:
            print(
                "\033[93m[CONFIG WARNING] ALLOWED_ORIGINS is '*'. "
                "Restrict this to your frontend domain(s) in production.\033[0m",
                file=sys.stderr,
            )
        return

    # ── Production mode (DEBUG=False): hard-fail on insecure defaults ─────────
    errors = []

    if settings.SECRET_KEY == _INSECURE_SECRET_KEY:
        errors.append(
            "SECRET_KEY is set to the insecure default value. "
            "Generate a strong key with: python -c \"import secrets; print(secrets.token_hex(32))\""
        )

    if len(settings.SECRET_KEY) < 32:
        errors.append(
            f"SECRET_KEY is too short ({len(settings.SECRET_KEY)} chars). "
            "Minimum 32 characters required."
        )

    if settings.WEBHOOK_VERIFY_TOKEN == _INSECURE_WEBHOOK_TOKEN:
        errors.append(
            "WEBHOOK_VERIFY_TOKEN is set to the insecure default 'myverifytoken123'. "
            "Set a strong random value in .env."
        )

    if settings.ALLOWED_ORIGINS == ["*"]:
        errors.append(
            "ALLOWED_ORIGINS is set to ['*'] which allows any domain to make "
            "authenticated cross-origin requests. "
            "Set it to your frontend domain(s), e.g.: "
            'ALLOWED_ORIGINS=["https://app.yourdomain.com"]'
        )

    if errors:
        print("\n\033[91m" + "=" * 70, file=sys.stderr)
        print("🚨 STARTUP ABORTED — INSECURE CONFIGURATION DETECTED", file=sys.stderr)
        print("=" * 70 + "\033[0m", file=sys.stderr)
        for i, err in enumerate(errors, 1):
            print(f"\n  [{i}] {err}", file=sys.stderr)
        print(
            "\n\033[91mFix the above issues in your .env file before starting "
            "in production (DEBUG=False).\033[0m\n",
            file=sys.stderr,
        )
        sys.exit(1)


_validate_production_secrets()
