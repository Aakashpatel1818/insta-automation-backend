"""
services/otp_service.py

Complete OTP service:
  - Generate & store OTP in Redis (TTL = OTP_EXPIRE_MINUTES)
  - Send beautiful HTML email via Gmail SMTP (aiosmtplib)
  - Verify OTP with attempt-count protection
  - Resend cooldown enforcement
"""

import secrets
import logging
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiosmtplib

from app.config import settings
from app.redis_pool import get_redis

logger = logging.getLogger(__name__)

# Redis key prefixes
_OTP_KEY      = "otp:code:{email}"        # stores the OTP code
_ATTEMPTS_KEY = "otp:attempts:{email}"    # wrong-attempt counter
_RESEND_KEY   = "otp:resend:{email}"      # resend cooldown flag


def _otp_key(email: str)      -> str: return _OTP_KEY.format(email=email.lower())
def _attempts_key(email: str) -> str: return _ATTEMPTS_KEY.format(email=email.lower())
def _resend_key(email: str)   -> str: return _RESEND_KEY.format(email=email.lower())


def _generate_otp(length: int = 6) -> str:
    """
    Cryptographically secure random 6-digit numeric OTP.
    Uses secrets.randbelow (CSPRNG) instead of random.choices (Mersenne Twister).
    """
    return str(secrets.randbelow(10 ** length)).zfill(length)


# ─────────────────────────────────────────────────────────────────────────────
# Email sender
# ─────────────────────────────────────────────────────────────────────────────

def _build_email_html(otp: str, email: str) -> str:
    expire_mins = settings.OTP_EXPIRE_MINUTES
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Your OTP — InstaAuto</title>
</head>
<body style="margin:0;padding:0;background:#0a0a1a;font-family:'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a1a;padding:40px 0;">
    <tr>
      <td align="center">
        <table width="480" cellpadding="0" cellspacing="0"
               style="background:linear-gradient(145deg,#1e293b,#0f172a);border-radius:20px;
                      border:1px solid rgba(139,92,246,0.25);overflow:hidden;max-width:480px;width:100%;">

          <!-- Header -->
          <tr>
            <td style="background:linear-gradient(135deg,#8b5cf6,#6366f1);padding:28px 32px;text-align:center;">
              <div style="font-size:28px;margin-bottom:6px;">⚡</div>
              <h1 style="margin:0;color:#fff;font-size:22px;font-weight:800;letter-spacing:-0.5px;">
                InstaAuto
              </h1>
              <p style="margin:4px 0 0;color:rgba(255,255,255,0.75);font-size:13px;">
                Email Verification
              </p>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="padding:32px;">
              <p style="color:#cbd5e1;font-size:15px;margin:0 0 8px;">Hi there 👋</p>
              <p style="color:#94a3b8;font-size:14px;line-height:1.6;margin:0 0 28px;">
                Use the verification code below to complete your InstaAuto registration.
                This code is valid for <strong style="color:#e2e8f0;">{expire_mins} minutes</strong>.
              </p>

              <!-- OTP Box -->
              <div style="background:rgba(139,92,246,0.1);border:2px dashed rgba(139,92,246,0.5);
                          border-radius:16px;padding:24px;text-align:center;margin-bottom:28px;">
                <p style="margin:0 0 8px;color:#94a3b8;font-size:12px;
                           text-transform:uppercase;letter-spacing:2px;">Your verification code</p>
                <div style="letter-spacing:0.35em;font-size:42px;font-weight:900;
                             color:#a78bfa;font-family:'Courier New',monospace;">
                  {otp}
                </div>
              </div>

              <!-- Security note -->
              <div style="background:rgba(245,158,11,0.08);border:1px solid rgba(245,158,11,0.2);
                          border-radius:12px;padding:14px 16px;margin-bottom:24px;">
                <p style="margin:0;color:#fbbf24;font-size:12px;line-height:1.5;">
                  🔒 <strong>Security tip:</strong> Never share this code with anyone.
                  InstaAuto staff will never ask for your OTP.
                </p>
              </div>

              <p style="color:#475569;font-size:12px;line-height:1.6;margin:0;">
                If you didn't try to register at InstaAuto, you can safely ignore this email.
                Someone may have typed your address by mistake.
              </p>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:rgba(0,0,0,0.3);padding:18px 32px;border-top:1px solid rgba(255,255,255,0.06);">
              <p style="margin:0;color:#334155;font-size:11px;text-align:center;">
                © {datetime.now().year} InstaAuto · This is an automated message, please do not reply.
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


async def send_otp_email(email: str, otp: str) -> None:
    """Send OTP email via Gmail SMTP (aiosmtplib, STARTTLS on port 587)."""
    _placeholder = ("your_gmail", "your_16char", "example.com")
    _unconfigured = (
        not settings.SMTP_USERNAME
        or not settings.SMTP_PASSWORD
        or any(p in settings.SMTP_USERNAME for p in _placeholder)
        or any(p in settings.SMTP_PASSWORD for p in _placeholder)
    )
    if _unconfigured:
        # Dev mode — log the OTP to uvicorn terminal instead of sending email
        logger.warning(f"[OTP] SMTP not configured — OTP for {email} is: {otp}")
        return

    from_email = settings.SMTP_FROM_EMAIL or settings.SMTP_USERNAME

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"{otp} is your InstaAuto verification code"
    msg["From"]    = f"{settings.SMTP_FROM_NAME} <{from_email}>"
    msg["To"]      = email

    # Plain text fallback
    plain = (
        f"Your InstaAuto verification code is: {otp}\n\n"
        f"This code expires in {settings.OTP_EXPIRE_MINUTES} minutes.\n"
        f"Do not share this code with anyone."
    )
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(_build_email_html(otp, email), "html"))

    try:
        await aiosmtplib.send(
            msg,
            hostname=settings.SMTP_HOST,
            port=settings.SMTP_PORT,
            username=settings.SMTP_USERNAME,
            password=settings.SMTP_PASSWORD,
            start_tls=True,
            timeout=15,
        )
        logger.info(f"[OTP] Email sent to {email}")
    except Exception as e:
        logger.error(f"[OTP] Failed to send email to {email}: {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# OTP lifecycle
# ─────────────────────────────────────────────────────────────────────────────

async def create_and_send_otp(email: str) -> dict:
    """
    Generate OTP, store in Redis, send email.
    Enforces resend cooldown.
    Returns: {"sent": True} or raises ValueError with reason.
    """
    redis = get_redis()
    email = email.lower().strip()

    # Check resend cooldown
    if await redis.exists(_resend_key(email)):
        ttl = await redis.ttl(_resend_key(email))
        raise ValueError(f"Please wait {ttl} seconds before requesting another code.")

    otp = _generate_otp()
    ttl_seconds = settings.OTP_EXPIRE_MINUTES * 60

    # Store OTP and reset attempt counter atomically
    pipe = redis.pipeline()
    pipe.setex(_otp_key(email), ttl_seconds, otp)
    pipe.delete(_attempts_key(email))
    pipe.setex(_resend_key(email), settings.OTP_RESEND_COOLDOWN_SECONDS, "1")
    await pipe.execute()

    # Send email (non-blocking — errors logged, not swallowed silently in prod)
    await send_otp_email(email, otp)

    return {"sent": True, "expires_in": ttl_seconds}


async def verify_otp(email: str, code: str) -> bool:
    """
    Verify OTP. Tracks wrong attempts.
    Returns True if correct. Raises ValueError with reason on failure.
    """
    redis = get_redis()
    email = email.lower().strip()

    stored = await redis.get(_otp_key(email))

    if stored is None:
        raise ValueError("OTP has expired or was never sent. Please request a new code.")

    # Increment attempt counter
    attempts = await redis.incr(_attempts_key(email))
    if attempts > settings.OTP_MAX_ATTEMPTS:
        # Invalidate OTP after too many wrong attempts
        await redis.delete(_otp_key(email), _attempts_key(email))
        raise ValueError(
            f"Too many incorrect attempts. Please request a new code."
        )

    if stored != code.strip():
        remaining = settings.OTP_MAX_ATTEMPTS - attempts
        if remaining > 0:
            raise ValueError(f"Incorrect code. {remaining} attempt(s) remaining.")
        else:
            await redis.delete(_otp_key(email), _attempts_key(email))
            raise ValueError("Too many incorrect attempts. Please request a new code.")

    # ✅ Correct — delete OTP so it can't be reused
    await redis.delete(_otp_key(email), _attempts_key(email))
    return True


async def mark_email_verified(email: str) -> None:
    """
    After OTP is verified, store a short-lived 'verified' token in Redis.
    The register endpoint checks this before creating the account.
    Expires in 15 minutes — user must complete registration within this window.
    """
    redis = get_redis()
    await redis.setex(f"otp:verified:{email.lower()}", 900, "1")


async def is_email_verified(email: str) -> bool:
    """Check if this email has a valid verification token."""
    redis = get_redis()
    return bool(await redis.exists(f"otp:verified:{email.lower()}"))


async def consume_email_verification(email: str) -> None:
    """Delete the verified flag after account is successfully created."""
    redis = get_redis()
    await redis.delete(f"otp:verified:{email.lower()}")
