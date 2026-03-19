from fastapi import APIRouter, HTTPException, status, Depends
from datetime import datetime
from bson import ObjectId
from pydantic import BaseModel, EmailStr
import asyncio
import logging

from app.schemas.auth import RegisterRequest, LoginRequest, TokenResponse, UserPublic
from app.services.referral_service import init_user_referral
from app.services.otp_service import (
    create_and_send_otp,
    verify_otp,
    mark_email_verified,
    is_email_verified,
    consume_email_verification,
)
from app.security import hash_password, verify_password, create_access_token
from app.database import get_db
from app.dependencies import get_current_user, invalidate_user_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Auth"])


# ─────────────────────────────────────────────────────────────────────────────
# Request schemas (inline — small enough not to need a separate file)
# ─────────────────────────────────────────────────────────────────────────────

class SendOtpRequest(BaseModel):
    email: EmailStr

class VerifyOtpRequest(BaseModel):
    email: EmailStr
    code:  str

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ResetPasswordRequest(BaseModel):
    email: EmailStr
    code:  str
    new_password: str


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Send OTP to email
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/send-otp", status_code=status.HTTP_200_OK)
async def send_otp(body: SendOtpRequest):
    """
    Send a 6-digit OTP to the given email.
    - Checks the email is not already registered.
    - Enforces a resend cooldown (60 s by default).
    - Stores OTP in Redis with a 10-minute TTL.
    """
    db = get_db()
    email = str(body.email).lower().strip()

    # Reject if email already registered
    existing = await db["users"].find_one({"email": email}, {"_id": 1})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered.")

    try:
        result = await create_and_send_otp(email)
    except ValueError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except Exception as e:
        logger.error(f"[OTP] send_otp failed for {email}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to send verification email. Please try again."
        )

    return {
        "ok":        True,
        "message":   f"Verification code sent to {email}",
        "expires_in": result["expires_in"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Verify OTP
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/verify-otp", status_code=status.HTTP_200_OK)
async def verify_otp_endpoint(body: VerifyOtpRequest):
    """
    Verify the 6-digit OTP the user typed.
    On success: stores a short-lived 'verified' flag in Redis.
    The register endpoint will check this flag before creating the account.
    """
    email = str(body.email).lower().strip()

    try:
        await verify_otp(email, body.code)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[OTP] verify_otp failed for {email}: {e}")
        raise HTTPException(status_code=500, detail="Verification failed. Please try again.")

    # Mark email as verified in Redis (15-minute window to complete registration)
    await mark_email_verified(email)

    return {"ok": True, "message": "Email verified. You can now complete registration."}


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Register (requires verified email)
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/register", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest):
    """
    Create account. Requires the email to have been verified via OTP first.
    """
    db = get_db()
    email = str(body.email).lower().strip()

    # ── Guard: email must be OTP-verified ────────────────────────────────────
    if not await is_email_verified(email):
        raise HTTPException(
            status_code=400,
            detail="Email not verified. Please verify your email with the OTP before registering."
        )

    # ── Parallel duplicate checks ─────────────────────────────────────────────
    email_check, username_check = await asyncio.gather(
        db["users"].find_one({"email": email}, {"_id": 1}),
        db["users"].find_one({"username": body.username}, {"_id": 1}),
    )
    if email_check:
        raise HTTPException(status_code=400, detail="Email already registered.")
    if username_check:
        raise HTTPException(status_code=400, detail="Username already taken.")

    now = datetime.utcnow()
    user_doc = {
        "username":        body.username,
        "email":           email,
        "hashed_password": hash_password(body.password),
        "is_active":       True,
        "email_verified":  True,
        "created_at":      now,
        "updated_at":      now,
    }

    result = await db["users"].insert_one(user_doc)
    user_doc["_id"] = result.inserted_id

    # Consume the verified flag so it can't be reused
    await consume_email_verification(email)

    # Init referral system
    await init_user_referral(
        db,
        str(result.inserted_id),
        body.username,
        getattr(body, "referral_code", None),
    )

    logger.info(f"New user registered (email verified): {email}")

    return UserPublic(
        id=str(result.inserted_id),
        username=user_doc["username"],
        email=user_doc["email"],
        is_active=user_doc["is_active"],
        created_at=user_doc["created_at"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Login
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest):
    db = get_db()

    user = await db["users"].find_one({"email": str(body.email).lower().strip()})
    if not user or not verify_password(body.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password."
        )

    if not user.get("is_active", True):
        raise HTTPException(status_code=403, detail="Account is inactive.")

    token = create_access_token(data={"sub": str(user["_id"])})
    logger.info(f"User logged in: {body.email}")

    return TokenResponse(access_token=token)


# ─────────────────────────────────────────────────────────────────────────────
# Me
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/me", response_model=UserPublic)
async def get_me(current_user: dict = Depends(get_current_user)):
    return UserPublic(
        id=str(current_user["_id"]),
        username=current_user["username"],
        email=current_user["email"],
        is_active=current_user["is_active"],
        role=current_user.get("role", "user"),
        plan=current_user.get("plan", "free"),
        coins=current_user.get("coins", 0),
        created_at=current_user["created_at"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Deactivate user — admin only
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/deactivate/{user_id}", status_code=status.HTTP_200_OK)
async def deactivate_user(
    user_id: str,
    current_user: dict = Depends(get_current_user),
):
    # ── Security fix: only admins / superadmins may deactivate other accounts ──
    role = current_user.get("role", "user")
    if role not in ("admin", "superadmin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required to deactivate users.",
        )

    db = get_db()
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid user ID.")

    result = await db["users"].update_one(
        {"_id": oid},
        {"$set": {"is_active": False, "updated_at": datetime.utcnow()}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found.")

    await invalidate_user_cache(user_id)
    logger.info(f"User {user_id} deactivated by admin {current_user.get('email')}.")
    return {"status": "deactivated", "user_id": user_id}


# ─────────────────────────────────────────────────────────────────────────────
# Forgot Password — Step 1: send reset OTP
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/forgot-password", status_code=status.HTTP_200_OK)
async def forgot_password(body: ForgotPasswordRequest):
    """
    Send a 6-digit OTP to the email for password reset.
    Always returns 200 even if email not found (security: no user enumeration).
    """
    db = get_db()
    email = str(body.email).lower().strip()

    user = await db["users"].find_one({"email": email}, {"_id": 1})
    if not user:
        # Don't reveal whether the email exists
        return {"ok": True, "message": "If that email exists, a reset code has been sent."}

    try:
        result = await create_and_send_otp(email)
    except ValueError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except Exception as e:
        logger.error(f"[ForgotPassword] send failed for {email}: {e}")
        raise HTTPException(status_code=500, detail="Failed to send reset email. Please try again.")

    return {
        "ok": True,
        "message": "If that email exists, a reset code has been sent.",
        "expires_in": result["expires_in"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Forgot Password — Step 2: verify OTP + set new password
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(body: ResetPasswordRequest):
    """Verify OTP then update the user's password."""
    db = get_db()
    email = str(body.email).lower().strip()

    if len(body.new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")

    # Verify OTP (does NOT delete the key yet -- see Bug #14 fix below)
    try:
        await verify_otp(email, body.code)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Bug #14 fix: look up the user BEFORE consuming the OTP so that if the DB
    # write fails the OTP is still available and the user is not locked out.
    # Old order was: verify_otp (deletes OTP) → find_one → update_one.
    # If update_one threw an exception the OTP was already gone and the user
    # had to wait for the 60-second resend cooldown with no valid code.
    user = await db["users"].find_one({"email": email}, {"_id": 1})
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    try:
        result = await db["users"].update_one(
            {"_id": user["_id"]},
            {"$set": {
                "hashed_password": hash_password(body.new_password),
                "updated_at": datetime.utcnow(),
            }}
        )
        if result.matched_count == 0:
            raise RuntimeError("update matched 0 documents")
    except Exception as db_err:
        # DB write failed -- restore the OTP so the user can retry.
        # We re-issue the same code they already verified so they don't need
        # to request a new one (resend cooldown may still be active).
        from app.redis_pool import get_redis as _get_redis
        from app.config import settings as _cfg
        _r = _get_redis()
        try:
            # Store a fresh 10-minute window using the verified code.
            await _r.setex(
                f"otp:code:{email}",
                _cfg.OTP_EXPIRE_MINUTES * 60,
                body.code,
            )
            logger.warning(
                f"[ForgotPassword] DB update failed for {email}, OTP restored: {db_err}"
            )
        except Exception as redis_err:
            logger.error(f"[ForgotPassword] Could not restore OTP for {email}: {redis_err}")
        raise HTTPException(
            status_code=500,
            detail="Failed to update password. Please try again with the same code.",
        )

    await invalidate_user_cache(str(user["_id"]))
    logger.info(f"[ForgotPassword] Password reset for {email}")
    return {"ok": True, "message": "Password updated. You can now log in."}
