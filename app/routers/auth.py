from fastapi import APIRouter, HTTPException, status, Depends
from datetime import datetime
from bson import ObjectId
import logging

from app.schemas.auth import RegisterRequest, LoginRequest, TokenResponse, UserPublic
from app.security import hash_password, verify_password, create_access_token
from app.database import get_db
from app.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Auth"])


# ── Register ──────────────────────────────────────────────
@router.post("/register", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterRequest):
    db = get_db()

    # Check duplicates
    if await db["users"].find_one({"email": body.email}):
        raise HTTPException(status_code=400, detail="Email already registered")
    if await db["users"].find_one({"username": body.username}):
        raise HTTPException(status_code=400, detail="Username already taken")

    now = datetime.utcnow()
    user_doc = {
        "username": body.username,
        "email": body.email,
        "hashed_password": hash_password(body.password),
        "is_active": True,
        "created_at": now,
        "updated_at": now,
    }

    result = await db["users"].insert_one(user_doc)
    user_doc["_id"] = result.inserted_id

    logger.info(f"New user registered: {body.email}")

    return UserPublic(
        id=str(result.inserted_id),
        username=user_doc["username"],
        email=user_doc["email"],
        is_active=user_doc["is_active"],
        created_at=user_doc["created_at"],
    )


# ── Login ─────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest):
    db = get_db()

    user = await db["users"].find_one({"email": body.email})
    if not user or not verify_password(body.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    if not user.get("is_active", True):
        raise HTTPException(status_code=403, detail="Account is inactive")

    token = create_access_token(data={"sub": str(user["_id"])})
    logger.info(f"User logged in: {body.email}")

    return TokenResponse(access_token=token)


# ── Protected: Get current user ───────────────────────────
@router.get("/me", response_model=UserPublic)
async def get_me(current_user: dict = Depends(get_current_user)):
    return UserPublic(
        id=str(current_user["_id"]),
        username=current_user["username"],
        email=current_user["email"],
        is_active=current_user["is_active"],
        created_at=current_user["created_at"],
    )
