from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime


# ── Register ──────────────────────────────────────────────
class RegisterRequest(BaseModel):
    username: str
    email: EmailStr
    password: str


# ── Login ─────────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: EmailStr
    password: str


# ── Token response ────────────────────────────────────────
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ── Public user (never expose hashed_password) ────────────
class UserPublic(BaseModel):
    id: str
    username: str
    email: str
    is_active: bool
    created_at: datetime
