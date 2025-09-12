from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
import jwt
import time
import os
import re

# Simple in-memory user store (replace with DB for production)
users_db = {}
SECRET_KEY = os.getenv("AUTH_SECRET_KEY", "supersecretkey")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

router = APIRouter(prefix="/auth", tags=["auth"])

class UserCreate(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

# Placeholder for email verification status
verified_emails = set()

@router.post("/register")
def register(user: UserCreate):
    # Strict email format validation
    if not re.match(r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$", user.email):
        raise HTTPException(status_code=400, detail="Invalid email format")
    if user.email in users_db:
        raise HTTPException(status_code=400, detail="Email already registered")
    hashed_pw = pwd_context.hash(user.password)
    users_db[user.email] = {"email": user.email, "hashed_pw": hashed_pw, "verified": False}
    # Placeholder: send verification email here
    # In production, generate a token and send a link to user.email
    return {"msg": "User registered. Please verify your email (feature coming soon)."}

@router.post("/verify-email")
def verify_email(email: str):
    # Placeholder: In production, verify token from email link
    if email in users_db:
        users_db[email]["verified"] = True
        verified_emails.add(email)
        return {"msg": "Email verified!"}
    raise HTTPException(status_code=404, detail="Email not found")

@router.post("/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = users_db.get(form_data.username)
    if not user or not pwd_context.verify(form_data.password, user["hashed_pw"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not user.get("verified"):
        raise HTTPException(status_code=403, detail="Email not verified")
    payload = {"sub": user["email"], "exp": time.time() + 3600}
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return {"access_token": token, "token_type": "bearer"}

# Dependency to get current user

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        email = payload.get("sub")
        if email not in users_db:
            raise HTTPException(status_code=401, detail="Invalid token")
        return users_db[email]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

# Example protected route
@router.get("/me")
def read_users_me(current_user: dict = Depends(get_current_user)):
    return {"email": current_user["email"]}
