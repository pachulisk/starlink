from dataclasses import dataclass
from fastapi import APIRouter, HTTPException,Depends,status
from typing import Annotated, Dict, Optional
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta, timezone
from jwt.exceptions import InvalidTokenError

from ..utils import is_empty
import os
from pydantic import BaseModel
from app.supabase import supabase
import logging
import bcrypt
from redis import Redis

# Setup our redis connection for storing the denylist tokens
redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)


@dataclass
class SolveBugBcryptWarning:
    __version__: str = getattr(bcrypt, "__version__")

setattr(bcrypt, "__about__", SolveBugBcryptWarning())

logger = logging.getLogger()
logger.setLevel(logging.INFO)

auth = APIRouter()

SECRET_KEY = os.getenv('AUTH_JWT_KEY')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: str | None = None

class UserBase(BaseModel):
    id: str
    username: str
    email: str | None = None
    full_name: str | None = None
    disabled: bool | None = None
    role: str

class UserInDB(UserBase):
    hashed_password: str | None = None

def create_access_token(data: Dict[str, str], expires_delta: Optional[timedelta] = None) -> str:
    """创建JWT访问令牌"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, 
        SECRET_KEY, 
        algorithm=ALGORITHM
    )
    return encoded_jwt

def verify_password(plain_password, hashed_password):
    print(plain_password)
    print(get_password_hash(plain_password))
    print(hashed_password)
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def get_user(username: str):
    table_name = "user_auth"
    print(username)
    response = supabase.table(table_name).select("*").eq("username", username).execute()
    print(response)
    if len(response.data) > 0:
        data = {
            "id": response.data[0]["global_id"],
            "username": response.data[0]["username"],
            "hashed_password": response.data[0]["hashed_password"],
            "full_name": response.data[0]["full_name"],
            "role": response.data[0]["role"],
            "disabled": False,
            "email": None
        }
        return UserInDB(**data)
    else:
        return None

def get_user_auth_gateways(userid: str):
    """
    根据userid，查询user_auth_gateways表，获取对应的gwids
    入参1: userid，用户id
    返回：数组，gwids
    """
    gwids = []
    TABLE_NAME = "user_auth_gateways"
    res = supabase.table(TABLE_NAME).select("*").eq("userid", userid).execute()
    for item in res.data:
        gwids.append(item["gwid"])
    return gwids

def create_user(username: str, password: str):
    table_name = "user_auth"
    hashed_password = get_password_hash(password)
    user_data = {
        "username": username,
        "hashed_password": hashed_password,
        "disabled": False,
        "full_name": username,
    }
    _ = supabase.table(table_name).insert(user_data).execute()
    user_auth = get_user(username)
    return user_auth

def reset_password_impl(userid: str, password: str):
    """
    重置某个用户的密码。
    输入：userid: 用户的global_id
    输入：password：用户的新密码，明文
    """
    # 检查userid是否为空
    if is_empty(userid):
        raise HTTPException(status_code=400, detail="[reset_password_impl]userid is required")
    TABLE_NAME = "user_auth"
    hashed_password = get_password_hash(password)
    res = supabase.table(TABLE_NAME).update({"hashed_password": hashed_password}).eq("global_id", userid).execute()
    print(f"[reset_password_impl]userid = {userid},res = {str(res)}")
    return {"data": res}

def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        print("用户不存在")
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except InvalidTokenError:
        raise credentials_exception
    user = get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: Annotated[UserBase, Depends(get_current_user)],
):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

async def super_admin_required(
        current_user: UserBase = Depends(get_current_user)
) -> UserBase:
    """
    检查用户是否为超级管理员。
    """
    if current_user.role != "SUPER_ADMIN":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="权限不足，需要超级管理员权限"
        )
    return current_user

@auth.post("/token", tags=["auth"])
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    print(form_data.username)
    print(form_data.password)
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或者密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user.username,
            "role": user.role
        }, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")

@auth.get("/users/me/", response_model=UserBase, tags=["auth"])
async def read_users_me(
    current_user: Annotated[UserBase, Depends(get_current_active_user)],
):
    return current_user

@auth.post("/users/pswd", tags=["auth"])
async def get_user_password_hide(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> None:
    pswd = get_password_hash(form_data.password)
    print(pswd)