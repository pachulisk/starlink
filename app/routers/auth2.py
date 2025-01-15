from fastapi import HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from fastapi_jwt_auth import AuthJWT
from fastapi_jwt_auth.exceptions import AuthJWTException
from pydantic import BaseModel
from fastapi import APIRouter
from datetime import timedelta
from redis import Redis
from .auth import create_user, get_user, authenticate_user
import os

# 获取.env 文件中的变量值
AUTH_JWT_KEY = os.getenv('AUTH_JWT_KEY')  

auth2 = APIRouter()

class User(BaseModel):
    username: str
    password: str

# in production you can use Settings management
# from pydantic to get secret key from .env
class Settings(BaseModel):
    authjwt_secret_key: str = AUTH_JWT_KEY
    authjwt_denylist_enabled: bool = True
    authjwt_denylist_token_checks: set = {"access","refresh"}
    access_expires: int = timedelta(minutes=15)
    refresh_expires: int = timedelta(days=30)

settings = Settings()
# callback to get your configuration
@AuthJWT.load_config
def get_config():
    return Settings()

# Setup our redis connection for storing the denylist tokens
redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Create our function to check if a token has been revoked. In this simple
# case, we will just store the tokens jti (unique identifier) in redis.
# This function will return the revoked status of a token. If a token exists
# in redis and value is true, token has been revoked
@AuthJWT.token_in_denylist_loader
def check_if_token_in_denylist(decrypted_token):
    jti = decrypted_token['jti']
    entry = redis_conn.get(jti)
    return entry and entry == 'true'

@auth2.post('/registerv2', tags=["v2"])
def register(user: User, Authorize: AuthJWT = Depends()):
    """
    使用registerv2接口创建新的用户。如果用户名已经存在，则返回401错误。
    """
    if get_user(user.username) == True:
        raise HTTPException(status_code=401,detail="User already exists")
    # create_access_token() function is used to actually generate the token to use authorization
    # later in endpoint protected
    create_user(user.username, user.password)
    access_token = Authorize.create_access_token(subject=user.username,fresh=True)
    refresh_token = Authorize.create_refresh_token(subject=user.username)
    return {"username": user.username, "access_token": access_token, "refresh_token": refresh_token}

# provide a method to create access tokens. The create_access_token()
# function is used to actually generate the token to use authorization
# later in endpoint protected
@auth2.post('/loginv2', tags=["v2"])
def login(user: User, Authorize: AuthJWT = Depends()):
    if authenticate_user(user.username, user.password) == False:
        raise HTTPException(status_code=401,detail="Bad username or password")
    # subject identifier for who this token is for example id or username from database
    access_token = Authorize.create_access_token(subject=user.username,fresh=True)
    refresh_token = Authorize.create_refresh_token(subject=user.username)
    return {"access_token": access_token, "refresh_token": refresh_token}

@auth2.post('/refreshv2', tags=["v2"])
def refresh(Authorize: AuthJWT = Depends()):
    """
    The jwt_refresh_token_required() function insures a valid refresh
    token is present in the request before running any code below that function.
    we can use the get_jwt_subject() function to get the subject of the refresh
    token, and use the create_access_token() function again to make a new access token
    """
    Authorize.jwt_refresh_token_required()

    current_user = Authorize.get_jwt_subject()
    new_access_token = Authorize.create_access_token(subject=current_user)
    return {"access_token": new_access_token}

# protect endpoint with function jwt_required(), which requires
# a valid access token in the request headers to access.
@auth2.post('/userv2', tags=["v2"])
def user(Authorize: AuthJWT = Depends()):
    Authorize.jwt_required()

    current_user = Authorize.get_jwt_subject()
    return {"user": current_user}

@auth2.post('/partially-protected',tags=["v2"])
def partially_protected(Authorize: AuthJWT = Depends()):
    Authorize.jwt_optional()

    # If no jwt is sent in the request, get_jwt_subject() will return None
    current_user = Authorize.get_jwt_subject() or "anonymous"
    return {"user": current_user}

# Only fresh JWT access token can access this endpoint
@auth2.post('/protected-fresh', tags=["v2"])
def protected_fresh(Authorize: AuthJWT = Depends()):
    Authorize.fresh_jwt_required()

    current_user = Authorize.get_jwt_subject()
    return {"user": current_user}

# Endpoint for revoking the current users access token
@auth2.post('/access-revokev2', tags=["v2"])
def access_revoke(Authorize: AuthJWT = Depends()):
    Authorize.jwt_required()

    # Store the tokens in redis with the value true for revoked.
    # We can also set an expires time on these tokens in redis,
    # so they will get automatically removed after they expired.
    jti = Authorize.get_raw_jwt()['jti']
    redis_conn.setex(jti,settings.access_expires,'true')
    return {"detail":"Access token has been revoke"}

# Endpoint for revoking the current users refresh token
@auth2.post('/refresh-revokev2', tags=["v2"])
def refresh_revoke(Authorize: AuthJWT = Depends()):
    Authorize.jwt_refresh_token_required()

    jti = Authorize.get_raw_jwt()['jti']
    redis_conn.setex(jti,settings.refresh_expires,'true')
    return {"detail":"Refresh token has been revoke"}