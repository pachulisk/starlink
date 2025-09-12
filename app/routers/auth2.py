from fastapi import HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi import APIRouter
from datetime import timedelta
from redis import Redis
from .auth import UserBase, create_access_token, get_current_user, create_user, get_user, authenticate_user, reset_password_impl
import os

# 获取.env 文件中的变量值
AUTH_JWT_KEY = os.getenv('AUTH_JWT_KEY')  

auth2 = APIRouter()

class User(BaseModel):
    username: str
    password: str

class UserAuth(BaseModel):
    username: str
    password: str
    userid: str

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


# Setup our redis connection for storing the denylist tokens
redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)


@auth2.post('/registerv2', tags=["v2"])
def register(user: User):
    """
    使用registerv2接口创建新的用户。如果用户名已经存在，则返回401错误。
    """
    if get_user(user.username) == True:
        raise HTTPException(status_code=401,detail="User already exists")
    # create_access_token() function is used to actually generate the token to use authorization
    # later in endpoint protected
    user_auth = create_user(user.username, user.password)
    access_token = create_access_token(data={"sub": user_auth.username, "id": user_auth.id})
    refresh_token = create_access_token(data={"sub": user_auth.username, "id": user_auth.id})
    return {"username": user.username, "userid": user_auth.id, "access_token": access_token, "refresh_token": refresh_token}

# provide a method to create access tokens. The create_access_token()
# function is used to actually generate the token to use authorization
# later in endpoint protected
@auth2.post('/loginv2', tags=["v2"])
def login(user: User):
    auth_result = authenticate_user(user.username, user.password)
    if auth_result == False:
        raise HTTPException(status_code=401,detail="Bad username or password")
    user_base = auth_result
    # subject identifier for who this token is for example id or username from database
    access_token = create_access_token(data={"sub": user_base.username, "id": user_base.id})
    refresh_token = create_access_token(data={"sub": user_base.username, "id": user_base.id})
    return {"access_token": access_token, "refresh_token": refresh_token}

class ResetPasswordV2Query(BaseModel):
    password: str
    old_password: str

@auth2.post("/reset_passwordv2", tags=["v2"])
async def reset_user_password(query: ResetPasswordV2Query, current_user: UserBase = Depends(get_current_user)):
    """
    普通用户重新设置密码
    入参1：旧密码
    入参2：新密码
    逻辑：先使用旧密码验证用户，然后设置新密码
    """
    username = current_user.username
    password = query.password
    old_password = query.old_password
    # 验证原密码是否可以验证
    if authenticate_user(username, old_password) == False:
        raise HTTPException(status_code=401,detail="[reset_passwordv2]: bad username or password")
    # 重置用户密码
    res = await reset_password_impl(current_user.id, password)
    print(f"[reset_user_password]: success reset password, userid = {current_user.id}, res = {str(res)}")
    return {"data": res.data}

# @auth2.post('/refreshv2', tags=["v2"])
# def refresh(Authorize: AuthJWT = Depends()):
#     """
#     The jwt_refresh_token_required() function insures a valid refresh
#     token is present in the request before running any code below that function.
#     we can use the get_jwt_subject() function to get the subject of the refresh
#     token, and use the create_access_token() function again to make a new access token
#     """
#     Authorize.jwt_refresh_token_required()

#     current_user = Authorize.get_jwt_subject()
#     new_access_token = Authorize.create_access_token(subject=current_user)
#     return {"access_token": new_access_token}