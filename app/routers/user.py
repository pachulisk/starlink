from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.supabase import supabase
from ..sdk import SDK
from ..utils import get_gateway_by_id, gw_login
from pydantic import BaseModel
import json

user = APIRouter()

class AddUserParam(BaseModel):
    gwid: str
    username: str
    password: str
    confirm_password: str
    group: str
    datelimit: str


# 检查用户名是否在supabase的gw_users表中存在
def check_username_exists(gwid: str, username: str):
    response = supabase.table('gw_users').select('*').eq('gwid', gwid).eq('username', username).execute()
    return response.data is not None and len(response.data) > 0

# 通过gwid、用户名username来检查用户的id
def get_user_id_by_gwid_username(gwid: str, username: str):
    response = supabase.table('gw_users').select('id').eq('gwid', gwid).eq('username', username).execute()
    if response.data:
        return response.data[0]['id']
    else:
        return None

def get_user_info_by_gwid_username(gwid: str, username: str):
    response = supabase.table('gw_users').select('*').eq('gwid', gwid).eq('username', username).execute()
    if response.data:
        return response.data[0]
    else:
        return None
    
# 在supabase中，通过gwid、username来标记用户的状态为已删除
def mark_user_as_deleted(gwid: str, username: str):
    response = supabase.table('gw_users').update({'delete_mark': "true"}).eq('gwid', gwid).eq('username', username).execute()
    return response.data is not None and len(response.data) > 0

@user.post("/add_user", tags=["user"])
async def add_user(param: AddUserParam):
    """
    调用sdk添加用户
    """
    # 思路:在wfilter-account配置项中添加用户
    # 典型用户为
    """
    "wfuser17339749341670": {
      ".anonymous": false,
      ".type": "wfuser",
      ".name": "wfuser17339749341670",
      ".index": 5,
      "username": "user1",
      "password": "<#254ef761e0fe2110#>",
      "remark": "ISP-1-bandwidth1733974887482",
      "pppoe": "false",
      "webauth": "true",
      "static": "false",
      "staticip": " ",
      "datelimit": "2034-10-01",
      "group": "0",
      "logins": "0",
      "macbound": "0",
      "changepwd": "false",
      "id": "wfuser17339749341670"
    },
    """
    gwid = param.gwid
    password = param.password
    group = param.group
    datelimit = param.datelimit
    confirm_password = param.confirm_password
    # 0. 检查用户名是否为空
    if param.username == "":
        raise HTTPException(status_code=400, detail="用户名不能为空")
    # 1. 检查用户名是否已经存在
    if check_username_exists(gwid, param.username):
        raise HTTPException(status_code=400, detail="用户名已存在")
    # 2. 检查密码是否一致
    if param.password != param.confirm_password:
        raise HTTPException(status_code=400, detail="密码不一致")
    # 3. 检查密码是否为空
    if param.password == "":
        raise HTTPException(status_code=400, detail="密码不能为空")
    # 4. 检查datelimit是否为空，如果不为空，则设置为2034-10-01
    if param.datelimit == "":
        datelimit = "2034-10-01"
    # 5. 检查group是否为空，如果不为空，则设置为0
    if param.group == "":
        group = "0"
    
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    # get gateway username, password and address
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        # 使用config.add(cfgname, type, key, value)增加新的账户
        # cfgname = ‘wfilter-account’
        # type = ‘wfuser’
        # key = ‘username’
        # value = {”username=user1”, “password=<pswdxxx>”, “remark=ISP-1-bandwidth1733974887482”,  “pppoe=false”, “webauth=true”, “static=false”, “staticip= “, datelimit=”2034-10-01”, group=”0”, logins=”0”, macbounds=”0”, changepwd=”false”, id=xxx}
        if sdk.login(address, username, password):
          cfgname = 'wfilter-account'
          type = 'wfuser'
          key = param.username
          value = {
              "id": key,
              ".anonymous": False,
              ".type": "wfuser",
              "username": param.username,
              "password": param.password,
              "remark": "ISP-1-bandwidth"+key,
              "pppoe": "false",
              "webauth": "true",
              "static": "false",
              "staticip": " ",
              "datelimit": datelimit,
              "group": group,
              "logins": "0",
              "macbound": "0",
              "changepwd": "false",
          }
          # 调用sdk
          result1 = sdk.config_add(cfgname, type, key, value)
          print("result1 = " + str(result1))
          result2 = sdk.config_apply()
          print("result2 = " + str(result2))
          return {"data": value}
        else:
          raise HTTPException(status_code=401, detail="登录失败")
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format"}
    except Exception as e:
        print("error: " + str(e))
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class DeleteUserParam(BaseModel):
    gwid: str
    username: str

@user.post("/delete_user", tags=["user"])
async def delete_user(param: DeleteUserParam):
    """
    调用sdk删除用户
    """
    # 思路:在wfilter-account配置项中删除用户
    # 典型用户为
    """
    "wfuser17339749341670": {
      ".anonymous": false,
      ".type": "wfuser",
      ".name": "wfuser17339749341670",
      ".index": 5,
      "username": "user1",
      "password": "<#254ef761e0fe2110#>",
      "remark": "ISP-1-bandwidth1733974887482",
      "pppoe": "false",
      "webauth": "true",
      "static": "false",
      "staticip": " ",
      "datelimit": "2034-10-01",
      "group": "0",
      "logins": "0",
      "macbound": "0",
      "changepwd": "false",
      "id": "wfuser17339749341670"
    },
    """
    # 0. 检查gwid和username是否为空
    if param.gwid == "" or param.username == "":
        raise HTTPException(status_code=400, detail="gwid和username不能为空")
    # 1. 检查用户名是否已经存在
    if not check_username_exists(param.gwid, param.username):
        raise HTTPException(status_code=400, detail="用户名不存在")
    # 2. 登陆sdk
    gwid = param.gwid
    with gw_login(gwid) as sdk_obj:
        user_info = get_user_info_by_gwid_username(gwid, param.username)
        if sdk_obj is None:
            raise HTTPException(status_code=400, detail="登录失败")
        # 3. 调用sdk.config_del删除相关内容
        cfgname = "wfilter-account"
        id = get_user_id_by_gwid_username(gwid, param.username)
        result1 = sdk_obj.config_del(cfgname, id)
        print("result1 = " + str(result1))
        # 4. 调用sdk.config_apply实现修改
        result2 = sdk_obj.config_apply()
        print("result2 = " + str(result2))
        # 5. 在supabase的gw_users表中，将username,id相关匹配到的行的删除标记置为true
        mark_user_as_deleted(gwid, param.username)
        return {"data": user_info}