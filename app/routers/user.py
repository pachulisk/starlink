from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.supabase import supabase
from ..sdk import SDK
from ..utils import is_future_date, yyyymmdd, get_milliseconds, gw_login, get_gateway_by_id, gw_login
from .group_service import update_user_group_impl, UpdateUserGroupQuery
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
    group_id: str # 组id,(例如punish)可选，如果为空则用户没有任何组
    sid: str # 策略id（例如bandwidth1234567），必选

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
    
# 在supabase中，通过gwid、username的用户删除。
def mark_user_as_deleted(gwid: str, username: str):
    """
    在gw_users表中删除用户。
    1. 在gw_users中获取到相关用户信息。
    2. 在gw_users表中删除用户。
    3. 在logging中记录删除的用户日志。
    """
    TABLE_NAME = "gw_users"
    # 1. 在gw_users中获取到相关用户信息。
    response = (supabase
                 .table(TABLE_NAME)
                  .select('*')
                  .eq('gwid', gwid)
                  .eq('username', username)
                  .execute())
    
    if len(response.data) <= 0:
        return {"data": None}
    
    user_data = response.data[0]
    # 2. 在gw_users表中删除用户。
    _ = (supabase
                .table('gw_users')
                .delete()
                .eq('gwid', gwid)
                .eq('username', username)
                .execute())
    # 3. 在logging中记录删除的用户日志。
    LOGGING_TABLE = "logging"
    log_data = {
        "action": "delete_user",
        "group": "user",
        "level": "info",
        "info": json.dumps(user_data)
    }
    response = (supabase
                .table(LOGGING_TABLE)
                .insert(log_data)
                .execute()
    )
    return user_data

def gen_user_id():
    timestamp = get_milliseconds()
    return f"wfuser{timestamp}"

def get_default_password():
    return "<#254ef761e0fe2110#>"

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
    username = param.username
    password = param.password
    group = param.group
    # group_id可以为空
    group_id = param.group_id
    datelimit = param.datelimit
    confirm_password = param.confirm_password
    # [TODO]只能设置默认123456的password
    default_password = get_default_password()
    # 策略id不能为空
    sid = param.sid
    # 0. 检查用户名是否为空
    if username == "":
        raise HTTPException(status_code=400, detail="用户名不能为空")
    # 1. 检查用户名是否已经存在
    if check_username_exists(gwid, username):
        raise HTTPException(status_code=400, detail="用户名已存在")
    # 2. 检查密码是否一致
    if password != confirm_password:
        raise HTTPException(status_code=400, detail="密码不一致")
    # 3. 检查密码是否为空
    if password == "":
        raise HTTPException(status_code=400, detail="密码不能为空")
    # 4. 检查datelimit是否为空，如果不为空，则设置为2034-10-01
    if param.datelimit == "":
        datelimit = "2034-10-01"
    # 5. 检查group是否为空，如果不为空，则设置为0
    if param.group == "":
        group = "0"
    # 6.检查bandwidth是否为空，如果不为空则返回错误
    if param.sid == "":
        raise HTTPException(status_code=400, detail="策略id不能为空")
    # 增加remark - 设置策略
    remark = f"ISP-1-{sid}"
    ret = None
    user_id = gen_user_id()
    with gw_login(gwid) as sdk_obj:
        cfgname = 'wfilter-account'
        type = 'wfuser'
        value = {
            "id": user_id,
            ".anonymous": False,
            ".type": "wfuser",
            "username": username,
            "password": default_password,
            "remark": remark,
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
        # 打印value
        print("add_user: value = " + str(value))
        # 调用sdk
        result1 = sdk_obj.config_add(cfgname, type, user_id, value)
        print("result1 = " + str(result1))
        result2 = sdk_obj.config_apply()
        print("result2 = " + str(result2))
        # 最终结果赋值
        ret = value
    # 在supabase的gw_users表中，upsert一条记录
    TABLENAME = "gw_users"
    r = {
        "global_id": f"{gwid}_{value.get('id')}",
        "id": value.get("id"),
        "gwid": gwid,
        "username": value.get("username"),
        "remark": value.get("remark"),
        "pppoe": value.get("pppoe"),
        "webauth": value.get("webauth"),
        "static": value.get("static"),
        "staticip": value.get("staticip"),
        "datelimit": value.get("datelimit"),
        "logins": value.get("logins"),
        "macbound": value.get("macbound"),
        "changepwd": value.get("changepwd"),
        "online": False,
        "delete_mark": False,
        "virtual_group": group_id,
    }
    # 打印日志
    print("add_user 同步supabase: " + str(r))
    # 插入supabase记录
    response = (supabase.table(TABLENAME).upsert(r).execute())
    
    # 更新group_id
    query = UpdateUserGroupQuery(gwid=gwid, username=username, groupid=group_id)
    update_user_group_impl(query)
    return {"data": ret}

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
    
class UpdateUserDatelimitParam(BaseModel):
    gwid: str
    userid: str
    datelimit: str

@user.post("/update_user_datelimit", tags=["user"])
async def update_user_datelimit(param: UpdateUserDatelimitParam):
    """
    更新用户datelimit
    """
    gwid = param.gwid
    userid = param.userid
    datelimit = param.datelimit
    # 0. 检查gwid和username是否为空
    if gwid == "" or userid == "":
        raise HTTPException(status_code=400, detail="gwid和userid不能为空")
    # 1. 检查datelimit是否符合yyyy-mm-dd的格式
    if yyyymmdd(datelimit) is False:
        # 不是合法的日期格式，返回错误
        raise HTTPException(status_code=400, detail="日期格式错误")
    # 2. 如果日期不是未来日期，返回错误
    if is_future_date(datelimit) is False:
        raise HTTPException(status_code=400, detail="日期应该是未来日期")
    # 3. 登陆sdk, 使用config_set和config_apply来设置
    cfgname = "wfilter-account"
    section = userid
    values = {"datelimit": datelimit}
    with gw_login(gwid) as sdk_obj:
        print("update_user_datelimit: cfgname = {cfgname}, section = {section}, values = {values}")
        result = sdk_obj.config_set(cfgname, section, values)
        # 应用配置更新
        sdk_obj.config_apply()
    # 4. 更新supabase的gw_users表中的datelimit字段
    TABLE_NAME = "gw_users"
    r = {"datelimit": datelimit}
    response = (supabase.table(TABLE_NAME).update(r).eq("gwid", gwid).eq("id", userid).execute())
    return { "data": response }

class UpdateUserPasswordParam(BaseModel):
    gwid: str
    userid: str
    password: str

@user.post("/update_user_password", tags=["user"])
async def update_user_password(param: UpdateUserPasswordParam):
    """
    更新用户password
    """
    gwid = param.gwid
    userid = param.userid
    password = param.password
    # 0. 检查gwid和username是否为空
    if gwid == "" or userid == "":
        raise HTTPException(status_code=400, detail="gwid和userid不能为空")
    # 1. 检查datelimit是否符合yyyy-mm-dd的格式
    # 2. password不能为空
    if password == "":
        raise HTTPException(status_code=400, detail="密码不能为空")
    # 3. 登陆sdk, 使用config_set和config_apply来设置
    cfgname = "wfilter-account"
    section = userid
    values = {"password": password}
    with gw_login(gwid) as sdk_obj:
        print("update_user_password: cfgname = {cfgname}, section = {section}, values = {values}")
        result = sdk_obj.config_set(cfgname, section, values)
        # 应用配置更新
        sdk_obj.config_apply()
    # 4. 更新supabase的gw_users表中的datelimit字段
    # TABLE_NAME = "gw_users"
    # r = {"datelimit": password}
    # response = (supabase.table(TABLE_NAME).update(r).eq("gwid", gwid).eq("id", userid).execute())
    return { "data": True }

@user.post("/get_client_list", tags=["user"])
async def get_client_list():
    """
    获取客户列表
    输入参数：无
    输出参数: { data: [{name: "client1"}, {name: "client2"}]}
    """
    r = supabase.table("client_count").select("client_name").execute()
    data = []
    for line in r.data:
        data.append({"name": line.get("client_name")})
    return { "data": data }

@user.post("/get_fleet_list", tags=["user"])
async def get_fleet_list():
    """
    获取船舶列表
    输入参数：无
    输出参数: { data: [{name: "fleet1"}, {name: "fleet2"} ]}
    """
    r = supabase.table("fleet_count").select("fleet").execute()
    data = []
    for line in r.data:
        data.append({"name": line.get("fleet")})
    return {"data": data}