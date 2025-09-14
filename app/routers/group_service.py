import urllib
from fastapi import HTTPException
from pydantic import BaseModel
from ..utils import is_empty, batch_update_users_group, batch_update_gw_group, get_basic_rpc_result, gw_login, normalize_traffic, get_date_obj_from_str, get_start_of_month, get_end_of_month, get_date
from app.supabase import supabase, to_date


def encode_username(username, useUrlEncode=True):
    """
    将格式为user1的用户名称，转化为'CN=user1,DC=wflocal'
    """
    # 1. 检测username是否为空，如果为空则返回空字符串
    if username is None:
        return ""
    # 2. 拼接'CN=xxx,DC=wflocal'
    target = f"CN={username},DC=wflocal"
    # 3. 如果useUrlEncode为True，则返回urlencode以后的结果，否则直接返回：
    if useUrlEncode:
        return urllib.parse.quote(target)
    else:
        return target
    
class RemoveUserGroupQuery(BaseModel):
    gwid: str
    username: str

def remove_user_group_impl(query:RemoveUserGroupQuery):
    """
    将用户移除出所有虚拟组
    输入1：gwid=网关id
    输入2：username=用户名
    """
    gwid = query.gwid
    username = query.username
    # 检查gwid以及username是否为空
    if is_empty(gwid) or is_empty(username):
        raise HTTPException(status_code=400, detail="[remove_user_group_impl]:invalid gwid or username")
    # 记录日志
    print(f"[remove_user_group_impl]: gwid = {gwid}, username = {username}")
    with gw_login(gwid) as sdk_obj:
        print(f"[remove_user_group_impl]: encode_username = f{username}")
        result = sdk_obj.rm_virtual_group(encode_username(username))
        print(f"[remove_user_group_impl]: rm_virtual_group result = {result}")
        resp = clear_supabase_user_virtual_group(gwid, username)
        print(f"[remove_user_group_impl]: clear supabase user virtual group, gwid = {gwid}, username = {username}, resp = {str(resp)}")


def clear_supabase_user_virtual_group(gwid: str, username: str):
    """
    在supabase的gw_users表中，清理指定用户的virtual_group
    """
    TABLE_NAME = "gw_users"
    data = {
        "virtual_group": ""
    }
    response = (
        supabase.table(TABLE_NAME).update(data).eq("username", username).eq("gwid", gwid).execute()
    )
    return response


class UpdateUserGroupQuery(BaseModel):
    gwid: str
    username: str
    groupid: str | None

def update_user_group_impl(query: UpdateUserGroupQuery):
    gwid = query.gwid
    username = query.username
    groupid = query.groupid
    print(f"update_user_group: gwid = {gwid}, username = {username}, groupid = {groupid}")
    action_remove = False
    if groupid is None:
        action_remove = True
    if len(groupid) <= 0:
        action_remove = True

    with gw_login(gwid) as sdk_obj:
        # 1. 首先调用remove_virtual_group移除用户的所有组，然后apply
        result = sdk_obj.rm_virtual_group(encode_username(username))
        print(f"update_user_group: rm_virtual_group result = {result}")
        
        # 3. 然后调用add_virtual_group增加指定组，然后apply
        # 时间为30000 * 1440
        if action_remove is False:
            timeout = 3000 * 1440
            # name = encode_username(username)
            name = f"CN={username},DC=wflocal"
            print(f"start calling add_virtual_group, groupid = {groupid}, name = {name}, timeout = {timeout}")
            result = sdk_obj.add_virtual_group(groupid, name, timeout)
            print(f"update_user_group: add_virtual_group result = {result}")
        # 4. 将组内容增加到gw_users的virtual_group列
        TABLENAME = "gw_users"
        global_group_id = f"{gwid}_{groupid}"
        if action_remove is True:
            # 如果是删除，则组id为None
            global_group_id = None
        item = {
            "virtual_group": global_group_id
        }
        response = (
            supabase.table(TABLENAME).update(item).eq("username", username).eq("gwid", gwid).execute()
        )
        print(f"update_user_group: result = {response}")
        return {"data": result}