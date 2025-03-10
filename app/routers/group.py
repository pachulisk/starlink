from fastapi import APIRouter, HTTPException, BackgroundTasks
import urllib
from app.supabase import supabase, to_date,str_strip
import uuid
from ..sdk import SDK
from ..task import TaskRequest, run_single_task
from ..utils import batch_update_users_group, batch_update_gw_group, get_basic_rpc_result, gw_login, normalize_traffic, get_date_obj_from_str, get_start_of_month, get_end_of_month, get_date
from pydantic import BaseModel
from datetime import datetime
import json
import pandas as pd

group = APIRouter()

class GetGWGroupParam(BaseModel):
    gwid: str

def get_group_alias(v):
    if v is None:
        return ""
    elif v.get("alias_zh_cn") is not None:
        return v.get("alias_zh_cn")
    else:
        return v.get("alias")

def get_virtual(v):
    virtual = v.get("virtual")
    if v is None:
        return False
    elif virtual is None:
        return False
    else:
        return virtual

def get_gw_group_impl(gwid:str):
    with gw_login(gwid) as sdk_obj:
        # 读取配置文件wfilter-isp
        config_key ="wfilter-groups"
        p = sdk_obj.config_load(config_key)
        p = get_basic_rpc_result(p)
        if p is None:
            return {}
        else:
            p = p["values"]
            result = []
            for k, v in p.items():
                item_type = v.get(".type")
                if item_type == "group":
                    val = {
                        "gwid": gwid,
                        "id": v.get("id"),
                        "aliaz_en_us": v.get("aliaz_en_us"),
                        "alias_zh_cn": v.get("alias_zh_cn"),
                        "index": v.get(".index"),
                        "anonymous": v.get(".anonymous"),
                        "type": v.get(".type"),
                        "alias": get_group_alias(v),
                        "virtual": get_virtual(v),
                        "global_id": f"{gwid}_{v.get('id')}",
                        "name": v.get(".name"),
                    }
                    result.append(val)
            return result
        
@group.post("/get_gw_group", tags=["group"])
async def get_gw_group(query: GetGWGroupParam):
    gwid = query.gwid
    result = get_gw_group_impl(gwid)
    return { "data": result }

class TestBatchSyncGroups(BaseModel):
    gwid: str

@group.post("/test_batch_sync_group", tags=["test"])
async def test_batch_sync_group(query: TestBatchSyncGroups):
    gwid = query.gwid
    group_list = get_gw_group_impl(gwid)
    print("group_list = ", group_list)
    response = batch_update_gw_group(group_list)
    return { "data": response }

class ListUserWithGroupsQuery(BaseModel):
    gwid: str


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

def decode_username(username):
    """
    将格式为'CN=user1,DC=wflocal'的用户名字格式化为user1
    """
    # 1. 检测username是否为空，如果为空则返空字符串
    if username is None:
        return ""
    # 1.5 检测username是否为urlencode之后结果，如果是的话先urldecode
    # 检测username中是否包含%
    if '%' in username:
        username = urllib.parse.unquote(username) 
       
    # 2. 检测username是否包含'CN='，如果不包含则返回username
    if 'CN=' not in username:
        return username
    # 3. 使用split函数将username按照','分割成列表
    parts = username.split(',')
    # 4. 遍历列表，找到包含'CN='的项
    for part in parts:
        if 'CN=' in part:
            # 5. 使用split函数将该项按照'='分割成列表，取第二项作为用户名字
            username = part.split('=')[1]
            return username
    # 都没有匹配，返回空字符串
    return ""

async def list_user_with_groups_impl(gwid: str):
    #1. 使用get_gw_group_impl获取已有的组信息
    group_list = get_gw_group_impl(gwid)
    #2. 设置返回值为data,一个空数组
    data = []
    # 对于group_list中每个组信息遍历
    for group in group_list:
        global_id = group.get("global_id")
        alias = get_group_alias(group)
        group_id = group.get("id")
        # 打印debug信息
        print(f"list_user_with_groups: Processing group: {global_id}, alias: {alias}, id: {group_id}")
        # 使用list_virtual_group来获取组关联的用户
        with gw_login(gwid) as sdk_obj:
            result = sdk_obj.list_virtual_group(group_id)
            r = get_basic_rpc_result(result)
            r = str_strip(r.get("result"))
            r = json.loads(r)
            # 打印r的信息
            print(f"list_user_with_groups: Users in group {global_id}: {r}")
            # 获取users
            users = r.get("users")
            # 对于users数组中的每一个项遍历
            for user in users:
                userid = user.get("user")
                userid = decode_username(userid)
                u = {
                    "gwid": gwid,
                    "userid": userid,
                    "groupid": group_id,
                    "group_global_id": global_id,
                    "group_name": alias
                }
                data.append(u)
    # 打印data的信息
    print(f"list_user_with_groups: Final data: {data}")
    return data

# 获取所有的用户和分组
@group.post("/list_user_with_groups", tags=["group"])
async def list_user_with_groups(query: ListUserWithGroupsQuery):
    """
    列出网关下的所有组(group)，然后列出每一个组关联的用户
    输入参数: query.gwid: 网关ID
    返回值: 一个列表data, 每一项包括用户的id、用户所在组的id，用户所在组的名称
    """
    gwid = query.gwid
    #1. 使用get_gw_group_impl获取已有的组信息
    data = await list_user_with_groups_impl(gwid)
    return { "data": data }

class SyncUserVirtualGroupQuery(BaseModel):
    gwid: str

# sync_user_virtual_group 将virtual_group的用户关系内容，同步到gw_users表中
@group.post("/sync_user_virtual_group", tags=["group"])
async def sync_user_virtual_group(query: SyncUserVirtualGroupQuery):
    gwid = query.gwid
    # 1. 调用list_user_with_groups获取用户和group之间的关系
    user_list = await list_user_with_groups_impl(gwid)
    # 1.5 打印user_list
    print("sync_user_virtual_group: user_list = f{user_list}")
    # 2. 调用batch_update_users_group
    result = batch_update_users_group(user_list)
    print(f"sync_user_virtual_group: batch update users group, result is {result}")
    return {"data": "success"}
