import asyncio
from fastapi import APIRouter, HTTPException, BackgroundTasks
import urllib
from app.supabase import supabase, to_date
import uuid
from ..sdk import SDK
from ..task import TaskRequest, run_single_task
from ..utils import str_strip, is_empty, is_not_empty, batch_update_users_group, batch_update_gw_group, get_basic_rpc_result, gw_login, get_date_obj_from_str, get_start_of_month, get_end_of_month, get_date
from pydantic import BaseModel
from datetime import datetime
import json
import pandas as pd
from .group_service import UpdateUserGroupQuery, update_user_group_impl

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
    """
    get_gw_group: 获取网关的所属组信息。
    输入: gwid=网关id，可选。如果gwid有值则返回对应的组参数；如果gwid没有值，则返回所有网关的组参数。
    输出：{ data: [item1, item2, ..., itemx ]}
    """
    gwid = query.gwid
    result = []
    if is_not_empty(gwid):
        result = get_gw_group_impl(gwid)
    else:
        # 使用supabase获取所有的group和网关
        r = supabase.table("gw_groups").select("*").execute()
        for item in r.data:
            gw_id = item.get("gwid")
            val = {
                "gwid": gw_id,
                "id": item.get("id"),
                "aliaz_en_us": item.get("aliaz_en_us"),
                "alias_zh_cn": item.get("alias_zh_cn"),
                "index": item.get("index"),
                "anonymous": item.get("anonymous"),
                "type": item.get("type"),
                "alias": get_group_alias(item),
                "virtual": get_virtual(item),
                "global_id": f"{gw_id}_{item.get('id')}",
                "name": item.get("name"),
            }
            result.append(val)
    return { "data": result }

class TestBatchSyncGroups(BaseModel):
    gwid: str

async def batch_sync_group_task(gwid: str):
    group_list = get_gw_group_impl(gwid)
    print("group_list = ", group_list)
    response = batch_update_gw_group(group_list)
    return { "data": response }

@group.post("/test_batch_sync_group", tags=["test"])
async def test_batch_sync_group(query: TestBatchSyncGroups):
    gwid = query.gwid
    return await batch_sync_group_task(gwid)

class ListUserWithGroupsQuery(BaseModel):
    gwid: str



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

async def sync_user_virtual_group_task(gwid: str):
    user_list = await list_user_with_groups_impl(gwid)
    # 1.5 打印user_list
    print(f"sync_user_virtual_group: user_list = {user_list}")
    # 2. 调用batch_update_users_group
    result = batch_update_users_group(user_list)
    print(f"sync_user_virtual_group: batch update users group, result is {result}")
    return {"data": "success"}


# sync_user_virtual_group 将virtual_group的用户关系内容，同步到gw_users表中
@group.post("/sync_user_virtual_group", tags=["group"])
async def sync_user_virtual_group(query: SyncUserVirtualGroupQuery):
    gwid = query.gwid
    res = await asyncio.gather(batch_sync_group_task(gwid), sync_user_virtual_group_task(gwid))
    return {"data": "success"}



@group.post("/update_user_group", tags=["group"])
async def update_user_group(query: UpdateUserGroupQuery):
    """
    更新用户所属组
    输入参数: 
    1. gwid: 必选，网关id
    2. username: 必选，用户名称
    3. groupid: 可选，网关的id(例如punish)，如果groupid为空，则将用户移除出所有的组
    返回值：
    1. {”data”: “success”}
    """
    return update_user_group_impl(query)

