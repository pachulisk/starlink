from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.supabase import supabase, to_date
import uuid
from ..sdk import SDK
from ..task import TaskRequest, run_single_task
from ..utils import batch_update_gw_group, get_basic_rpc_result, gw_login, normalize_traffic, get_date_obj_from_str, get_start_of_month, get_end_of_month, get_date
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