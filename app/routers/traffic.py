from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile
import luigi
import csv
import codecs
from app.supabase import supabase, to_date
import uuid
from ..sdk import SDK
from ..task import TaskRequest, run_single_task
from ..utils import get_ratio_by_gwid, get_digits, str2float, get_unit_from_format, parse_int, is_empty, is_not_empty, batch_update_gw_strategy, get_basic_rpc_result, gw_login, normalize_traffic, get_date_obj_from_str, get_start_of_month, get_end_of_month, get_date
from pydantic import BaseModel
from datetime import datetime, timedelta
import json
import pandas as pd
from ..tasks.sync_user import SyncGwUsers, ReadGwUsers 
from .group_service import remove_user_group_impl, RemoveUserGroupQuery
traffic = APIRouter()


async def perform_task(task_str: str):
  print("perform_task_celery: {}".format(task_str))
  task = json.loads(task_str)
  task_id = str(uuid.uuid4())
  command = task.get("cmd")
  data = task.get("data")
  print(f"task_id: {task_id}, command: {command}, data: {data}")
  tr = TaskRequest(id=task_id, command=command, data=data)
  res = await run_single_task(tr)
  return res

# 同步网关的流量数据到supabase
#!/bin/bash
# curl -X 'POST' \
#   'http://107.172.190.217:8000/sync_task_celery' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "table_name": "hourreport",
#   "gwid": "97935833-c028-4f7b-ad5f-26f296cf935a",
#   "column": "happendate"
# }'

class GetGWTrafficParam(BaseModel):
    gwid: str
    start_date: str | None = None
    end_date: str | None = None
    format: str | None = None

def calculate_start_and_end_str(start_date, end_date):
    start_time_str = ""
    end_time_str = ""
    # 如果start_date和end_date都为空，则将start_time_str设置为月初，end_time_str设置为月末
    if start_date is None and end_date is None:
        start_time_str = get_start_of_month(datetime.today(), True)
        end_time_str = get_end_of_month(datetime.today(), True)
    elif start_date is not None and end_date is None:
        # 开始日期不空，结束日期为空，则为开始日期-现在
        start_d = get_date_obj_from_str(start_date)
        start_time_str = get_date(start_d, True)
        end_time_str = get_date(datetime.today(), True)
    elif start_date is not None and end_date is not None:
        # 开始日期和结束日期都不空
        start_time_str = get_date(get_date_obj_from_str(start_date), True)
        end_time_str = get_date(get_date_obj_from_str(end_date), True)
    else:
        raise(HTTPException(status_code=400, detail="start_date and end_date must be provided"))
    return [start_time_str, end_time_str]

def get_data_with_format(data, format):
    if format == "csv":
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    else:
        return data
    
def get_traffic_total(data):
    """
    data是一个list，对list中所有item中的total项目加和，获取traffic total
    """
    total = 0
    for item in data:
        sub = parse_int(item.get("total", 0))
        total += sub
    return total
    
@traffic.post("/get_bandwidth_strategy_impl", tags=["traffic"])
def get_bandwidth_strategy_impl(gwid:str):
    with gw_login(gwid) as sdk_obj:
        # 读取配置文件wfilter-isp
        config_key ="wfilter-isp"
        p = sdk_obj.config_load(config_key)
        p = get_basic_rpc_result(p)
        if p is None:
            return { "data": [] }
        else:
            p = p["values"]
            result = []
            for k, v in p.items():
                item_type = v.get(".type")
                if item_type == "bandwidth":
                    val = {
                        "gwid": gwid,
                        "period": v.get("period"),
                        "threshold": v.get("threshold"),
                        "exceed": v.get("exceed"),
                        "id": v.get("id"),
                        "remark": v.get("remark")
                    }
                    result.append(val)
            return result

def aggregate_gw_user_traffic_view(gwid, response, format):
    """
    根据gw_user_traffic_view的结果，归集汇总流量数据。
    gw_user_traffic_view数据包括的列: happendate, gwid, up, down
    返回的数据包括的列: up, down, total, happendate
    """
    # get_unit_from_format归集结果单位: GB
    unit = get_unit_from_format(format)
    ratio = get_ratio_by_gwid(gwid)
    print(f"[aggregate_gw_user_traffic_view]: gwid = {gwid}, ratio = {ratio}, unit = {unit}")
    if len(response.data) <= 0:
        return {"data": get_data_with_format([], format), "total": get_traffic_total([])}
    else:
        # 最终返回的数据列表
        lst = []
        total_bytes = 0
        for d in response.data:
            happendate = d.get("happendate")
            date = to_date(happendate)
            date_str = date.strftime("%Y-%m-%d")
            upbytes = d.get("up")
            downbytes = d.get("down")
            local_total = int(upbytes) + int(downbytes)
            total_bytes = total_bytes + int(upbytes) + int(downbytes)
            lst.append({
                "up": normalize_traffic(upbytes, unit, ratio),
                "down": normalize_traffic(downbytes, unit, ratio),
                "total": normalize_traffic(local_total, unit, ratio),
                "happendate": date_str
            })
        return {"data": get_data_with_format(lst, format), "total": normalize_traffic(total_bytes, unit, ratio)}

@traffic.post("/get_gw_traffic", tags=["traffic"])
async def get_gw_traffic(query: GetGWTrafficParam):
    """
    get_gw_traffic：获取网关流量
    输入：gwid=网关id，如果为空则获取所有网关流量数据
    输入:start_date=开始日期，yyyy-mm-dd
    输入:end_date=结束日期，yyyy-mm-dd
    输入：format=返回数据格式，csv/json，默认json
    输出：流量数据，包含up, down, total, happendate字段
    """
    # 1. 获取gwid, user和日期date
    # 2. 如果date为空，则默认查询时间设置为本年本月；否则按照date查询
    # 3. 从supabase查询hourreport表，筛选日期在date的月份范围内
    gwid = query.gwid
    start_date = query.start_date
    end_date = query.end_date
    times = calculate_start_and_end_str(start_date, end_date)
    start_time_str = times[0]
    end_time_str = times[1]
    format = query.format
    
    TABLE_NAME = "gw_user_traffic_view"
    response = None
    if is_empty(gwid):
        response = supabase.table(TABLE_NAME).select("*").gte("happendate", start_time_str).lte("happendate", end_time_str).execute()
    else:
        response = (supabase
            .table(TABLE_NAME)
            .select("*")
            .eq("gwid", gwid)
            .gte("happendate", start_time_str)
            .lte("happendate", end_time_str)
            .execute())
    # 4. 归集结果
    print(f"[DEBUG][get_gw_traffic]:response.data = f{response.data}")
    return aggregate_gw_user_traffic_view(gwid, response, format)

class GetUserTrafficParam(BaseModel):
    gwid: str
    start_date: str | None = None
    end_date: str | None = None
    format: str | None = None
    user: str | None = None

@traffic.post("/get_user_traffic", tags=["traffic"])
async def get_user_traffic(query: GetUserTrafficParam):
    # 1. 获取gwid, user和日期date
    # 2. 如果date为空，则默认查询时间设置为本年本月；否则按照date查询
    # 3. 从supabase查询acctreport表，筛选日期在date的月份范围内
    gwid = query.gwid
    user = query.user
    start_date = query.start_date
    end_date = query.end_date
    times = calculate_start_and_end_str(start_date, end_date)
    start_time_str = times[0]
    end_time_str = times[1]
    format = query.format
    TABLE_NAME = "acctreport_view"
    response = None
    ratio = get_ratio_by_gwid(gwid)
    # fix: 根据format的格式来选择使用的单位
    # 当格式为csv格式的时候，使用GB作为单位
    # 当格式是其他的时候，使用None(bytes)作为单位
    unit = get_unit_from_format(format)
    if is_empty(gwid):
        response = (
            supabase.table(TABLE_NAME)
            .select("*")
            .gte("happendate", start_time_str)
            .lte("happendate", end_time_str)
            .order("happendate", desc=False)
            .execute()
        )
    else:
        if is_empty(user):
            response = (supabase
                .table(TABLE_NAME)
                .select("*")
                .eq("gwid", gwid)
                .gte("happendate", start_time_str)
                .lte("happendate", end_time_str)
                .order("happendate", desc=False)
                .execute())
        else:
            response = (supabase
                .table(TABLE_NAME)
                .select("*")
                .eq("gwid", gwid)
                .eq("acct", user)
                .gte("happendate", start_time_str)
                .lte("happendate", end_time_str)
                .order("happendate", desc=False)
                .execute())
    # 4. 归集结果
    if len(response.data) <= 0:
        return {"data": get_data_with_format([], format), "total": get_traffic_total([])}
    else:
        list = []
        totalbytes = 0
        for d in response.data:
            up = d["uptraffic"]
            down = d["downtraffic"]
            totalbytes = totalbytes + float(up) + float(down)
            list.append({
                "acct": d["acct"],
                "up": normalize_traffic(up, unit, ratio),
                "down": normalize_traffic(down, unit, ratio),
                "total": f"{normalize_traffic(float(up) + float(down), unit, ratio)}",
                "happendate": to_date(d["happendate"])
            })
        return {"data": get_data_with_format(list, format), "total": normalize_traffic(totalbytes, unit, ratio)}
    
class GetbandwidthStrategyParam(BaseModel):
    gwid: str

@traffic.post("/get_bandwidth_strategy", tags=["traffic"])
async def get_bandwidth_strategy(query: GetbandwidthStrategyParam):
    """
    获取带宽策略
    输入：gwid=网关id，可选。如果网关id不为空，则查询具体网关上的带宽策略；如果为空则返回所有网关的带宽策略。
    输出: { data: [r1, r2, ..., rn] }
    """
    gwid = query.gwid
    result = []
    if is_not_empty(gwid):
        result = get_bandwidth_strategy_impl(gwid)
    else:
        # 从gw_bandwidth_strategy表中读取所有的策略
        TABLE_NAME = "gw_bandwidth_strategy"
        r = supabase.table(TABLE_NAME).select("*").execute()
        for item in r.data:
            gw_id = item.get("gwid")
            val = {
                "gwid": gw_id,
                "period": item.get("period"),
                "threshold": item.get("threshold"),
                "exceed": item.get("exceed"),
                "id": item.get("id"),
                "remark": item.get("remark")
            }
            result.append(val)
    return { "data": result }

class TestBatchSyncStrategy(BaseModel):
    gwid: str
    
@traffic.post("/test_batch_sync_strategy", tags=["test"])
async def test_batch_sync_strategy(query: TestBatchSyncStrategy):
    gwid = query.gwid
    strategy_list = get_bandwidth_strategy_impl(gwid)
    print("strategy_list = ", strategy_list)
    response = batch_update_gw_strategy(strategy_list)
    return { "data": response }

@traffic.post("/batch_remove_strategy", tags=["traffic"])
async def batch_remove_strategy(gwid:str):
    """
    批量删除策略
    """
    # 如果gwid为空，则抛出异常
    if is_empty(gwid):
        raise HTTPException(status_code=400, detail="gwid is required")
    TABLE_NAME = "gw_bandwidth_strategy"
    response = supabase.table(TABLE_NAME).delete().eq("gwid", gwid).execute()
    return { "data": response }

@traffic.post("/batch_sync_strategy", tags=["traffic"])
async def batch_sync_strategy(query: TestBatchSyncStrategy):
    gwid = query.gwid
    strategy_list = get_bandwidth_strategy_impl(gwid)
    print("strategy_list = ", strategy_list)
    # 在同步之前，先去掉现有的strategy
    await batch_remove_strategy(gwid)
    response = batch_update_gw_strategy(strategy_list)
    return { "data": response }

@traffic.post("/test_date_and_hour", tags=["test"])
async def test_date_and_hour():
    TABLE_NAME = "test_date_and_hour"
    test_date = "2025-04-01"
    test_hour = "17"
    date_format = "%Y-%m-%d"
    test_datetime = datetime.strptime(test_date, date_format)
    print(test_datetime)
    new_time = test_datetime + timedelta(hours=int(test_hour))
    supabase.table(TABLE_NAME).insert({
        "timestamp": f"{new_time}"
    }).execute()
    return {"dt": test_datetime, "ndt": new_time}


class UpdateUserTrafficStrategyQuery(BaseModel):
    gwid: str
    userid: str
    sid: str    

def build_remark(sid):
    return f"ISP-1-{sid}"

def extract_remark(s):
    """
    从符合"ISP-1-{xxx}"格式的字符串中提取{xxx}部分的ID
    
    参数:
        s (str): 符合格式的输入字符串
    
    返回:
        str: 提取出的ID部分，如果格式不符合则返回空字符串
    """
    # 检查字符串是否以"ISP-1-"开头
    prefix = "ISP-1-"
    if s.startswith(prefix):
        # 提取前缀之后的部分作为ID
        return s[len(prefix):]
    else:
        # 如果格式不符合，返回空字符串或可以根据需要抛出异常
        return s

async def insert_user_strategy_logs(gwid, userid, old_sid, new_sid):
    """
    向user_strategy_logs表中，插入一条更新字段
    """
    # 检查gwid是否为空
    if is_empty(gwid):
        # 如果gwid为空，抛出异常
        raise HTTPException(status_code=400, detail="[insert_user_strategy_logs]: gwid cannot be empty")
    # 检查userid是否为空
    if is_empty(userid):
        raise HTTPException(status_code=400, detail="[insert_user_strategy_logs]: userid cannot be empty")
    # 检查new_sid是否为空
    if is_empty(new_sid):
        raise HTTPException(status_code=400, detail="[insert_user_strategy_logs]: new_sid cannot be empty")
    data = {
        "old_sid": old_sid,
        "new_sid": new_sid,
        "gwid": gwid,
        "user_id": userid 
    }
    print(f"[insert_user_strategy_logs]: data = {str(data)}")
    TABLE_NAME = "user_strategy_logs"
    response = supabase.table(TABLE_NAME).insert(data).execute()
    print(f"[insert_user_strategy_logs]: result = {str(response)}")
    return { "data": response }


async def fetch_user_sid(gwid:str, userid:str):
    TABLE_NAME = "gw_users"
    global_id = f"{gwid}_{userid}"
    response = (supabase.table(TABLE_NAME).select("*").eq("global_id", global_id)).execute()
    if len(response.data) > 0:
        return {
            "sid": extract_remark(response.data[0].get("remark")),
            "username": response.data[0].get("username")
        }
    else:
        return {"sid": "", "username": ""}

async def update_user_traffic_strategy_impl(gwid:str, userid:str, sid: str):
    with gw_login(gwid) as sdk_obj:
        # $values = "{\"enabled\":\"false\"}";    //把规则状态改成不启用
        # $result = $ngf->config_set( "wfilter-appcontrol",  "rule12345", $values );
        # echo "config_set:$result";
        # config_set(self, cfgname, section, values):
        cfgname = "wfilter-account"
        section = userid
        values = {"remark": build_remark(sid)}
        result = sdk_obj.config_set(cfgname, section, values)
        # 应用配置更新
        sdk_obj.config_apply()

        # 获取用户的sid
        # old_sid = await fetch_user_sid(gwid, userid)
        user_data = await fetch_user_sid(gwid, userid)
        username = user_data["username"]
        old_sid = user_data["sid"]
        # 更新supabase上的gw_users表中，对应的用户名称
        # global_id = gwid + "_" + userid
        global_id = f"{gwid}_{userid}"
        TABLE_NAME = "gw_users"
        kv = {
            "remark": build_remark(sid)
        }
        # 使用global_id来更新supabase
        response = (supabase.table(TABLE_NAME).update(kv).eq("global_id", global_id)).execute()
        print("result = ", result)
        print("response = ", response)
        
        # 插入用户策略更新日志
        await insert_user_strategy_logs(gwid, userid, old_sid, sid)
        # 将用户的虚拟组清理
        print("[update_user_traffic_strategy_impl] remove_user_group, gwid = {gwid}, username = {username}")
        await remove_user_group_impl(RemoveUserGroupQuery(gwid=gwid, username=username))
        # 启动luigi任务，同步用户表到supabase
        tasks = [
            SyncGwUsers(gwid=gwid),
        ]
        luigi.build(tasks, local_scheduler=True)
        return { "data": result }

@traffic.post("/update_user_traffic_strategy", tags=["traffic"])
async def update_user_traffic_strategy(query: UpdateUserTrafficStrategyQuery):
    gwid = query.gwid
    userid = query.userid
    sid = query.sid
    return await update_user_traffic_strategy_impl(gwid, userid, sid)
    
class SetUserMonthlyStrategyQuery(BaseModel):
    gwid: str
    userid: str
    sid: str

@traffic.post("/set_user_monthly_strategy", tags=["traffic"])
async def set_user_monthly_strategy(query: SetUserMonthlyStrategyQuery):
    """
    设置用户原始月套餐，参数包括：
    - gwid: 网关id，必填，【这里多租户要注意】
    - userid: 用户id，必填
    - sid: 策略id，必填
    """
    gwid = query.gwid
    userid = query.userid
    sid = query.sid
    if is_empty(gwid):
        raise HTTPException(status_code=400, detail="[set_user_monthly_strategy]: gwid cannot be empty")
    if is_empty(userid):
        raise HTTPException(status_code=400, detail="[set_user_monthly_strategy]: userid cannot be empty")
    if is_empty(sid):
        raise HTTPException(status_code=400, detail="[set_user_monthly_strategy]: sid cannot be empty")
    #[TODO]: 检查gwid、userid和sid是否存在，暂时先不检查
    # 表名称为user_monthly_strategy
    TABLE_NAME = "user_monthly_strategy"
    # 拼接global_id
    global_id = f"{gwid}_{userid}"
    # 设置data
    data = {
        "global_id": global_id,
        "gwid": gwid,
        "userid": userid,
        "sid": sid
    }
    print(f"[set_user_monthly_strategy]: data = {str(data)}")
    response = supabase.table(TABLE_NAME).upsert(data).execute()
    print(f"[set_user_monthly_strategy]: resp = {str(response)}")
    return {"success":"True", "message": f"{str(response)}"}

class SyncUserMonthlyStrategyQuery(BaseModel):
    gwid: str
    userid: str

async def sync_user_monthly_strategy_impl(gwid:str, userid:str):
    if is_empty(gwid):
        raise HTTPException(status_code=400, detail="[sync_user_monthly_strategy]gwid不能为空") 
    if is_empty(userid):
        raise HTTPException(status_code=400, detail="[sync_user_monthly_strategy]userid不能为空") 
    # 相关表：user_monthly_strategy
    TABLE_NAME = "user_monthly_strategy"
    resp = supabase.table(TABLE_NAME).select("*").eq("gwid", gwid).eq("userid", userid).execute()
    if len(resp.data) <= 0:
        raise HTTPException(status_code=400, detail="[sync_user_monthly_strategy]没有相关记录")
    sid = resp.data[0]["sid"]
    return await update_user_traffic_strategy_impl(gwid, userid, sid)

class SyncGatewayUserMonthlyStrategyQuery(BaseModel):
    gwid: str

@traffic.post("/sync_gateway_user_monthly_strategy", tags=["traffic"])
async def sync_gateway_user_monthly_strategy(query: SyncGatewayUserMonthlyStrategyQuery):
    """
    同步网关上所有用户的初始月流量。
    输入参数:
    gwid: 网关id
    逻辑：
    - 查询user_monthly_strategy表，根据给定的gwid进行查询
    - 遍历查询出的所有gwid、userid
    - 调用sync_user_monthly_strategy_impl，挨个同步
    """
    gwid = query.gwid
    if is_empty(gwid):
        raise HTTPException(status_code=400, detail="[sync_gateway_user_monthly_strategy]gwid不能为空") 
    TABLE_NAME = "user_monthly_strategy"
    resp = supabase.table(TABLE_NAME).select("*").eq("gwid", gwid).execute()
    ret = []
    for row in resp.data:
        userid = row["userid"]
        r = await sync_user_monthly_strategy_impl(gwid, userid)
        ret.append({
            "gwid": gwid,
            "userid": userid,
            "result": r
        })
    return {"data": ret }


@traffic.post("/sync_user_monthly_strategy", tags=["traffic"])
async def sync_user_monthly_strategy(query: SyncUserMonthlyStrategyQuery):
    """
    sync_user_monthly_strategy
    同步给定网关上用户的每月初始策略，到当前用户的策略。
    输入:
    - gwid: 网关id
    - userid: 用户id
    """
    gwid = query.gwid
    userid = query.userid
    return await sync_user_monthly_strategy_impl(gwid, userid)

class GetUserMonthlyStrategyQuery(BaseModel):
    gwid: str
    userid: str

@traffic.post("/get_user_monthly_strategy", tags=["traffic"])
async def get_user_monthly_strategy(query: GetUserMonthlyStrategyQuery):
    gwid = query.gwid
    userid = query.userid
    TABLE_NAME = "user_monthly_strategy_view"
    if is_empty(gwid):
        # 选出所有的网关的所有用户(多租户注意)
        resp = supabase.table(TABLE_NAME).select("*").execute()
        return { "data": resp }
    elif is_empty(userid):
        # 选出该网关下的所有用户
        resp = supabase.table(TABLE_NAME).select("*").eq("gwid", gwid).execute()
        return {"data": resp}
    else:
        resp = supabase.table(TABLE_NAME).select("*").eq("gwid", gwid).eq("userid", userid).execute()
        return {"data": resp}

class GetTrafficForUserQuery(BaseModel):
    username: str
    gwid: str
    date: str

def get_mb_by_1024(traffic):
    return get_digits(traffic/1024/1024,2)

@traffic.post("/get_traffic_for_user", tags=["traffic"])
async def get_traffic_for_user(query: GetTrafficForUserQuery):
    """
    用来获取某天acctreport中的某个用户的流量
    """
    # 获取日期并验证日期不是空
    date = query.date
    if not date:
        raise HTTPException(status_code=400, detail="Date cannot be empty")
    # 获取网关并验证网关不是空
    gwid = query.gwid
    if not gwid:
        raise HTTPException(status_code=400, detail="Gwid cannot be empty")
    # 获取用户名并验证用户名不是空
    username = query.username
    if not username:
        raise HTTPException(status_code=400, detail="Username cannot be empty")
    # 将用户名补全为CN%3xxxx%2cDC%3dwflocal
    username = f"CN%3d{username}%2cDC%3dwflocal"
    # 进行supabase表的acctreport进行查询
    times = calculate_start_and_end_str(date, date)
    date_str = times[0]
    # 返回查询的uptraffic和downtraffic
    TABLE_NAME="acctreport"
    response = (supabase
            .table(TABLE_NAME)
            .select("*")
            .eq("gwid", gwid)
            .eq("acct", username)
            .gte("happendate", date_str)
            .lte("happendate", date_str)
            .execute())
    up = 0.0
    down = 0.0
    total = 0.0
    print(f"[get_traffic_for_user], username=f{username}, date=f{date}, gwid=f{gwid}, response=f{response.data}")
    for item in response.data:
        uptraffic = str2float(item.get("uptraffic", "0"))
        downtraffic = str2float(item.get("downtraffic", "0"))
        up += uptraffic
        down += downtraffic
        total += uptraffic + downtraffic
    return {
        "uptraffic": get_mb_by_1024(up),
        "downtraffic": get_mb_by_1024(down),
        "totaltraffic": get_mb_by_1024(total)
    }



@traffic.post("/batch_get_traffic_for_user", tags=["traffic"])
async def batch_get_traffic_for_user(gwid: str, info: UploadFile):
    """
    用来批量获取某天acctreport中的多个用户的流量
    入参1: 网关名称 gwid
    入参2: info，由csv分割的多行，每行第一列是时间，第二列是账号。
    返回值: outputs， 由csv分割的多行，每行第一列是时间，第二列是账号，第三列是uptraffic, 第四列是downtraffic, 第五列是totaltraffic。
    """
    # 获取网关并验证网关不是空
    if not gwid:
        raise HTTPException(status_code=400, detail="Gwid cannot be empty")
    # 获取info并验证info不是空
    if not info:
        raise HTTPException(status_code=400, detail="Info cannot be empty")
    # info是一个UploadFile文件，从文件中读取csv
    csvReader = csv.DictReader(codecs.iterdecode(info.file, 'utf-8'))
    
    print(f"[batch_get_traffic_for_user], gwid=f{gwid}")
    # 解析info
    # lines = info.split('\n')
    outputs = []
    # 解析csv，获取多行
    for row in csvReader:
        parts = row
        if len(parts) != 2:
            raise HTTPException(status_code=400, detail="Info format is incorrect, should be 'date,username'")
        # 对于每一行，获取第一列为时间，第二列为账号，调用get_traffic_for_user
        date = parts["date"]
        username = parts["username"]
        result = await get_traffic_for_user(GetTrafficForUserQuery(date=date, gwid=gwid, username=username))
        result['date'] = date
        result['username'] = username
        outputs.append(result)
    info.file.close()
    return {"data": get_data_with_format(outputs, 'csv')}

class GetUserTrafficRechargeBillQuery(BaseModel):
    gwid: str
    userid: str
    date: str

@traffic.post("/get_user_traffic_recharge_bill", tags=["traffic"])
async def get_user_traffic_recharge_bill(query: GetUserTrafficRechargeBillQuery):
    """
    get_user_traffic_recharge_bill，获取用户充值历史记录
    入参1： 必选，gwid，网关的id
    入参2：必选，userid，用户的id
    入参3：必选，date， 当前的年月，格式为yyyy-mm
    返回值：data是一个数组，包含若干记录，每一条记录中包含:
    traffic_increment: 充值流量，数字，单位GB
    username： 用户名
    remark: 总流量，string，格式为“xxGB”
    """
    gwid = query.gwid
    userid = query.userid
    d = query.date
    # 检查gwid不为空
    if is_empty(gwid):
        raise HTTPException(status_code=400, detail="[get_user_traffic_recharge_bill] gwid cannot be empty")
    # 检查userid不为空
    if is_empty(userid):
        raise HTTPException(status_code=400, detail="[get_user_traffic_recharge_bill] userid cannot be empty")
    d = get_date_obj_from_str(d)
    start_time_str = get_start_of_month(d, True)
    end_time_str = get_end_of_month(d, True)
    TABLE_NAME = "user_strategy_logs_view"
    response = (supabase
        .table(TABLE_NAME)
        .select("*")
        .eq("gwid", gwid)
        .eq("userid", userid)
        .gte("record_time", start_time_str)
        .lte("record_time", end_time_str)
        .execute())
    return {"data": response.data}

class GetGatewayMonthlyTrafficRechargeBillQuery(BaseModel):
    gwid: str
    date: str

@traffic.post("/get_gateway_monthly_traffic_recharge_bill", tags=["traffic"])
async def get_gateway_monthly_traffic_recharge_bill(query: GetGatewayMonthlyTrafficRechargeBillQuery):
    """
    get_gateway_monthly_traffic_recharge_bill，获取整个网关所有用户在某月的充值记录
    入参1： 必选，gwid，网关的id
    入参2：必选，date， 当前的年月，格式为yyyy-mm
    返回值：data是一个数组，包含若干记录，每一条记录中包含:
    traffic_increment: 充值流量，数字，单位GB
    username： 用户名
    remark: 总流量，string，格式为“xxGB”
    """
    gwid = query.gwid
    d = query.date
    # 检查gwid不为空
    if is_empty(gwid):
        raise HTTPException(status_code=400, detail="[get_user_traffic_recharge_bill] gwid cannot be empty")
    d = get_date_obj_from_str(d)
    start_time_str = get_start_of_month(d, True)
    end_time_str = get_end_of_month(d, True)
    TABLE_NAME = "user_strategy_logs_view"
    response = (supabase
        .table(TABLE_NAME)
        .select("*")
        .eq("gwid", gwid)
        .gte("record_time", start_time_str)
        .lte("record_time", end_time_str)
        .execute())
    return {"data": response.data}