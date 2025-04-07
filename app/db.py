from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile, File
from pydantic import BaseModel
from app.utils import get_gateway_by_id
from app.sdk import SDK
from app.supabase import supabase, get_supabase_table_latest_row, build_sql_for_latest_row, get_db_for_table, meta_for_table_name, formalize_supabase_datetime, build_dict_from_line, to_date, str_strip
from .task import post_single_task
import uuid
from datetime import datetime, date, timedelta
import time
import urllib.parse
import calendar
from .celery_app import perform_task_celery, add
from .tasks.sync_user import UpsertUsersToSupabase, UpsertDeviceToSupabase
import urllib.parse
import abc
from .task import TaskRequest, run_single_task
import json
import re
import luigi
from .utils import is_empty, is_not_empty, get_gw_users_list, get_basic_rpc_result, ping, upsert_user, haskv, getkv, setkv, gw_login, normalize_traffic

# class DBParser(abc.ABC):
#     def parse_table(self, table):
#       # assume table is a string with \n separated
#       list = table.splitlines()
#       result = []
#       for row in list:
#           result += self.parse_row(row)
#       return result
    
#     @abc.ABCmethod
#     def parse_row(self, row):
#       pass
#     @abc.ABCmethod
#     def get_table(self):
#         pass
    

# class HourReportDBParser(DBParser):
#     def __init__(self, table):
#         self.table = table
#     def get_table(self):
#         return self.table
#     def parse(self):
#         pass


DB = APIRouter()

class DBQuery(BaseModel):
    gwid: str
    db: str
    sql: str


class TestBuildQuery(BaseModel):
    meta: str
    line: str

@DB.post("/test_build", tags=["DB"])
async def test_build(query: TestBuildQuery):
    meta = query.meta
    line = query.line
    dict = build_dict_from_line(meta, line)
    return { "result": str(dict) }

class SupabaseTableLatestRowQuery(BaseModel):
    gwid: str
    table: str
    column: str

@DB.post("/supabase_table_latest_row", tags=["DB"])
async def sb_table_latest_row_query(query: SupabaseTableLatestRowQuery):
    if query.gwid is None or query.gwid == "":
        raise HTTPException(status_code=400, detail="gwid should not be empty")
    if query.table is None or query.table == "":
        raise HTTPException(status_code=400, detail="table should not be empty")
    data = await get_supabase_table_latest_row(query.table, query.gwid, query.column or "happendate")
    return data

@DB.post("/get_gw_table_latest_row", tags=["DB"])
async def get_gw_table_latest_row(table_name, gwid, column="happendate"):
    # 1. 登陆gw
    # 2. 调用sdk查询gw
    # 3. 返回
    # use mysql client to query db
    # check if gwid exists
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    ### 根据表名称获取db名称
    db = get_db_for_table(table_name)
    ### 根据table_name, gwid, column获取sql
    sql = build_sql_for_latest_row(table_name, column)
    sql = urllib.parse.quote(sql)
    print(f"[get_gw_table_latest_row] >>> db = f{db}, sql = f{sql}")
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.query_db(db, sql)
            print(result)
            print(type(result))
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()



def compare_supabase_and_gw_lastrow(supabase_data, gw_data):
    if gw_data is None:
        return False
    elif supabase_data is None:
        return True
    else:
        return to_date(gw_data) > to_date(supabase_data)



def get_rpc_result(data):
    p = get_basic_rpc_result(data)
    if p is None:
        return None
    if "stdout" in p:
        return p['stdout']
    return None


class BaseIterable:
    def __iter__(self):
        pass
    def __next__(self):
        pass

class LineIterable(BaseIterable):
    def __init__(self, table_name, lines):
        super().__init__()
        self.table_name = table_name
        self.lines = lines
    def __iter__(self):
        meta = meta_for_table_name(self.table_name)
        if meta is None:
            raise HTTPException(status_code=400, detail="不存在的表名称")
        self.meta = meta
        r = get_rpc_result(self.lines)
        self.query_list = r.splitlines()
        self.idx = 0
        return self
    def __next__(self):
        if self.idx < len(self.query_list):
            idx = self.idx
            self.idx = idx + 1
            line = self.query_list[idx]
            if line is None or line == "":
                return ""
            else:
                return build_dict_from_line(self.meta, line)
        else:
            raise StopIteration

def build_sync_task(table_name, query_result):
    count = 0
    tasks = []
    meta = meta_for_table_name(table_name)
    if meta is None:
        raise HTTPException(status_code=400, detail="不存在的表名称")
    # 1. 将result按照\n进行分割
    r = get_rpc_result(query_result)
    query_list = r.splitlines()
    # 2. 创建task: 按照meta和single_result获取dict，把dict作为一条task插入
    for line in query_list:
        # 构建dict
        if line is None or line == "":
            continue
        tasks.append(build_dict_from_line(meta, line))
        count += 1
     # 3. 返回tasks数组和count值
    ret = { "tasks": tasks, "count": count }
    return ret
    
class CreateSyncTaskQuery(BaseModel):
    table_name: str
    gwid: str
    column: str
    full_sync: bool = False


# 创建同步任务
# column: 同步比较的列名称，默认是happendate
# gwid: 网关id
# table_name: 需要同步的表名称，在supabase和gw两边应该相同
@DB.post("/create_sync_task", tags=["DB"])
async def create_sync_task(query: CreateSyncTaskQuery):
    # 1. 获取supabase的表，以及gw的表，对比column所在列的最后一行的值
    column = query.column or "happendate"
    gwid = query.gwid
    table_name = query.table_name
    full_sync = query.full_sync
    supabase_last_row = await get_supabase_table_latest_row(table_name, gwid, column)
    gw_last_row = await get_gw_table_latest_row(table_name, gwid, column)
    print(f"supabase_last_row is {supabase_last_row}, gw_last_row is {gw_last_row}")
    should_update = compare_supabase_and_gw_lastrow(supabase_last_row, str_strip(get_rpc_result(gw_last_row['result'])))
    # 2. 如果最后一行的值相同则无需创建同步任务
    # 判断是否需要全量更新
    # 如果full_sync为True，则should_update为true, supabase_last_row为None
    if full_sync is True:
        should_update = True
        supabase_last_row = None
    
    if not should_update:
        return { "tasks": [], "count": 0 }
    else:
        # 3. 如果最后一行的值比对不同，则需要创建同步任务
        # 3.1 同步方向：从gw同步到supabase
        # 获取sql
        gw_sql = ""
        if supabase_last_row is None:
            # 3.2 同步条数：从gw中选出happendate>supabase.last_happendate的行，或者supabase为空，则gw选出所有行
            gw_sql = f"SELECT * FROM {query.table_name}"
        else:
            gw_sql = f"SELECT * FROM {query.table_name} WHERE {column} > '{formalize_supabase_datetime(supabase_last_row)}'"
        # 执行sql, 获取内容
        db_query = DBQuery(gwid=gwid, sql=gw_sql, db=get_db_for_table(query.table_name))
        query_result = await query_db(db_query)
        print(query_result)
        # 3.3 同步任务创建：生成相关task
        tasks = build_sync_task(query.table_name, query_result["result"])
        return tasks


def build_sync_command(gwid, data, table_name, keys):
    # id = str(uuid.uuid4())
    command = "UPSERT_SUPABASE"
    d = {}
    data["gwid"] = gwid
    d["table_name"] = table_name
    d["table_data"] = data
    d["keys"] = keys
    return {
        "cmd": command,
        "data": d,
        "result": "",
        "status": "todo"
    }

class PostSyncTasks(BaseModel):
    table_name: str
    gwid: str
    column: str
@DB.post("/post_sync_tasks", tags=["tasks"])
async def post_sync_tasks(query: PostSyncTasks):
    column = query.column or "happendate"
    gwid = query.gwid
    table_name = query.table_name
    q = CreateSyncTaskQuery(table_name=table_name, gwid=gwid, column=column, full_sync=True)
    task_ret = await create_sync_task(q)
    tasks = task_ret.get("tasks")
    cmds = []
    for task in tasks: 
        # 生成sync命令
        d = build_sync_command(gwid, task, table_name, ["gwid", column])
        # 发送sync命令
        print(f"task  ==== {task}, d === {d}")
        cmds.append(d)
    # 对于cmds中的命令，开始执行
    result = []
    for cmd in cmds:
        task_id = str(uuid.uuid4())
        command = cmd.get("cmd")
        data = cmd.get("data")
        print(f"task_id: {task_id}, command: {command}, data: {data}")
        tr = TaskRequest(id=task_id, command=command, data=data)
        res = await run_single_task(tr)
        print(f"finish task result =  ", str(res))
        result.append(res)
    return result

class SyncTrafficParam(BaseModel):
    gwid: str

@DB.post("/sync_traffics", tags=["traffic"])
async def sync_traffics(query: SyncTrafficParam):
    """
    将hourreport, acctreport两张表的内容，从db中同步到supabase的同名表中。
    gwid=网关id，必选
    """
    # 1. 检查gwid是否为空
    gwid = query.gwid
    if gwid is None or gwid == "":
        raise HTTPException(status_code=400, detail="gwid should not be empty")
    # 2. 检查gwid是否存在
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    # 3. 调用post_sync_tasks，同步hourreport表
    q = PostSyncTasks(table_name="hourreport", gwid=gwid, column="happendate")
    task_ret = await post_sync_tasks(q)
    # 对结果记录日志
    print(f"同步hourreport表: task_ret = {task_ret}")
    # 4. 调用post_sync_tasks，同步acctreport表
    q = PostSyncTasks(table_name="acctreport", gwid=gwid, column="happendate")
    task_ret = await post_sync_tasks(q)
    # 对结果记录日志
    print(f"同步acctreport表: task_ret = {task_ret}")
    # 5. 返回结果
    return { "result": "success" }

@DB.post("/sync_gateway_online_status", tags=["tasks"])
async def sync_gateway_online_status():
    # 1. get all gateway information from supabase's gateway table
    response = supabase.table("gateway").select("*").execute()
    # 2. for each entry in list
    for gw in response.data:
        # 3. get the gateway's online status from gw
        gwid = gw.get("id")
        data = { "gwid": gwid }
        command = "PING_SERVER"
        cmd = {
            "cmd": command,
            "data": data,
            "result": "",
            "status": "todo"
        }
        dump_str = json.dumps(cmd)
        result = perform_task_celery.delay(dump_str)
        print("任务已提交任务ID:", result.id)


class TestDelayAddQuery(BaseModel):
    a: int
    b: int

@DB.post("/test_delay_add", tags=["tasks"])
async def test_delay_add(query: TestDelayAddQuery):
    a = query.a
    b = query.b
    obj = { "a": a, "b": b }
    task_return = add.delay(obj)
    print("任务已提交任务ID:", task_return.id)

@DB.post("/sync_task_celery", tags=["tasks"])
async def sync_task_celery(query: PostSyncTasks):
    column = query.column or "happendate"
    gwid = query.gwid
    table_name = query.table_name
    q = CreateSyncTaskQuery(table_name=table_name, gwid=gwid, column=column)
    task_ret = await create_sync_task(q)
    tasks = task_ret.get("tasks")
    for task in tasks: 
        # 生成sync命令
        d = build_sync_command(gwid, task, table_name, ["gwid", column])
        # 发送sync命令
        print(f"task  ==== {task}, d === {d}")
        dump_str = json.dumps(d)
        print(f"dump_str === {dump_str}")
        result = perform_task_celery.delay(dump_str)
        print("任务已提交任务ID:", result.id)

@DB.post("/query", tags=["DB"])
async def query_db(query: DBQuery):
    # db should not be empty
    if query.db is None or query.db == "":
        raise HTTPException(status_code=400, detail="db should not be empty")
    # sql should not be empty
    if query.sql is None or query.sql == "":
        raise HTTPException(status_code=400, detail="sql should not be empty")
    # check if gwid exists
    gw = await get_gateway_by_id(query.gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    # get gateway username, password and address
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    # urlencode sql
    sql = urllib.parse.quote(query.sql)

    # use mysql client to query db
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.query_db(query.db, sql)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()




def to_timestamp(d:str):
    t = time.strptime(d, '%Y-%m-%d %H:%M:%S')
    return int(time.mktime(t))

class GetBandwidthQuery(BaseModel):
    gwid: str
    date: str | None

@DB.post("/get_bandwidth", tags=["DB"])
async def get_bandwidth(query: GetBandwidthQuery):
    #上行带宽和下行带宽
    #逻辑: 查hourreport表，筛选出当前小时的hourreport加和，返回上行带宽、下行带宽和总带宽
    # 0. 获取gwid和date
    gwid = query.gwid
    date = query.date
    # 0.1 处理date
    today = None
    if date is None:
        # 如果date为空，则选择今天
        today = date.today().strftime('%Y-%m-%d')
    else:
        # 如果date不是空，则使用date作为时间，时间格式为'2024-03-22'
        today = date
    # 1. 获取当前的日期和小时
    start_time = f"{today} 00:00:00"
    # start_time_stamp = to_timestamp(start_time)
    end_time = f"{today} 23:59:59"
    # end_time_stamp = to_timestamp(end_time)
    print(f"start_time = {start_time}, end_time = {end_time}")
    # 2. 查询
    response = (supabase
    .table('hourreport')
    .select("*")
    .eq("gwid", gwid)
    .gte("happendate", start_time)
    .lte("happendate", end_time)
    .execute())
    # 3. 归集结果
    # 3.1 如果查询结果为0， 则返回空值
    if len(response.data) <= 0:
        #
        return { "up": "0", "down": "0", "total": "0" }
    else:
        # 3.2 如果查询结果不是0，则将查询结果加和返回
        up = 0
        down = 0
        total = 0
        for entry in response.data:
            up = up + int(entry.get("uptraffic"))
            down = down + int(entry.get("downtraffic"))
            total = total + up + down
        return { "up": f"{up}", "down": f"{down}", "total": f"{total}" }

class GetTerminalAndConnsQuery(BaseModel):
    gwid: str

def get_total_connection_count(r):
    # r should be array
    count = 0
    for conn in r:
        count = count + conn["conn"]
    return count

@DB.post("/get_terminal_and_conns", tags=["DB"])
async def get_terminal_and_conns(query: GetTerminalAndConnsQuery):
    gwid = query.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    # get gateway username, password and address
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    total_terminal_count = 0
    try:
        if sdk.login(address, username, password):
            # top = 1000
            # search = ""
            result = sdk.list_online_users(1000, "")
            r = get_basic_rpc_result(result)
            r = str_strip(r["result"])
            print(r)
            r = json.loads(r)
            total_terminal_count = r["total"]
            total_conn_count = get_total_connection_count(r["result"])
            return { "total_terminal": f"{total_terminal_count}", "total_connection_count": f"{total_conn_count}"}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()
    

class GetAccountListQuery(BaseModel):
    gwid: str

@DB.post("/sync_account_list", tags=["DB"])
async def sync_account_list(query: GetAccountListQuery):
    gwid = query.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    # get gateway username, password and address
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            # top = 1000
            # search = ""
            result = sdk.list_account(1000, "")
            r = get_basic_rpc_result(result)
            r = str_strip(r["result"])
            print(r)
            r = json.loads(r)
            total_terminal_count = r["total"]
            total_conn_count = get_total_connection_count(r["result"])
            return { "total_terminal": f"{total_terminal_count}", "total_connection_count": f"{total_conn_count}"}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class ListConfigQuery(BaseModel):
    gwid: str

@DB.post("/list_config", tags=["DB"])
async def list_config(query: ListConfigQuery):
    gwid = query.gwid
    with gw_login(gwid) as sdk_obj:
        rval = {}
        config_list = ["network", "firewall", "wfilter-groups", "wfilter-times", "dhcp", "wfilter-appcontrol", "wfilter-webfilter", "wfilter-exception", "wfilter-imfilter", "wfilter-mailfilter", "wfilter-sslinspect", "wfilter-natdetector", "wfilter-webpush", "wfilter-bwcontrol", "wfilter-ipcontrol", "wfilter-mwan", "wfilter-account", "wfilter-adconf", "wfilter-webauth", "wfilter-pppoe", "wfilter-pptpd", "wfilter-ipsec", "openvpn", "wfilter-webvpn", "wfilter-sdwan", "antiddos", "wfilter-snort", "wfilter-aisecurity", "wfilter-isp"]
        for config_key in config_list:
            p = sdk_obj.config_load(config_key)
            p = get_basic_rpc_result(p)
            if p is None:
                continue
            else:
                p = p["values"]
                rval[config_key] = p
        return { "data": rval }

def sync_firewall(sdk, firewall_json):
    """
    对firewall的json进行同步。下面是可能的配置样式:
    cfg03dc81": {
        ".anonymous": true,
        ".type": "zone",
        ".name": "cfg03dc81",
        ".index": 2,
        "name": "lan",
        "input": "ACCEPT",
        "output": "ACCEPT",
        "forward": "ACCEPT",
        "network": [
        "lan1",
        "lan2"
        ],
        "device": [
        "ppp+",
        "zt+"
        ]
    },
    """
    cfgname = "firewall"
    # 对于firewall这个json对象中的每一个kv进行遍历
    for key, value in firewall_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()
    
def sync_groups(sdk, groups_json):
    cfgname = "wfilter-groups"
    # 对于firewall这个json对象中的每一个kv进行遍历
    for key, value in groups_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

def sync_times(sdk, times_json):
    cfgname = "wfilter-times"
    # 对于firewall这个json对象中的每一个kv进行遍历
    for key, value in times_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

def sync_app_control(sdk, appcontrol_json):
    cfgname = "wfilter-appcontrol"
    # 对于firewall这个json对象中的每一个kv进行遍历
    for key, value in appcontrol_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

def sync_web_filter(sdk, webfilter_json):
    cfgname = "wfilter-webfilter"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in webfilter_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步wfilter_exception
def sync_wfilter_exception(sdk, exception_json):
    cfgname = "wfilter-exception"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in exception_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步wfilter-imfilter
def sync_wfilter_imfilter(sdk, imfilter_json):
    cfgname = "wfilter-imfilter"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in imfilter_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

def sync_mail_filter(sdk, mail_json):
    cfgname = "wfilter-mailfilter"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in mail_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步sslinspect
def sync_sslinspect(sdk, sslinspect_json):
    cfgname = "wfilter-sslinspect"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in sslinspect_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步natdetector
def sync_natdetector(sdk, natdetector_json):
    cfgname = "wfilter-natdetector"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in natdetector_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步webpush
def sync_webpush(sdk, webpush_json):
    cfgname = "wfilter-webpush"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in webpush_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()
# 同步bwcontrol
def sync_bwcontrol(sdk, bwcontrol_json):
    cfgname = "wfilter-bwcontrol"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in bwcontrol_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

#同步ipcontrol
def sync_ipcontrol(sdk, ipcontrol_json):
    cfgname = "wfilter-ipcontrol"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in ipcontrol_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步mwan
def sync_mwan(sdk, mwan_json):
    cfgname = "wfilter-mwan"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in mwan_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        if type == "system":
            continue
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步adconf
def sync_adconf(sdk, adconf_json):
    cfgname = "wfilter-adconf"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in adconf_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步webauth
def sync_webauth(sdk, webauth_json):
    cfgname = "wfilter-webauth"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in webauth_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步pppoe
def sync_pppoe(sdk, pppoe_json):
    cfgname = "wfilter-pppoe"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in pppoe_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步pptpd
def sync_pptpd(sdk, pptpd_json):
    cfgname = "wfilter-pptpd"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in pptpd_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步ipsec
def sync_ipsec(sdk, ipsec_json):
    cfgname = "wfilter-ipsec"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in ipsec_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()
# 同步openvpn
def sync_openvpn(sdk, openvpn_json):
    cfgname = "openvpn"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in openvpn_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步webvpn
def sync_webvpn(sdk, webvpn_json):
    cfgname = "wfilter-webvpn"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in webvpn_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        if type == "system":
            continue
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步sdwan
def sync_sdwan(sdk, sdwan_json):
    cfgname = "wfilter-sdwan"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in sdwan_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步antiddos
def sync_antiddos(sdk, antiddos_json):
    cfgname = "antiddos"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in antiddos_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

# 同步snort
def sync_snort(sdk, snort_json):
    cfgname = "wfilter-snort"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in snort_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()
# 同步aisecurity
def sync_aisecurity(sdk, aisecurity_json):
    cfgname = "wfilter-aisecurity"
    # 对于wfilter-webfilter这个json对象中的每一个kv进行遍历
    for key, value in aisecurity_json.items():
        type = value.get(".type")
        # 通过sdk同步config
        if type == "system":
            continue
        sdk.config_add(cfgname, type, key, value)
    sdk.config_apply()

def sync_users(sdk, users_json):
    """
    对users的json进行同步。下面是可能的配置样式:
    "wfuser1737514663429": {
        ".anonymous": false,
        ".type": "wfuser",
        ".name": "wfuser1737514663429",
        ".index": 17,
        "username": "rflh",
        "remark": "ISP-1-bandwidth1733974887482",
        "changepwd": "true",
        "emails": " ",
        "pppoe": "false",
        "webauth": "true",
        "datelimit": "2029-01-01",
        "group": "0",
        "logins": "0",
        "static": "false",
        "staticip": " ",
        "macbound": "0",
        "macaddr": " ",
        "password": "<#3e2b89a35b96b30c70d5c07221eb784f#>",
        "id": "wfuser1737514663429"
    }
    """
    cfgname = "wfilter-account"
    # 对于users_json这个json对象中的每一个kv进行遍历
    for k, v in users_json.items():
        type = v.get(".type")
        # 只同步类型为wfuser的项:
        if type != "wfuser":
            continue
        # 单独处理admin，如果用户名为admin,则不同步
        if v.get("username") == "admin":
            continue 
        # 通过sdk同步config
        sdk.config_add(cfgname, type, k, v)
    sdk.config_apply()
        

@DB.post("/upload_config", tags=["DB"])
async def upload_config(gwid: str, file: UploadFile = File(...)):
    # 读取上传的文件内容（异步方式）
    content = await file.read()
    # 解析JSON内容  
    with gw_login(gwid) as sdk_obj:
        json_data = json.loads(content)
        # 兼容处理data
        if "data" in json_data:
           json_data = json_data["data"]
        # 同步防火墙策略
        sync_firewall(sdk_obj, firewall_json=json_data["firewall"])
        # 同步用户
        sync_users(sdk_obj, users_json=json_data["wfilter-account"])
        # 同步组
        sync_groups(sdk_obj, groups_json=json_data["wfilter-groups"])
        # 同步时间
        sync_times(sdk_obj, times_json=json_data["wfilter-times"])
        # 同步app_control
        sync_app_control(sdk_obj, appcontrol_json=json_data["wfilter-appcontrol"])
        # 同步wfilter_webfilter
        sync_web_filter(sdk_obj, webfilter_json=json_data["wfilter-webfilter"])
        # 同步wfilter_exception
        sync_wfilter_exception(sdk_obj, exception_json=json_data["wfilter-exception"])
        # 同步wfilter-imfilter
        sync_wfilter_imfilter(sdk_obj, imfilter_json=json_data["wfilter-imfilter"])
        # 同步wfilter-mailfilter
        sync_mail_filter(sdk_obj, mail_json=json_data["wfilter-mailfilter"])
        # 同步wfilter-sslinspect
        sync_sslinspect(sdk_obj, sslinspect_json=json_data["wfilter-sslinspect"])
        # 同步natdetector
        sync_natdetector(sdk_obj, natdetector_json=json_data["wfilter-natdetector"])
        # 同步webpush
        sync_webpush(sdk_obj, webpush_json=json_data["wfilter-webpush"])
        # 同步bwcontrol
        sync_bwcontrol(sdk_obj, bwcontrol_json=json_data["wfilter-bwcontrol"])
        # 同步ipcontrol
        sync_ipcontrol(sdk_obj, ipcontrol_json=json_data["wfilter-ipcontrol"])
        # 同步adconf
        sync_adconf(sdk_obj, adconf_json=json_data["wfilter-adconf"])
        # 同步webauth
        sync_webauth(sdk_obj, webauth_json=json_data["wfilter-webauth"])
        # 同步pppoe
        sync_pppoe(sdk_obj, pppoe_json=json_data["wfilter-pppoe"])
        # 同步pptpd
        sync_pptpd(sdk_obj, pptpd_json=json_data["wfilter-pptpd"])
        # 同步ipsec
        sync_ipsec(sdk_obj, ipsec_json=json_data["wfilter-ipsec"])
        # 同步openvpn
        sync_openvpn(sdk_obj, openvpn_json=json_data["openvpn"])
        # 同步webvpn
        sync_webvpn(sdk_obj, webvpn_json=json_data["wfilter-webvpn"])
        # 同步sdwan
        sync_sdwan(sdk_obj, sdwan_json=json_data["wfilter-sdwan"])
        # 同步antiddos
        sync_antiddos(sdk_obj, antiddos_json=json_data["antiddos"])
        # 同步snort
        sync_snort(sdk_obj, snort_json=json_data["wfilter-snort"])
        # 同步aisecurity
        sync_aisecurity(sdk_obj, aisecurity_json=json_data["wfilter-aisecurity"])
        return { "result": "success" }

class GetUserBandwidthQuery(BaseModel):
    gwid: str
    user: str
    date: str | None


def get_date_obj_from_str(s):
    # s 必须是yyyy-mm-dd一样的
    regex = re.compile(r'^\d{4}-\d{2}$')
    if not regex.match(s):
        return date.today()
    else:
        c = s.split('-')
        return datetime(int(c[0]), int(c[1]), 1).date()


def get_start_of_month(d, hrs=True):
    date_str = f"{d.strftime('%Y-%m')}-01"
    if hrs is True:
        return f"{date_str} 00:00:00"
    else:
        return date_str
def get_end_of_month(d, hrs=True):
    # d是某月任意开始时间
    yrs = int(d.strftime('%Y'))
    mths = int(d.strftime('%m'))
    day = calendar.monthrange(yrs, mths)[1]
    date_str = f"{yrs}-{d.strftime('%m')}-{day}"
    if hrs is True:
        return f"{date_str} 23:59:59"
    else:
        return date_str

@DB.post("/get_user_bandwidth", tags=["DB"])
async def get_user_bandwidth(query: GetUserBandwidthQuery):
    # 1. 获取gwid, user和日期date
    # 2. 如果date为空，则默认查询时间设置为本年本月；否则按照date查询
    # 3. 从supabase查询acctreport表，筛选日期在date的月份范围内
    gwid = query.gwid
    d = query.date
    d = get_date_obj_from_str(d)
    start_time_str = get_start_of_month(d, True)
    end_time_str = get_end_of_month(d, True)

    if is_empty(gwid):
        response = (supabase
        .table('acctreport')
        .select("*")
        .gte("happendate", start_time_str)
        .lte("happendate", end_time_str)
        .execute())
    else:
        response = (supabase
        .table('acctreport')
        .select("*")
        .eq("gwid", gwid)
        .gte("happendate", start_time_str)
        .lte("happendate", end_time_str)
        .execute())
    # 4.归集本月结果 
    if len(response.data) <= 0:
        #
        return { "up": "0", "down": "0", "total": "0" }
    else:
        # 3.2 如果查询结果不是0，则将查询结果加和返回
        up = 0
        down = 0
        total = 0
        for entry in response.data:
            up = up + int(float(entry.get("uptraffic")))
            down = down + int(float(entry.get("downtraffic")))
            total = total + up + down
        return { "up": f"{normalize_traffic(up)}", "down": f"{normalize_traffic(down)}", "total": f"{normalize_traffic(total)}" }

class GetUserBandwidthDetailQuery(BaseModel):
    gwid: str
    user: str
    date: str | None

@DB.post("/get_user_bandwidth_detail", tags=["DB"])
async def get_user_bandwidth_detail(query: GetUserBandwidthDetailQuery):
    # 1. 获取gwid, user和日期date
    # 2. 如果date为空，则默认查询时间设置为本年本月；否则按照date查询
    # 3. 从supabase查询acctreport表，筛选日期在date的月份范围内
    gwid = query.gwid
    d = query.date
    d = get_date_obj_from_str(d)
    start_time_str = get_start_of_month(d, True)
    end_time_str = get_end_of_month(d, True)
    response = (supabase
        .table('acctreport')
        .select("*")
        .eq("gwid", gwid)
        .gte("happendate", start_time_str)
        .lte("happendate", end_time_str)
        .execute())
    # 4. 归集结果
    if len(response.data) <= 0:
        return {"data": []}
    else:
        list = []
        for d in response.data:
            up = d["uptraffic"]
            down = d["downtraffic"]
            list.append({
                "acct": d["acct"],
                "up": normalize_traffic(up),
                "down": normalize_traffic(down),
                "total": f"{normalize_traffic(float(up) + float(down))}",
                "happendate": to_date(d["happendate"])
            })
        return {"data": list}
    

class GetAccountListQuery(BaseModel):
    gwid: str


def get_gw_online_status_by_id(gwid, id):
    # 在supabase的gw_users表中，查询gwid和id匹配的条款，返回online字段
    # 如果online字段不是true或者false，统一返回false
    response = (supabase
       .table('gw_users')
       .select("online")
       .eq("gwid", gwid)
       .eq("id", id)
       .execute())
    if len(response.data) <= 0:
        print(f"gwid = {gwid}, id = {id} not found its online status")
        return False
    else:
        online = response.data[0]["online"]
        if online == "true" or online == "True":
            return True
        elif online == "false" or online == "False":
            return False
        else:
            return False

@DB.post("/get_device_list", tags=["DB"])
async def get_device_list(query: GetAccountListQuery):
    gwid = query.gwid
    with gw_login(gwid) as sdk_obj:
        r = sdk_obj.list_online_users(1000, "")
        r = get_basic_rpc_result(r)
        r = str_strip(r["result"])
        print(r)
        r = json.loads(r)
        r = r["result"]
        list = []
        for item in r:
            device = {}
            device["ip"] = item["ip"]
            device["macaddr"] = item["mac"]
            device["group"] = item["group"]
            device["up"] = normalize_traffic(item["up"])
            device["down"] = normalize_traffic(item["down"])
            device["gwid"] = gwid
            list.append(device)
        luigi.build([UpsertDeviceToSupabase(json.dumps(list))], local_scheduler=True)
        return { "data": list }

def get_total_traffic(item):
    up = item.get("uptraffic") or 0.0
    down = item.get("downtraffic") or 0.0
    return normalize_traffic(up + down)

@DB.post("/get_account_list", tags=["DB"])
async def get_account_list(query: GetAccountListQuery):
    """
    POST /get_account_list获取用户列表
    参数1： gwid=当前网关id，非必须。当gwid为空字符串时候，显示所有用户。
    """
    # 1. 获取gwid
    gwid = query.gwid
    if is_not_empty(gwid):
        print(f"[DEBUG][get_account_list]: gwid = {gwid}")
        lst = get_gw_users_list(gwid)
        print("====start build luigi, list = " + json.dumps(lst))
        luigi.build([UpsertUsersToSupabase(json.dumps(lst))], local_scheduler=True)
    # 2. 查user_traffic_view
    r = None
    if is_empty(gwid):
        r = supabase.table("user_traffic_view").select("*").order("userid", desc=True).execute()
    else:
        r = supabase.table("user_traffic_view").select("*").eq("gwid", gwid).order("userid", desc=True).execute()
    print(f"[DEBUG][get_account_list]: user_traffic_view = {r}")
    data = []
    for item in r.data:
        print("====get_account_list, item = " + str(item))
        # 2.2 遍历r.data，将r.data中的数据转换为list
        delete_mark = item.get("delete_mark")
        gw_id = item.get("gwid")
        if delete_mark is True or delete_mark == "true":
            continue
        if is_not_empty(gwid) and item.get("gwid") != gwid:
            continue
        data.append({
            "gwid": gw_id,
            "gateway_name": item["gateway_name"],
            "username": item["username"],
            "userid": item.get("userid"),
            "total_traffic": get_total_traffic(item),
            "group": item["group"],
            "online": item["online"],
            "datelimit": item["datelimit"],
            "remark": item.get("remark"), # 套餐名称
            "sid": item.get("sid"),
            "group_id": item.get("group_id"), #组id
            "group_alias": item.get("group_alias") #组名称
        })
    # 2.2 将r.data返回
    return { "data": data }

@DB.post("/get_stats", tags=["DB"])
async def get_stats():
    # 1. 查询supabase的gateway_count表，获取count值
    r = supabase.table("gateway_count").select("count").execute()
    gateway_count = 0
    if len(r.data) > 0:
        #获取count
        gateway_count = r.data[0]["count"]
    # 2. 查询supabase中的client_count，获取客户数量count值
    r = supabase.table("client_count").select("count").execute()
    client_count = 0
    for line in r.data:
        client_count += line.get("count")
    # 3. 查询supabase中的fleet_count，获取舰船数量count值
    r = supabase.table("fleet_count").select("count").execute()
    fleet_count = 0
    for line in r.data:
        fleet_count += line.get("count")
    
    # 4. 总流量:查询supabase中的total_traffic，获取up和down字段
    r = supabase.table("total_traffic").select("up, down").execute()
    up = 0
    down = 0
    if len(r.data) > 0:
        #获取count
        up = r.data[0]["up"]
        down = r.data[0]["down"]

    # 5. 总用户数: 查询supabase中的gw_users_count，获取count字段
    users_count = 0
    r = supabase.table("gw_users_count").select("count").execute()
    if len(r.data) > 0:
        #获取count
        users_count = r.data[0]["count"]
    return { "data": {
            "gateway_count": gateway_count,
            "client_count": client_count,
            "fleet_count": fleet_count,
            "user_count": users_count,
            "total_traffic": normalize_traffic(up+down)
        } }

@DB.post("/test_upsert_user", tags=["test"])
async def test_upsert_user():
    fake_user = {
      "gwid": "97935833-c028-4f7b-ad5f-26f296cf935a",
      "username": "rflh",
      "remark": "ISP-1-bandwidth1733974887482",
      "pppoe": "false",
      "webauth": "true",
      "static": "false",
      "staticip": " ",
      "datelimit": "2029-01-01",
      "group": "0",
      "logins": "0",
      "macbound": "0",
      "changepwd": "true",
      "id": "wfuser1737514663430"
    }
    # "online": "False"
    response = upsert_user(fake_user)
    return response

class TestHasKVQuery(BaseModel):
    type: str
    id: str
    key: str

@DB.post("/test_haskv", tags=["test"])
async def test_haskv(query: TestHasKVQuery):
    type = query.type
    id = query.id
    key = query.key
    response = haskv(type, id, key)
    return response

@DB.post("/test_getkv", tags=["test"])
async def test_getkv(query: TestHasKVQuery):
    type = query.type
    id = query.id
    key = query.key
    response = getkv(type, id, key)
    return response

class TestSetKVQuery(BaseModel):
    type: str
    id: str
    key: str
    value: str

@DB.post("/test_setkv", tags=["test"])
async def test_setkv(query: TestSetKVQuery):
    type = query.type
    id = query.id
    value = query.value
    key = query.key
    response = setkv(type, id, key, value)
    return response

# 对用户进行上线和下线操作

# 传入参数: 
class UserSessionParam(BaseModel):
    gwid: str # 网关的id
    user: str # 用户名称
    op: str # 操作, up = 上线， down = 下线

# 用户上下线
async def kill_user(param: UserSessionParam):
    # 打印参数
    gwid = param.gwid
    print("kill_user param = {}".format(param))
    with gw_login(gwid) as sdk_obj:
        # 根据op决定type
        # 如果op是up，则type为REMOVE；如果op是down，则type为ALL
        type = "REMOVE" if param.op == "up" else "ALL"
        result = sdk_obj.kill_connection(param.user, 0, type, "", "")
        print(result)
        return result

@DB.post("/update_online_status", tags=["DB"])
async def update_online_status(param: UserSessionParam):
    """
    用户上线下线
    参数1： gwid=当前网关id，必须
    参数2: user=用户名，必须，例如user1
    参数3：op=操作，必须，枚举值up=上线，down=下线，其他值报错
    返回值: 
    data: 用户的列表，其中每一项显示信息包括: 
    online: 是否在线，true为在线，false为不在线（在线状态更新)
    """
    gwid = param.gwid
    user = param.user
    op = param.op
    # 1. 根据gwid，从kv中读取BAN_GROUP这个key的值，用这个值作为group
    type = "gateway"
    group = getkv(type, id = gwid, key = "BAN_GROUP")
    print("update_online_status: gwid={}, user={}, op={}, group={}".format(gwid, user, op, group))
    # 2. 如果group为none或者空，则抛出异常
    if group is None or group == "":
        raise HTTPException(status_code=400, detail="group not found")
    # 3. 判断op是up还是down，如果不是up或者down则抛出异常
    if op != "up" and op != "down":
        raise HTTPException(status_code=400, detail="op必须是up或者是down")
    # 4. 如果op是up，则执行上线操作，上线操作是将用户从group中移除；如果op是down，则执行下线操作，下线操作是将用户添加到group中。
    await kill_user(param)
    # 5. 更新supabase中的user表中的online字段
    online = "true" if op == "up" else "false"
    response = supabase.table('gw_users').update({"online": online}).eq("gwid", gwid).eq("username", user).execute()
    return response
    
class TestPingQuery(BaseModel):
    server: str

# 测试ping服务器
@DB.post("/test_ping", tags=["test"])
async def test_ping(query: TestPingQuery):
    server = query.server
    response = ping(server)
    return response