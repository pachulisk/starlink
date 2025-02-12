from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile, File
from pydantic import BaseModel
from app.utils import get_gateway_by_id
from app.sdk import SDK
from app.supabase import supabase
from .task import post_single_task
from datetime import datetime, date
import time
import urllib.parse
import calendar
from .celery_app import perform_task_celery, add
from .tasks.sync_user import UpsertUsersToSupabase
import urllib.parse
import abc
import json
import re
import luigi
from .utils import upsert_user, haskv, getkv, setkv

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

def make_list(row):
    my_lst = row.split("|")
    return list(filter(lambda x: len(x)>0, my_lst))

def build_dict_from_line(meta, line):
    """
    meta: happendate|hour|uptraffic|downtraffic|
    line: 2024-11-07|14|5772895|21432009|
    """
    dict = {}
    meta_list = make_list(meta)
    meta_len = len(meta_list)
    data_list = make_list(line)
    data_len = len(data_list)
    length = min(meta_len, data_len)
    for i in range(length):
        dict[meta_list[i]] = data_list[i]
    return dict
def meta_for_table_name(table_name):
    meta_mapping = {
        "hourreport": "happendate|hour|uptraffic|downtraffic|",
        "ipreport": "happendate|ip|uptraffic|downtraffic|",
        "acctreport": "happendate|acct|uptraffic|downtraffic|",
        "webreport": "ip|group1|acct|happendate|host|category1|category2|visitcnt|uptraffic|downtraffic|during|",
        "webreport_today": "ip|group1|acct|happendate|host|category1|category2|visitcnt|uptraffic|downtraffic|during|",
        "protocolreport": "ip|group1|acct|happendate|category|protocol|uptraffic|downtrafficduring|",
        "protocolreport_today": "ip|group1|acct|happendate|category|protocol|uptraffic|downtrafficduring|",
        "sessionslog": "ip|group1|acct|mac|happentime|direction|proto|target|cmd|remark|",
        "ftplog": "ip|group1|acct|mac|happentime|direction|type|target|filesize|refer|filename|title|useragent|fileid|remark|targetip|",
        "ipmaclog": "ip|group1|acct|mac|happentime|hostname|",
        "maillog": "ip|group1|acct|mac|happentime|direction|fromid|toid|subject|messageid|fileid|proto|remark|targetip|",
        "webpostlog": "ip|group1|acct|mac|happentime|host|webtitle|postsize|posturl|fileid|refer|useragent|tls|remark|targetip|",
        "websurflog": "ip|group1|acct|mac|happentime|host|url|webtitle|tls|useragent|remark|targetip|"
    }
    if table_name in meta_mapping:
        return meta_mapping[table_name]
    else:
        return None
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

async def get_supabase_table_latest_row(table_name, gwid, column="happendate"):
    response = supabase.table(table_name).select(column).eq("gwid", gwid).order(column, desc=True).limit(1).execute()
    data = response.data
    if len(data) <= 0:
        return None
    else:
        return data[0].get(column)

def get_db_for_table(table_name):
    if  table_name in ["ipreport", "acctreport"]:
        return "ISP.db"
    elif table_name in ["hourreport", "webreport", "protocolreport", "protocolreport_today"]:
        return "report.db"
    else: 
        return "wfilter.db"
def build_sql_for_latest_row(table_name, column="happendate"):
    return f"SELECT {column} FROM {table_name} ORDER BY {column} DESC LIMIT 1"

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


def to_date(data):
    print(f"to_date: {data}")
    d = data
    # regex = re.compile(r'T')
    # if regex.match(d):
    index = d.find('T')
    if index != -1:
        d = data[:index]
    print(f"to_date, after process d = {d}")
    processed = datetime.strptime(d, "%Y-%m-%d")
    print(f"processed = {processed}")
    return processed

def compare_supabase_and_gw_lastrow(supabase_data, gw_data):
    if gw_data is None:
        return False
    elif supabase_data is None:
        return True
    else:
        return to_date(gw_data) > to_date(supabase_data)

def str_strip(data):
    if data is None:
        return None
    else:
        ret = data.replace("\n", "")
        ret = ret.replace("|", "")
        return ret

def get_basic_rpc_result(data):
    if data is not None:
        print(data)
        if data['result'] is not None:
            print(data['result'])
            p = data['result'][1]
            return p
    return None
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

def formalize_supabase_datetime(dt):
    d = dt
    # regex = re.compile(r'T')
    # if regex.match(d):
    index = d.find('T')
    if index != -1:
        d = dt[:index]
    return d

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
    supabase_last_row = await get_supabase_table_latest_row(table_name, gwid, column)
    gw_last_row = await get_gw_table_latest_row(table_name, gwid, column)
    print(f"supabase_last_row is {supabase_last_row}, gw_last_row is {gw_last_row}")
    should_update = compare_supabase_and_gw_lastrow(supabase_last_row, str_strip(get_rpc_result(gw_last_row['result'])))
    # 2. 如果最后一行的值相同则无需创建同步任务
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
async def post_sync_tasks(query: PostSyncTasks, background_tasks: BackgroundTasks):
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
        background_tasks.add_task(post_single_task, d)


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
async def get_account_list(query: GetAccountListQuery):
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
            rval = {}
            config_list = ["network", "firewall", "wfilter-groups", "wfilter-times", "dhcp", "wfilter-appcontrol", "wfilter-webfilter", "wfilter-exception", "wfilter-imfilter", "wfilter-mailfilter", "wfilter-sslinspect", "wfilter-natdetector", "wfilter-webpush", "wfilter-bwcontrol", "wfilter-ipcontrol", "wfilter-mwan", "wfilter-account", "wfilter-adconf", "wfilter-webauth", "wfilter-pppoe", "wfilter-pptpd", "wfilter-ipsec", "openvpn", "wfilter-webvpn", "wfilter-sdwan", "antiddos", "wfilter-snort", "wfilter-aisecurity"]
            for config_key in config_list:
                p = sdk.config_load(config_key)
                p = get_basic_rpc_result(p)
                p = p["values"]
                rval[config_key] = p
            return { "data": rval }
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()





@DB.post("/upload_config", tags=["DB"])
async def upload_config(file: UploadFile = File(...)):
    # 读取上传的文件内容（异步方式）
    content = await file.read()
    # 解析JSON内容
    try:
        json_data = json.loads(content)
        # 兼容处理data
        if "data" in json_data:
           json_data = json_data["data"] 
        # config_list = ["network", "firewall", "wfilter-groups", "wfilter-times", "dhcp", "wfilter-appcontrol", "wfilter-webfilter", "wfilter-exception", "wfilter-imfilter", "wfilter-mailfilter", "wfilter-sslinspect", "wfilter-natdetector", "wfilter-webpush", "wfilter-bwcontrol", "wfilter-ipcontrol", "wfilter-mwan", "wfilter-account", "wfilter-adconf", "wfilter-webauth", "wfilter-pppoe", "wfilter-pptpd", "wfilter-ipsec", "openvpn", "wfilter-webvpn", "wfilter-sdwan", "antiddos", "wfilter-snort", "wfilter-aisecurity"]
        # 对于config_list中的每一个key,
        print("Received JSON data:", json_data)

        return {"status": "success", "filename": file.filename}
    
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format"}


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
        return { "up": f"{up}", "down": f"{down}", "total": f"{total}" }

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
                "up": up,
                "down": down,
                "total": f"{float(up) + float(down)}",
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

@DB.post("/get_account_list", tags=["DB"])
async def get_account_list(query: GetAccountListQuery):
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
            p = sdk.list_account()
            p = get_basic_rpc_result(p)
            p = p["values"]

            r = sdk.list_online_users(1000, "")
            r = get_basic_rpc_result(r)
            r = str_strip(r["result"])
            print(r)
            r = json.loads(r)
            r = r["result"]
            kv = {}
            for u in r:
                account = u["account"]
                account = urllib.parse.quote_plus(account)
                kv[account] = True

            list = []
            for _, value in p.items():
                if value[".type"] == "wfuser":
                    user = {
                        "gwid": gwid,
                        "username": value["username"],
                        "remark": value["remark"],
                        "pppoe": value["pppoe"],
                        "webauth": value["webauth"],
                        "static": value["static"],
                        "staticip": value["staticip"],
                        "datelimit": value["datelimit"],
                        "group": value["group"],
                        "logins": value["logins"],
                        "macbound": value["macbound"],
                        "changepwd": value["changepwd"],
                        "id": value["id"],
                        "online": str(get_gw_online_status_by_id(gwid, value["id"]))
                    }
                    list.append(user)
            luigi.build([UpsertUsersToSupabase(json.dumps(list))], local_scheduler=True)
            # for user in list:
            #     if "online" in user:
            #         #删除online字段
            #         del user["online"]
            #     # 执行upsert操作
            #     # logger.info(f"upsert user: {user}")
            #     print(f"upsert user: {user}")
            #     response = upsert_user(user)
            #     print(f"response: {response}")
            #     # response = supabase.table("gw_users").upsert(user).execute()
            #     # 检查错误（supabase-python的响应结构可能不同，请根据实际情况调整）
            #     # logger.info(f"response: {response}")
            #     if hasattr(response, 'error') and response.error:
            #         raise Exception(f"Supabase操作失败: {response.error}")
            return { "data": list }
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

@DB.post("/get_stats", tags=["DB"])
async def get_stats():
    return { "data": {
        "gateway_count": 1,
        "user_count": 24,
        "device_count": 5
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
    print("kill_user param = {}".format(param))
    sdk = SDK()
    gw = await get_gateway_by_id(param.gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    # get gateway username, password and address
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    try:
        if sdk.login(address, username, password):
            # 根据op决定type
            # 如果op是up，则type为REMOVE；如果op是down，则type为ALL
            type = "REMOVE" if param.op == "up" else "ALL"
            result = sdk.kill_connection(param.user, 0, type, "", "")
            print(result)
            print(type(result))
            return result
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

@DB.post("/update_online_status", tags=["DB"])
async def update_online_status(param: UserSessionParam):
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
    