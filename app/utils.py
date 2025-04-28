from urllib.parse import urlparse
from app.supabase import supabase
import platform    # For getting the operating system name
import subprocess  # For executing a shell command
import re
import time
import ipaddress
from fastapi import HTTPException
from .sdk import SDK
from datetime import datetime, date
import calendar
import os
from .config import Config
from multiping import multi_ping

from contextlib import contextmanager


env = os.environ.get('env')
# if env is empty, set env to "test"
if env is None:
    env = "test"
cfg = Config(env)

def is_empty(param):
    return param is None or len(param) <= 0

def is_not_empty(param):
    if param is None:
        return False
    return len(param) > 0

def get_traffic_ratio():
    """返回流量乘数"""
    return cfg.TRAFFIC_RATIO

def normalize_traffic(traffic):
    """
    归一化流量, 根据流量乘数和流量计算最终归一化的流量数值
    """
    traffic_num = traffic
    if traffic is None:
        traffic_num = 0
    # ratio = get_traffic_ratio()
    # if ratio is None:
    n = 0
    try:
        n = float(traffic_num)
    except ValueError:
        pass
    ratio = 1.3
    return n * ratio

def is_valid_ipv4(ip):
    """
    判断输入的字符串是否为合法的 IPv4 地址
    """
    try:
        ipaddress.IPv4Address(ip)
        return True
    except ValueError:
        return False
    
def is_valid_domain(domain):
    """
    判断输入的字符串是否为合法的域名
    """
    pattern = re.compile(
        r'^(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)$',
        re.IGNORECASE
    )
    return bool(pattern.match(domain))

def is_valid_netloc(netloc):
    """
    判断输入的 netloc 是否为合法的 IPv4 地址或者网址
    """
    # 处理可能包含端口号的情况
    netloc = netloc.split(':')[0]
    return is_valid_ipv4(netloc) or is_valid_domain(netloc)

async def get_gateway_by_id(gwid: str):
    response = supabase.table("gateway").select("*").eq("id", gwid).execute()
    return response

def is_url(url):
    try:
        result = urlparse(url)
        print("result = " + str(result) + "")
        print("netloc = " + result.netloc + "")
        return is_valid_netloc(result.netloc)
    except Exception as e:
        return False

def ping_multi_hosts(hosts):
    addrs = hosts or []
    print(hosts)
    responses, no_response = multi_ping(addrs, timeout=0.5, retry=2,
                                        ignore_lookup_errors=True)
    print("responses = ", responses)
    print("no_response = ", no_response)
    return responses
    

def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """
    # if is_url(host) is False:
    #     return False
    # Option for the number of packets as a function of
    param = '-n' if platform.system().lower()=='windows' else '-c'

    # Building the command. Ex: "ping -c 1 google.com"
    command = ['ping', param, '1', host]
    return subprocess.call(command) == 0

def upsert_user(user):
    """
    将用户列表数据upsert到Supabase的gw_user表
    """
    TABLE_NAME = "gw_users"
    gwid = user["gwid"]
    id = user["id"]
    global_id = f"{gwid}_{id}"
    user["global_id"] = global_id
    response = supabase.table(TABLE_NAME).upsert(user).execute()
    return response

def batch_update_gw_strategy(strategy_list):
    """
    将策略列表数据批量upsert到Supabase的gw_strategy表
    """
    TABLE_NAME = "gw_bandwidth_strategy"
    list = []
    for item in strategy_list:
        r = {}
        gwid = item.get("gwid")
        id = item.get("id")
        r["gwid"] = gwid
        r["id"] = id
        r["strategy_id"] = f"{gwid}_{id}"
        r["period"] = item["period"]
        r["threshold"] = item["threshold"]
        r["exceed"] = item["exceed"]
        r["remark"] = item["remark"]
        list.append(r)
    response = supabase.table(TABLE_NAME).upsert(list).execute()
    return response

def batch_update_gw_group(group_list):
    """
    将策略列表数据批量upsert到Supabase的gw_groups表,同步下列域:
    1. gwid
    2. id
    3. global_id = gwid_id
    4. name
    5. virtual
    6. alias
    7. alias_zh_cn
    8. aliaz_en_us
    9. index
    10. type
    11. anonymous
    """
    TABLE_NAME = "gw_groups"
    list = []
    for item in group_list:
        r = {}
        gwid = item.get("gwid")
        id = item.get("id")
        r["gwid"] = gwid
        r["id"] = id
        r["global_id"] = f"{gwid}_{id}"
        r["name"] = item.get("name")
        r["virtual"] = item.get("virtual")
        r["alias"] = item.get("alias")
        r["alias_zh_cn"] = item.get("alias_zh_cn")
        r["alias_en_us"] = item.get("alias_en_us")
        r["index"] = item.get("index")
        r["type"] = item.get("type")
        r["anonymous"] = item.get("anonymous")
        list.append(r)
    response = supabase.table(TABLE_NAME).upsert(list).execute()
    return response

def batch_update_users_group(user_group_list):
    """
    将用户和表之间的关系，更新到gw_users表中的virtual_group字段。
    输入参数: 
    "gwid": gwid,
    "userid": userid,
    "groupid": group_id,
    "group_global_id": global_id,
    "group_name": alias
    """
    TABLENAME = "gw_users"
    target_list = []
    for ug in user_group_list:
        # gwid
        gwid = ug.get("gwid")
        userid = ug.get("userid")

        # group_global_id
        group_global_id = ug.get("group_global_id")
        item = {
            "virtual_group": group_global_id
        }
        response = (
            supabase.table(TABLENAME).update(item).eq("username", userid).eq("gwid", gwid).execute()
        )
        target_list.append(response)
    return { "data": target_list }

def haskv(type, id, key):
    """
    判断gw_kv表中是否存在type和id对应的key
    """
    TABLE_NAME = "gw_kv"
    response = supabase.table(TABLE_NAME).select("*").eq("type", type).eq("key", key).eq("id", id).execute()
    return len(response.data) > 0

def getkv(type, id, key):
    """
    获取gw_kv表中type和id对应的key的值
    """
    TABLE_NAME = "gw_kv"
    response = supabase.table(TABLE_NAME).select("*").eq("type", type).eq("key", key).eq("id", id).execute()
    if len(response.data) > 0:
        return response.data[0]["value"]
    return None

def setkv(type, id, key, value):
    """
    设置gw_kv表中type和id对应的key的值
    """
    TABLE_NAME = "gw_kv"
    if haskv(type, id, key):
        response = supabase.table(TABLE_NAME).update({"value": value}).eq("type", type).eq("key", key).eq("id", id).execute()
    else:
        response = supabase.table(TABLE_NAME).insert({"type": type, "key": key, "id": id, "value": value}).execute()
    return response

@contextmanager
def gw_login(gwid:str):
    print("==gw_login")
    try:
        gw = supabase.table("gateway").select("*").eq("id", gwid).execute()
        if gw is None or len(gw.data) == 0:
            raise HTTPException(status_code=400, detail="gateway not found")
        # get gateway username, password and address
        username = gw.data[0].get('username')
        password = gw.data[0].get('password')
        address = gw.data[0].get('address')
        sdk = SDK()
        if sdk.login(address, username, password):
            yield sdk
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

def get_date_obj_from_str(s):
    # 处理yyyy-mm-dd的情况
    regex1 = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    regex = re.compile(r'^\d{4}-\d{2}$')
    if regex1.match(s):
        c = s.split('-')
        return datetime(int(c[0]), int(c[1]), int(c[2])).date()
    elif regex.match(s):
        # 处理yyyy-mm的情况
        c = s.split('-')
        return datetime(int(c[0]), int(c[1]), 1).date()
    else:
        # 其他情况:返回今天的日期
        return date.today()
    
def get_start_of_month(d, hrs=True):
    date_str = f"{d.strftime('%Y-%m')}-01"
    if hrs is True:
        return f"{date_str} 00:00:00"
    else:
        return date_str
    
def get_date(d, hrs=True):
    date_str = f"{d.strftime('%Y-%m-%d')}"
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
    
def get_basic_rpc_result(data):
    if data is not None:
        print(data)
        if data['result'] is not None:
            print(data['result'])
            if len(data['result']) > 1:
                p = data['result'][1]
                return p
    return None

# 从网关设备sdk中读取用户列表
def get_gw_users_list(gwid: str):
    with gw_login(gwid) as sdk_obj:
        p = sdk_obj.list_account()
        print("====list_account, p = " + str(p))
        p = get_basic_rpc_result(p)
        print("====list_account, get_basic_rpc_result p = ", str(p))
        p = p["values"]
        print("====list_account, value p = ", str(p))
        
        lst = []   
        for key, value in p.items():
            if value[".type"] == "wfuser" and key != "admin":
                print("====list_account, value = ", str(value))
                user = {
                    "gwid": gwid,
                    "username": value["username"],
                    "remark": value.get("remark") or "",
                    "pppoe": value["pppoe"],
                    "webauth": value["webauth"],
                    "static": value["static"],
                    "staticip": value["staticip"],
                    "datelimit": value["datelimit"],
                    "group": value["group"],
                    "logins": value["logins"],
                    "macbound": value["macbound"],
                    "changepwd": value["changepwd"],
                    "id": value.get("id") or "",
                    # "online": str(get_gw_online_status_by_id(gwid, value["id"]))
                }
                lst.append(user)
        return lst
    
def get_milliseconds():
    milis = int(round(time.time() * 1000))
    return milis

def yyyymmdd(date_str:str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
    
def is_future_date(date_str):
    try:
        given_date = datetime.strptime(date_str, '%Y-%m-%d')
        current_date = datetime.now()
        return given_date > current_date
    except ValueError:
        return False
    
def parse_int(str):
    try:
        data = float(str)
        return int(data)
    except ValueError:
        return 0
    