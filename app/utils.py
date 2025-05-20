from urllib.parse import urlparse
from app.supabase import supabase
import platform    # For getting the operating system name
import subprocess  # For executing a shell command
import re
import json
import time
import ipaddress
from fastapi import HTTPException
from .sdk import SDK
from datetime import datetime, date
import calendar
import os
import asyncio
from .config import Config
from multiping import multi_ping

from contextlib import contextmanager


settings = None

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

def get_gb_from_bytes(bytes):
    """
    将字节转换为GB
    """
    return bytes / (1000 ** 3)

def get_mb_from_bytes(bytes):
    """
    将字节转换为MB
    """
    return bytes / (1000 ** 2)

def get_kb_from_bytes(bytes):
    """
    将字节转换为KB
    """
    return bytes / 1000

def get_digits(num, x):
    """
    保留x位小数
    """
    return round(num, x) if num is not None else None

def str2float(s):
    """
    将字符串转换为浮点数
    """
    try:
        return float(s)
    except ValueError:
        return 0.0

def normalize_traffic(traffic, unit=None, ratio=None):
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
    if ratio is None:
        ratio = 1.3
    if unit is None:
        # do nothing
        pass
    elif unit == "gb" or unit == "GB":
        n = get_gb_from_bytes(n * ratio)
    elif unit == "mb" or unit == "MB":
        n = get_mb_from_bytes(n * ratio)
    elif unit == "kb" or unit == "KB":
        n = get_kb_from_bytes(n * ratio) 
    else:
        pass
    n = get_digits(n, 2)
    return n


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
    online = user.get("online")
    if online is None:
        user["online"] = "true"
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
        # 从supabase获取网关信息
        gw = supabase.table("gateway").select("*").eq("id", gwid).execute()
    except Exception as e:
        # 如果没有获取到，抛出网关不在列表中的错误信息
        raise HTTPException(status_code=400, detail="网关不在列表中")
    # 如果成功获取supabase的列表但是列表内容为空， 则返回未找到网关
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="未找到网关")
    # 获取用户名、密码和地址
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    # 初始化online变量值为False
    online = False
    try:
        # 调用is_online来判断对应的地址是否在线
        online = is_online(address)
    except Exception as e:
        # 如果过程中发生问题，说明网关不在线
        raise HTTPException(status_code=400, detail="网关不在线1")
    # 如果online值为False，停止前进，抛出网关不在线的信息
    if online is False:
        raise HTTPException(status_code=400, detail="网关不在线")
    
    # TODO:增加要ping通网关才行
    try:
        # get gateway username, password and address
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
    # 如果s是以/作为分隔符，将/替换为-
    if '/' in s:
        s = s.replace('/', '-')
    # 处理yyyy-mm-dd的情况
    regex1 = re.compile(r'\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])')
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
    
def starts_with_number(str):
    if len(str) <= 0:
        return False
    return str[0].isdigit()

def get_unit_from_format(format):
    return "GB"

def is_online(ip):
    # 检查ip的格式
    # 1. 如果以http://或者https://开头，则去掉http://或者https://的部分，检查剩下的部分是否是合法的ipv4地址
    ipv4 = ""
    if ip is None:
        return False
    if ip.startswith("http://"):
        ipv4 = ip[7:]
    elif ip.startswith("https://"):
        ipv4 = ip[8:]
    else:
        ipv4 = ip
    # 2. 如果ip不是合法的ipv4地址，则返回"false"
    if not is_valid_ipv4(ipv4):
        return False
    # 3. 如果能ping通，则返回True，否则返回False
    if ping(ipv4):
        return True
    else:
        return False
    
def get_ratio_by_gwid(gwid: str):
    default_val = 1.3
    if gwid is None:
        print("[get_ratio_by_gwid]: gwid is None, return default value 1.3")    
        return default_val
    else:
        # 获取settings变量
        
        if settings is None:
            print("[get_ratio_by_gwid]: settings is None, return default value 1.3") 
            return default_val
        elif settings.ratio_table is None:
            print("[get_ratio_by_gwid]: settings.ratio_table is None, return default value 1.3") 
            return default_val
        else:
            # 从ratio_table中获取ratio
            print(f"[get_ratio_by_gwid]: get ratio from settings.ratio_table, ratio_table is {settings.ratio_table}")
            ratio = settings.ratio_table[gwid]
            return ratio
    return default_val

def str_strip(data):
    if data is None:
        return None
    else:
        ret = data.replace("\n", "")
        ret = ret.replace("|", "")
        return ret

async def get_device_count(gwid):
    """
    根据gwid,获取网关的设备数量
    如果发生错误，则返回0
    """
    try:
        with gw_login(gwid) as sdk_obj:
            r = sdk_obj.list_online_users(1000, "")
            r = get_basic_rpc_result(r)
            r = str_strip(r["result"])
            print(r)
            r = json.loads(r)
            r = r["result"]

            count = len(r)
            print(f"[get_device_count]: gwid = {gwid}, count = {count}")
            return count
    except Exception as e:
        print(f"Error occurred while getting device count for gwid {gwid}: {e}")
        return 0
    
async def get_gws_device_count(gws):
    """
    从给定网关列表，返回key/value对，key是网关id，value是网关的设备数量
    """
    kv = {}
    response = supabase.table("gateway").select("*").execute()
    gws = []
    for r in response.data:
        gwid = r.get("id")
        gws.append(gwid)
    # 2. 使用asyncio.gather来将每一个get_device_count(gwid)的结果，成为一个数组results
    tasks = [get_device_count(gwid) for gwid in gws]
    results = await asyncio.gather(*tasks)
    for i, gwid in enumerate(gws):
        key = gws[i]
        value = results[i]
        kv[key] = value
    return kv
