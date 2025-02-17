from urllib.parse import urlparse
from app.supabase import supabase
import platform    # For getting the operating system name
import subprocess  # For executing a shell command
import re
import ipaddress
from fastapi import HTTPException
from .sdk import SDK

from contextlib import contextmanager


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

def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """
    if is_url(host) is False:
        return False
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
async def gw_login(gwid):
    print("==gw_login")
    try:
        gw = await get_gateway_by_id(gwid)
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