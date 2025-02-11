from app.supabase import supabase
import platform    # For getting the operating system name
import subprocess  # For executing a shell command

async def get_gateway_by_id(gwid: str):
    response = supabase.table("gateway").select("*").eq("id", gwid).execute()
    return response


def ping(host):
    """
    Returns True if host (str) responds to a ping request.
    Remember that a host may not respond to a ping (ICMP) request even if the host name is valid.
    """
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