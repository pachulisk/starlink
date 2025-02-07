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