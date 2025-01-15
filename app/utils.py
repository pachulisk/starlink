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