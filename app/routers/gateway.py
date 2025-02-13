from datetime import datetime
from fastapi import APIRouter
from app.supabase import supabase
from pydantic import BaseModel,ConfigDict
from fastapi.encoders import jsonable_encoder

router = APIRouter()

class Gateway(BaseModel):
    name: str
    password: str | None = None
    username: str | None = None
    port: str | None = None
    address: str | None = None
    serial_no: str | None = None
    client_name: str | None = None
    enable_time: str | None = None
    fleet: str | None = None

# get one
@router.get("/gateways/{gwid}", tags=["gateway"])
async def get_gateway(gwid: str):
    # use supabase client to read one gateway from 'gateway' table
    response = supabase.table("gateway").select("*").eq("id", gwid).execute()
    print(response.data)
    ret = []
    for item in response.data:
        print(item)
        print(type(item))
        ret.append({
            "id": item.get('id'),
            "name": item.get('name'),
            "username": item.get('username'),
            "port": item.get('port'),
            "address": item.get('address'),
            "password": item.get('password'),
            "serial_no": item.get('serial_no'),
            "client_name": item.get('client_name'),
            "enable_time": item.get('enable_time'),
            "online": item.get('online'),
            "fleet": item.get('fleet'),
        })
    return ret

def get_total_traffic_by_gwid(gwid: str):
    """
    输入网关的gwid，获取网关的上行流量和下行流量，单位是bytes
    """
    # 查询supabase的total_traffic_by_gwid表，查询up, down两个字段
    response = supabase.table("total_traffic_group_by_gwid").select("up, down").eq("gwid", gwid).execute()
    if response.data:
        up = response.data[0]["up"]
        down = response.data[0]["down"]
        return [up, down]
    else:
        return [0, 0]
    
# 根据gwid查询设备数量
def get_device_by_gwid(gwid: str):
    """
    输入网关的gwid，获取网关的设备数量
    """
    # 查询supabase的device_count_group_by_gwid表，查询count(*)
    response = supabase.table("device_count_group_by_gwid").select("down").eq("gwid", gwid).execute()
    if response.data:
        return response.data[0]["down"]
    else:
        return 0

# 根据gwid查询用户数量
def get_users_count_by_gwid(gwid: str):
    """
    输入网关的gwid，获取网关的用户数量
    """
    # 查询supabase的gw_user_count_group_by_gwid表，查询count(*)
    response = supabase.table("gw_user_count_group_by_gwid").select("count").eq("gwid", gwid).execute()
    if response.data:
        return response.data[0]["count"]
    else:
        return 0

# 列表
@router.get("/gateways/", tags=["gateway"])
async def read_gateways():
    # use supabase client to read all gateways from 'gateway' table
    response = supabase.table("gateway").select("*").execute()
    """
    {
      "data": [
        {
          "id": 1,
          "name": "Afghanistan"
        },
        {
          "id": 2,
          "name": "Albania"
        },
        {
          "id": 3,
          "name": "Algeria"
        }
      ],
      "count": null
    }
    """
    list = []
    for item in response.data:
        gwid = item["id"]
        trafffic = get_total_traffic_by_gwid(gwid)
        total_traffic = trafffic[0] + trafffic[1]
        device_count = get_device_by_gwid(gwid)
        user_count = get_users_count_by_gwid(gwid)
        list.append({
            "id": item.get('id'),
            "name": item.get('name'),
            "username": item.get('username'),
            "port": item.get('port'),
            "address": item.get('address'),
            "password": item.get('password'),
            "serial_no": item.get('serial_no'),
            "client_name": item.get('client_name'),
            "enable_time": item.get('enable_time'),
            "online": item.get('online'),
            "fleet": item.get('fleet'), 
            "total_traffic": total_traffic, # 网关流量
            "device_count": device_count, # 网关设备数
            "user_count": user_count, # 网关用户数
        })        
    return response.data

# 创建
@router.post("/add_gateway", tags=["gateway"])
async def create_gateway(gw: Gateway):
    res = supabase.table("gateway").insert({
        "name": gw.name,
        "password": gw.password,
        "username": gw.username,
        "port": gw.port,
        "password": gw.password,
        "address": gw.address,
        "serial_no": gw.serial_no,
        "client_name": gw.client_name,
        "enable_time": gw.enable_time,
        "online": "false",
        "fleet": gw.fleet,
    }).execute()
    return res


# 删除
@router.delete("/gateways/{gwid}", tags=["gateway"])
async def delete_gateway(gwid: str):
    res = supabase.table('gateway').delete().eq('id', gwid).execute()
    return res

# 更新
@router.patch("/gateways/{gwid}", response_model=Gateway, tags=["gateway"])
async def update_gateway(gwid: str, gw: Gateway):
    update_gw_encoded = jsonable_encoder(gw)
    res = supabase.table('gateway').update(update_gw_encoded).eq('id', gwid).execute()
    return res
    
# creation
@router.put("gateways", response_model=Gateway, tags=["gateway"])
async def create_gateway(gw: Gateway):
    gw_json = jsonable_encoder(gw)
    res = supabase.table('gateway').insert(gw_json).execute()
    return res