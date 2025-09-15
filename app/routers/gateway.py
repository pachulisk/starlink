from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends
from app.supabase import supabase
from pydantic import BaseModel,ConfigDict
from fastapi.encoders import jsonable_encoder
from ..utils import get_device_count, gw_login, get_gws_device_count, get_ratio_by_gwid, is_online, starts_with_number, ping_multi_hosts, is_valid_ipv4, ping, normalize_traffic
from .auth import UserBase, get_current_user, get_user_auth_gateways, is_super_admin
router = APIRouter()

def parse_valid_ipv4(addr):
    ipv4 = ""
    if addr is None:
        return ""
    if addr.startswith("http://"):
        ipv4 = addr[7:]
    elif addr.startswith("https://"):
        ipv4 = addr[8:]
    else:
        ipv4 = addr
    if not is_valid_ipv4(ipv4):
        return ""
    else:
        return ipv4

def check_online_multi(addrs, gws=None):
    # addrs是候选的地址列表。例如["10.188.188.12", "10.188.188.13"]
    # 返回一个kv, key是addrs中的每一个地址，value是True或者False,
    # True代表online, False代表offline
    # 1. 检查addrs是否为空，如果为空，直接返回空kv
    if addrs is None or []:
        return {}
    # 2. 构造返回值，初始化为全部false
    ret = {}
    for addr in addrs:
        ret[addr] = False
    # 3. 从addrs中筛选出合法的ipv4地址，放到valid_ipv4中
    valid_ipv4 = []
    mapping = {}
    addr2gwid = {}
    for i, addr in enumerate(addrs):
        candidate = parse_valid_ipv4(addr)
        if len(candidate) > 0:
            valid_ipv4.append(candidate)
            mapping[candidate] = addr
            addr2gwid[addr] = gws[i] if gws is not None and i < len(gws) else None
    print("candidates = ", valid_ipv4)
    # 4. 对于所有valid_ipv4作为地址，传入到ping_multi_hosts
    multi_ping_result = ping_multi_hosts(valid_ipv4)
    # 5. 遍历multi_ping_result中的key, 从key反查addr，更新ret
    for key in multi_ping_result.keys():
        addr = mapping.get(key)
        if addr is None:
            continue
        gwid = addr2gwid.get(addr)
        if gwid is None:
            continue
        try:
            with gw_login(gwid) as _:
                print(f"[check_online_multi]: login success gwid={gwid}, addr={addr}")
                ret[addr] = True
        except Exception as e:
            continue
    # 6. 返回ret
    return ret

class TestMultiPingParam(BaseModel):
    addr: list[str]

@router.post("/test_multi_ping", tags=["test"])
async def test_multi_ping(query: TestMultiPingParam):
    print(query.addr)
    response = check_online_multi(query.addr)
    return { "data": response }
    
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
            "online": is_online(item.get('address')),
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
    

class GetDeviceCountByGwidQuery(BaseModel):
    gwid: str    
    
@router.post("/get_device_count", tags=["gateway"])
async def get_device_count_by_gwid(query: GetDeviceCountByGwidQuery):
    """
    根据gwid查询网关的设备
    入参: gwid=网关id，不为空
    返回值: device_count, 设备数量
    """
    gwid = query.gwid
    # 如果gwid为空则返回错误
    if gwid is None or gwid == "":
        raise HTTPException(status_code=400, detail="gwid为空")
    count = await get_device_count(gwid)
    return {"device_count": count }

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
async def read_gateways(current_user: UserBase = Depends(get_current_user)):
    """
    根据当前的用户权限，获取所有的网关
    """
    # 获取userid
    userid = current_user.id
    print(f"[read_gateways]:开始获取gwids, userid = {userid}")
    gwids = get_user_auth_gateways(userid)
    print(f"[read_gateways]:完成获取gwids, gwids = {str(gwids)}")
    # 特殊判断：如果当前用户是SUPER_ADMIN，则获取所有网关，否则获取对应gwids的
    if is_super_admin(current_user):
        response = supabase.table("gateway_monthly_view").select("*").execute()
    else:
        response = supabase.table("gateway_monthly_view").select("*").in_("id", gwids).execute()
    
    # 处理gws
    gws = []
    for item in response.data:
        id = item.get("id")
        gws.append(id)

    list = []
    # 从response.data中，抽取address部分，做ip连通性检测
    addrs = []
    for item in response.data:
        addrs.append(item.get("address"))
    check_ip_result = check_online_multi(addrs, gws)

    # kvs = await get_gws_device_count(gws)

    for item in response.data:
        gwid = item["id"]
        up = item.get("up") or 0
        down = item.get("down") or 0
        total_traffic = up + down
        device_count = 0
        user_count = item.get("user_count") or 0
        ratio = get_ratio_by_gwid(gwid)

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
            "online": check_ip_result.get(item.get("address")),
            "fleet": item.get('fleet'), 
            "total_traffic": normalize_traffic(total_traffic, "GB", ratio), # 网关流量
            "device_count": device_count, # 网关设备数
            "user_count": user_count, # 网关用户数
        })        
    return list

@router.post("/get_full_gateways", tags=["gateway"])
async def get_full_gateways():
    # use supabase client to read all gateways from 'gateway' table
    response = supabase.table("gateway_view").select("*").execute()
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
    # 处理gws
    gws = []
    for item in response.data:
        id = item.get("id")
        gws.append(id)
    

    list = []
    # 从response.data中，抽取address部分，做ip连通性检测
    addrs = []
    for item in response.data:
        addrs.append(item.get("address"))
    check_ip_result = check_online_multi(addrs, gws)

    kvs = await get_gws_device_count(gws)

    for item in response.data:
        gwid = item["id"]
        up = item.get("up") or 0
        down = item.get("down") or 0
        total_traffic = up + down
        device_count = kvs.get(gwid, 0)
        user_count = item.get("user_count") or 0
        ratio = get_ratio_by_gwid(gwid)

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
            "online": check_ip_result.get(item.get("address")),
            "fleet": item.get('fleet'), 
            "total_traffic": normalize_traffic(total_traffic, "GB", ratio), # 网关流量
            "device_count": device_count, # 网关设备数
            "user_count": user_count, # 网关用户数
        })        
    return list

# 创建
@router.post("/add_gateway", tags=["gateway"])
async def create_gateway(gw: Gateway, current_user: UserBase = Depends(get_current_user)):
    """
    增加新的网关，只有超级管理员才能使用
    """
    if not is_super_admin(current_user):
        raise HTTPException(status_code=401, detail="无权限访问该接口。")
    ip = gw.address
    # 如果是以数字开头的，则加上http://前缀
    if starts_with_number(ip):
        ip = f"http://{ip}"
    online = is_online(ip)
    if online is False:
        raise HTTPException(status_code=400, detail="网关不在线")
    res = supabase.table("gateway").insert({
        "name": gw.name,
        "password": gw.password,
        "username": gw.username,
        "port": gw.port,
        "password": gw.password,
        "address": ip,
        "serial_no": gw.serial_no,
        "client_name": gw.client_name,
        "enable_time": gw.enable_time,
        "online": "false",
        "fleet": gw.fleet,
    }).execute()
    return res


# 删除
@router.delete("/gateways/{gwid}", tags=["gateway"])
async def delete_gateway(gwid: str, current_user: UserBase = Depends(get_current_user)):
    """
    删除网关，只有超级管理员才能使用
    """
    if not is_super_admin(current_user):
        raise HTTPException(status_code=401, detail="无权限访问该接口。")
    res = supabase.table('gateway').delete().eq('id', gwid).execute()
    return res

@router.post("/fetch_gwids", tags=["gateway"])
async def fetch_gwids():
    """
    获取所有gwids
    """
    TABLE_NAME = "gateway"
    resp = supabase.table(TABLE_NAME).select("id").execute()
    data = [item['id'] for item in resp.data]
    return {"data": data}

# 更新
@router.patch("/gateways/{gwid}", tags=["gateway"])
async def update_gateway(gwid: str, gw: Gateway, current_user: UserBase = Depends(get_current_user)):
    """
    更新网关内容
    只对当前有权限的用户生效
    """
    gwids = get_user_auth_gateways(current_user.id)
    if gwid not in gwids:
        if not is_super_admin(current_user):
            raise HTTPException(status_code=401, detail="无权限访问该接口。")
    print("== start update gateway, gwid = ", gwid)
    update_gw_encoded = jsonable_encoder(gw)
    print("== update_gw_encoded: ", update_gw_encoded)
    res = supabase.table('gateway').update(update_gw_encoded).eq('id', gwid).execute()
    return res
    
# creation
@router.put("gateways", tags=["gateway"])
async def create_gateway(gw: Gateway):
    gw_json = jsonable_encoder(gw)
    res = supabase.table('gateway').insert(gw_json).execute()
    return res