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
        })
    return ret

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