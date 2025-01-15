from datetime import datetime
from fastapi import APIRouter, HTTPException
from app.supabase import supabase
from app.utils import get_gateway_by_id
from pydantic import BaseModel,ConfigDict
from fastapi.encoders import jsonable_encoder
from app.sdk import SDK
account = APIRouter()

class AccountRequest(BaseModel):
    gwid: str

@account.post("/list_account", tags=["account"])
async def list_account(request: AccountRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="未找到网关")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            accounts = sdk.list_account()
            return {"accounts": accounts}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

# 将list_account获得的结果，同步到supabase
@account.post("/sync_account", tags=["account"])
async def sync_account(request: AccountRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="")
    username = gw.data[0].get('username')