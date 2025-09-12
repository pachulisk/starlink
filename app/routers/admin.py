from fastapi import APIRouter, Depends, HTTPException
from .auth import super_admin_required, UserBase, reset_password_impl
from pydantic import BaseModel
from typing import List, Any
from ..utils import is_empty
from app.supabase import supabase
admin_router = APIRouter()


class ResetPasswordQuery(BaseModel):
  userid: str
  password: str

@admin_router.post("/reset_password", tags=["admin"])
async def reset_password(query: ResetPasswordQuery, current_user: UserBase = Depends(super_admin_required)):
  """
  重置密码，超级管理员专用
  输入1: userid，用户global_id
  输入2: password，str，明文密码
  """
  userid = query.userid
  password = query.password
  if is_empty(userid) or is_empty(password):
    raise HTTPException(status_code=400, detail="[reset_password]userid and password are required")
  ret = reset_password_impl(userid, password)
  print(f"[reset_password]: userid = {userid}, ret = {str(ret)}")
  return {"data": ret}

@admin_router.post("/get_user_auth_list", tags=["admin"])
async def get_user_auth_list(current_user: UserBase = Depends(super_admin_required)):
  """
  超级管理员专属调用
  获取所有的user_auth列表
  """
  TABLE_NAME = "user_auth"
  resp = supabase.table(TABLE_NAME).select("*").execute()
  data = []
  for item in resp.data:
    p = {
      "id": item["global_id"],
      "username": item["username"]
    }
    data.append(p)
  return {"data": data}

class GetUserGwsQuery(BaseModel):
  userid: str

@admin_router.post("/get_user_gws", tags=["admin"])
async def get_user_gws(query: GetUserGwsQuery, current_user: UserBase = Depends(super_admin_required)):
  """
  超级管理员专属调用
  获取对应用户的网关。
  输入参数:
  1. 用户id = userid, str
  """
  userid = query.userid
  if is_empty(userid):
    return {"data": []}
  TABLE_NAME = "user_auth_gateways_view"
  response = supabase.table(TABLE_NAME).select("*").eq("userid", userid).execute()
  print(f"[get_user_gws]: userid = {userid}, response = {str(response.data)}")
  return {"data": response.data}


class AppendUserGwsQuery(BaseModel):
  userid: str
  gwids: List[str]

@admin_router.post('/append_user_gws', tags=["admin"])
async def append_user_gws(query: AppendUserGwsQuery, current_user: UserBase = Depends(super_admin_required)):
  """
  超级管理员专属调用
  填补用户对应的网关。
  输入参数：
  1. 用户id = userid，str
  2. 网关id列表 = gwids, List[str]
  """
  userid = query.userid
  gwids = query.gwids
  # 检查username是否为空，如果为空则直接抛出异常
  if is_empty(userid):
    raise HTTPException(status_code=400, detail="[append_user_gws]username is required")
  TABLE_NAME = "user_auth_gateways"
  # 批量插入
  data = []
  for gwid in gwids:
    data.append({
      "global_id": f"{userid}_{gwid}",
      "userid": userid,
      "gwid": gwid
    })
  print(f"[append_user_gws]prepare to append data: f{str(data)}")
  res = supabase.table(TABLE_NAME).upsert(data).execute()
  print(f"[append_user_gws]successfully update user gws: res = f{str(res)}")
  return { "data": res.data }

class UpdateUserGwsQuery(BaseModel):
  userid: str
  gwids: List[str]

@admin_router.post('/update_user_gws', tags=["admin"])
async def update_user_gws(query: UpdateUserGwsQuery, current_user: UserBase = Depends(super_admin_required)):
  """
  超级管理员专属调用
  更新用户对应的网关。
  输入参数：
  1. 用户id = userid，str
  2. 网关id列表 = gwids, List[str]
  """
  userid = query.userid
  gwids = query.gwids
  # 检查username是否为空，如果为空则直接抛出异常
  if is_empty(userid):
    raise HTTPException(status_code=400, detail="[update_user_gws]username is required")
  # 首先，对于user_auth_gateways表中，删除所有userid为{userid}的行
  TABLE_NAME = "user_auth_gateways"
  res = supabase.table(TABLE_NAME).delete().eq('userid', userid).execute()
  print(f"[update_user_gws]remove original userid = {userid}, result = {str(res)}")
  # 然后批量插入
  data = []
  for gwid in gwids:
    data.append({
      "global_id": f"{userid}_{gwid}",
      "userid": userid,
      "gwid": gwid
    })
  print(f"[update_user_gws]prepare to append data: f{str(data)}")
  res = supabase.table(TABLE_NAME).upsert(data).execute()
  print(f"[update_user_gws]successfully update user gws: res = f{str(res)}")
  return { "data": res.data }