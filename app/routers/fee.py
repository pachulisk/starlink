
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from app.supabase import supabase, to_date
from ..utils import is_empty
fee = APIRouter()


class CreateFeePackageParam(BaseModel):
    gwid: str
    effective_date: str
    expiration_date: str
    remark: str
    traffic: float
    amount: float
    
@fee.post("/create_fee_package", tags=["fee"])
async def create_fee_package(query: CreateFeePackageParam):
    """
    新增网关流量资费包接口
    - 接收资费包信息并进行验证
    - 实际应用中，这里会将数据保存到数据库
    - 返回创建的资费包信息
    """
    # 检查gwid是否为空，如果gwid为空，则返回错误
    gwid = query.gwid
    if is_empty(gwid):
        raise HTTPException(status_code=400, detail="gwid is required")
    # 在fee_packages的supabase表中，插入一条新的数据
    data = {
        "gwid": query.gwid,
        "effective_date": to_date(query.effective_date),
        "expiration_date": to_date(query.expiration_date),
        "remark": query.remark,
        "traffic": query.traffic,
        "amount": query.amount,
    }
    TABLE_NAME = "fee_packages"
    _ = supabase.table(TABLE_NAME).insert(data).execute()
    return { "data": data }