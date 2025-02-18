from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.supabase import supabase, to_date
import uuid
from ..sdk import SDK
from ..task import TaskRequest, run_single_task
from ..utils import get_gateway_by_id, gw_login, get_date_obj_from_str, get_start_of_month, get_end_of_month, get_date
from pydantic import BaseModel
from datetime import datetime
import json
import pandas as pd

traffic = APIRouter()

class SyncTableTask(BaseModel):
    table_name: str
    gwid: str
    column: str


async def sync_table(query: SyncTableTask):
  """
  同步网关的流量数据到supabase
  """
  column = query.column or "happendate"
  gwid = query.gwid
  table_name = query.table_name

async def perform_task(task_str: str):
  print("perform_task_celery: {}".format(task_str))
  task = json.loads(task_str)
  task_id = str(uuid.uuid4())
  command = task.get("cmd")
  data = task.get("data")
  print(f"task_id: {task_id}, command: {command}, data: {data}")
  tr = TaskRequest(id=task_id, command=command, data=data)
  res = await run_single_task(tr)
  return res

# 同步网关的流量数据到supabase
#!/bin/bash
# curl -X 'POST' \
#   'http://107.172.190.217:8000/sync_task_celery' \
#   -H 'accept: application/json' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "table_name": "hourreport",
#   "gwid": "97935833-c028-4f7b-ad5f-26f296cf935a",
#   "column": "happendate"
# }'

class GetGWTrafficParam(BaseModel):
    gwid: str
    start_date: str | None = None
    end_date: str | None = None
    format: str | None = None

def calculate_start_and_end_str(start_date, end_date):
    start_time_str = ""
    end_time_str = ""
    # 如果start_date和end_date都为空，则将start_time_str设置为月初，end_time_str设置为月末
    if start_date is None and end_date is None:
        start_time_str = get_start_of_month(datetime.today(), True)
        end_time_str = get_end_of_month(datetime.today(), True)
    elif start_date is not None and end_date is None:
        # 开始日期不空，结束日期为空，则为开始日期-现在
        start_d = get_date_obj_from_str(start_date)
        start_time_str = get_date(start_d, True)
        end_time_str = get_date(datetime.today(), True)
    elif start_date is not None and end_date is not None:
        # 开始日期和结束日期都不空
        start_time_str = get_date(get_date_obj_from_str(start_date), True)
        end_time_str = get_date(get_date_obj_from_str(end_date), True)
    else:
        raise(HTTPException(status_code=400, detail="start_date and end_date must be provided"))
    return [start_time_str, end_time_str]

def get_data_with_format(data, format):
    if format == "csv":
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    else:
        return data

@traffic.post("/get_gw_traffic", tags=["traffic"])
async def get_gw_traffic(query: GetGWTrafficParam):
    # 1. 获取gwid, user和日期date
    # 2. 如果date为空，则默认查询时间设置为本年本月；否则按照date查询
    # 3. 从supabase查询hourreport表，筛选日期在date的月份范围内
    gwid = query.gwid
    start_date = query.start_date
    end_date = query.end_date
    times = calculate_start_and_end_str(start_date, end_date)
    start_time_str = times[0]
    end_time_str = times[1]
    format = query.format
    
    response = (supabase
        .table('hourreport')
        .select("*")
        .eq("gwid", gwid)
        .gte("happendate", start_time_str)
        .lte("happendate", end_time_str)
        .execute())
    # 4. 归集结果
    if len(response.data) <= 0:
        return {"data": get_data_with_format([], format)}
    else:
        list = []
        for d in response.data:
            up = d["uptraffic"]
            down = d["downtraffic"]
            list.append({
                # "acct": d["acct"],
                "up": up,
                "down": down,
                "total": f"{float(up) + float(down)}",
                "happendate": to_date(d["happendate"])
            })
        return {"data": get_data_with_format(list, format)}