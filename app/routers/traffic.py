from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.supabase import supabase
import uuid
from ..sdk import SDK
from ..task import TaskRequest, run_single_task
from ..utils import get_gateway_by_id, gw_login
from pydantic import BaseModel
import json

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
