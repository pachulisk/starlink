from fastapi import APIRouter, HTTPException
from .supabase import supabase
from pydantic import BaseModel
from .utils import ping
import pika
import json

mq_url = 'rmq.bingbing.tv'
mq_port = 80

task = APIRouter()

class TaskRequest(BaseModel):
    id: str
    command: str
    data: dict

# status: todo, doing, done
async def change_task_status(task_id, status):
    valid_status = {
        "todo": "todo",
        "doing": "doing",
        "done": "done"
    }
    if status not in valid_status:
        raise HTTPException(status_code=400, detail="status只能是todo, doing, done")
    res = supabase.table("tasks").update({ "status": status }).eq("task_id", task_id).execute()
    return res

async def set_task_result(task_id, result):
    res = supabase.table("tasks").update({ "result": result }).eq("task_id", task_id).execute()
    return res

class GetTaskStatusRequest(BaseModel):
    id: str
async def get_task_status(req: GetTaskStatusRequest):
    task_id = req.id
    res = supabase.table("tasks").select("status").eq("task_id", task_id).execute()
    return res.data[0].get("status")


async def perform_task(task):
    task_id = task.get("id")
    command = task.get("cmd")
    data = task.get("data")
    task_id = task.id
    if task_id is None:
        raise HTTPException(status_code=400, detail="task_id不能为空")
    # 1. 检查task的状态是否为todo,如果不是todo则不做任何处理，直接结束
    gts_req = GetTaskStatusRequest(id=task_id)
    status = await get_task_status(gts_req)
    if status != "todo":
        return {"data": [], "result": "status不是todo,跳过"}
    # 2. 将task的状态改变为doing, 开始执行任务
    await change_task_status(task_id, "doing")
    tr = TaskRequest(id=task_id, command=command, data=data)
    # 3. 执行，按照执行结果分情况处理
    # 3.0 执行
    res = await run_single_task(tr)
    # 3.1.1 将结果写回到result字段
    await set_task_result(task_id, res)
    if "error" not in res:
        # 3.1 执行成功：将task的状态改变为done
        await change_task_status(task_id, "done")
    else:
        # 3.2 执行失败：将task的状态改变为todo，将错误信息写入task的result字段
        await change_task_status(task_id, "todo")
    return res



# async def fetch_task_list(background_tasks: BackgroundTasks):
#     res = supabase.table("tasks").select("*").eq("status", "todo").execute()
#     task_list = res.data
#     for task in task_list:
#         background_tasks.add_task()
#     return res


async def insert_supabase(id, data):
    table_name = data.get("table_name")
    table_data = data.get("table_data")
    # get table_name
    if table_name is None:
        raise HTTPException(status_code=400, detail="table_name不能为空")
    # get table_data
    if table_data is None:
        raise HTTPException(status_code=400, detail="table_data不能为空")
    res = supabase.table(table_name).insert(table_data).execute()
    return res

async def upsert_supabase(id, data):
    table_name = data.get("table_name")
    table_data = data.get("table_data")
    keys = data.get("keys") or []
    strategy = data.get("strategy") or "UPSERT"
    # first build query against keys
    # conflict = False
    # if len(keys) > 0:
    #     r = supabase.table(table_name).select("*").limit(1)
    #     for i in range(len(keys)):
    #         r = r.eq(keys[i], table_data[keys[i]])
    #     r = r.execute()
    #     if len(r.data) > 0:
    #         conflict = True
    # if conflict is not True:
    #     res = supabase.table(table_name).insert(table_data).execute()
    #     return res
    # else:
    if strategy == "UPSERT":
        # r = supabase.table(table_name).update(table_data)
        # for i in range(len(keys)):
        #   r = r.eq(keys[i], table_data[keys[i]])
        # res = r.execute()
        # return res
        supabase.table(table_name).upsert(table_data)
    else: # IGNORE
        return {"data": [], "count": 0 }


def get_address_for_gwid(gwid: str):
    """get address field from supabase's gateway table"""
    res = supabase.table("gateway").select("address").eq("gwid", gwid).execute()
    if len(res.data) > 0:
        return res.data[0].get("address")
    else:
        return None
    
def set_supabase_gateway_status(gwid: str, is_online: bool):
    """setup gateway status in supabase's gateway table. 
    if is_online is true, set supabase's gateway table's 'online' field to 'online', else set to 'offline'"""
    if is_online is True:
        res = supabase.table("gateway").update({"online": "online"}).eq("gwid", gwid).execute()
    else:
        res = supabase.table("gateway").update({"online": "offline"}).eq("gwid", gwid).execute()
    return res

async def ping_server(id, data):
    gwid = data.get("gwid")
    address = get_address_for_gwid(gwid)
    # ping this address
    if address is None:
        return set_supabase_gateway_status(gwid, False)
    else:
        # use util's ping function
        val = ping(address)
        if val is False:
            return set_supabase_gateway_status(gwid, False)
        else:
            return set_supabase_gateway_status(gwid, True)

@task.post("/run_single_task", tags=["task"])
async def run_single_task(request: TaskRequest):
    id = request.id
    command = request.command
    data = request.data
    if command == "INSERT_SUPABSE":
        return await insert_supabase(id, data)
    elif command == "UPSERT_SUPABASE":
        return await upsert_supabase(id, data)
    elif command == "PING_SERVER":
        return await ping_server(id, data)
    else:
        return {"error": "command not found"}
    
async def post_single_task(task):
    return supabase.table("tasks").insert(task).execute()

@task.post("/post_single_task", tags=["task"])
async def post_single_task_mq(task):
    # TODO
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_url, port=mq_port))
    channel = connection.channel()
    channel.queue_declare(queue='tasks')
    # 发送任务到队列中
    channel.basic_publish(exchange='',routing_key='tasks', body=json.dumps(task))
    print(f"[x] send {json.dumps(task)}")