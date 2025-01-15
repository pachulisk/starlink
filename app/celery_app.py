from celery.app import Celery
from celery.schedules import crontab
import uuid
from .task import TaskRequest, run_single_task
import json
import asyncio
# user="gateway"
# password='Tplink753'
# host="rmq.bingbing.tv"
# vhost="gateway"
protocol="redis"
vhost="0"
host="107.172.190.217"
port="6379"
broker_url = f"{protocol}://{host}:{port}/{vhost}"
backend_url = broker_url
celery = Celery(__name__, broker=broker_url, backend=backend_url)


@celery.task
def add(obj):
    print("add delay: {}".format(obj))
    x = obj.get("x")
    y = obj.get("y")
    return x + y

@celery.task
def perform_task_celery(task_str:str):
    asyncio.run(do_perform_task_celery(task_str))

async def do_perform_task_celery(task_str:str):
    print("perform_task_celery: {}".format(task_str))
    task = json.loads(task_str)
    task_id = str(uuid.uuid4())
    command = task.get("cmd")
    data = task.get("data")
    print(f"task_id: {task_id}, command: {command}, data: {data}")
    tr = TaskRequest(id=task_id, command=command, data=data)
    res = await run_single_task(tr)
    return res