import luigi
import json
import logging
from typing import List, Dict
from ..supabase import supabase
from ..utils import get_gw_users_list, upsert_user
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 输出到控制台
        logging.FileHandler("app.log")  # 输出到文件
    ]
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.FileHandler("luigi.log"))

class UpsertDeviceToSupabase(luigi.Task):
    """
    将设备列表数据upsert到Supabase的device_list表
    参数说明：
    - device_json: 需要upsert的设备列表（JSON字符串格式）
    """
    device_json = luigi.Parameter()
    def run(self):
        # 初始化Supabase客户端
        # 将JSON字符串解析为Python对象
        try:
            devices: List[Dict] = json.loads(self.device_json)
        except json.JSONDecodeError as e:
            raise ValueError("无效的JSON格式数据") from e
        # 批量执行upsert操作
        for device in devices:
            # 执行upsert操作
            response = supabase.table("device_list").upsert(device).execute()
            # 检查错误（supabase-python的响应结构可能不同，请根据实际情况调整）
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Supabase操作失败: {response.error}")
        # 写入完成标记
        with self.output().open("w") as f:
            f.write(json.dumps({"status": "success", "count": len(devices)}))
    def output(self):
        """任务完成标记文件"""
        return luigi.LocalTarget(f"data/upsert_complete_{self.task_id}.json")

class UpsertUsersToSupabase(luigi.Task):
    """
    将用户列表数据upsert到Supabase的gw_user表
    
    参数说明：
    - users_json: 需要upsert的用户列表（JSON字符串格式）
    """

    users_json = luigi.Parameter()
    def run(self):
        # 初始化Supabase客户端

        # 将JSON字符串解析为Python对象
        try:
            users: List[Dict] = json.loads(self.users_json)
        except json.JSONDecodeError as e:
            raise ValueError("无效的JSON格式数据") from e

        # 批量执行upsert操作
        for user in users:
            # if "online" in user:
            #     #删除online字段
            #     del user["online"]
            # 执行upsert操作
            logger.info(f"upsert user: {user}")
            response = upsert_user(user)
            # response = supabase.table("gw_users").upsert(user).execute()
            # 检查错误（supabase-python的响应结构可能不同，请根据实际情况调整）
            logger.info(f"response: {response}")
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Supabase操作失败: {response.error}")
        # 写入完成标记
        with self.output().open("w") as f:
            f.write(json.dumps({"status": "success", "count": len(users)}))

    def output(self):
        """任务完成标记文件"""
        return luigi.LocalTarget(f"data/upsert_complete_{self.task_id}.json")

class ReadGwUsers(luigi.Task):
    """
    从网关的sdk里面，读取用户列表的数据
    参数说明：
    - gwid: 需要同步的网关名称，字符串
    """
    gwid = luigi.Parameter()
    def run(self):
        # 从网关的sdk里面，读取用户列表的数据
        lst = get_gw_users_list(self.gwid)
        with self.output().open("w") as f:
            f.write(json.dumps(lst))
    def output(self):
        """任务完成标记文件"""
        return luigi.LocalTarget(f"data/read_gwusers_complete_{self.task_id}.json")
    
class SyncGwUsers(luigi.Task):
    """
    将用户列表数据upsert到Supabase的gw_user表
    参数说明：
    - users_json: 需要upsert的用户列表（JSON字符串格式）
    """
    gwid = luigi.Parameter()
    def requires(self):
        # 定义此任务依赖的任务
        return ReadGwUsers(gwid=self.gwid)
    
    def output(self):
        """任务完成标记文件"""
        return luigi.LocalTarget(f"data/sync_gwusers_complete_{self.task_id}.json")

    def run(self):
        # 读取上一个任务生成的数据文件
        with self.input().open('r') as infile, self.output().open('w') as outfile:
            # 将JSON字符串解析为Python对象
            try:
                users_json = infile.read()
                users: List[Dict] = json.loads(users_json)
            except json.JSONDecodeError as e:
                raise ValueError("无效的JSON格式数据") from e

            # 批量执行upsert操作
            for user in users:
                # if "online" in user:
                #     #删除online字段
                #     del user["online"]
                # 执行upsert操作
                logger.info(f"upsert user: {user}")
                response = upsert_user(user)
                # response = supabase.table("gw_users").upsert(user).execute()
                # 检查错误（supabase-python的响应结构可能不同，请根据实际情况调整）
                logger.info(f"response: {response}")
                if hasattr(response, 'error') and response.error:
                    raise Exception(f"Supabase操作失败: {response.error}")
            # 写入完成标记
            outfile.write(json.dumps({"status": "success", "count": len(users)}))
    