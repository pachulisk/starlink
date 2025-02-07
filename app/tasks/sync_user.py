import luigi
import json
import logging
from typing import List, Dict
from ..supabase import supabase
from ..utils import upsert_user
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

    