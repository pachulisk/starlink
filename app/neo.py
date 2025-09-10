from neo4j import GraphDatabase, exceptions
import os

class Neo4jConnection:
    def __init__(self, uri, user, password):
        """初始化Neo4j连接"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        """关闭连接"""
        self.driver.close()
        
    def run_query(self, query, parameters=None):
        """
        执行Cypher查询
        :param query: Cypher查询语句
        :param parameters: 查询参数（字典形式）
        :return: 查询结果列表
        """
        results = []
        try:
            with self.driver.session() as session:
                # 执行查询
                result = session.run(query, parameters)
                # 处理结果
                for record in result:
                    results.append(record.data())
            return results
        except exceptions.AuthError:
            print("认证失败：请检查用户名和密码")
        except exceptions.ServiceUnavailable:
            print("连接失败：请检查Neo4j服务是否可用或网络是否通畅")
        except exceptions.Neo4jError as e:
            print(f"查询执行错误：{str(e)}")
        return None
    
    def upsert_gateway(self, gateway_id, ratio):
        """
        创建或更新gateway节点
        :param gateway_id: 节点的id属性
        :param ratio: 节点的ratio属性值
        :return: 处理后的节点信息
        """
        # Cypher语句：存在则更新，不存在则创建
        query = """
        MERGE (g:gateway {id: $id})
        SET g.ratio = $ratio
        RETURN g.id AS id, g.ratio AS ratio
        """
        
        try:
            with self.driver.session() as session:
                # 执行参数化查询
                result = session.run(
                    query, 
                    id=gateway_id,  # 传递参数
                    ratio=ratio
                )
                # 获取结果（返回的是处理后的节点属性）
                record = result.single()
                if record:
                    return {
                        "id": record["id"],
                        "ratio": record["ratio"],
                        "status": "created"
                    }
                return None
        except exceptions.AuthError:
            print("认证失败：请检查用户名和密码")
        except exceptions.ServiceUnavailable:
            print("连接失败：请检查Neo4j服务是否可用或网络是否通畅")
        except exceptions.Neo4jError as e:
            print(f"操作错误：{str(e)}")
        return None

NEO4J_URI = os.getenv("APP_NEO4J_URI")
NEO4J_USER = os.getenv("APP_NEO4J_USER")
NEO4J_PASSWORD = os.getenv("APP_NEO4J_PASSWORD")

neo = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
