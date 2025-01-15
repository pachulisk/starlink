import requests
import json
import hashlib
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# 将 WFilterNGF 类重命名为 SDK
class SDK:
    def __init__(self):
        self.session_id = "00000000000000000000000000000000"
        self.cmd_id = 1
        self.server = ""

    def rpc_call(self, object, method, para):
        headers = {'Content-Type': 'application/json'}
        data = {
            "jsonrpc": "2.0",
            "id": self.cmd_id,
            "method": "call",
            "params": [self.session_id, object, method, para]
        }
        self.cmd_id += 1
        
        response = requests.post(f"{self.server}/ubus", json=data, headers=headers, timeout=20)
        response.raise_for_status()
        return response.json()

    def login(self, server, username, password):
        self.server = server
        data = {"username": username, "password": hashlib.md5(password.encode()).hexdigest()}
        response = self.rpc_call("session", "login", data)
        if "result" in response and len(response["result"]) > 1:
            self.session_id = response["result"][1]["ubus_rpc_session"]
            return True
        return False

    def logout(self):
        self.rpc_call("session", "destroy", "")

    def renew_user(self, userid, newdate):
        data = {"data": f"wfilter-isp renew {userid} {newdate}"}
        response = self.rpc_call("wfilter", "exec", data)
        return newdate in str(response)

    def add_virtual_group(self, groupid, ip, minutes):
        data = {"id": groupid, "ip": ip, "minute": str(minutes)}
        response = self.rpc_call("wfilter", "add-virtual-group", data)
        return "0000" in str(response)

    def list_virtual_group(self, groupid):
        data = {"data": groupid}
        return self.rpc_call("wfilter", "list-virtual-group", data)

    def rm_virtual_group(self, ip):
        data = {"data": ip}
        response = self.rpc_call("wfilter", "rm-virtual-group", data)
        return "0000" in str(response)

    def list_group(self):
        data = {"config": "wfilter-groups"}
        return self.rpc_call("uci", "get", data)

    def list_account(self):
        data = {"config": "wfilter-account"}
        return self.rpc_call("uci", "get", data)

    def list_bandwidth(self, seconds):
        data = {"data": str(seconds)}
        return self.rpc_call("wfilter", "list-bandwidth", data)

    def list_online_users(self, top, search):
        data = {"row": str(top), "search": search}
        return self.rpc_call("wfilter", "get-top-bandwidth", data)

    def list_online_connections(self, ip):
        data = {"data": ip}
        return self.rpc_call("wfilter", "get-links", data)

    def kill_connection(self, ip, port, type, minutes, message):
        data = {
            "ip": ip,
            "port": str(port),
            "proto": type,
            "minutes": str(minutes),
            "message": message
        }
        response = self.rpc_call("wfilter", "kill-link", data)
        return "0000" in str(response)

    def get_network_interfaces(self):
        return self.rpc_call("network.interface", "dump", {})

    def get_network_status(self):
        return self.rpc_call("network.device", "status", {})

    def add_user(self, ip, user, from_source, expire):
        data = {
            "ip": ip,
            "user": user,
            "from": from_source,
            "expire": str(expire)
        }
        response = self.rpc_call("wfilter", "add-user", data)
        return "0000" in str(response)

    def rm_user(self, user):
        data = {"data": user}
        response = self.rpc_call("wfilter", "rm-user", data)
        return "0000" in str(response)

    def config_load(self, cfgname):
        data = {"config": cfgname}
        return self.rpc_call("uci", "get", data)

    def config_add(self, cfgname, type, name, values):
        data = {
            "config": cfgname,
            "type": type,
            "name": name,
            "values": values
        }
        return self.rpc_call("uci", "add", data)

    def config_set(self, cfgname, section, values):
        data = {
            "config": cfgname,
            "section": section,
            "values": values
        }
        return self.rpc_call("uci", "set", data)

    def config_del(self, cfgname, section):
        data = {
            "config": cfgname,
            "section": section
        }
        return self.rpc_call("uci", "delete", data)

    def config_apply(self):
        data = {"data": "busybox sh /usr/sbin/wfilter-apply"}
        return self.rpc_call("wfilter", "exec", data)

    def query_db(self, dbname, querysql):
        data = {
            "dbname": dbname,
            "sql": querysql
        }
        return self.rpc_call("wfilter", "querydb", data)

# app = FastAPI()

# 删除或注释掉这行
# ngf = WFilterNGF()

class LoginData(BaseModel):
    server: str
    username: str
    password: str

# @app.post("/login")
# def login(data: LoginData):
#     if ngf.login(data.server, data.username, data.password):
#         return {"status": "success"}
#     raise HTTPException(status_code=401, detail="登录失败")

# @app.post("/logout")
# def logout():
#     ngf.logout()
#     return {"status": "success"}

# 其他API端点的实现...

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
