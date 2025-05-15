from fastapi import FastAPI, HTTPException, Request ,Depends,status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings
import csv
from dotenv import load_dotenv
import asyncio
from starlette.status import HTTP_504_GATEWAY_TIMEOUT
load_dotenv()
from fastapi_jwt_auth.exceptions import AuthJWTException
from app.sdk import SDK
from .routers import gateway
from .routers import auth
from . import db, task
from .routers import auth2, traffic
from .routers import account, weather
from .routers import user, group
from .utils import settings, gw_login, get_gateway_by_id

import time
from fastapi import Request, Response
from starlette.types import ASGIApp

# 环境变量配置类
class Settings(BaseSettings):
    app_name: str = "默认应用名称"
    debug: bool = False
    ratio_file: str # 网关流量比例所在的文件，以csv为结尾的文件名
    ratio_table: any = None
    class Config:
        env_file = ".env"  # 从 .env 文件加载环境变量
        env_prefix = "APP_"  # 环境变量前缀，例如 APP_DATABASE_URL



# class DynamicTimeoutMiddleware:
#     def __init__(self, app: ASGIApp):
#         self.app = app

#     async def __call__(self, request: Request, call_next) -> Response:
#         # 针对特定路径调整超时
#         long_timeout_urls = {
#             "/upload_config": 100000,
#             "/sync_traffics": 100000,
#         }
#         if request.url.path in long_timeout_urls :
#             timeout = long_timeout_urls[request.url.path]
#             request.scope["extensions"]["http.response.template"] = {"timeout": timeout}
#         return await call_next(request)


# 加载.env 文件

app = FastAPI()

async def get_csv_as_dict(file_path: str):
    result = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) >= 2:
                    try:
                        result[row[0]] = float(row[1])
                    except ValueError:
                        # 处理ratio无法转换为float的情况
                        continue
    except FileNotFoundError:
        return {"error": "文件未找到"}
    except Exception as e:
        return {"error": str(e)}
    return result

@app.on_event("startup")
async def startup_event():
    global settings
    # 读取环境变量
    settings = Settings()
    ratio_file = settings.ratio_file
    if ratio_file is not None:
        settings.ratio_table = await get_csv_as_dict(ratio_file)
    
@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    long_timeout_urls = {
        "/upload_config": 100000,
        "/sync_traffics": 100000,
    }
    timeout = 1000
    if request.url.path in long_timeout_urls:
        timeout = long_timeout_urls[request.url.path]
    try:
        start_time = time.time()
        return await asyncio.wait_for(call_next(request), timeout=timeout)

    except asyncio.TimeoutError:
        process_time = time.time() - start_time
        return JSONResponse({'detail': 'Request processing time excedeed limit',
                             'processing_time': process_time},
                            status_code=HTTP_504_GATEWAY_TIMEOUT)

# def credential_exception_handler(request: Request, exc: AuthJWTException):
#     return JSONResponse(
#         status_code=exc.status_code,
#         content={"detail": exc.message}
#     )

# app.add_exception_handler(AuthJWTException, credential_exception_handler)
@app.exception_handler(AuthJWTException)
def authjwt_exception_handler(request: Request, exc: AuthJWTException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.message}
    )
    
app.include_router(gateway.router)
app.include_router(auth.auth)
app.include_router(db.DB)
app.include_router(account.account)
app.include_router(task.task)
app.include_router(weather.weather)
app.include_router(auth2.auth2)
app.include_router(user.user)
app.include_router(traffic.traffic)
app.include_router(group.group)


class ListVirtualGroupRequest(BaseModel):
    gwid: str
    groupid: str

@app.post("/list_virtual_group")
async def list_virtual_group(request: ListVirtualGroupRequest):
    gwid = request.gwid
    groupid = request.groupid
    with gw_login(gwid) as sdk_obj:
        virtual_groups = sdk_obj.list_virtual_group(groupid)  # 传入空字符串作为 groupid
        return {"virtual_groups": virtual_groups}

@app.get("/")
def read_root():
    return {
        "app_name": settings.app_name
    }

class ConfigLoadRequest(BaseModel):
    gwid: str
    cfgname: str

@app.post("/config_load")
async def config_load(request: ConfigLoadRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    cfgname = request.cfgname
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            config = sdk.config_load(cfgname)
            return {"config": config}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class ConfigSetRequest(BaseModel):
    gwid: str
    cfgname: str
    section: str
    values: dict

@app.post("/config_set")
async def config_set(request: ConfigSetRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    cfgname = request.cfgname
    section = request.section
    values = request.values
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.config_set(cfgname, section, values)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class AddUserRequest(BaseModel):
    gwid: str
    ip: str
    user: str
    from_source: str
    expire: int

@app.post("/add_user_binding")
async def add_user_binding(request: AddUserRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    ip = request.ip
    user = request.user
    from_source = request.from_source
    expire = request.expire
    try:
        if sdk.login(address, username, password):
            result = sdk.add_user(ip, user, from_source, expire)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class RmUserRequest(BaseModel):
    gwid: str
    user: str

@app.post("/rm_user")
async def rm_user(request: RmUserRequest):
    gwid = request.gwid
    user = request.user
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.rm_user(user)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class AddVirtualGroupRequest(BaseModel):
    gwid: str
    groupid: str
    ip: str
    minutes: int

@app.post("/add_virtual_group")
async def add_virtual_group(request: AddVirtualGroupRequest):
    gwid = request.gwid
    groupid = request.groupid
    ip = request.ip
    minutes = request.minutes
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.add_virtual_group(groupid, ip, minutes)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class RmVirtualGroupRequest(BaseModel):
    gwid: str
    ip: str

@app.post("/rm_virtual_group")
async def rm_virtual_group(request: RmVirtualGroupRequest):
    gwid = request.gwid
    ip = request.ip
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.rm_virtual_group(ip)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class NetworkRequest(BaseModel):
    gwid: str

@app.post("/get_network_interfaces")
async def get_network_interfaces(request: NetworkRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            interfaces = sdk.get_network_interfaces()
            return {"interfaces": interfaces}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

@app.post("/get_network_status")
async def get_network_status(request: NetworkRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            status = sdk.get_network_status()
            return {"status": status}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

@app.post("/list_group")
async def list_group(request: NetworkRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            groups = sdk.list_group()
            return {"groups": groups}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()


class ListBandWidthRequest(BaseModel):
    gwid: str
    seconds: int

@app.post("/list_bandwidth")
async def list_bandwidth(request: ListBandWidthRequest):
    gwid = request.gwid
    seconds = request.seconds
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            bandwidth = sdk.list_bandwidth(seconds)
            return {"bandwidth": bandwidth}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class ListOnlineUsersRequest(BaseModel):
    gwid: str
    top: int
    search: str

@app.post("/list_online_users")
async def list_online_users(request: ListOnlineUsersRequest):
    gwid = request.gwid
    top = request.top
    search = request.search
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            online_users = sdk.list_online_users(top, search)
            return {"online_users": online_users}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class ListOnlineConnectionsRequest(BaseModel):
    gwid: str
    ip: str

@app.post("/list_online_connections")
async def list_online_connections(request: ListOnlineConnectionsRequest):
    gwid = request.gwid
    ip = request.ip
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            online_connections = sdk.list_online_connections(ip)
            return {"online_connections": online_connections}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class KillConnectionRequest(BaseModel):
    gwid: str
    ip: str
    port: int
    type: str
    minutes: int
    message: str

@app.post("/kill_connection")
async def kill_connection(request: KillConnectionRequest):
    gwid = request.gwid
    ip = request.ip
    port = request.port
    type = request.type
    minutes = request.minutes
    message = request.message
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.kill_connection(ip, port, type, minutes, message)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class ConfigAddRequest(BaseModel):
    gwid: str
    cfgname: str
    type: str
    name: str
    values: dict

@app.post("/config_add")
async def config_add(request: ConfigAddRequest):
    gwid = request.gwid
    cfgname = request.cfgname
    type = request.type
    name = request.name
    values = request.values
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.config_add(cfgname, type, name, values)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class ConfigDelRequest(BaseModel):
    gwid: str
    cfgname: str
    section: str

@app.post("/config_del")
async def config_del(request: ConfigDelRequest):
    gwid = request.gwid
    cfgname = request.cfgname
    section = request.section
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.config_del(cfgname, section)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

@app.post("/config_apply")
async def config_apply(request: NetworkRequest):
    gwid = request.gwid
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.config_apply()
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class QueryDbRequest(BaseModel):
    gwid: str
    dbname: str
    querysql: str

@app.post("/query_db")
async def query_db(request: QueryDbRequest):
    gwid = request.gwid
    dbname = request.dbname
    querysql = request.querysql
    gw = await get_gateway_by_id(gwid)
    if gw is None or len(gw.data) == 0:
        raise HTTPException(status_code=400, detail="gateway not found")
    username = gw.data[0].get('username')
    password = gw.data[0].get('password')
    address = gw.data[0].get('address')
    sdk = SDK()
    try:
        if sdk.login(address, username, password):
            result = sdk.query_db(dbname, querysql)
            return {"result": result}
        else:
            raise HTTPException(status_code=401, detail="登录失败")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        sdk.logout()

class LoginRequest(BaseModel):
    username: str
    password: str
