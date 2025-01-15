from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_config_load():
    response = client.post("/config_load", json={
        "server": "http://test-server.com",
        "username": "testuser",
        "password": "testpass",
        "cfgname": "test-config"
    })
    assert response.status_code == 200
    assert "config" in response.json()

def test_config_set():
    response = client.post("/config_set", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "cfgname": "test_config",
        "section": "test_section",
        "values": {"key1": "value1", "key2": "value2"}
    })
    assert response.status_code == 200
    assert "result" in response.json()

def test_add_user():
    response = client.post("/add_user", json={
        "server": "http://test.server",
        "username": "testuser",
        "password": "testpass",
        "ip": "192.168.1.100",
        "user": "newuser",
        "from_source": "test",
        "expire": 3600
    })
    assert response.status_code == 200
    assert response.json() == {"result": True}

def test_rm_user():
    response = client.post("/rm_user", json={
        "server": "http://test.server",
        "username": "testuser",
        "password": "testpass",
        "user": "usertoremove"
    })
    assert response.status_code == 200
    assert response.json() == {"result": True}

def test_add_virtual_group():
    response = client.post("/add_virtual_group", json={
        "server": "http://test.server",
        "username": "testuser",
        "password": "testpass",
        "groupid": "test_group",
        "ip": "192.168.1.1",
        "minutes": 60
    })
    assert response.status_code == 200
    assert response.json() == {"result": True}

def test_rm_virtual_group():
    response = client.post("/rm_virtual_group", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "ip": "192.168.1.1"
    })
    assert response.status_code == 200
    assert "result" in response.json()

def test_get_network_interfaces():
    response = client.post("/get_network_interfaces", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass"
    })
    assert response.status_code == 200
    assert "interfaces" in response.json()

def test_get_network_status():
    response = client.post("/get_network_status", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass"
    })
    assert response.status_code == 200
    assert "status" in response.json()

def test_list_group():
    response = client.post("/list_group", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass"
    })
    assert response.status_code == 200
    assert "groups" in response.json()

def test_list_account():
    response = client.post("/list_account", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass"
    })
    assert response.status_code == 200
    assert "accounts" in response.json()

def test_list_bandwidth():
    response = client.post("/list_bandwidth", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "seconds": 3600
    })
    assert response.status_code == 200
    assert "bandwidth" in response.json()

def test_list_online_users():
    response = client.post("/list_online_users", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "top": 10,
        "search": ""
    })
    assert response.status_code == 200
    assert "online_users" in response.json()

def test_list_online_connections():
    response = client.post("/list_online_connections", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "ip": "192.168.1.1"
    })
    assert response.status_code == 200
    assert "online_connections" in response.json()

def test_kill_connection():
    response = client.post("/kill_connection", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "ip": "192.168.1.1",
        "port": 8080,
        "type": "tcp",
        "minutes": 30,
        "message": "Connection killed"
    })
    assert response.status_code == 200
    assert "result" in response.json()

def test_config_add():
    response = client.post("/config_add", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "cfgname": "test_config",
        "type": "test_type",
        "name": "test_name",
        "values": {"key": "value"}
    })
    assert response.status_code == 200
    assert "result" in response.json()

def test_config_del():
    response = client.post("/config_del", json={
        "server": "http://example.com",
        "username": "testuser",
        "password": "testpass",
        "cfgname": "test_config",
        "section": "test_section"
    })
    assert response.status_code == 200
    assert "result" in response.json()