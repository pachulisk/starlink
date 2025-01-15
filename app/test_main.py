from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_list_virtual_group():
    response = client.post("/list_virtual_group", json={"username": "test_user", "password": "test_pass"})
    assert response.status_code == 200
    assert "virtual_groups" in response.json()
    assert isinstance(response.json()["virtual_groups"], list)
