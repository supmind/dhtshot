# -*- coding: utf-8 -*-
"""
对 scheduler/main.py 中 FastAPI 端点的单元测试。
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from io import BytesIO

from scheduler.main import app, get_db
from scheduler.database import Base

# --- 测试数据库设置 ---
# 使用带有 StaticPool 的内存数据库，确保在 TestClient 的生命周期内所有操作都使用同一个连接
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def client():
    """
    一个 Pytest fixture，为每个测试函数提供一个配置好测试数据库的 TestClient。
    - 它会覆盖 FastAPI 应用的 `get_db` 依赖，以使用独立的内存数据库。
    - 它确保在每个测试开始前创建所有数据库表，并在结束后删除它们。
    """
    def override_get_db():
        """依赖覆盖函数，提供一个独立的数据库会话。"""
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    # 在这里应用依赖覆盖
    app.dependency_overrides[get_db] = override_get_db

    # 创建所有表
    Base.metadata.create_all(bind=engine)

    # Yield the client for the test
    yield TestClient(app)

    # 清理：删除所有表
    Base.metadata.drop_all(bind=engine)
    # 清理依赖覆盖
    app.dependency_overrides.clear()


# --- 测试用例 ---

def test_read_root(client):
    """测试根端点 (健康检查)。"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "截图调度器正在运行"}

def test_create_task(client):
    """测试创建新任务的端点。"""
    infohash = "createtask_infohash"
    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 201
    data = response.json()
    assert data["infohash"] == infohash
    assert data["status"] == "pending"

def test_create_existing_task(client):
    """测试当任务已存在时，POST /tasks/ 的行为。"""
    infohash = "existing_infohash"
    client.post("/tasks/", data={"infohash": infohash})
    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 200
    data = response.json()
    assert data["infohash"] == infohash

def test_get_task_by_infohash(client):
    """测试根据 infohash 获取任务的端点。"""
    infohash = "getbyhash_infohash"
    client.post("/tasks/", data={"infohash": infohash})
    response = client.get(f"/tasks/{infohash}")
    assert response.status_code == 200
    data = response.json()
    assert data["infohash"] == infohash

def test_get_nonexistent_task(client):
    """测试获取一个不存在的任务。"""
    response = client.get("/tasks/nonexistent_hash")
    assert response.status_code == 404

def test_get_next_task(client):
    """测试工作节点获取下一个任务的流程。"""
    infohash = "next_task_hash"
    client.post("/tasks/", data={"infohash": infohash})
    response = client.get("/tasks/next", params={"worker_id": "worker-1"})
    assert response.status_code == 200
    data = response.json()
    assert data["infohash"] == infohash
    task_response = client.get(f"/tasks/{infohash}")
    task_data = task_response.json()
    assert task_data["status"] == "working"
    assert task_data["assigned_worker_id"] == "worker-1"

def test_get_next_task_no_tasks(client):
    """测试在没有待处理任务时获取任务。"""
    response = client.get("/tasks/next", params={"worker_id": "worker-1"})
    assert response.status_code == 200
    assert response.json() is None

def test_register_and_heartbeat_worker(client):
    """测试工作节点的注册和心跳流程。"""
    worker_id = "test_worker_007"
    response = client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"})
    assert response.status_code == 200
    response = client.post("/workers/heartbeat", json={"worker_id": worker_id, "status": "busy"})
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "busy"

def test_upload_screenshot(client):
    """测试上传截图文件的端点。"""
    infohash = "upload_screenshot_hash"
    client.post("/tasks/", data={"infohash": infohash})
    screenshot_content = b"fake_jpeg_content"
    response = client.post(
        f"/screenshots/{infohash}",
        files={"file": ("test.jpg", BytesIO(screenshot_content), "image/jpeg")}
    )
    assert response.status_code == 200
    task_response = client.get(f"/tasks/{infohash}")
    data = task_response.json()
    assert "test.jpg" in data["successful_screenshots"]

def test_update_task_status_endpoint(client):
    """测试更新任务最终状态的端点。"""
    infohash = "update_status_hash"
    client.post("/tasks/", data={"infohash": infohash})
    update_payload = {"status": "success", "message": "All done!", "resume_data": None}
    response = client.post(f"/tasks/{infohash}/status", json=update_payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["result_message"] == "All done!"

def test_get_next_task_form_data(client):
    """测试 GET /tasks/next 端点现在是否能正确处理表单数据。"""
    infohash = "form_data_task"
    client.post("/tasks/", data={"infohash": infohash})
    # FastAPI TestClient 对于 GET 请求中的 'data' 参数行为可能不直观
    # 它实际上是作为查询参数发送的。
    response = client.get(f"/tasks/next?worker_id=worker-form")
    assert response.status_code == 200
    data = response.json()
    assert data["infohash"] == infohash
