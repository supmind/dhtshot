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
from scheduler import crud, schemas

# --- 测试数据库设置 ---
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
    """
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    Base.metadata.create_all(bind=engine)
    yield TestClient(app)
    Base.metadata.drop_all(bind=engine)
    app.dependency_overrides.clear()

# --- 测试用例 ---

def test_read_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "截图调度器正在运行"}

def test_create_task_new(client):
    infohash = "new_task_hash"
    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 201
    data = response.json()
    assert data["infohash"] == infohash
    assert data["status"] == "pending"

def test_create_task_with_torrent_file(client):
    infohash = "task_with_file"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    response = client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")}
    )
    assert response.status_code == 201
    # 可以在这里添加断言，检查文件是否已在 temp_metadata 目录中创建
    # 但为了保持单元测试的独立性，我们信任 os 模块的功能

def test_create_task_existing_pending(client):
    infohash = "existing_pending_hash"
    client.post("/tasks/", data={"infohash": infohash})
    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_create_task_permanent_failure(client):
    infohash = "permanent_failure_hash"
    db = TestingSessionLocal()
    crud.create_task(db, schemas.TaskCreate(infohash=infohash))
    crud.update_task_status(db, infohash, "permanent_failure")
    db.close()

    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 400
    assert "永久失败" in response.text

def test_create_task_recoverable_failure(client):
    infohash = "recoverable_failure_hash"
    db = TestingSessionLocal()
    crud.create_task(db, schemas.TaskCreate(infohash=infohash))
    crud.update_task_status(db, infohash, "recoverable_failure")
    db.close()

    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "pending" # 状态应被重置为 pending

def test_get_task_by_infohash(client):
    infohash = "get_by_hash"
    client.post("/tasks/", data={"infohash": infohash})
    response = client.get(f"/tasks/{infohash}")
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_get_nonexistent_task(client):
    response = client.get("/tasks/nonexistent_hash")
    assert response.status_code == 404

def test_get_next_task(client):
    infohash = "next_task_hash"
    client.post("/tasks/", data={"infohash": infohash})
    response = client.get("/tasks/next", params={"worker_id": "worker-1"})
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_get_next_task_no_tasks(client):
    response = client.get("/tasks/next", params={"worker_id": "worker-1"})
    assert response.status_code == 200
    assert response.json() is None

def test_worker_registration_and_reregistration(client):
    """
    测试工作节点的完整生命周期：
    1. 首次注册一个新工作节点。
    2. 验证首次注册成功。
    3. 模拟该工作节点重新注册。
    4. 验证重新注册（即更新）也成功，并且时间戳被更新。
    """
    worker_id = "test_worker_001"

    # 1. 首次注册
    response1 = client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"})
    assert response1.status_code == 200
    data1 = response1.json()
    assert data1["worker_id"] == worker_id
    assert data1["status"] == "idle"

    # 记录首次注册的时间
    first_seen_at = data1["last_seen_at"]

    # 2. 模拟工作节点重新注册
    response2 = client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"})
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["worker_id"] == worker_id

    # 3. 验证时间戳已更新
    # 重新注册后，'last_seen_at' 时间应该晚于（或等于，如果执行速度极快）首次注册的时间
    assert data2["last_seen_at"] >= first_seen_at

def test_worker_heartbeat(client):
    """测试已注册工作节点的心跳功能。"""
    worker_id = "test_worker_002"

    # 必须先注册
    client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"})

    # 发送心跳
    response = client.post("/workers/heartbeat", json={"worker_id": worker_id, "status": "busy"})
    assert response.status_code == 200
    assert response.json()["status"] == "busy"

def test_upload_screenshot(client):
    infohash = "upload_hash"
    client.post("/tasks/", data={"infohash": infohash})
    response = client.post(
        f"/screenshots/{infohash}",
        files={"file": ("test.jpg", BytesIO(b"content"), "image/jpeg")}
    )
    assert response.status_code == 200
    task_data = client.get(f"/tasks/{infohash}").json()
    assert "test.jpg" in task_data["successful_screenshots"]

def test_update_task_status_endpoint(client):
    infohash = "update_status_hash"
    client.post("/tasks/", data={"infohash": infohash})
    payload = {"status": "success", "message": "All done!", "resume_data": None}
    response = client.post(f"/tasks/{infohash}/status", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["result_message"] == "All done!"

def test_list_all_tasks_with_pagination(client):
    """测试 /tasks/all/ 分页端点是否能正常工作。"""
    # 1. 在数据库中创建 25 个任务
    db = TestingSessionLocal()
    for i in range(25):
        # 使用 f-string 创建唯一的 infohash
        crud.create_task(db, schemas.TaskCreate(infohash=f"hash_{i:02d}"))
    db.close()

    # 2. 调用 API，请求第 2 页，每页 10 个
    response = client.get("/tasks/all/", params={"skip": 10, "limit": 10})
    assert response.status_code == 200

    data = response.json()

    # 3. 验证返回的数据结构和内容
    assert data["total"] == 25
    assert len(data["tasks"]) == 10

    # 4. 验证分页逻辑是否正确
    # 因为是按创建时间降序排序，所以第 11 个任务 (index 10) 应该是 "hash_14"
    # (hash_24, hash_23, ..., hash_15 | hash_14, ..., hash_5 | hash_4, ...)
    assert data["tasks"][0]["infohash"] == "hash_14"
    assert data["tasks"][-1]["infohash"] == "hash_05"
