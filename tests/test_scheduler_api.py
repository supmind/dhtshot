# -*- coding: utf-8 -*-
"""
对 scheduler/main.py 的 API 端点进行单元测试。
此文件遵循 FastAPI/SQLAlchemy 官方文档推荐的测试模式。
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

from scheduler.main import app, get_db
from scheduler.models import Base, Task
from scheduler.security import get_api_key

# --- 测试数据库设置 ---

@pytest.fixture(name="session")
def session_fixture():
    """
    创建一个独立的、内存中的数据库会话用于单个测试。
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db_session = TestingSessionLocal()

    try:
        yield db_session
    finally:
        db_session.close()
        Base.metadata.drop_all(engine)

@pytest.fixture(name="client")
def client_fixture(session):
    """
    创建一个 TestClient，它会覆盖 get_db 依赖以使用
    独立的测试数据库会话。
    """
    def get_session_override():
        return session

    def override_get_api_key():
        return "test_api_key"

    app.dependency_overrides[get_db] = get_session_override
    app.dependency_overrides[get_api_key] = override_get_api_key

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


# --- /tasks/ Endpoint Tests ---

def test_create_task_success(client: TestClient):
    """测试成功创建一个新任务。"""
    infohash = "a" * 40
    response = client.post("/tasks/", data={"infohash": infohash})

    assert response.status_code == 201, response.text
    data = response.json()
    assert data["infohash"] == infohash
    assert data["status"] == "pending"
    assert "id" in data

def test_create_task_already_exists(client: TestClient, session):
    """测试当任务已存在时，返回现有任务且状态码为 200。"""
    infohash = "b" * 40
    existing_task = Task(infohash=infohash)
    session.add(existing_task)
    session.commit()
    session.refresh(existing_task)

    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 200
    assert response.json()["id"] == existing_task.id

def test_reactivate_recoverable_failure_task(client: TestClient, session):
    """测试重新提交一个可恢复失败的任务会将其状态重置为 pending。"""
    infohash = "c" * 40
    task = Task(infohash=infohash, status="recoverable_failure")
    session.add(task)
    session.commit()

    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 200
    assert response.json()["status"] == "pending"

def test_create_task_permanent_failure(client: TestClient, session):
    """测试无法重新提交一个永久失败的任务。"""
    infohash = "d" * 40
    task = Task(infohash=infohash, status="permanent_failure")
    session.add(task)
    session.commit()

    response = client.post("/tasks/", data={"infohash": infohash})
    assert response.status_code == 400
    assert "永久失败" in response.json()["detail"]

def test_create_task_invalid_infohash_length(client: TestClient):
    """测试使用无效长度的 infohash 创建任务会失败 (422 Unprocessable Entity)。"""
    # 太短
    response_short = client.post("/tasks/", data={"infohash": "short"})
    assert response_short.status_code == 422

    # 太长
    response_long = client.post("/tasks/", data={"infohash": "l" * 41})
    assert response_long.status_code == 422


# --- Other Endpoints ---

def test_read_task(client: TestClient, session: Session):
    """测试根据 infohash 读取单个任务。"""
    infohash = "e" * 40
    task = Task(infohash=infohash, status="testing")
    session.add(task)
    session.commit()

    response = client.get(f"/tasks/{infohash}")
    assert response.status_code == 200
    data = response.json()
    assert data["infohash"] == infohash
    assert data["status"] == "testing"

def test_read_task_not_found(client: TestClient):
    """测试查询一个不存在的任务返回 404。"""
    infohash = "f" * 40
    response = client.get(f"/tasks/{infohash}")
    assert response.status_code == 404

def test_get_next_task(client: TestClient, session: Session):
    """测试获取下一个待处理任务。"""
    # 创建一个已完成的任务和一个待处理的任务
    session.add(Task(infohash="g" * 40, status="success"))
    session.add(Task(infohash="h" * 40, status="pending"))
    session.commit()

    response = client.get("/tasks/next?worker_id=worker-1")
    assert response.status_code == 200
    data = response.json()
    assert data["infohash"] == "h" * 40

    # 验证任务状态已被更新为 'working'
    db_task = session.query(Task).filter(Task.infohash == "h" * 40).one()
    assert db_task.status == "working"
    assert db_task.assigned_worker_id == "worker-1"

def test_get_next_task_none_available(client: TestClient):
    """测试没有待处理任务时返回 200 OK 和 null body。"""
    # 数据库是空的，或者只有非 pending 状态的任务
    response = client.get("/tasks/next?worker_id=worker-1")
    assert response.status_code == 200
    assert response.json() is None


def test_update_task_status(client: TestClient, session: Session):
    """测试成功更新任务状态。"""
    infohash = "i" * 40
    session.add(Task(infohash=infohash, status="working"))
    session.commit()

    update_payload = {"status": "success", "message": "All good"}
    response = client.post(f"/tasks/{infohash}/status", json=update_payload)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["result_message"] == "All good"
    assert data["assigned_worker_id"] is None # 任务完成后应被解除分配
