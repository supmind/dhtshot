# -*- coding: utf-8 -*-
"""
对 scheduler/main.py 中 FastAPI 端点的单元测试。
"""
import pytest
import os
import json
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from io import BytesIO

from scheduler.main import app, get_db, get_redis_client
from scheduler.database import Base
from scheduler import crud, schemas
from config import Settings

# --- 测试数据库设置 ---
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Fixtures ---

@pytest.fixture(scope="function")
def mock_redis_client():
    """提供一个 mock 的异步 Redis 客户端。"""
    mock_redis = AsyncMock()
    mock_redis.sadd = AsyncMock()
    mock_redis.smembers = AsyncMock()
    return mock_redis

@pytest.fixture(scope="function")
def client(mock_redis_client):
    """
    一个 Pytest fixture，为每个测试函数提供一个配置好测试数据库和 mock Redis 的 TestClient。
    """
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    def override_get_redis_client():
        return mock_redis_client

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis_client] = override_get_redis_client
    Base.metadata.create_all(bind=engine)
    yield TestClient(app)
    Base.metadata.drop_all(bind=engine)
    app.dependency_overrides.clear()

@pytest.fixture(scope="session")
def api_key_headers():
    """提供带有正确 API 密钥的请求头。"""
    settings = Settings()
    return {"X-API-Key": settings.scheduler_api_key}

# --- 测试用例 ---

def test_read_root(client):
    """测试根端点，它不应该需要认证。"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "截图调度器正在运行"}

# --- 认证测试 ---

def test_endpoint_no_api_key(client):
    """测试在没有提供 API 密钥时，受保护的端点是否会拒绝访问。"""
    response = client.post("/tasks/", data={"infohash": "some_hash"})
    assert response.status_code == 403
    assert "无效的 API Key 或未提供" in response.json()["detail"]

def test_endpoint_wrong_api_key(client):
    """测试在提供了错误的 API 密钥时，受保护的端点是否会拒绝访问。"""
    response = client.post(
        "/tasks/",
        data={"infohash": "some_hash"},
        headers={"X-API-Key": "wrong_key"}
    )
    assert response.status_code == 403
    assert "无效的 API Key 或未提供" in response.json()["detail"]

# --- 任务管理端点测试 (带认证) ---

def test_create_task_new(client, api_key_headers):
    infohash = "new_task_hash"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    response = client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    assert response.status_code == 201
    data = response.json()
    assert data["infohash"] == infohash
    assert data["status"] == "pending"

def test_create_task_new_without_metadata_fails(client, api_key_headers):
    """测试在没有提供元数据文件时创建新任务会失败。"""
    infohash = "no_metadata_hash"
    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 400
    assert "required" in response.json()["detail"]

def test_create_task_with_torrent_file(client, api_key_headers):
    infohash = "task_with_file"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    response = client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    assert response.status_code == 201

    metadata_dir = "temp_metadata"
    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    assert os.path.exists(metadata_file)

    # 清理
    os.remove(metadata_file)
    if not os.listdir(metadata_dir):
        os.rmdir(metadata_dir)

def test_create_task_existing_pending(client, api_key_headers):
    infohash = "existing_pending_hash"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    # First call creates the task
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    # Second call should find the existing task (no file needed)
    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_create_task_permanent_failure(client, api_key_headers):
    infohash = "permanent_failure_hash"
    db = TestingSessionLocal()
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    crud.update_task_status(db, infohash, "permanent_failure")
    db.close()

    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 400
    assert "永久失败" in response.text

def test_create_task_recoverable_failure(client, api_key_headers):
    infohash = "recoverable_failure_hash"
    db = TestingSessionLocal()
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    crud.update_task_status(db, infohash, "recoverable_failure")
    db.close()

    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "pending"

def test_reactivate_task_resets_retry_count(client, api_key_headers):
    """测试重新激活一个失败次数过多的可恢复任务会重置其重试次数。"""
    infohash = "reset_retries_hash"
    db = TestingSessionLocal()
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    task = crud.get_task_by_infohash(db, infohash)
    task.status = "recoverable_failure"
    task.retry_count = 3
    db.commit()
    db.close()

    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "pending"
    assert data["retry_count"] == 0

def test_get_task_by_infohash(client, api_key_headers):
    infohash = "get_by_hash"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    response = client.get(f"/tasks/{infohash}", headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_get_nonexistent_task(client, api_key_headers):
    response = client.get("/tasks/nonexistent_hash", headers=api_key_headers)
    assert response.status_code == 404

def test_get_next_task(client, api_key_headers):
    infohash = "next_task_hash"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    response = client.get("/tasks/next", params={"worker_id": "worker-1"}, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_get_next_task_no_tasks(client, api_key_headers):
    response = client.get("/tasks/next", params={"worker_id": "worker-1"}, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json() is None

def test_get_next_task_with_metadata_file(client, api_key_headers):
    infohash = "next_with_metadata"
    metadata_dir = "temp_metadata"
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    with open(metadata_file, "wb") as f:
        f.write(b"test content")

    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    response = client.get("/tasks/next", params={"worker_id": "worker-1"}, headers=api_key_headers)
    assert response.status_code == 200
    assert os.path.exists(metadata_file)

    os.remove(metadata_file)
    if not os.listdir(metadata_dir):
        os.rmdir(metadata_dir)

def test_worker_registration_and_reregistration(client, api_key_headers):
    worker_id = "test_worker_001"
    response1 = client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"}, headers=api_key_headers)
    assert response1.status_code == 200
    first_seen_at = response1.json()["last_seen_at"]
    response2 = client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"}, headers=api_key_headers)
    assert response2.status_code == 200
    assert response2.json()["last_seen_at"] >= first_seen_at

def test_worker_heartbeat(client, api_key_headers):
    worker_id = "test_worker_002"
    client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"}, headers=api_key_headers)
    heartbeat_payload = {"worker_id": worker_id, "status": "busy", "active_tasks_count": 1, "queue_size": 5}
    response = client.post("/workers/heartbeat", json=heartbeat_payload, headers=api_key_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "busy"

def test_record_screenshot_endpoint(client, api_key_headers, mock_redis_client):
    infohash = "record_screenshot_hash"
    # The task doesn't need to exist in the DB anymore for this endpoint
    payload = {"filename": "screenshot_01.jpg"}
    response = client.post(f"/tasks/{infohash}/screenshots", json=payload, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json() == {"message": "Screenshot recorded successfully"}
    mock_redis_client.sadd.assert_called_once_with(f"screenshots:{infohash}", "screenshot_01.jpg")

def test_get_recorded_screenshots_endpoint(client, api_key_headers, mock_redis_client):
    infohash = "get_screenshots_hash"
    expected_screenshots = ["shot1.jpg", "shot2.jpg"]
    mock_redis_client.smembers.return_value = set(expected_screenshots)
    response = client.get(f"/tasks/{infohash}/screenshots", headers=api_key_headers)
    assert response.status_code == 200
    # Compare sets to ignore order differences from Redis smembers
    assert set(response.json()) == set(expected_screenshots)
    mock_redis_client.smembers.assert_called_once_with(f"screenshots:{infohash}")

def test_update_status_success_deletes_metadata(client, api_key_headers):
    infohash = "success_deletes_metadata"
    metadata_dir = "temp_metadata"
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    with open(metadata_file, "wb") as f:
        f.write(torrent_content)
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    payload = {"status": "success", "message": "All done!"}
    response = client.post(f"/tasks/{infohash}/status", json=payload, headers=api_key_headers)
    assert response.status_code == 200
    assert not os.path.exists(metadata_file)
    if not os.listdir(metadata_dir):
        os.rmdir(metadata_dir)

def test_update_status_failure_preserves_metadata(client, api_key_headers):
    infohash = "failure_preserves_metadata"
    metadata_dir = "temp_metadata"
    # The endpoint will create the directory
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"

    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )

    payload = {"status": "recoverable_failure", "message": "An error"}
    response = client.post(f"/tasks/{infohash}/status", json=payload, headers=api_key_headers)
    assert response.status_code == 200

    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    assert os.path.exists(metadata_file)

    # Cleanup
    os.remove(metadata_file)
    if not os.listdir(metadata_dir):
        os.rmdir(metadata_dir)


def test_list_all_tasks_with_filtering_and_pagination(client, api_key_headers):
    db = TestingSessionLocal()
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    for i in range(5):
        client.post(
            "/tasks/",
            data={"infohash": f"pending_{i:02d}"},
            files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
            headers=api_key_headers
        )
    for i in range(3):
        infohash = f"success_{i:02d}"
        client.post(
            "/tasks/",
            data={"infohash": infohash},
            files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
            headers=api_key_headers
        )
        crud.update_task_status(db, infohash, "success")
    db.close()
    response_all = client.get("/tasks/all/", headers=api_key_headers)
    assert response_all.json()["total"] == 8
    response_pending = client.get("/tasks/all/", params={"status": "pending"}, headers=api_key_headers)
    assert response_pending.json()["total"] == 5
    response_success = client.get("/tasks/all/", params={"status": "success"}, headers=api_key_headers)
    assert response_success.json()["total"] == 3

def test_update_task_details_endpoint(client, api_key_headers):
    infohash = "details_update_hash"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    details_payload = {"torrent_name": "My Test Torrent", "video_filename": "movie.mp4", "video_duration_seconds": 3600}
    response = client.post(f"/tasks/{infohash}/details", json=details_payload, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["torrent_name"] == "My Test Torrent"
