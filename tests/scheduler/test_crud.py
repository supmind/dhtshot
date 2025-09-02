# -*- coding: utf-8 -*-
"""
对 scheduler/crud.py 中数据库操作函数的单元测试。
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scheduler.database import Base
from scheduler import crud, models, schemas

# --- 测试数据库设置 ---
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def db_session():
    """
    一个 Pytest fixture，为每个测试函数提供一个独立的数据库会话。
    它会在测试开始前创建所有表，并在测试结束后删除所有表。
    """
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

# --- 测试用例 ---

def test_create_and_get_task(db_session):
    """测试任务的创建和根据 infohash 的查询功能。"""
    infohash = "test_infohash_123"
    task_create = schemas.TaskCreate(infohash=infohash)
    created_task = crud.create_task(db_session, task=task_create)
    assert created_task.infohash == infohash
    retrieved_task = crud.get_task_by_infohash(db_session, infohash=infohash)
    assert retrieved_task is not None
    assert retrieved_task.id == created_task.id

def test_create_and_get_worker(db_session):
    """测试工作节点的创建和根据 worker_id 的查询功能。"""
    worker_create = schemas.WorkerCreate(worker_id="worker-001", status="idle")
    created_worker = crud.create_worker(db_session, worker=worker_create)
    assert created_worker.worker_id == "worker-001"
    retrieved_worker = crud.get_worker_by_id(db_session, worker_id="worker-001")
    assert retrieved_worker is not None
    assert retrieved_worker.id == created_worker.id

def test_update_worker_status(db_session):
    """测试更新工作节点状态的功能。"""
    crud.create_worker(db_session, worker=schemas.WorkerCreate(worker_id="worker-002"))
    updated_worker = crud.update_worker_status(db_session, worker_id="worker-002", status="busy")
    assert updated_worker.status == "busy"

def test_get_and_assign_next_task(db_session):
    """测试原子性地获取并分配下一个待处理任务的功能。"""
    crud.create_task(db_session, task=schemas.TaskCreate(infohash="task_to_be_assigned"))
    task = crud.get_and_assign_next_task(db_session, worker_id="worker-003")
    assert task is not None
    assert task.status == "working"
    next_task = crud.get_and_assign_next_task(db_session, worker_id="worker-004")
    assert next_task is None

def test_update_task_status(db_session):
    """测试更新任务最终状态的功能。"""
    crud.create_task(db_session, task=schemas.TaskCreate(infohash="task_to_update"))
    crud.get_and_assign_next_task(db_session, worker_id="worker-005")
    updated_task = crud.update_task_status(db_session, infohash="task_to_update", status="success", message="Completed")
    assert updated_task.status == "success"
    assert updated_task.assigned_worker_id is None

def test_record_screenshot_multiple_times(db_session):
    """
    测试对同一个任务多次调用 record_screenshot，验证所有记录都被正确添加。
    这是一个简化的、单线程的测试，用于验证追加逻辑的正确性。
    """
    infohash = "multiple_records_task"
    crud.create_task(db_session, task=schemas.TaskCreate(infohash=infohash))

    # 多次调用 record_screenshot
    crud.record_screenshot(db_session, infohash, "file1.jpg")
    crud.record_screenshot(db_session, infohash, "file2.jpg")
    crud.record_screenshot(db_session, infohash, "file1.jpg") # 测试重复添加
    crud.record_screenshot(db_session, infohash, "file3.jpg")

    # 检查最终结果
    final_task = crud.get_task_by_infohash(db_session, infohash)
    assert final_task is not None
    final_screenshots = final_task.successful_screenshots

    assert isinstance(final_screenshots, list)
    # 验证最终列表包含所有不重复的文件名
    assert len(final_screenshots) == 3
    assert set(final_screenshots) == {"file1.jpg", "file2.jpg", "file3.jpg"}
