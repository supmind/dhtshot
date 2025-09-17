# -*- coding: utf-8 -*-
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from datetime import datetime, timedelta

from scheduler import crud, models, schemas
from scheduler.database import Base

# --- 测试数据库设置 ---
# 使用内存中的 SQLite 数据库进行测试，以确保测试的隔离性和速度
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False)

@pytest.fixture(scope='session')
def db_engine():
    """创建一个会话级别的内存数据库引擎。"""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    return engine

@pytest.fixture(scope='function')
def db_session(db_engine):
    """
    为每个测试函数创建一个独立的数据库会话和事务。
    测试结束后，事务被回滚，确保测试之间互不影响。
    """
    connection = db_engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()

# --- 测试用例 ---

def test_create_and_get_task(db_session):
    """测试能否成功创建一个任务，并随后通过 infohash 获取它。"""
    infohash = "test_hash_01"
    task_create = schemas.TaskCreate(infohash=infohash)
    crud.create_task(db_session, task=task_create)

    retrieved_task = crud.get_task_by_infohash(db_session, infohash)
    assert retrieved_task is not None
    assert retrieved_task.infohash == infohash
    assert retrieved_task.status == 'pending'

def test_get_non_existent_task(db_session):
    """测试获取一个不存在的任务时应返回 None。"""
    retrieved_task = crud.get_task_by_infohash(db_session, "non_existent_hash")
    assert retrieved_task is None

def test_get_tasks_with_filter(db_session):
    """测试分页和按状态过滤任务列表的功能。"""
    # 创建一些测试数据
    for i in range(5):
        crud.create_task(db_session, task=schemas.TaskCreate(infohash=f"pending_task_{i}"))
    for i in range(3):
        task = crud.create_task(db_session, task=schemas.TaskCreate(infohash=f"working_task_{i}"))
        task.status = 'working'
    db_session.commit()

    # 测试不带过滤
    total, tasks = crud.get_tasks(db_session, limit=10)
    assert total == 8
    assert len(tasks) == 8

    # 测试按状态过滤
    total_pending, pending_tasks = crud.get_tasks(db_session, status='pending', limit=10)
    assert total_pending == 5
    assert len(pending_tasks) == 5

    # 测试分页
    total_all, paged_tasks = crud.get_tasks(db_session, skip=6, limit=5)
    assert total_all == 8
    assert len(paged_tasks) == 2

def test_upsert_worker_creation(db_session):
    """测试当工作节点不存在时，upsert 操作是否能正确创建新节点。"""
    worker_id = "worker_01"
    worker = crud.upsert_worker(db_session, worker_id=worker_id, status='idle')
    assert worker is not None
    assert worker.worker_id == worker_id
    assert worker.status == 'idle'
    assert worker.last_seen_at is not None

def test_upsert_worker_update(db_session):
    """测试当工作节点已存在时，upsert 操作是否能正确更新其状态和心跳时间。"""
    worker_id = "worker_02"
    crud.upsert_worker(db_session, worker_id=worker_id, status='idle')
    first_worker = crud.get_worker_by_id(db_session, worker_id)
    first_seen_at = first_worker.last_seen_at

    # 模拟一段时间后的更新
    updated_worker = crud.upsert_worker(db_session, worker_id=worker_id, status='busy')
    assert updated_worker.status == 'busy'
    assert updated_worker.last_seen_at > first_seen_at

def test_get_and_assign_next_task(db_session):
    """测试能否原子性地获取并分配下一个待处理任务。"""
    infohash = "assign_task_01"
    crud.create_task(db_session, task=schemas.TaskCreate(infohash=infohash))

    # 第一个工作节点获取任务
    assigned_task = crud.get_and_assign_next_task(db_session, worker_id="worker_A")
    assert assigned_task is not None
    assert assigned_task.infohash == infohash
    assert assigned_task.status == 'working'
    assert assigned_task.assigned_worker_id == 'worker_A'

    # 第二个工作节点尝试获取，应该获取不到任何任务
    next_task = crud.get_and_assign_next_task(db_session, worker_id="worker_B")
    assert next_task is None

def test_update_task_status(db_session):
    """测试更新任务状态、结果消息和 assigned_worker_id 的逻辑。"""
    infohash = "update_status_task"
    crud.create_task(db_session, task=schemas.TaskCreate(infohash=infohash))
    task = crud.get_and_assign_next_task(db_session, worker_id="worker_C")
    assert task.assigned_worker_id == "worker_C"

    # 更新为成功状态
    crud.update_task_status(db_session, infohash=infohash, status='success', message='Completed')
    updated_task = crud.get_task_by_infohash(db_session, infohash)
    assert updated_task.status == 'success'
    assert updated_task.result_message == 'Completed'
    # 任务完成后，assigned_worker_id 应该被清除
    assert updated_task.assigned_worker_id is None

def test_update_task_details(db_session):
    """测试更新任务的详细信息。"""
    infohash = "update_details_task"
    crud.create_task(db_session, task=schemas.TaskCreate(infohash=infohash))

    details_to_update = schemas.TaskDetailsUpdate(
        torrent_name="My Awesome Movie",
        video_filename="movie.mp4",
        video_duration_seconds=3600
    )
    crud.update_task_details(db_session, infohash=infohash, details=details_to_update)

    updated_task = crud.get_task_by_infohash(db_session, infohash)
    assert updated_task.torrent_name == "My Awesome Movie"
    assert updated_task.video_filename == "movie.mp4"
    assert updated_task.video_duration_seconds == 3600

def test_reset_stuck_tasks(db_session):
    """测试重置卡死任务的逻辑。"""
    infohash_stuck = "stuck_task"
    infohash_ok = "ok_task"

    # 创建一个一小时前更新的卡死任务
    stuck_task = crud.create_task(db_session, task=schemas.TaskCreate(infohash=infohash_stuck))
    stuck_task.status = 'working'
    stuck_task.updated_at = datetime.utcnow() - timedelta(hours=1)

    # 创建一个刚刚更新的正常任务
    ok_task = crud.create_task(db_session, task=schemas.TaskCreate(infohash=infohash_ok))
    ok_task.status = 'working'
    db_session.commit()

    # 执行重置操作，超时时间为 30 分钟
    reset_count = crud.reset_stuck_tasks(db_session, timeout_seconds=1800)
    assert reset_count == 1

    # 验证任务状态
    stuck_task_after = crud.get_task_by_infohash(db_session, infohash_stuck)
    ok_task_after = crud.get_task_by_infohash(db_session, infohash_ok)
    assert stuck_task_after.status == 'pending'
    assert ok_task_after.status == 'working'

def test_get_retryable_tasks_and_reset(db_session):
    """测试获取可重试任务并重置它们的逻辑。"""
    # 创建一个可重试的任务
    retry_task = crud.create_task(db_session, task=schemas.TaskCreate(infohash="retry_task"))
    retry_task.status = 'recoverable_failure'
    retry_task.retry_count = 1

    # 创建一个不可重试的任务 (次数超限)
    non_retry_task = crud.create_task(db_session, task=schemas.TaskCreate(infohash="non_retry_task"))
    non_retry_task.status = 'recoverable_failure'
    non_retry_task.retry_count = 3
    db_session.commit()

    # 获取可重试任务
    retryable_tasks = crud.get_retryable_tasks(db_session, limit=10)
    assert len(retryable_tasks) == 1
    assert retryable_tasks[0].infohash == "retry_task"

    # 重置任务以供重试
    crud.reset_task_for_retry(db_session, retryable_tasks[0])
    reset_task = crud.get_task_by_infohash(db_session, "retry_task")
    assert reset_task.status == 'pending'
    assert reset_task.retry_count == 2
