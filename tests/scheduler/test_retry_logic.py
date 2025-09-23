# -*- coding: utf-8 -*-
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from scheduler import crud, models, schemas
from scheduler.database import Base

# 使用内存中的 SQLite 数据库进行测试
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def db_session():
    """为每个测试函数提供一个独立的数据库会话，并在测试后回滚所有更改。"""
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.rollback()
        db.close()
        Base.metadata.drop_all(bind=engine)


def test_get_retryable_tasks(db_session):
    """测试 get_retryable_tasks 是否能正确获取可重试的任务。"""
    # 创建一个可重试的任务
    crud.create_task(db_session, schemas.TaskCreate(infohash="retry_task_1"))
    crud.update_task_status(db_session, "retry_task_1", "recoverable_failure", "test failure")

    # 创建一个重试次数过多的任务
    task2 = crud.create_task(db_session, schemas.TaskCreate(infohash="retry_task_2"))
    task2.status = "recoverable_failure"
    task2.retry_count = 3
    db_session.commit()

    # 创建一个永久失败的任务
    crud.create_task(db_session, schemas.TaskCreate(infohash="permanent_failure_task"))
    crud.update_task_status(db_session, "permanent_failure_task", "permanent_failure", "test failure")

    # 创建一个成功的任务
    crud.create_task(db_session, schemas.TaskCreate(infohash="success_task"))
    crud.update_task_status(db_session, "success_task", "success", "test success")

    retryable_tasks = crud.get_retryable_tasks(db_session)
    assert len(retryable_tasks) == 1
    assert retryable_tasks[0].infohash == "retry_task_1"
    assert retryable_tasks[0].status == "recoverable_failure"
    assert retryable_tasks[0].retry_count == 0


def test_reset_task_for_retry(db_session):
    """测试 reset_task_for_retry 是否能正确重置任务。"""
    task = crud.create_task(db_session, schemas.TaskCreate(infohash="reset_task"))
    crud.update_task_status(db_session, "reset_task", "recoverable_failure", "test failure")

    task_to_reset = crud.get_task_by_infohash(db_session, "reset_task")
    assert task_to_reset.status == "recoverable_failure"
    assert task_to_reset.retry_count == 0

    crud.reset_task_for_retry(db_session, task_to_reset)

    resetted_task = crud.get_task_by_infohash(db_session, "reset_task")
    assert resetted_task.status == "pending"
    assert resetted_task.retry_count == 1


def test_reset_stuck_tasks_succeeds_on_worker_timeout(db_session):
    """
    验证 `reset_stuck_tasks` 在修复后能够正确工作。

    该测试模拟一个 worker 领取任务后失联的场景，并断言 `reset_stuck_tasks`
    能够基于 worker 的 `last_seen_at` 时间戳来正确地重置任务。
    这是一个回归测试，确保此功能不会在未来被破坏。
    """
    from datetime import datetime, timedelta

    # 1. 设置一个 worker 和一个 task
    worker = crud.upsert_worker(db_session, "test_worker_1", "idle")
    task = crud.create_task(db_session, schemas.TaskCreate(infohash="stuck_task_hash"))
    db_session.commit()

    # 2. 模拟 worker 领取任务
    assigned_task = crud.get_and_assign_next_task(db_session, "test_worker_1")
    assert assigned_task.status == "working"

    # 3. 模拟 worker 失联，将其 last_seen_at 设置为1小时前
    stale_time = datetime.utcnow() - timedelta(hours=1)
    db_session.query(models.Worker).filter(models.Worker.worker_id == "test_worker_1").update({"last_seen_at": stale_time})
    db_session.commit()

    # 4. 运行被测函数
    reset_count = crud.reset_stuck_tasks(db_session, timeout_seconds=300)

    # 5. 断言修复后的正确行为
    # `reset_stuck_tasks` 现在应该能找到并重置这个任务。
    assert reset_count == 1, "reset_stuck_tasks 未能按预期重置卡死的任务"

    # 确认任务状态已被正确重置
    final_task = crud.get_task_by_infohash(db_session, "stuck_task_hash")
    assert final_task.status == "pending", "任务状态应被重置为 'pending'"
    assert final_task.assigned_worker_id is None, "任务的 assigned_worker_id 应被清除"
