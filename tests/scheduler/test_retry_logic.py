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
