# -*- coding: utf-8 -*-
"""
本模块定义了与数据库表对应的 SQLAlchemy ORM 数据模型。
"""
import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from .database import Base

class Task(Base):
    """
    任务数据模型，对应于 `tasks` 表。
    存储了所有与截图任务相关的信息。
    """
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True, comment="任务的唯一标识符")
    infohash = Column(String(40), unique=True, index=True, nullable=False, comment="任务关联的 Torrent Infohash，40个字符的十六进制字符串")

    # 任务状态: 'pending', 'working', 'success', 'recoverable_failure', 'permanent_failure'
    status = Column(String(50), nullable=False, default='pending', comment="任务的当前状态")

    # 使用 JSON 类型存储成功生成的截图文件名列表。
    # 在支持原生 JSON 的数据库（如 PostgreSQL）上性能更佳，在 SQLite 上会回退到 TEXT 类型。
    successful_screenshots = Column(JSON, nullable=True, comment="已成功生成的截图文件名列表")

    # 用于存储任务失败时可用于恢复的上下文数据。
    # 它由工作节点生成，并由调度器保存，以便在重试时传递给下一个工作节点。
    resume_data = Column(JSON, nullable=True, comment="用于任务断点续传的恢复数据")

    result_message = Column(Text, nullable=True, comment="记录任务完成（成功或失败）时的最终消息")
    assigned_worker_id = Column(String(255), nullable=True, comment="当前正在处理此任务的工作节点的ID")

    created_at = Column(DateTime, default=datetime.datetime.utcnow, comment="任务创建时间")
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, comment="任务最后更新时间")

class Worker(Base):
    """
    工作节点数据模型，对应于 `workers` 表。
    存储了所有已注册的工作节点的信息。
    """
    __tablename__ = "workers"

    id = Column(Integer, primary_key=True, index=True, comment="工作节点的唯一标识符")
    worker_id = Column(String(255), unique=True, nullable=False, comment="工作节点自定义的唯一ID（例如UUID）")

    # 工作节点状态: 'idle' (空闲), 'busy' (繁忙)
    status = Column(String(50), nullable=False, default='idle', comment="工作节点的当前状态")
    active_tasks_count = Column(Integer, default=0, comment="该工作节点正在执行的任务数")
    queue_size = Column(Integer, default=0, comment="该工作节点内部任务队列的大小")

    last_seen_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, comment="工作节点最后一次发送心跳的时间")
    created_at = Column(DateTime, default=datetime.datetime.utcnow, comment="工作节点首次注册的时间")
