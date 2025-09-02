import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from .database import Base

class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    infohash = Column(String(40), unique=True, index=True, nullable=False)
    status = Column(String(50), nullable=False, default='pending')
    # The JSON type provides a generic way to store JSON.
    # On backends that support it, it uses the native JSON type.
    # On others (like older SQLite), it falls back to TEXT.
    successful_screenshots = Column(JSON, nullable=True)
    # 用于存储任务失败时可用于恢复的上下文数据。
    # 它由工作节点生成，并由调度器保存，以便在重试时传递给下一个工作节点。
    resume_data = Column(JSON, nullable=True)
    result_message = Column(Text, nullable=True)
    assigned_worker_id = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

class Worker(Base):
    __tablename__ = "workers"

    id = Column(Integer, primary_key=True, index=True)
    worker_id = Column(String(255), unique=True, nullable=False)
    status = Column(String(50), nullable=False, default='idle')
    last_seen_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
