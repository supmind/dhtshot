# -*- coding: utf-8 -*-
"""
本模块定义了 API 端点使用的所有 Pydantic 模型 (schemas)。
这些模型用于数据验证、序列化和 API 文档生成。
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Union, Dict, Any
import datetime

# --- 任务 (Task) 相关的模型 ---

class TaskBase(BaseModel):
    """任务模型的基础，包含了最核心的 infohash。"""
    infohash: str = Field(..., max_length=40, description="任务的唯一标识，即 Infohash。")

class TaskCreate(TaskBase):
    """用于创建新任务的模型。"""
    pass

class Task(TaskBase):
    """用于 API 响应的完整任务模型，包含了所有任务详情。"""
    id: int
    status: str = Field("pending", description="任务的当前状态 (e.g., pending, working, success, failure)。")
    retry_count: int = Field(0, description="任务重试次数")
    torrent_name: Optional[str] = None
    video_filename: Optional[str] = None
    video_duration_seconds: Optional[int] = None
    created_at: datetime.datetime
    assigned_worker_id: Optional[str] = None
    result_message: Optional[str] = None

    class Config:
        orm_mode = True

class TaskList(BaseModel):
    """用于返回任务列表的响应模型，包含了任务总数和任务列表。"""
    total: int
    tasks: List[Task]

class NextTaskResponse(BaseModel):
    """当工作节点请求下一个任务时，返回此模型。"""
    infohash: str
    metadata: Optional[str] = None # Base64-encoded

class TaskStatusUpdate(BaseModel):
    """用于更新任务状态的请求体模型。"""
    status: str
    message: Optional[str] = None


class TaskDetailsUpdate(BaseModel):
    """用于更新任务元数据详情的请求体模型。"""
    torrent_name: Optional[str] = None
    video_filename: Optional[str] = None
    video_duration_seconds: Optional[int] = None


class ScreenshotRecord(BaseModel):
    """用于记录单个截图文件名的请求体模型。"""
    filename: str = Field(..., description="要记录的截图文件名。")


# --- 工作节点 (Worker) 相关的模型 ---

class WorkerBase(BaseModel):
    """工作节点模型的基础，包含了 worker_id。"""
    worker_id: str

class WorkerCreate(WorkerBase):
    """用于注册新工作节点的模型。"""
    status: str = "idle"

class WorkerHeartbeat(WorkerBase):
    """用于工作节点发送心跳的请求体模型。"""
    status: str
    active_tasks_count: int
    queue_size: int

class Worker(WorkerBase):
    """用于 API 响应的完整工作节点模型。"""
    id: int
    status: str
    created_at: datetime.datetime
    last_seen_at: datetime.datetime
    active_tasks_count: int
    queue_size: int

    class Config:
        orm_mode = True

class WorkerList(BaseModel):
    """用于返回工作节点列表的响应模型。"""
    total: int
    workers: List[Worker]
