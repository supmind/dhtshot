# -*- coding: utf-8 -*-
"""
本模块定义了所有用于 API 请求和响应验证的 Pydantic 数据模型（Schemas）。
这些模型定义了 API 的数据契约。
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Any
import datetime

# --- 任务相关的 Schemas ---

class TaskBase(BaseModel):
    """任务模型的基础，包含了所有任务共有的字段。"""
    infohash: str = Field(..., max_length=40, description="任务关联的 Torrent Infohash，40个字符的十六进制字符串。")

class TaskCreate(TaskBase):
    """用于创建新任务的 Schema。"""
    pass

class Task(TaskBase):
    """用于 API 响应的完整任务模型，包含了数据库中的所有字段。"""
    id: int
    status: str
    successful_screenshots: Optional[List[str]] = Field([], description="已成功生成的截图文件名列表。")
    result_message: Optional[str] = Field(None, description="任务完成（成功或失败）时的最终消息。")
    resume_data: Optional[dict] = Field(None, description="用于任务断点续传的恢复数据。")
    assigned_worker_id: Optional[str] = Field(None, description="当前正在处理此任务的工作节点的ID。")
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        from_attributes = True  # 允许模型从 ORM 对象属性中读取数据

class ScreenshotRecord(BaseModel):
    """用于记录单个截图信息的 Schema (当前未使用)。"""
    filename: str

class TaskStatusUpdate(BaseModel):
    """用于工作节点更新任务状态的请求体 Schema。"""
    status: str = Field(..., description="任务的最终状态 ('success', 'recoverable_failure', 'permanent_failure')。")
    message: Optional[str] = Field(None, description="与状态相关的消息。")
    resume_data: Optional[dict] = Field(None, description="如果任务失败但可恢复，则包含恢复数据。")

class NextTaskResponse(BaseModel):
    """调度器向工作节点下发新任务的响应体 Schema。"""
    infohash: str
    metadata: Optional[bytes] = Field(None, description="Torrent 的元数据文件内容 (如果可用)。")
    resume_data: Optional[dict] = Field(None, description="用于恢复失败任务的上下文数据 (如果可用)。")

class TaskList(BaseModel):
    """用于分页列出所有任务的响应体 Schema。"""
    total: int = Field(..., description="数据库中任务的总数。")
    tasks: List[Task] = Field(..., description="当前页的任务列表。")


# --- 工作节点相关的 Schemas ---

class WorkerBase(BaseModel):
    """工作节点模型的基础。"""
    worker_id: str = Field(..., description="工作节点的唯一ID。")
    status: str = Field('idle', description="工作节点的当前状态 ('idle', 'busy')。")

class WorkerCreate(WorkerBase):
    """用于注册新工作节点的请求体 Schema。"""
    pass

class Worker(WorkerBase):
    """用于 API 响应的完整工作节点模型。"""
    id: int
    last_seen_at: datetime.datetime
    created_at: datetime.datetime

    class Config:
        from_attributes = True

class WorkerHeartbeat(BaseModel):
    """用于工作节点发送心跳的请求体 Schema。"""
    worker_id: str
    status: str
