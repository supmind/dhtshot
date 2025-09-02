from pydantic import BaseModel, Field
from typing import List, Optional, Any
import datetime

# --- Task Schemas ---
class TaskBase(BaseModel):
    infohash: str = Field(..., max_length=40, description="The 40-character infohash of the torrent.")

class TaskCreate(TaskBase):
    pass

class Task(TaskBase):
    id: int
    status: str
    successful_screenshots: Optional[List[str]] = []
    result_message: Optional[str] = None
    assigned_worker_id: Optional[str] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    class Config:
        from_attributes = True

class ScreenshotRecord(BaseModel):
    filename: str

class TaskStatusUpdate(BaseModel):
    status: str
    message: Optional[str] = None
    resume_data: Optional[dict] = None

class NextTaskResponse(BaseModel):
    infohash: str
    metadata: Optional[bytes] = None
    resume_data: Optional[dict] = None

# --- Worker Schemas ---
class WorkerBase(BaseModel):
    worker_id: str
    status: str = 'idle'

class WorkerCreate(WorkerBase):
    pass

class Worker(WorkerBase):
    id: int
    last_seen_at: datetime.datetime
    created_at: datetime.datetime
    class Config:
        from_attributes = True

class WorkerHeartbeat(BaseModel):
    worker_id: str
    status: str
