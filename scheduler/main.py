from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File, Form, Response
from sqlalchemy.orm import Session
from typing import Optional
import datetime
import os
import shutil
import json
import base64

from . import crud, models, schemas
from .database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Distributed Screenshot Scheduler",
    description="The central scheduler for managing and distributing screenshot tasks.",
    version="0.1.0",
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/", tags=["General"])
def read_root():
    """A simple health check endpoint."""
    return {"message": "Screenshot Scheduler is running"}

@app.post("/tasks/", response_model=schemas.Task, status_code=status.HTTP_200_OK, tags=["Tasks"])
async def create_or_reactivate_task(
    response: Response,
    infohash: str = Form(..., max_length=40),
    torrent_file: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db)
):
    db_task = crud.get_task_by_infohash(db, infohash=infohash)
    if db_task:
        if db_task.status in ["pending", "working", "success"]:
            return db_task
        elif db_task.status == "permanent_failure":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Task has permanently failed and cannot be resubmitted.")
        elif db_task.status == "recoverable_failure":
            db_task.status = "pending"
            db.commit()
            db.refresh(db_task)
            return db_task
    response.status_code = status.HTTP_201_CREATED
    if torrent_file:
        METADATA_DIR = "temp_metadata"
        os.makedirs(METADATA_DIR, exist_ok=True)
        file_path = os.path.join(METADATA_DIR, f"{infohash}.torrent")
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(torrent_file.file, buffer)
    new_task = crud.create_task(db=db, task=schemas.TaskCreate(infohash=infohash))
    return new_task

@app.get("/tasks/next", response_model=Optional[schemas.NextTaskResponse], tags=["Tasks"])
def get_next_task(worker_id: str, db: Session = Depends(get_db)):
    """
    为工作节点获取下一个待处理的任务。
    此端点现在从数据库中直接读取 resume_data，而不是从临时文件中读取。
    """
    METADATA_DIR = "temp_metadata"
    task = crud.get_and_assign_next_task(db, worker_id=worker_id)
    if not task:
        return None

    response_data = {"infohash": task.infohash}
    if task.resume_data:
        self.log.info("在任务 %s 中发现恢复数据，将其发送给工作节点。", task.infohash)
        response_data["resume_data"] = task.resume_data
        # 清除恢复数据，因为它是一次性的
        task.resume_data = None
        db.commit()
    else:
        # 仅在没有恢复数据时才发送元数据
        metadata_path = os.path.join(METADATA_DIR, f"{task.infohash}.torrent")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                metadata_bytes = f.read()
                response_data["metadata"] = base64.b64encode(metadata_bytes).decode('ascii')
            # 考虑在成功分配后删除元数据文件，以防磁盘空间膨胀
            os.remove(metadata_path)

    return schemas.NextTaskResponse(**response_data)

@app.get("/tasks/{infohash}", response_model=schemas.Task, tags=["Tasks"])
def read_task(infohash: str, db: Session = Depends(get_db)):
    db_task = crud.get_task_by_infohash(db, infohash=infohash)
    if db_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return db_task

@app.post("/workers/register", response_model=schemas.Worker, tags=["Workers"])
def register_worker(worker: schemas.WorkerCreate, db: Session = Depends(get_db)):
    db_worker = crud.get_worker_by_id(db, worker_id=worker.worker_id)
    if db_worker:
        db_worker.last_seen_at = datetime.datetime.utcnow()
        db_worker.status = 'idle'
        db.commit()
        db.refresh(db_worker)
        return db_worker
    return crud.create_worker(db=db, worker=worker)

@app.post("/workers/heartbeat", response_model=schemas.Worker, tags=["Workers"])
def worker_heartbeat(heartbeat: schemas.WorkerHeartbeat, db: Session = Depends(get_db)):
    db_worker = crud.update_worker_status(db, worker_id=heartbeat.worker_id, status=heartbeat.status)
    if db_worker is None:
        raise HTTPException(status_code=404, detail="Worker not found. Please register first.")
    return db_worker

@app.post("/screenshots/{infohash}", response_model=schemas.Task, tags=["Screenshots"])
async def upload_screenshot(infohash: str, db: Session = Depends(get_db), file: UploadFile = File(...)):
    """
    Uploads a screenshot and records it for the associated task in a single atomic operation.
    """
    save_dir = f"screenshots_output/{infohash}"
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, file.filename)

    # Save the uploaded file
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Record the screenshot in the database
    db_task = crud.record_screenshot(db, infohash=infohash, filename=file.filename)
    if db_task is None:
        # If the task doesn't exist, we should probably remove the orphaned file
        os.remove(file_path)
        raise HTTPException(status_code=404, detail="Task not found for this screenshot.")

    return db_task

@app.post("/tasks/{infohash}/status", response_model=schemas.Task, tags=["Tasks"])
async def update_task_status_endpoint(infohash: str, update: schemas.TaskStatusUpdate, db: Session = Depends(get_db)):
    """
    更新一个任务的最终状态。
    如果提供了 resume_data，它将被直接存储在数据库中，而不是保存在临时文件里。
    """
    try:
        db_task = crud.update_task_status(
            db,
            infohash=infohash,
            status=update.status,
            message=update.message,
            resume_data=update.resume_data
        )
        if db_task is None:
            raise HTTPException(status_code=404, detail="任务未找到")
        return db_task
    except Exception as e:
        # 添加日志记录，以便在数据库操作失败时进行调试
        # log.error("更新任务状态时数据库出错: %s", e)
        raise HTTPException(status_code=500, detail=f"更新任务状态时发生内部错误: {e}")
