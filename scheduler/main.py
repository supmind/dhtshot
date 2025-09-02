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
    METADATA_DIR = "temp_metadata"
    RESUME_DIR = "temp_resume_data"
    task = crud.get_and_assign_next_task(db, worker_id=worker_id)
    if not task:
        return None
    response_data = {"infohash": task.infohash}
    resume_path = os.path.join(RESUME_DIR, f"{task.infohash}.resume.json")
    if os.path.exists(resume_path):
        with open(resume_path, "r") as f:
            response_data["resume_data"] = json.load(f)
        os.remove(resume_path)
    else:
        metadata_path = os.path.join(METADATA_DIR, f"{task.infohash}.torrent")
        if os.path.exists(metadata_path):
            with open(metadata_path, "rb") as f:
                metadata_bytes = f.read()
                response_data["metadata"] = base64.b64encode(metadata_bytes).decode('ascii')
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

@app.post("/screenshots/{infohash}", tags=["Screenshots"])
async def upload_screenshot(infohash: str, file: UploadFile = File(...)):
    save_dir = f"screenshots_output/{infohash}"
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"filename": file.filename, "infohash": infohash, "path": file_path}

@app.post("/tasks/{infohash}/screenshots", response_model=schemas.Task, tags=["Tasks"])
def record_screenshot_for_task(infohash: str, screenshot: schemas.ScreenshotRecord, db: Session = Depends(get_db)):
    db_task = crud.record_screenshot(db, infohash=infohash, filename=screenshot.filename)
    if db_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return db_task

@app.post("/tasks/{infohash}/status", response_model=schemas.Task, tags=["Tasks"])
async def update_task_status_endpoint(infohash: str, update: schemas.TaskStatusUpdate, db: Session = Depends(get_db)):
    if update.resume_data:
        resume_dir = "temp_resume_data"
        os.makedirs(resume_dir, exist_ok=True)
        resume_path = os.path.join(resume_dir, f"{infohash}.resume.json")
        with open(resume_path, "w") as f:
            json.dump(update.resume_data, f)
    db_task = crud.update_task_status(db, infohash=infohash, status=update.status, message=update.message)
    if db_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return db_task
