import datetime
from sqlalchemy.orm import Session
from typing import Optional
from . import models, schemas

def get_task_by_infohash(db: Session, infohash: str):
    """
    Retrieves a task from the database by its infohash.
    """
    return db.query(models.Task).filter(models.Task.infohash == infohash).first()

def create_task(db: Session, task: schemas.TaskCreate):
    """
    Creates a new task in the database.
    """
    db_task = models.Task(infohash=task.infohash)
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task

def get_worker_by_id(db: Session, worker_id: str):
    """
    Retrieves a worker from the database by its worker_id.
    """
    return db.query(models.Worker).filter(models.Worker.worker_id == worker_id).first()

def create_worker(db: Session, worker: schemas.WorkerCreate):
    """
    Creates a new worker in the database.
    """
    db_worker = models.Worker(worker_id=worker.worker_id, status=worker.status)
    db.add(db_worker)
    db.commit()
    db.refresh(db_worker)
    return db_worker

def update_worker_status(db: Session, worker_id: str, status: str):
    """
    Updates the status and last_seen_at timestamp for a worker.
    """
    db_worker = get_worker_by_id(db, worker_id=worker_id)
    if db_worker:
        db_worker.last_seen_at = datetime.datetime.utcnow()
        db_worker.status = status
        db.commit()
        db.refresh(db_worker)
    return db_worker

def get_and_assign_next_task(db: Session, worker_id: str):
    """
    Atomically fetches the next pending task and assigns it to the given worker.
    Uses a pessimistic lock (SELECT ... FOR UPDATE) to prevent race conditions.
    """
    # The .with_for_update() call is crucial for locking
    db_task = db.query(models.Task).filter(models.Task.status == 'pending').order_by(models.Task.created_at).with_for_update().first()

    if db_task:
        db_task.status = 'working'
        db_task.assigned_worker_id = worker_id
        db.commit()
        db.refresh(db_task)

    return db_task

def update_task_status(db: Session, infohash: str, status: str, message: Optional[str] = None):
    """
    Updates the status and result message of a task.
    Also clears the assigned worker when a task is finished.
    """
    db_task = get_task_by_infohash(db, infohash=infohash)
    if db_task:
        db_task.status = status
        db_task.result_message = message
        if status in ['success', 'permanent_failure', 'recoverable_failure']:
            db_task.assigned_worker_id = None
        db.commit()
        db.refresh(db_task)
    return db_task

def record_screenshot(db: Session, infohash: str, filename: str):
    """
    Appends a successfully generated screenshot's filename to a task's record.
    """
    db_task = get_task_by_infohash(db, infohash=infohash)
    if db_task:
        if db_task.successful_screenshots:
            # Note: This is not an atomic operation and can lead to race conditions
            # in high-concurrency scenarios. A more robust solution would use
            # database-specific JSON functions or a separate table.
            current_screenshots = list(db_task.successful_screenshots)
            if filename not in current_screenshots:
                current_screenshots.append(filename)
                db_task.successful_screenshots = current_screenshots
        else:
            db_task.successful_screenshots = [filename]
        db.commit()
        db.refresh(db_task)
    return db_task
