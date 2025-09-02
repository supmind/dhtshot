# Distributed Video Screenshot System

## 1. Overview

This project provides a high-performance, distributed service for generating screenshots from video files contained within torrents. The key feature of this service is its **minimal download strategy**. Instead of downloading an entire torrent, it only fetches the necessary metadata and the specific data chunks required to extract keyframes, thus saving significant bandwidth and time.

The system is built with Python using `asyncio` and is composed of a central scheduler and multiple worker nodes.

## 2. Architecture and Design

The system is composed of two main applications:

### 2.1. The Central Scheduler (`scheduler/`)

-   **Framework:** Built with FastAPI.
-   **Responsibilities:**
    -   Provides a REST API for submitting new tasks and querying their status.
    -   Manages a database (SQLite by default) of all tasks and registered workers.
    -   Assigns pending tasks to available workers.
    -   Handles `resume_data` for tasks that fail with recoverable errors, allowing them to be resumed.
    -   Receives and stores the final screenshot images uploaded by workers.

### 2.2. The Worker (`worker.py`)

-   **Framework:** A standalone `asyncio` application using `aiohttp`.
-   **Responsibilities:**
    -   On startup, automatically registers itself with the Central Scheduler.
    -   Periodically sends heartbeats to the scheduler to remain active.
    -   Polls the scheduler for new tasks.
    -   When a task is received, it uses the core `screenshot` library to perform the screenshot generation.
    -   Reports progress back to the scheduler in real-time, including uploading successfully generated screenshots and their filenames as they are created.
    -   Reports the final task status (`success`, `permanent_failure`, etc.) back to the scheduler.

### 2.3. The Core Library (`screenshot/`)

This is the underlying engine that performs the actual screenshotting.
-   **`TorrentClient`:** Manages all `libtorrent` interactions.
-   **`KeyframeExtractor`:** Parses MP4 metadata to find keyframe locations.
-   **`ScreenshotGenerator`:** Decodes video frames and saves them as JPGs.
-   **`ScreenshotService`:** Orchestrates the process for a single task.

## 3. Deployment and Usage

### 3.1. Installation

The project uses Python 3.10+. All dependencies are listed in `requirements.txt`.

```bash
# Install all required packages
pip install -r requirements.txt
```

### 3.2. Running the System

You need to run the two components in separate terminals.

**1. Start the Scheduler:**
```bash
# From the project root directory
uvicorn scheduler.main:app --host 0.0.0.0 --port 8000
```
The scheduler API will now be available at `http://localhost:8000`.

**2. Start one or more Workers:**
```bash
# From the project root directory, in a new terminal
python worker.py
```
You can run `python worker.py` on multiple different machines, as long as they can reach the scheduler's URL (update `SCHEDULER_URL` in `worker.py` if necessary).

### 3.3. API Usage

You can interact with the system via the scheduler's REST API.

-   **Create a new task:**
    ```bash
    curl -X POST -F "infohash=<YOUR_40_CHAR_INFOHASH>" http://localhost:8000/tasks/
    ```

-   **Query a task's status:**
    ```bash
    curl http://localhost:8000/tasks/<YOUR_40_CHAR_INFOHASH>
    ```

## 4. Design Details

-   **Database:** Uses SQLite by default (`/tmp/scheduler.db`), but can be easily configured to use PostgreSQL or MySQL in `scheduler/database.py`.
-   **Task Assignment:** Uses a `SELECT ... FOR UPDATE` database lock to ensure a pending task is atomically assigned to only one worker, preventing race conditions.
-   **Fault Tolerance:** The system is resilient to temporary network errors. If a worker fails to fetch data, the task is marked as `recoverable_failure`, and its `resume_data` is saved by the scheduler. The task can then be re-queued and resumed by another worker.
-   **Real-time Callbacks:** The worker uses a per-screenshot success callback to immediately upload generated images and record their filenames, ensuring no data is lost even if the worker fails mid-task.
