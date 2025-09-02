# 分布式视频截图系统

## 1. 概述

本项目提供一个高性能的、分布式的服务，用于从 torrent 中的视频文件生成截图。本服务的核心特性是其**最小化下载策略**。它不会下载整个 torrent 文件，而仅获取必要的元数据和提取关键帧所需的特定数据块，从而极大地节省了带宽和时间。

本系统使用 Python 的 `asyncio` 构建，由一个中心调度器和多个工作节点组成。

## 2. 架构设计

本系统由两个主要应用组成：

### 2.1. 中心调度器 (`scheduler/`)

-   **框架:** 使用 FastAPI 构建。
-   **职责:**
    -   提供 REST API 用于提交新任务和查询任务状态。
    -   管理一个包含所有任务和已注册工作节点的数据库（默认为 SQLite）。
    -   将待处理的任务分配给可用的工作节点。
    -   处理因可恢复错误而失败的任务的 `resume_data`，使其能够被续传。
    -   接收并存储由工作节点上传的最终截图文件。

### 2.2. 工作节点 (`worker.py`)

-   **框架:** 一个使用 `aiohttp` 的独立 `asyncio` 应用。
-   **职责:**
    -   启动时，自动向中心调度器注册。
    -   周期性地向调度器发送心跳以保持活动状态。
    -   向调度器轮询新任务。
    -   当接收到任务时，使用核心的 `screenshot` 库来执行截图生成工作。
    -   实时向调度器报告进度，包括在成功生成截图时立即上传文件和记录文件名。
    -   向调度器报告最终的任务状态（`success`, `permanent_failure` 等）。

### 2.3. 核心库 (`screenshot/`)

这是执行实际截图工作的底层引擎。
-   **`TorrentClient`:** 管理所有 `libtorrent` 交互。
-   **`KeyframeExtractor`:** 解析 MP4 元数据以定位关键帧。
-   **`ScreenshotGenerator`:** 解码视频帧并将其保存为 JPG 图片。
-   **`ScreenshotService`:** 为单个任务协调整个流程。

## 3. 部署与使用

### 3.1. 安装

本项目使用 Python 3.10+。所有依赖项均在 `requirements.txt` 中列出。

```bash
# 安装所有必需的包
pip install -r requirements.txt
```

### 3.2. 运行系统

您需要在不同的终端中分别运行这两个组件。

**1. 启动调度器:**
```bash
# 在项目根目录
uvicorn scheduler.main:app --host 0.0.0.0 --port 8000
```
调度器 API 现在位于 `http://localhost:8000`。

**2. 启动一个或多个工作节点:**
```bash
# 在项目根目录的一个新终端中
python worker.py
```
您可以在多台不同的机器上运行 `python worker.py`，只要它们能访问到调度器的 URL（如有必要，请修改 `worker.py` 中的 `SCHEDULER_URL`）。

### 3.3. API 用法

您可以通过调度器的 REST API 与系统交互。

-   **创建新任务:**
    ```bash
    curl -X POST -F "infohash=<你的40位INFOHASH>" http://localhost:8000/tasks/
    ```

-   **查询任务状态:**
    ```bash
    curl http://localhost:8000/tasks/<你的40位INFOHASH>
    ```

## 4. 设计细节

-   **数据库:** 默认为 SQLite (`/tmp/scheduler.db`)，但可以在 `scheduler/database.py` 中轻松配置为 PostgreSQL 或 MySQL。
-   **任务分配:** 使用 `SELECT ... FOR UPDATE` 数据库锁来确保一个待处理任务被原子性地分配给唯一一个工作节点，防止竞态条件。
-   **容错性:** 系统对临时的网络错误具有弹性。如果一个工作节点获取数据失败，任务将被标记为 `recoverable_failure`，其 `resume_data` 会被调度器保存。任务随后可以被重新排队并由另一个工作节点续传。
-   **实时回调:** 工作节点使用“每截图成功回调”机制来立即上传生成的图片并记录其文件名，确保即使工作节点在任务中途失败也不会丢失数据。
