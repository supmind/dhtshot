# -*- coding: utf-8 -*-
"""
端到端集成测试，模拟从任务创建到截图完成的整个流程。
"""
import pytest
import asyncio
import httpx
import subprocess
import time
import os
import sys
from config import Settings

# 使用用户提供的真实、活跃的 infohash
TEST_INFOHASH = "08ada5a7a6183aae1e09d831df6748d566095a10"
SCHEDULER_HOST = "127.0.0.1"
# 使用一个非默认端口以避免与开发服务器冲突
SCHEDULER_PORT = 8001
SCHEDULER_URL = f"http://{SCHEDULER_HOST}:{SCHEDULER_PORT}"

@pytest.fixture(scope="module")
def api_key_headers():
    """提供 API 密钥请求头。"""
    settings = Settings()
    return {"X-API-Key": settings.scheduler_api_key}

@pytest.fixture(scope="module")
def scheduler_server():
    """
    在后台运行调度器 (uvicorn) 的 fixture。
    使用 'module' 范围，使得整个测试模块共享同一个服务实例。
    """
    # 为测试数据库设置一个单独的文件，避免污染主数据库
    test_db_file = "test_scheduler.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    env = os.environ.copy()
    env["DB_URL"] = f"sqlite:///{test_db_file}"

    cmd = [
        sys.executable,
        "-m", "uvicorn",
        "scheduler.main:app",
        "--host", SCHEDULER_HOST,
        "--port", str(SCHEDULER_PORT)
    ]
    # 使用 Popen 以非阻塞方式启动
    proc = subprocess.Popen(cmd, env=env)
    # 等待一段时间以确保服务器完全启动
    time.sleep(5)
    yield
    # 测试结束后，终止子进程
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    # 清理测试数据库
    if os.path.exists(test_db_file):
        os.remove(test_db_file)


@pytest.fixture(scope="module")
def worker_process():
    """在后台运行工作节点 (worker.py) 的 fixture。"""
    # 工作节点需要通过环境变量知道调度器的 URL 和 API 密钥
    env = os.environ.copy()
    env["SCHEDULER_URL"] = SCHEDULER_URL

    settings = Settings()
    env["SCHEDULER_API_KEY"] = settings.scheduler_api_key
    # 设置一个极短的元数据超时时间来强制触发竞态条件失败
    env["METADATA_TIMEOUT"] = "20"

    cmd = [sys.executable, "worker.py"]
    proc = subprocess.Popen(cmd, env=env, stdout=sys.stdout, stderr=sys.stderr)
    yield
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_flow_with_infohash_only(scheduler_server, worker_process, api_key_headers):
    """
    测试端到端完整流程，在短超时设置下，验证任务能否成功。
    在修复前，此测试会因为元数据获取超时而失败。
    在修复后，此测试应该通过。
    """
    async with httpx.AsyncClient() as client:
        # 1. 创建任务
        create_response = await client.post(
            f"{SCHEDULER_URL}/tasks/",
            data={"infohash": TEST_INFOHASH},
            headers=api_key_headers
        )
        assert create_response.status_code in [200, 201]

        # 2. 轮询任务状态直到成功或超时
        max_wait_time = 120  # 2 分钟
        start_time = time.time()
        final_status = ""
        task_data = {}

        while time.time() - start_time < max_wait_time:
            await asyncio.sleep(10)  # 轮询间隔
            status_response = await client.get(
                f"{SCHEDULER_URL}/tasks/{TEST_INFOHASH}",
                headers=api_key_headers,
                timeout=10
            )
            if status_response.status_code == 200:
                task_data = status_response.json()
                final_status = task_data.get("status")

                if final_status == "success":
                    break

                if final_status in ["permanent_failure", "recoverable_failure"]:
                    pytest.fail(
                        f"任务提前进入失败状态: {final_status}。"
                        f" 失败信息: {task_data.get('message')}"
                    )

        # 3. 断言最终状态
        assert final_status == "success", (
            f"任务在 {max_wait_time} 秒内未能成功。"
            f" 最终状态为 '{final_status}'。 "
            f" 最终任务详情: {task_data}"
        )

        # 如果任务成功，则必须有截图记录
        # 注意：此断言可能会因为 R2 上传失败而失败，但这本身也是一个需要关注的问题
        screenshots = task_data.get("successful_screenshots")
        assert screenshots is not None and len(screenshots) > 0, "任务成功了，但没有任何截图记录。"
