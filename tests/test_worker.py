# -*- coding: utf-8 -*-
"""
对 worker.py 中工作节点逻辑的单元测试。
"""
import pytest
import respx
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import status

# 必须在导入 worker 模块之前设置环境变量，因为它在导入时就会被读取
import os
os.environ['SCHEDULER_URL'] = 'http://test-scheduler'

# 现在可以安全地导入 worker 模块
import worker

@pytest.fixture
def mock_aiohttp_routes():
    """一个 Pytest fixture，使用 respx 来模拟所有对调度器 API 的 HTTP 请求。"""
    with respx.mock(base_url="http://test-scheduler") as mock:
        # 模拟注册成功
        mock.post("/workers/register", name="register").respond(200, json={"worker_id": worker.WORKER_ID, "status": "idle"})
        # 模拟心跳成功
        mock.post("/workers/heartbeat", name="heartbeat").respond(200, json={"worker_id": worker.WORKER_ID, "status": "idle"})
        # 模拟获取任务
        mock.get("/tasks/next", name="get_task").respond(200, json={"infohash": "test_infohash"})
        # 模拟上传截图成功
        mock.post("/screenshots/test_infohash", name="upload_screenshot").respond(200, json={"status": "success"})
        # 模拟更新状态成功
        mock.post("/tasks/test_infohash/status", name="update_status").respond(200, json={"status": "success"})
        yield mock

@pytest.mark.asyncio
@patch('worker.ScreenshotService', spec=True)
async def test_worker_main_loop(MockScreenshotService, mock_aiohttp_routes):
    """
    测试工作节点的主循环是否能成功注册、获取任务并提交给 ScreenshotService。
    """
    # --- 模拟设置 ---
    mock_service_instance = MockScreenshotService.return_value
    mock_service_instance.run = AsyncMock()
    mock_service_instance.stop = AsyncMock()
    mock_service_instance.submit_task = AsyncMock()
    # 模拟 service.active_tasks 行为，以便主循环不会无限等待
    mock_service_instance.active_tasks = set()

    # --- 运行主程序 ---
    # 我们在一个单独的任务中运行 main，以便我们可以控制其生命周期
    main_task = asyncio.create_task(worker.main())

    # 让事件循环运行一小段时间以执行注册和第一次任务轮询
    await asyncio.sleep(0.1)

    # --- 断言 ---
    # 1. 验证工作节点是否已注册
    assert mock_aiohttp_routes["register"].called

    # 2. 验证是否已请求任务
    assert mock_aiohttp_routes["get_task"].called

    # 3. 验证任务是否已提交给本地服务
    mock_service_instance.submit_task.assert_called_once_with(
        infohash="test_infohash", metadata=None, resume_data=None
    )

    # --- 清理 ---
    # 通过设置 stop_event 来优雅地停止 main 循环
    worker.stop_event.set()
    await asyncio.sleep(0.1) # 允许任务响应事件
    assert main_task.done()

@pytest.mark.asyncio
async def test_on_screenshot_saved_callback(mock_aiohttp_routes):
    """测试 on_screenshot_saved 回调函数是否能正确上传文件。"""
    # 创建一个临时的假文件用于上传
    with open("test_screenshot.jpg", "wb") as f:
        f.write(b"fake-jpeg")

    mock_session = MagicMock(spec=respx.mock.session)
    await worker.on_screenshot_saved(mock_session, "test_infohash", "test_screenshot.jpg")

    # 验证上传端点是否被调用
    assert mock_aiohttp_routes["upload_screenshot"].called

    # 清理临时文件
    os.remove("test_screenshot.jpg")

@pytest.mark.asyncio
async def test_on_task_finished_callback_with_resume_data(mock_aiohttp_routes):
    """
    测试 on_task_finished 回调函数在处理带有 resume_data (包含 bytes) 的情况时，
    是否能正确地进行 Base64 编码并发送。
    """
    mock_session = MagicMock(spec=respx.mock.session)
    resume_data = {
        "extractor_info": {
            "extradata": b'\x01\x02\x03\x04' # 这是需要被编码的 bytes
        }
    }

    await worker.on_task_finished(
        mock_session,
        status="recoverable_failure",
        infohash="test_infohash",
        message="Test failure",
        resume_data=resume_data
    )

    # 验证状态更新端点是否被调用
    assert mock_aiohttp_routes["update_status"].called

    # 验证请求体中的 extradata 是否已被正确编码为 base64 字符串
    request = mock_aiohttp_routes["update_status"].calls.last.request
    payload = request.content.decode('utf-8')
    import json
    payload_json = json.loads(payload)

    encoded_extradata = payload_json["resume_data"]["extractor_info"]["extradata"]
    import base64
    assert encoded_extradata == base64.b64encode(b'\x01\x02\x03\x04').decode('ascii')
