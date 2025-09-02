# -*- coding: utf-8 -*-
"""
对 worker.py 中工作节点逻辑的单元测试。
"""
import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
import os

from config import Settings
# 在导入 worker 之前设置环境变量
os.environ['SCHEDULER_URL'] = 'http://test-scheduler'
import worker

@pytest.fixture
def mock_settings():
    """提供一个带有测试 URL 的 Settings 对象。"""
    return Settings(scheduler_url="http://test-scheduler")

@pytest.fixture
def mock_client():
    """提供一个 mock 的 SchedulerAPIClient 实例。"""
    client = MagicMock(spec=worker.SchedulerAPIClient)
    client.register = AsyncMock(return_value=True)
    client.get_next_task = AsyncMock(return_value={"infohash": "test_infohash"})
    client.upload_screenshot = AsyncMock()
    client.update_task_status = AsyncMock()
    client.send_heartbeat = AsyncMock()
    return client

@pytest.mark.asyncio
@patch('worker.ScreenshotService', spec=True)
@patch('worker.SchedulerAPIClient')
async def test_worker_main_loop(MockAPIClient, MockScreenshotService, mock_settings):
    """
    测试工作节点的主循环是否能成功注册、获取任务并提交给 ScreenshotService。
    """
    # --- 模拟客户端和 Service ---
    mock_client_instance = MockAPIClient.return_value
    # 关键修复：确保 mock 的异步方法返回一个可等待的对象（coroutine）
    mock_client_instance.register = AsyncMock(return_value=True)
    mock_client_instance.get_next_task = AsyncMock(return_value={"infohash": "test_infohash", "metadata": None})

    mock_service_instance = MockScreenshotService.return_value
    mock_service_instance.submit_task = AsyncMock()
    mock_service_instance.active_tasks = set()

    # --- 运行主程序 ---
    # aiohttp.ClientSession 在这里是虚拟的，因为它不会被实际使用
    main_task = asyncio.create_task(worker.main(session=MagicMock()))
    await asyncio.sleep(0.1) # 让事件循环运行

    # --- 断言 ---
    mock_client_instance.register.assert_awaited_once_with(worker.WORKER_ID)
    mock_client_instance.get_next_task.assert_awaited_once_with(worker.WORKER_ID)
    mock_service_instance.submit_task.assert_awaited_once_with(infohash="test_infohash", metadata=None)

    # --- 清理 ---
    main_task.cancel()
    # 等待任务完成其清理逻辑。由于 main 内部处理了 CancelledError，
    # 我们不应该在这里期望异常被抛出。
    await main_task

@pytest.mark.asyncio
async def test_on_screenshot_saved_callback(mock_client):
    """测试 on_screenshot_saved 回调函数是否能正确调用客户端的上传方法。"""
    await worker.on_screenshot_saved(mock_client, "test_infohash", "/path/to/test.jpg")
    mock_client.upload_screenshot.assert_called_once_with("test_infohash", "/path/to/test.jpg")

@pytest.mark.asyncio
async def test_on_task_finished_callback(mock_client):
    """测试 on_task_finished 回调是否能正确调用客户端的状态更新方法。"""
    resume_data = {"key": "value"}
    await worker.on_task_finished(
        mock_client, status="success", infohash="test_infohash",
        message="Done", resume_data=resume_data
    )
    mock_client.update_task_status.assert_called_once_with(
        "test_infohash", "success", "Done", resume_data
    )
