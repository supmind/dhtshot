# -*- coding: utf-8 -*-
"""
对 worker.py 中工作节点逻辑的单元测试。
"""
import pytest
import respx
import asyncio
from unittest.mock import patch, AsyncMock
import aiohttp
import os

from config import Settings
# 在导入 worker 之前设置环境变量
os.environ['SCHEDULER_URL'] = 'http://test-scheduler'
import worker

@pytest.fixture
def mock_settings():
    """提供一个带有测试 URL 的 Settings 对象。"""
    return Settings(scheduler_url="http://test-scheduler")

@pytest.mark.asyncio
@patch('worker.ScreenshotService', spec=True)
async def test_worker_main_loop(MockScreenshotService, mock_settings):
    """
    测试工作节点的主循环是否能成功注册、获取任务并提交给 ScreenshotService。
    """
    # 使用 respx 作为上下文管理器来 mock HTTP 请求
    async with respx.mock(base_url=mock_settings.scheduler_url) as mock:
        # --- 模拟 API 路由 ---
        register_route = mock.post("/workers/register", name="register").respond(200)
        task_route = mock.get("/tasks/next", name="get_task").respond(200, json={"infohash": "test_infohash"})

        # --- 模拟 ScreenshotService ---
        mock_service_instance = MockScreenshotService.return_value
        mock_service_instance.run = AsyncMock()
        mock_service_instance.stop = AsyncMock()
        mock_service_instance.submit_task = AsyncMock()
        mock_service_instance.active_tasks = set()

        # --- 运行主程序 ---
        # 重构 main 以接受 session，但为了测试，我们可以在这里 patch settings
        with patch('worker.Settings', return_value=mock_settings):
             # 创建一个真实的 aiohttp.ClientSession，它将被 respx 拦截
            async with aiohttp.ClientSession() as session:
                main_task = asyncio.create_task(worker.main(session))
                await asyncio.sleep(0.1)

                # --- 断言 ---
                assert register_route.called
                assert task_route.called
                mock_service_instance.submit_task.assert_called_once_with(infohash="test_infohash", metadata=None, resume_data=None)

                # --- 清理 ---
                main_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await main_task

@pytest.mark.asyncio
async def test_on_screenshot_saved_callback(mock_settings):
    """测试 on_screenshot_saved 回调函数是否能正确上传文件。"""
    async with respx.mock(base_url=mock_settings.scheduler_url) as mock:
        upload_route = mock.post("/screenshots/test_infohash", name="upload").respond(200)

        with open("test.jpg", "wb") as f:
            f.write(b"jpeg_data")

        async with aiohttp.ClientSession() as session:
            await worker.on_screenshot_saved(session, mock_settings.scheduler_url, "test_infohash", "test.jpg")

        assert upload_route.called
        os.remove("test.jpg")

@pytest.mark.asyncio
async def test_on_task_finished_callback_with_resume_data(mock_settings):
    """
    测试 on_task_finished 回调在处理带 resume_data 时是否能正确编码。
    """
    async with respx.mock(base_url=mock_settings.scheduler_url) as mock:
        status_route = mock.post("/tasks/test_infohash/status", name="status").respond(200)
        resume_data = {"extractor_info": {"extradata": b'\x01\x02'}}

        async with aiohttp.ClientSession() as session:
            await worker.on_task_finished(
                session, mock_settings.scheduler_url, status="recoverable_failure", infohash="test_infohash",
                message="Test", resume_data=resume_data
            )

        assert status_route.called
        request_payload = respx.calls.last.request.content
        import json
        payload_json = json.loads(request_payload)
        import base64
        assert payload_json["resume_data"]["extractor_info"]["extradata"] == base64.b64encode(b'\x01\x02').decode('ascii')
