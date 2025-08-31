# -*- coding: utf-8 -*-
"""
这个文件包含了专门用于测试 ScreenshotService 并发行为的测试用例。
"""
import pytest
import asyncio
from unittest.mock import AsyncMock

from screenshot.service import ScreenshotService
from screenshot.config import Settings

# 将此文件中的所有测试标记为异步测试
pytestmark = pytest.mark.asyncio

async def test_submit_task_is_thread_safe(mock_service_dependencies, status_callback):
    """
    验证 `submit_task` 方法是并发安全的。

    这个测试通过并发地多次提交同一个 infohash，来验证只有一个任务
    被实际添加到队列中。这确保了 `_submit_lock` 正常工作，
    防止了在高并发场景下可能出现的竞态条件。
    """
    # --- 1. 测试设置 ---
    loop = asyncio.get_running_loop()
    service = ScreenshotService(settings=Settings(), loop=loop, status_callback=status_callback)

    # 我们不需要实际处理任务，所以模拟掉核心处理逻辑
    service._handle_screenshot_task = AsyncMock()

    # 模拟底层的任务队列，以便我们可以监视它的 `put` 方法
    service.task_queue = AsyncMock()

    infohash = "test_concurrent_hash"
    submission_count = 10

    # --- 2. 执行 ---
    # 使用 asyncio.gather 来并发执行多次 `submit_task` 调用
    await asyncio.gather(
        *[service.submit_task(infohash) for _ in range(submission_count)]
    )

    # --- 3. 断言 ---
    # 核心断言：尽管我们尝试了 10 次，但 `put` 方法应该只被调用一次。
    service.task_queue.put.assert_awaited_once()

    # 验证被放入队列的任务是否是我们期望的那个
    call_args, _ = service.task_queue.put.call_args
    submitted_task = call_args[0]
    assert submitted_task['infohash'] == infohash
    assert submitted_task['resume_data'] is None

    # 验证 active_tasks 集合中只有一个该任务的记录
    assert len(service.active_tasks) == 1
    assert infohash in service.active_tasks
