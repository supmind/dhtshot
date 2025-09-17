# -*- coding: utf-8 -*-
"""
本模块定义了 ScreenshotService，它是协调整个截图生成过程的核心服务。
"""
import asyncio
import logging
from typing import Optional, Callable, Awaitable, Any
import psutil

from .client import TorrentClient, TorrentClientError
from .errors import (
    TaskError, MoovFetchError, MetadataTimeoutError, FrameDownloadTimeoutError
)
from .generator import ScreenshotGenerator
from .video_processor import VideoProcessor
from config import Settings

# 为 status_callback 定义一个类型签名，以增强可读性和静态检查能力
StatusCallback = Callable[..., Awaitable[None]]

class ScreenshotService:
    """
    协调截图生成过程的核心服务类。

    此类通过内部的 asyncio.Queue 管理任务，并由多个工作协程并发处理。
    其主要职责包括：
    1. 启动和停止服务以及底层的 Torrent 客户端。
    2. 接受新的截图任务并将其放入队列。
    3. 管理一组工作协程，这些协程从队列中获取任务并处理。
    4. 为每个任务实例化一个 `VideoProcessor` 来处理具体的截图生成逻辑。
    5. 通过回调函数向上层报告任务的最终状态。
    """
    def __init__(
        self,
        settings: Settings,
        loop=None,
        client=None,
        status_callback: Optional[StatusCallback] = None,
        screenshot_callback: Optional[Callable] = None,
        details_callback: Optional[Callable] = None,
        screenshot_check_callback: Optional[Callable] = None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.settings = settings
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False

        self.client = client or TorrentClient(
            loop=self.loop,
            settings=self.settings
        )
        self.generator = ScreenshotGenerator(
            loop=self.loop,
            output_dir=self.settings.output_dir,
            on_success=screenshot_callback
        )
        self.status_callback = status_callback
        self.details_callback = details_callback
        self.screenshot_check_callback = screenshot_check_callback
        self.active_tasks = set()
        self._submit_lock = asyncio.Lock()

    def get_queue_size(self) -> int:
        """返回当前在服务内部队列中等待的任务数量。"""
        return self.task_queue.qsize()

    async def run(self):
        """启动服务，包括底层的 Torrent 客户端和处理任务的工作协程。"""
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.settings.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info("ScreenshotService 已启动，拥有 %d 个工作进程。", self.settings.num_workers)

    async def stop(self):
        """异步地、优雅地停止服务，包括 Torrent 客户端和所有工作协程。"""
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        await self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, metadata: bytes = None):
        """
        提交一个新的截图任务。
        使用锁来防止同一 infohash 的任务被重复提交。
        """
        async with self._submit_lock:
            if infohash in self.active_tasks:
                self.log.warning("任务 %s 已在处理中，本次提交被忽略。", infohash)
                return
            self.active_tasks.add(infohash)

        await self.task_queue.put({'infohash': infohash, 'metadata': metadata})
        self.log.info("为 infohash: %s 提交了新任务", infohash)

    async def _send_status_update(self, **kwargs: Any) -> None:
        if self.status_callback:
            await self.status_callback(**kwargs)

    async def _handle_screenshot_task(self, task_info: dict):
        infohash_hex = task_info['infohash']
        self.log.info("正在处理任务: %s", infohash_hex)
        handle = None
        should_delete_files = True
        try:
            handle = await self.client.add_torrent(infohash_hex, metadata=task_info.get('metadata'))
            if not handle or not handle.is_valid():
                raise TorrentClientError(f"无法为 {infohash_hex} 获取有效的 torrent handle。")

            processor = VideoProcessor(
                settings=self.settings,
                client=self.client,
                generator=self.generator,
                loop=self.loop,
                details_callback=self.details_callback,
                screenshot_check_callback=self.screenshot_check_callback
            )
            await processor.process(handle, infohash_hex)

            self.log.info("任务 %s 成功完成。", infohash_hex)
            await self._send_status_update(status='success', infohash=infohash_hex, message='任务成功完成。')
        except (MetadataTimeoutError, MoovFetchError, FrameDownloadTimeoutError, TorrentClientError) as e:
            self.log.warning("任务 %s 遇到可恢复的错误: %s", e.infohash, e)
            await self._send_status_update(status='recoverable_failure', infohash=e.infohash, message=str(e), error=e)
            should_delete_files = False
        except TaskError as e:
            self.log.error("任务 %s 因永久性错误而失败: %s", e.infohash, e, exc_info=True)
            await self._send_status_update(status='permanent_failure', infohash=e.infohash, message=str(e), error=e)
        except Exception as e:
            self.log.exception("处理 %s 时发生意外的严重错误。", infohash_hex)
            await self._send_status_update(status='permanent_failure', infohash=infohash_hex, message=f"发生意外错误: {e}", error=e)
        finally:
            if handle and handle.is_valid():
                await self.client.remove_torrent(handle, delete_files=should_delete_files)
            self.active_tasks.discard(infohash_hex)

    async def _worker(self):
        p = psutil.Process()
        while self._running:
            try:
                while p.num_fds() >= self.settings.MAX_FILE_DESCRIPTORS:
                    await asyncio.sleep(2)
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作进程中发生未捕获的错误。")
