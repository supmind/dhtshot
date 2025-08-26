# -*- coding: utf-8 -*-
import asyncio
import logging
from collections import namedtuple

from .client import TorrentClient
from .video import VideoFile
from .generator import ScreenshotGenerator

# 该结构体现在仅用于类型提示和 VideoFile 内部，
# 但由于 service 是主入口点，所以在这里定义是合适的。
KeyframeInfo = namedtuple('KeyframeInfo', ['pts', 'pos', 'size', 'timescale'])


class ScreenshotService:
    """
    截图服务的主协调器。
    它管理一个任务队列和多个工作线程，并使用 TorrentClient、
    VideoFile 和 ScreenshotGenerator 类来执行实际工作。
    """
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output'):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False
        self.client = TorrentClient(loop=self.loop)
        self.generator = ScreenshotGenerator(loop=self.loop, output_dir=self.output_dir)

    async def run(self):
        """启动服务，包括 torrent 客户端和工作线程。"""
        self.log.info("正在启动截图服务...")
        self._running = True
        await self.client.start()
        for _ in range(self.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info(f"截图服务已启动，共有 {self.num_workers} 个工作线程。")

    def stop(self):
        """停止服务，包括 torrent 客户端和工作线程。"""
        self.log.info("正在停止截图服务...")
        self._running = False
        self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("截图服务已停止。")

    async def submit_task(self, infohash: str):
        """通过 infohash 提交一个新的截图任务。"""
        await self.task_queue.put({'infohash': infohash})
        self.log.info(f"已提交新的任务，infohash: {infohash}")

    async def _handle_screenshot_task(self, task_info: dict):
        """处理单个截图任务的核心协调逻辑。"""
        infohash_hex = task_info['infohash']
        self.log.info(f"正在处理任务: {infohash_hex}")
        handle = None
        try:
            # 1. 从客户端获取 torrent 句柄
            handle = await self.client.add_torrent(infohash_hex)
            if not handle or not handle.is_valid():
                self.log.error(f"无法为 {infohash_hex} 获取有效的 torrent 句柄。")
                return

            # 2. 使用 VideoFile 获取关键帧元数据
            video_file = VideoFile(self.client, handle)
            if video_file.file_index == -1:
                self.log.warning(f"在 torrent {infohash_hex} 中未找到视频文件。")
                return

            keyframe_infos, moov_data = await video_file.get_keyframes_and_moov()
            if not keyframe_infos or not moov_data:
                self.log.error(f"无法为 {infohash_hex} 提取关键帧或 moov_data。")
                return

            # 3. 为每个关键帧下载其数据并生成截图
            decode_tasks = []
            for keyframe_info in keyframe_infos:
                # a. 下载特定帧的数据
                keyframe_data = await video_file.download_keyframe_data(keyframe_info)
                if not keyframe_data:
                    self.log.warning(f"因下载失败，跳过帧 (PTS: {keyframe_info.pts})。")
                    continue

                # b. 安排截图生成任务
                ts_sec = keyframe_info.pts / keyframe_info.timescale
                m, s = divmod(ts_sec, 60)
                h, m = divmod(m, 60)
                timestamp_str = f"{int(h):02d}:{int(m):02d}:{int(round(s)):02d}"

                task = self.generator.generate(
                    moov_data=moov_data,
                    keyframe_data=keyframe_data,
                    keyframe_info=keyframe_info,
                    infohash_hex=infohash_hex,
                    timestamp_str=timestamp_str
                )
                decode_tasks.append(task)

            await asyncio.gather(*decode_tasks)
            self.log.info(f"{infohash_hex} 的截图任务已完成。")

        except Exception:
            self.log.exception(f"处理 {infohash_hex} 时发生未知错误。")
        finally:
            if handle:
                self.client.remove_torrent(handle)

    async def _worker(self):
        """工作线程，从队列中获取并处理任务。"""
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作者发生错误。")
