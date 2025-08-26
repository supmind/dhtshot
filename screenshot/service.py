# -*- coding: utf-8 -*-
import asyncio
import logging
from collections import namedtuple

from .client import TorrentClient
from .video import VideoFile
from .generator import ScreenshotGenerator

# This is now only used for type hinting and within VideoFile,
# but service is the main entry point, so it's okay to define it here.
KeyframeInfo = namedtuple('KeyframeInfo', ['pts', 'pos', 'size', 'timescale'])


class ScreenshotService:
    """
    The main orchestrator for the screenshot service.
    It manages a task queue and workers, and uses the TorrentClient,
    VideoFile, and ScreenshotGenerator classes to perform the work.
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
        """Starts the service, including the torrent client and workers."""
        self.log.info("Starting screenshot service...")
        self._running = True
        await self.client.start()
        for _ in range(self.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info(f"Screenshot service started with {self.num_workers} workers.")

    def stop(self):
        """Stops the service, including the torrent client and workers."""
        self.log.info("Stopping screenshot service...")
        self._running = False
        self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("Screenshot service stopped.")

    async def submit_task(self, infohash: str):
        """Submits a new screenshot task by its infohash."""
        await self.task_queue.put({'infohash': infohash})
        self.log.info(f"Submitted new task for infohash: {infohash}")

    async def _handle_screenshot_task(self, task_info: dict):
        """The core orchestration logic for a single screenshot task."""
        infohash_hex = task_info['infohash']
        self.log.info(f"Processing task: {infohash_hex}")
        handle = None
        try:
            # 1. Get torrent handle from the client
            handle = await self.client.add_torrent(infohash_hex)
            if not handle or not handle.is_valid():
                self.log.error(f"Could not get a valid torrent handle for {infohash_hex}.")
                return

            # 2. Use VideoFile to get keyframe metadata
            video_file = VideoFile(self.client, handle)
            if video_file.file_index == -1:
                self.log.warning(f"No video file found in torrent {infohash_hex}.")
                return

            keyframe_infos, moov_data = await video_file.get_keyframes_and_moov()
            if not keyframe_infos or not moov_data:
                self.log.error(f"Could not extract keyframes or moov_data for {infohash_hex}.")
                return

            # 3. For each keyframe, download its data and generate a screenshot
            decode_tasks = []
            for keyframe_info in keyframe_infos:
                # a. Download the specific frame's data
                keyframe_data = await video_file.download_keyframe_data(keyframe_info)
                if not keyframe_data:
                    self.log.warning(f"Skipping frame (PTS: {keyframe_info.pts}) due to download failure.")
                    continue

                # b. Schedule the screenshot generation
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
            self.log.info(f"Screenshot task for {infohash_hex} completed.")

        except Exception:
            self.log.exception(f"An unknown error occurred while processing {infohash_hex}.")
        finally:
            if handle:
                self.client.remove_torrent(handle)

    async def _worker(self):
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作者发生错误。")
