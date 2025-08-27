# -*- coding: utf-8 -*-
import asyncio
import logging
from collections import namedtuple

from .client import TorrentClient
from .video import VideoFile
from .generator import ScreenshotGenerator


class ScreenshotService:
    """
    Orchestrates the screenshot generation process.
    """
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output', torrent_save_path='/dev/shm'):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False
        self.client = TorrentClient(loop=self.loop, save_path=torrent_save_path)
        self.generator = ScreenshotGenerator(loop=self.loop, output_dir=self.output_dir)

    async def run(self):
        """Starts the service, including the torrent client and workers."""
        self.log.info("Starting ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info(f"ScreenshotService started with {self.num_workers} workers.")

    def stop(self):
        """Stops the service, including the torrent client and workers."""
        self.log.info("Stopping ScreenshotService...")
        self._running = False
        self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService stopped.")

    async def submit_task(self, infohash: str):
        """Submits a new screenshot task by infohash."""
        await self.task_queue.put({'infohash': infohash})
        self.log.info(f"Submitted new task for infohash: {infohash}")

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex):
        """Handles the detailed logic of generating screenshots for a given torrent."""
        video_file = VideoFile(self.client, handle)
        if video_file.file_index == -1:
            self.log.warning(f"No video file found in torrent {infohash_hex}.")
            return

        # 1. Get the list of keyframes to screenshot
        keyframes = await video_file.get_keyframes()
        if not keyframes:
            self.log.error(f"Could not extract keyframes from {infohash_hex}.")
            return

        self.log.info(f"Found {len(keyframes)} keyframes to process for {infohash_hex}.")

        decode_tasks = []
        for keyframe in keyframes:
            # 2. Download the data for each keyframe
            packet_info = await video_file.download_keyframe_data(keyframe.index)
            if not packet_info:
                self.log.warning(f"Skipping frame (PTS: {keyframe.pts}) due to download failure.")
                continue

            extradata, packet_data = packet_info

            # 3. Calculate timestamp string for the filename
            if keyframe.timescale > 0:
                ts_sec = keyframe.pts / keyframe.timescale
                m, s = divmod(ts_sec, 60)
                h, m = divmod(m, 60)
                timestamp_str = f"{int(h):02d}-{int(m):02d}-{int(s):02d}"
            else:
                timestamp_str = f"keyframe_{keyframe.index}"

            # 4. Call the generator with the required data
            task = self.generator.generate(
                extradata=extradata,
                packet_data=packet_data,
                infohash_hex=infohash_hex,
                timestamp_str=timestamp_str
            )
            decode_tasks.append(task)

        await asyncio.gather(*decode_tasks)
        self.log.info(f"Screenshot task for {infohash_hex} completed.")

    async def _handle_screenshot_task(self, task_info: dict):
        """Full lifecycle of a screenshot task, including torrent handle management."""
        infohash_hex = task_info['infohash']
        self.log.info(f"Processing task: {infohash_hex}")
        handle = None
        try:
            handle = await self.client.add_torrent(infohash_hex)
            if not handle or not handle.is_valid():
                self.log.error(f"Could not get a valid torrent handle for {infohash_hex}.")
                return

            await self._generate_screenshots_from_torrent(handle, infohash_hex)
        except Exception:
            self.log.exception(f"An unknown error occurred while processing {infohash_hex}.")
        finally:
            if handle:
                self.client.remove_torrent(handle)

    async def _worker(self):
        """The worker coro that pulls tasks from the queue."""
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("Error in screenshot worker.")
