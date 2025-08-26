# -*- coding: utf-8 -*-
import asyncio
import logging
from collections import namedtuple

from .client import TorrentClient
from .video import VideoFile
from .generator import ScreenshotGenerator

# This is defined here because the service is the main entry point,
# but it's used by the VideoFile to create the info objects.
KeyframeInfo = namedtuple('KeyframeInfo', ['pts', 'pos', 'size', 'timescale'])

class ScreenshotService:
    """
    Orchestrates the screenshot generation process.
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

        # 1. Get decoder configuration (SPS/PPS for H.264)
        sps, pps = await video_file.get_decoder_config()
        if not sps or not pps:
            self.log.error(f"Could not extract SPS/PPS from {infohash_hex}. Assuming not H.264 or file is corrupt.")
            return

        # 2. Get the list of keyframes to screenshot
        keyframes = await video_file.get_keyframes()
        if not keyframes:
            self.log.error(f"Could not extract keyframes from {infohash_hex}.")
            return

        self.log.info(f"Found {len(keyframes)} keyframes to process for {infohash_hex}.")

        decode_tasks = []
        for keyframe_info in keyframes:
            # 3. Download the data for each keyframe
            keyframe_data = await video_file.download_keyframe_data(keyframe_info)
            if not keyframe_data:
                self.log.warning(f"Skipping frame (PTS: {keyframe_info.pts}) due to download failure.")
                continue

            # 4. Calculate timestamp string for the filename
            ts_sec = keyframe_info.pts / keyframe_info.timescale
            m, s = divmod(ts_sec, 60)
            h, m = divmod(m, 60)
            timestamp_str = f"{int(h):02d}-{int(m):02d}-{int(s):02d}"

            # 5. Call the generator with the required data
            task = self.generator.generate(
                sps=sps,
                pps=pps,
                keyframe_data=keyframe_data,
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
