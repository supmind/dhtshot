# -*- coding: utf-8 -*-
import asyncio
import logging
import io
import struct
from .client import TorrentClient
from .extractor import H264KeyframeExtractor, Keyframe
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
        ti = handle.get_torrent_info()

        # 1. Find the largest video file in the torrent
        video_file_index, video_file_size = -1, -1
        for i in range(ti.num_files()):
            file_entry = ti.file_at(i)
            if file_entry.path.lower().endswith(('.mp4', '.mkv', '.avi')) and file_entry.size > video_file_size:
                video_file_size = file_entry.size
                video_file_index = i

        if video_file_index == -1:
            self.log.warning(f"No video file found in torrent {infohash_hex}.")
            return

        # 2. Phase 1: Fetch pieces for moov atom
        self.log.info("Phase 1: Fetching pieces for moov atom.")
        PROBE_SIZE = 2 * 1024 * 1024

        # Calculate piece indices for head and tail
        head_slices = ti.map_file(video_file_index, 0, min(PROBE_SIZE, video_file_size))
        tail_offset = max(0, video_file_size - PROBE_SIZE)
        tail_slices = ti.map_file(video_file_index, tail_offset, min(PROBE_SIZE, video_file_size - tail_offset))

        moov_piece_indices = list(set([s.piece_index for s in head_slices] + [s.piece_index for s in tail_slices]))

        try:
            moov_pieces_data = await self.client.fetch_pieces(handle, moov_piece_indices)
        except Exception as e:
            self.log.error(f"Failed to fetch pieces for moov atom: {e}")
            return

        # Assemble head and tail data from pieces
        def assemble_data(slices, pieces_data):
            buffer = bytearray()
            for s in slices:
                if s.piece_index in pieces_data:
                    piece_data = pieces_data[s.piece_index]
                    buffer.extend(piece_data[s.start:s.start + s.size])
            return bytes(buffer)

        head_data = assemble_data(head_slices, moov_pieces_data)
        tail_data = assemble_data(tail_slices, moov_pieces_data)

        # Find moov atom in the assembled data
        moov_data = None
        # Simplified moov search, assuming it's in the tail first, then head
        for chunk in [tail_data, head_data]:
            pos = len(chunk)
            while pos >= 8:
                found_pos = chunk.rfind(b'moov', 0, pos)
                if found_pos == -1: break
                size_pos = found_pos - 4
                if size_pos < 0:
                    pos = found_pos
                    continue
                try:
                    size = struct.unpack('>I', chunk[size_pos:found_pos])[0]
                    if size >= 8 and (size_pos + size) <= len(chunk):
                        moov_data = chunk[size_pos : size_pos + size]
                        break
                except struct.error: pass
                pos = found_pos
            if moov_data: break

        if not moov_data:
            self.log.error(f"Could not find moov atom for {infohash_hex}.")
            return

        # 3. Initialize extractor and select keyframes
        extractor = H264KeyframeExtractor(moov_data)
        all_keyframes = extractor.keyframes
        if not all_keyframes:
            self.log.error(f"Could not extract keyframes from {infohash_hex}.")
            return

        # Frame selection logic (simplified from VideoFile)
        MIN_SCREENSHOTS, MAX_SCREENSHOTS, TARGET_INTERVAL_SEC = 5, 50, 180
        duration_sec = extractor.samples[-1].pts / extractor.timescale if extractor.timescale > 0 else 0
        num_screenshots = max(MIN_SCREENSHOTS, min(int(duration_sec / TARGET_INTERVAL_SEC), MAX_SCREENSHOTS)) if duration_sec > 0 else 20

        if len(all_keyframes) <= num_screenshots:
            selected_keyframes = all_keyframes
        else:
            indices = [int(i * len(all_keyframes) / num_screenshots) for i in range(num_screenshots)]
            selected_keyframes = [all_keyframes[i] for i in sorted(list(set(indices)))]

        self.log.info(f"Found {len(selected_keyframes)} keyframes to process for {infohash_hex}.")

        # 4. Phase 2: Fetch pieces for each keyframe and generate screenshot
        for keyframe in selected_keyframes:
            try:
                sample = extractor.samples[keyframe.sample_index - 1]
                keyframe_slices = ti.map_file(video_file_index, sample.offset, sample.size)
                keyframe_piece_indices = list(set([s.piece_index for s in keyframe_slices]))

                keyframe_pieces_data = await self.client.fetch_pieces(handle, keyframe_piece_indices)

                packet_data_bytes = assemble_data(keyframe_slices, keyframe_pieces_data)

                if len(packet_data_bytes) != sample.size:
                    self.log.warning(f"Incomplete data for keyframe {keyframe.index}, skipping.")
                    continue

                if extractor.mode == 'avc1':
                    annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
                    while cursor < len(packet_data_bytes):
                        nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + extractor.nal_length_size], 'big')
                        cursor += extractor.nal_length_size
                        nal_data = packet_data_bytes[cursor : cursor + nal_length]
                        annexb_data.extend(start_code + nal_data)
                        cursor += nal_length
                    packet_data = bytes(annexb_data)
                else:
                    packet_data = packet_data_bytes

                ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
                m, s = divmod(ts_sec, 60)
                h, m = divmod(m, 60)
                timestamp_str = f"{int(h):02d}-{int(m):02d}-{int(s):02d}"

                await self.generator.generate(
                    extradata=extractor.extradata,
                    packet_data=packet_data,
                    infohash_hex=infohash_hex,
                    timestamp_str=timestamp_str
                )
            except Exception as e:
                self.log.error(f"Failed to process keyframe {keyframe.index}: {e}", exc_info=True)

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
