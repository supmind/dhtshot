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

    def _get_pieces_for_range(self, offset_in_torrent, size, piece_length):
        """Calculates piece indices for a given byte range in the torrent."""
        if size <= 0: return []
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data, offset_in_torrent, size, piece_length):
        """Assembles a byte range from a dictionary of piece data."""
        buffer = bytearray(size)
        buffer_offset = 0

        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length

        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data: continue

            piece_data = pieces_data[piece_index]

            # Determine the slice of the piece to use
            copy_from_start = 0
            if piece_index == start_piece:
                copy_from_start = offset_in_torrent % piece_length

            copy_to_end = piece_length
            if piece_index == end_piece:
                copy_to_end = (offset_in_torrent + size - 1) % piece_length + 1

            chunk = piece_data[copy_from_start:copy_to_end]

            # Determine where to place the chunk in the buffer
            bytes_to_copy = len(chunk)
            if (buffer_offset + bytes_to_copy) > size:
                bytes_to_copy = size - buffer_offset

            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy

        return bytes(buffer)

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex):
        """Handles the detailed logic of generating screenshots for a given torrent."""
        ti = handle.get_torrent_info()
        piece_length = ti.piece_length()

        # 1. Find the largest video file in the torrent
        video_file_index, video_file_size, video_file_offset = -1, -1, -1
        fs = ti.files()
        for i in range(fs.num_files()):
            if fs.file_path(i).lower().endswith(('.mp4', '.mkv', '.avi')) and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)

        if video_file_index == -1:
            self.log.warning(f"No video file found in torrent {infohash_hex}.")
            return

        # 2. Phase 1: Fetch pieces for moov atom
        self.log.info("Phase 1: Fetching pieces for moov atom.")
        PROBE_SIZE = 2 * 1024 * 1024

        head_offset = video_file_offset
        head_size = min(PROBE_SIZE, video_file_size)
        head_pieces = self._get_pieces_for_range(head_offset, head_size, piece_length)

        tail_file_offset = max(0, video_file_size - PROBE_SIZE)
        tail_torrent_offset = video_file_offset + tail_file_offset
        tail_size = min(PROBE_SIZE, video_file_size - tail_file_offset)
        tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)

        moov_piece_indices = list(set(head_pieces + tail_pieces))

        try:
            moov_pieces_data = await self.client.fetch_pieces(handle, moov_piece_indices)
        except Exception as e:
            self.log.error(f"Failed to fetch pieces for moov atom: {e}")
            return

        head_data = self._assemble_data_from_pieces(moov_pieces_data, head_offset, head_size, piece_length)
        tail_data = self._assemble_data_from_pieces(moov_pieces_data, tail_torrent_offset, tail_size, piece_length)

        # Find moov atom in the assembled data
        moov_data = None
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
                keyframe_torrent_offset = video_file_offset + sample.offset
                keyframe_piece_indices = self._get_pieces_for_range(keyframe_torrent_offset, sample.size, piece_length)

                keyframe_pieces_data = await self.client.fetch_pieces(handle, keyframe_piece_indices)
                packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, keyframe_torrent_offset, sample.size, piece_length)

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
