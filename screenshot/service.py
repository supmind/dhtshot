# -*- coding: utf-8 -*-
import asyncio
import logging
import io
import struct
from typing import Generator, Tuple
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

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        """A robust parser for MP4 boxes, yielding the box type, its full data, offset and size."""
        # Use getbuffer() to have a view of the entire byte array, which is efficient.
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)

        current_offset = stream.tell()
        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)

            try:
                header_data = stream.read(8)
                if not header_data or len(header_data) < 8:
                    break
                size, box_type_bytes = struct.unpack('>I4s', header_data)
                box_type = box_type_bytes.decode('ascii', 'ignore')
            except struct.error:
                self.log.warning(f"Could not unpack box header at offset {current_offset}. Incomplete data.")
                break

            original_size = size
            box_header_size = 8

            if size == 1:
                if current_offset + 16 > buffer_size:
                    self.log.warning(f"Box '{box_type}' has a 64-bit size but not enough data for the header.")
                    break
                size_64_data = stream.read(8)
                size = struct.unpack('>Q', size_64_data)[0]
                box_header_size = 16
            elif size == 0:
                size = buffer_size - current_offset

            if size < box_header_size:
                self.log.warning(f"Box '{box_type}' has an invalid size {size}. Stopping parse.")
                break

            # Check if the full box is available in the buffer.
            if current_offset + size > buffer_size:
                self.log.warning(f"Box '{box_type}' with size {size} extends beyond the available data. Cannot parse fully.")
                # We can still yield the partial data we have
                full_box_data = stream_buffer[current_offset:buffer_size]
                yield box_type, bytes(full_box_data), current_offset, size
                break

            full_box_data = stream_buffer[current_offset:current_offset + size]
            yield box_type, bytes(full_box_data), current_offset, size

            # Move to the next box.
            current_offset += size

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length) -> bytes | None:
        """
        Smartly finds and fetches the moov atom data.
        It first probes the head of the file, parsing boxes to find the moov atom precisely.
        If not found, it falls back to probing the tail.
        """
        self.log.info("Smartly searching for moov atom...")
        mdat_size_from_head = 0

        # --- Head Probe ---
        self.log.info("Phase 1: Probing file head for moov atom...")
        initial_probe_size = 256 * 1024  # 256KB
        head_size = min(initial_probe_size, video_file_size)
        head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)

        try:
            head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=120)
            head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)

            stream = io.BytesIO(head_data)
            for box_type, partial_box_data, box_offset, box_size in self._parse_mp4_boxes(stream):
                self.log.info(f"Found box '{box_type}' at offset {box_offset} with size {box_size} in head.")
                if box_type == 'moov':
                    self.log.info("Found 'moov' atom in head probe.")
                    if len(partial_box_data) < box_size:
                        self.log.info(f"Initial probe got {len(partial_box_data)}/{box_size} bytes of moov. Fetching remainder.")
                        full_moov_offset_in_torrent = video_file_offset + box_offset
                        needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, box_size, piece_length)

                        moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=120)
                        return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, box_size, piece_length)
                    else:
                        return partial_box_data

                if box_type == 'mdat':
                    if box_size > video_file_size * 0.8:
                         self.log.info("Found large 'mdat' box in head. Assuming 'moov' is at the tail.")
                         mdat_size_from_head = box_size
                         break
        except Exception as e:
            self.log.error(f"Error during head probe for moov atom: {e}", exc_info=True)

        # --- Tail Probe (Fallback) ---
        self.log.info("Phase 2: Moov not in head, probing file tail.")

        if mdat_size_from_head > 0:
            tail_probe_size = video_file_size - mdat_size_from_head
            self.log.info(f"Using dynamic tail probe size based on mdat: {tail_probe_size} bytes.")
        else:
            tail_probe_size = 10 * 1024 * 1024
            self.log.info(f"Using fixed tail probe size: {tail_probe_size} bytes.")

        tail_file_offset = max(0, video_file_size - tail_probe_size)
        tail_torrent_offset = video_file_offset + tail_file_offset
        tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
        tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)

        try:
            tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=180)
            tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)

            search_pos = len(tail_data)
            while search_pos > 4:
                found_pos = tail_data.rfind(b'moov', 0, search_pos)
                if found_pos == -1:
                    break
                potential_start_pos = found_pos - 4
                if potential_start_pos < 0:
                    search_pos = found_pos
                    continue
                try:
                    stream = io.BytesIO(tail_data)
                    stream.seek(potential_start_pos)
                    box_type, full_box_data, _, _ = next(self._parse_mp4_boxes(stream), (None, None, None, None))

                    if box_type == 'moov':
                        self.log.info(f"Successfully parsed 'moov' atom from tail with size {len(full_box_data)}.")
                        return full_box_data
                except Exception:
                    pass
                search_pos = found_pos
        except Exception as e:
            self.log.error(f"Error during tail probe for moov atom: {e}", exc_info=True)

        return None

    async def _process_and_generate_screenshot(self, keyframe, extractor, handle, video_file_offset, piece_length, infohash_hex):
        """Helper to process a single keyframe once its pieces are downloaded."""
        try:
            sample = extractor.samples[keyframe.sample_index - 1]
            keyframe_torrent_offset = video_file_offset + sample.offset
            keyframe_piece_indices = self._get_pieces_for_range(keyframe_torrent_offset, sample.size, piece_length)

            # Fetch pieces (should be fast as they are already downloaded)
            keyframe_pieces_data = await self.client.fetch_pieces(handle, keyframe_piece_indices, timeout=60)
            packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, keyframe_torrent_offset, sample.size, piece_length)

            if len(packet_data_bytes) != sample.size:
                self.log.warning(f"Incomplete data for keyframe {keyframe.index}, skipping.")
                return

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

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex):
        """Handles the detailed logic of generating screenshots for a given torrent."""
        ti = handle.get_torrent_info()
        piece_length = ti.piece_length()

        # 1. Find the largest video file in the torrent
        video_file_index, video_file_size, video_file_offset = -1, -1, -1
        fs = ti.files()
        for i in range(fs.num_files()):
            if fs.file_path(i).lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)

        if video_file_index == -1:
            self.log.warning(f"No video file found in torrent {infohash_hex}.")
            return

        video_file_path = fs.file_path(video_file_index)
        self.log.info(f"Found largest video file: '{video_file_path}' with size {video_file_size} bytes.")

        # 2. Phase 1: Smartly find and fetch the moov atom
        moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length)

        if not moov_data:
            self.log.error(f"Could not find or parse moov atom for {infohash_hex}.")
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

        # 4. Phase 2: Setup for event-driven screenshot generation
        keyframe_info = {}
        piece_to_keyframes = {}
        all_needed_pieces = set()

        for kf in selected_keyframes:
            sample = extractor.samples[kf.sample_index - 1]
            keyframe_torrent_offset = video_file_offset + sample.offset
            needed = self._get_pieces_for_range(keyframe_torrent_offset, sample.size, piece_length)

            keyframe_info[kf.index] = {
                'keyframe': kf,
                'needed_pieces': set(needed)
            }
            for piece_idx in needed:
                if piece_idx not in piece_to_keyframes:
                    piece_to_keyframes[piece_idx] = []
                piece_to_keyframes[piece_idx].append(kf.index)
            all_needed_pieces.update(needed)

        self.client.request_pieces(handle, list(all_needed_pieces))

        # 5. Phase 3: Consume finished pieces and trigger screenshot generation
        processed_keyframes = 0
        generation_tasks = []
        timeout = 300 # 5 minutes timeout for the whole process
        try:
            while processed_keyframes < len(selected_keyframes):
                finished_piece = await asyncio.wait_for(self.client.finished_piece_queue.get(), timeout=timeout)

                if finished_piece not in piece_to_keyframes:
                    continue

                # Using .pop() is safe because we won't get the same piece index again from the queue
                affected_kf_indices = piece_to_keyframes.pop(finished_piece)
                for kf_index in affected_kf_indices:
                    info = keyframe_info.get(kf_index)
                    if not info: continue # Already processed

                    if finished_piece in info['needed_pieces']:
                        info['needed_pieces'].remove(finished_piece)

                    if not info['needed_pieces']:
                        # All pieces for this keyframe are ready
                        self.log.info(f"All pieces for keyframe {kf_index} are ready. Spawning generation task.")
                        kf = info['keyframe']
                        task = asyncio.create_task(self._process_and_generate_screenshot(
                            kf, extractor, handle, video_file_offset, piece_length, infohash_hex
                        ))
                        generation_tasks.append(task)
                        processed_keyframes += 1
                        # Remove from dict to prevent reprocessing
                        keyframe_info.pop(kf_index)

                self.client.finished_piece_queue.task_done()
        except asyncio.TimeoutError:
            self.log.error(f"Timeout waiting for pieces for {infohash_hex}. Processed {processed_keyframes}/{len(selected_keyframes)} keyframes.")

        # Wait for all spawned screenshot tasks to complete before returning
        if generation_tasks:
            self.log.info(f"Waiting for {len(generation_tasks)} screenshot generation tasks to complete.")
            await asyncio.gather(*generation_tasks)

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
