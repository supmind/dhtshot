# -*- coding: utf-8 -*-
import asyncio
import logging
import io
import struct
from typing import Generator, Tuple

import av

from .client import TorrentClient, TorrentClientError
from .errors import (
    TaskError,
    NoVideoFileError,
    MoovNotFoundError,
    MoovParsingError,
    FrameDownloadTimeoutError,
    FrameDecodeError
)
from .extractor import H264KeyframeExtractor, Keyframe, SampleInfo
from .generator import ScreenshotGenerator


class ScreenshotService:
    """
    Orchestrates the screenshot generation process.
    """
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output', torrent_save_path='/dev/shm', client=None):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False
        self.client = client or TorrentClient(loop=self.loop, save_path=torrent_save_path)
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

    async def submit_task(self, infohash: str, resume_data: dict = None):
        """
        Submits a new screenshot task.

        :param infohash: The infohash of the torrent.
        :param resume_data: Optional dictionary with data to resume a previous task.
        """
        await self.task_queue.put({'infohash': infohash, 'resume_data': resume_data})
        if resume_data:
            self.log.info(f"Resubmitted task for infohash: {infohash}")
        else:
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

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length, infohash_hex) -> bytes:
        """
        Smartly finds and fetches the moov atom data.
        It first probes the head of the file, then the tail.
        Raises MoovNotFoundError if it cannot be found.
        """
        self.log.info("Smartly searching for moov atom...")
        mdat_size_from_head = 0

        # --- Head Probe ---
        try:
            self.log.info("Phase 1: Probing file head for moov atom...")
            initial_probe_size = 256 * 1024
            head_size = min(initial_probe_size, video_file_size)
            head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
            head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=120)
            head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)

            stream = io.BytesIO(head_data)
            for box_type, partial_box_data, box_offset, box_size in self._parse_mp4_boxes(stream):
                self.log.info(f"Found box '{box_type}' at offset {box_offset} with size {box_size} in head.")
                if box_type == 'moov':
                    self.log.info("Found 'moov' atom in head probe.")
                    if len(partial_box_data) < box_size:
                        self.log.info(f"Probe got partial moov ({len(partial_box_data)}/{box_size} bytes). Fetching full.")
                        full_moov_offset_in_torrent = video_file_offset + box_offset
                        needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, box_size, piece_length)
                        moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=120)
                        return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, box_size, piece_length)
                    else:
                        return partial_box_data
                if box_type == 'mdat' and box_size > video_file_size * 0.8:
                    self.log.info("Found large 'mdat' box in head. Assuming 'moov' is at the tail.")
                    mdat_size_from_head = box_size
                    break
        except TorrentClientError as e:
            self.log.error(f"A torrent client error occurred during head probe: {e}")
            # This could be a dead torrent, reraise as a clear error.
            raise MoovNotFoundError("Torrent client error during moov search, torrent may be dead.", infohash_hex) from e

        # --- Tail Probe (Fallback) ---
        try:
            self.log.info("Phase 2: Moov not in head, probing file tail.")
            tail_probe_size = (video_file_size - mdat_size_from_head) if mdat_size_from_head > 0 else (10 * 1024 * 1024)
            tail_file_offset = max(0, video_file_size - tail_probe_size)
            tail_torrent_offset = video_file_offset + tail_file_offset
            tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
            tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)
            tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=180)
            tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)

            search_pos = len(tail_data)
            while search_pos > 4:
                found_pos = tail_data.rfind(b'moov', 0, search_pos)
                if found_pos == -1: break
                potential_start_pos = found_pos - 4
                if potential_start_pos < 0:
                    search_pos = found_pos
                    continue
                stream = io.BytesIO(tail_data)
                stream.seek(potential_start_pos)
                box_type, full_box_data, _, _ = next(self._parse_mp4_boxes(stream), (None, None, None, None))
                if box_type == 'moov':
                    self.log.info(f"Successfully parsed 'moov' atom from tail with size {len(full_box_data)}.")
                    return full_box_data
                search_pos = found_pos
        except TorrentClientError as e:
            self.log.error(f"A torrent client error occurred during tail probe: {e}")
            raise MoovNotFoundError("Torrent client error during moov search, torrent may be dead.", infohash_hex) from e

        raise MoovNotFoundError("Could not locate 'moov' atom in the first/last part of the file.", infohash_hex)

    async def _process_and_generate_screenshot(self, keyframe, extractor, handle, video_file_offset, piece_length, infohash_hex):
        """Helper to process a single keyframe once its pieces are downloaded."""
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

        try:
            await self.generator.generate(
                extradata=extractor.extradata,
                packet_data=packet_data,
                infohash_hex=infohash_hex,
                timestamp_str=timestamp_str
            )
        except (av.error.InvalidDataError, av.error.DecodeError) as e:
            # Catch specific decoding errors from PyAV
            raise FrameDecodeError(f"Failed to decode keyframe {keyframe.index} at {timestamp_str}: {e}", infohash_hex) from e
        except Exception as e:
            # Catch any other unexpected error during generation
            raise FrameDecodeError(f"An unexpected error occurred while generating screenshot for keyframe {keyframe.index}: {e}", infohash_hex) from e

    def _find_video_file(self, ti: "lt.torrent_info") -> Tuple[int, int, int]:
        """Finds the largest .mp4 file in a torrent and returns its info."""
        video_file_index, video_file_size, video_file_offset = -1, -1, -1
        fs = ti.files()
        for i in range(fs.num_files()):
            if fs.file_path(i).lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)
        return video_file_index, video_file_size, video_file_offset

    def _select_keyframes(self, all_keyframes: list, timescale: int, samples: list) -> list:
        """Selects a representative subset of keyframes to screenshot."""
        if not all_keyframes:
            return []

        MIN_SCREENSHOTS, MAX_SCREENSHOTS, TARGET_INTERVAL_SEC = 5, 50, 180
        duration_sec = samples[-1].pts / timescale if timescale > 0 else 0
        num_screenshots = max(MIN_SCREENSHOTS, min(int(duration_sec / TARGET_INTERVAL_SEC), MAX_SCREENSHOTS)) if duration_sec > 0 else 20

        if len(all_keyframes) <= num_screenshots:
            return all_keyframes
        else:
            indices = [int(i * len(all_keyframes) / num_screenshots) for i in range(num_screenshots)]
            return [all_keyframes[i] for i in sorted(list(set(indices)))]

    def _serialize_task_state(self, state: dict) -> dict:
        """Converts a live task state object into a JSON-serializable dictionary."""
        extractor = state['extractor']
        if not extractor: return None

        return {
            "infohash": state['infohash'],
            "piece_length": state['piece_length'],
            "video_file_offset": state['video_file_offset'],
            "video_file_size": state['video_file_size'],
            "extractor_info": {
                "extradata": extractor.extradata,
                "mode": extractor.mode,
                "nal_length_size": extractor.nal_length_size,
                "timescale": extractor.timescale,
                # Full sample map is needed to reconstruct extractor state
                "samples": [s._asdict() for s in extractor.samples],
            },
            "all_keyframes": [k._asdict() for k in state['all_keyframes']],
            "selected_keyframes": [k._asdict() for k in state['selected_keyframes']],
            "completed_pieces": list(state['completed_pieces']),
            "processed_keyframes": list(state['processed_keyframes']),
        }

    def _load_state_from_resume_data(self, data: dict) -> dict:
        """Restores a task state from a resume_data dictionary."""
        # Re-create extractor without moov data, then patch it
        extractor = H264KeyframeExtractor(moov_data=None)
        ext_info = data['extractor_info']
        extractor.extradata = ext_info['extradata']
        extractor.mode = ext_info['mode']
        extractor.nal_length_size = ext_info['nal_length_size']
        extractor.timescale = ext_info['timescale']
        extractor.samples = [SampleInfo(**s) for s in ext_info['samples']]

        all_keyframes = [Keyframe(**k) for k in data['all_keyframes']]
        extractor.keyframes = all_keyframes # The extractor needs the full list

        return {
            "infohash": data['infohash'],
            "piece_length": data['piece_length'],
            "video_file_offset": data['video_file_offset'],
            "video_file_size": data['video_file_size'],
            "extractor": extractor,
            "all_keyframes": all_keyframes,
            "selected_keyframes": [Keyframe(**k) for k in data['selected_keyframes']],
            "completed_pieces": set(data['completed_pieces']),
            "processed_keyframes": set(data['processed_keyframes']),
        }

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex, resume_data=None):
        """Handles the detailed logic of generating screenshots for a given torrent."""
        task_state = {}

        if resume_data:
            self.log.info(f"[{infohash_hex}] Resuming task from provided data.")
            try:
                task_state = self._load_state_from_resume_data(resume_data)
            except (KeyError, TypeError) as e:
                self.log.error(f"[{infohash_hex}] Failed to load resume data, starting fresh. Error: {e}")
                resume_data = None # Fallback to fresh start

        if not resume_data:
            self.log.info(f"[{infohash_hex}] Starting new screenshot task analysis.")
            ti = handle.get_torrent_info()
            piece_length = ti.piece_length()

            video_file_index, video_file_size, video_file_offset = self._find_video_file(ti)
            if video_file_index == -1:
                raise NoVideoFileError("No .mp4 file found in torrent.", infohash_hex)

            video_file_path = ti.files().file_path(video_file_index)
            self.log.info(f"[{infohash_hex}] Found video file: '{video_file_path}' ({video_file_size} bytes).")

            moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length, infohash_hex)

            try:
                extractor = H264KeyframeExtractor(moov_data)
                if not extractor.keyframes:
                    raise MoovParsingError("Could not extract any keyframes from the moov atom.", infohash_hex)
            except Exception as e:
                raise MoovParsingError(f"Failed to parse moov data: {e}", infohash_hex) from e

            all_keyframes = extractor.keyframes
            selected_keyframes = self._select_keyframes(all_keyframes, extractor.timescale, extractor.samples)
            self.log.info(f"[{infohash_hex}] Selected {len(selected_keyframes)} of {len(all_keyframes)} total keyframes.")

            task_state = {
                "infohash": infohash_hex, "piece_length": piece_length,
                "video_file_offset": video_file_offset, "video_file_size": video_file_size,
                "extractor": extractor, "all_keyframes": all_keyframes,
                "selected_keyframes": selected_keyframes, "completed_pieces": set(),
                "processed_keyframes": set()
            }

        # --- COMBINED LOGIC ---
        extractor = task_state['extractor']
        selected_keyframes = task_state['selected_keyframes']
        video_file_offset = task_state['video_file_offset']
        piece_length = task_state['piece_length']

        keyframe_info, piece_to_keyframes, all_needed_pieces = {}, {}, set()
        remaining_keyframes = [kf for kf in selected_keyframes if kf.index not in task_state['processed_keyframes']]

        self.log.info(f"[{infohash_hex}] Need to process {len(remaining_keyframes)} keyframes.")
        if not remaining_keyframes:
            self.log.info(f"[{infohash_hex}] All selected keyframes already processed.")
            return

        for kf in remaining_keyframes:
            sample = extractor.samples[kf.sample_index - 1]
            offset = video_file_offset + sample.offset
            needed = self._get_pieces_for_range(offset, sample.size, piece_length)
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes.setdefault(piece_idx, []).append(kf.index)
            all_needed_pieces.update(needed)

        pieces_to_request = list(all_needed_pieces - task_state['completed_pieces'])
        self.log.info(f"[{infohash_hex}] Requesting {len(pieces_to_request)} new pieces from torrent client.")
        self.client.request_pieces(handle, pieces_to_request)

        processed_this_run, generation_tasks = set(), []
        timeout = 300
        try:
            while len(processed_this_run) < len(remaining_keyframes):
                finished_piece = await asyncio.wait_for(self.client.finished_piece_queue.get(), timeout=timeout)
                task_state['completed_pieces'].add(finished_piece)

                if finished_piece not in piece_to_keyframes: continue

                for kf_index in piece_to_keyframes.pop(finished_piece):
                    info = keyframe_info.get(kf_index)
                    if not info or finished_piece not in info['needed_pieces']: continue

                    info['needed_pieces'].remove(finished_piece)
                    if not info['needed_pieces']:
                        self.log.info(f"[{infohash_hex}] All pieces for keyframe {kf_index} are ready. Spawning generation task.")
                        task = self.loop.create_task(self._process_and_generate_screenshot(
                            info['keyframe'], extractor, handle, video_file_offset, piece_length, infohash_hex
                        ))
                        generation_tasks.append(task)
                        processed_this_run.add(kf_index)
                        keyframe_info.pop(kf_index)

                self.client.finished_piece_queue.task_done()
        except asyncio.TimeoutError:
            self.log.warning(f"[{infohash_hex}] Timeout waiting for pieces.")
            task_state['processed_keyframes'].update(processed_this_run)
            current_resume_data = self._serialize_task_state(task_state)
            processed_count = len(task_state['processed_keyframes'])
            total_count = len(task_state['selected_keyframes'])

            raise FrameDownloadTimeoutError(
                f"Timeout waiting for pieces. Processed {processed_count}/{total_count} keyframes.",
                infohash_hex,
                resume_data=current_resume_data
            )

        results = await asyncio.gather(*generation_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                self.log.error(f"[{infohash_hex}] A frame processing task failed: {result}")
                raise result

        self.log.info(f"[{infohash_hex}] Screenshot task completed successfully.")

    async def _handle_screenshot_task(self, task_info: dict):
        """Full lifecycle of a screenshot task, including torrent handle management."""
        infohash_hex = task_info['infohash']
        resume_data = task_info.get('resume_data')

        log_message = f"Processing task: {infohash_hex}"
        if resume_data:
            log_message += " (resuming)"
        self.log.info(log_message)

        handle = None
        try:
            # Always add the torrent to the session
            handle = await self.client.add_torrent(infohash_hex)
            if not handle or not handle.is_valid():
                self.log.error(f"Could not get a valid torrent handle for {infohash_hex}.")
                return

            # Pass both handle and potential resume_data to the main logic
            await self._generate_screenshots_from_torrent(handle, infohash_hex, resume_data)

        except TaskError as e:
            # Catch our specific, structured errors and log them clearly.
            # Using .exception() will automatically add exception info to the log.
            if getattr(e, 'resume_data', None):
                self.log.exception(f"Task failed for {infohash_hex} with a resumable error: {e}. Resume data is available.")
            else:
                self.log.exception(f"Task failed for {infohash_hex} with a known, non-resumable error: {e}")
        except TorrentClientError as e:
            # Catch errors from the torrent client layer
            self.log.error(f"Task failed for {infohash_hex} due to a torrent client error: {e}")
        except Exception:
            # Catch any other unexpected errors.
            self.log.exception(f"An unexpected and severe error occurred while processing {infohash_hex}.")
        finally:
            if handle and handle.is_valid():
                await self.client.remove_torrent(handle)

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
