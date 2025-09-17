# -*- coding: utf-8 -*-
"""
This module defines the VideoProcessor, which is responsible for processing a
single torrent to generate screenshots.
"""
import asyncio
import logging
import io
import struct
from typing import Generator, Tuple, Optional, Callable, Awaitable, Any
from collections import defaultdict

import av

from .client import TorrentClient, TorrentClientError
from .errors import (
    NoVideoFileError, MoovNotFoundError, MoovFetchError,
    MoovParsingError, FrameDownloadTimeoutError, MP4ParsingError
)
from .extractor import KeyframeExtractor, Keyframe
from .generator import ScreenshotGenerator
from config import Settings


class VideoProcessor:
    """
    Handles the detailed processing of a single torrent to generate screenshots.
    """
    def __init__(
        self,
        settings: Settings,
        client: TorrentClient,
        generator: ScreenshotGenerator,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        details_callback: Optional[Callable] = None,
        screenshot_check_callback: Optional[Callable] = None,
    ):
        self.settings = settings
        self.client = client
        self.generator = generator
        self.loop = loop or asyncio.get_event_loop()
        self.details_callback = details_callback
        self.screenshot_check_callback = screenshot_check_callback
        self.log = logging.getLogger("VideoProcessor")

    async def process(self, handle, infohash_hex: str):
        """
        Main processing method for a torrent.
        This was originally _generate_screenshots_from_torrent in ScreenshotService.
        """
        ti = handle.get_torrent_info()
        piece_length = ti.piece_length()
        video_file_index, video_file_size, video_file_offset, video_filename = self._find_video_file(ti)
        if video_file_index == -1:
            raise NoVideoFileError("在 torrent 中没有找到 .mp4 文件。", infohash_hex)

        moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length, infohash_hex)

        try:
            extractor = KeyframeExtractor(moov_data)
            if not extractor.keyframes:
                raise MoovParsingError("无法从 moov atom 中提取任何关键帧。", infohash_hex)
        except (MP4ParsingError, Exception) as e:
            raise MoovParsingError(f"解析 moov 数据时失败: {e}", infohash_hex) from e

        if self.details_callback:
            duration_sec = extractor.duration_pts / extractor.timescale if extractor.timescale > 0 else 0
            details = {"torrent_name": ti.name(), "video_filename": video_filename, "video_duration_seconds": int(duration_sec)}
            await self.details_callback(infohash_hex, details)

        all_keyframes = extractor.keyframes
        selected_keyframes = self._select_keyframes(all_keyframes, extractor.timescale, extractor.duration_pts, extractor.samples)

        if self.screenshot_check_callback and selected_keyframes:
            existing_filenames = await self.screenshot_check_callback(infohash=infohash_hex)
            if existing_filenames:
                existing_timestamps = {fn.split('_')[-1].split('.')[0] for fn in existing_filenames}
                original_count = len(selected_keyframes)
                selected_keyframes = [kf for kf in selected_keyframes if str(int(kf.pts / kf.timescale)) not in existing_timestamps]
                if (filtered_count := original_count - len(selected_keyframes)) > 0:
                    self.log.info("[%s] 发现了 %d 个已存在的截图。将跳过它们，仅处理剩余的 %d 个。", infohash_hex, filtered_count, len(selected_keyframes))

        if not selected_keyframes:
            self.log.info("[%s] 没有需要生成的截图，任务完成。", infohash_hex)
            return

        task_state = {"infohash": infohash_hex, "piece_length": piece_length, "video_file_offset": video_file_offset, "video_file_size": video_file_size, "extractor": extractor}
        keyframe_info, piece_to_keyframes, all_needed_pieces = {}, defaultdict(list), set()
        for kf in selected_keyframes:
            sample = task_state['extractor'].samples[kf.sample_index - 1]
            offset = task_state['video_file_offset'] + sample.offset
            needed = self._get_pieces_for_range(offset, sample.size, task_state['piece_length'])
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes[piece_idx].append(kf.index)
            all_needed_pieces.update(needed)

        local_queue = asyncio.Queue()
        self.client.subscribe_pieces(infohash_hex, local_queue)
        try:
            self.client.request_pieces(handle, list(all_needed_pieces))
            _, generation_tasks_map = await self._process_keyframe_pieces(handle, local_queue, task_state, keyframe_info, piece_to_keyframes, selected_keyframes)
            if not generation_tasks_map and selected_keyframes:
                 raise FrameDownloadTimeoutError(f"没有成功下载任何关键帧的数据 ({len(selected_keyframes)} failed)。", infohash_hex)
            results = await asyncio.gather(*generation_tasks_map.values(), return_exceptions=True)
        finally:
            self.client.unsubscribe_pieces(infohash_hex, local_queue)

        successful_kf_indices = {kf_idx for i, kf_idx in enumerate(generation_tasks_map.keys()) if not isinstance(results[i], Exception)}
        if len(successful_kf_indices) < len(selected_keyframes):
            raise FrameDownloadTimeoutError(f"未能为所有选定的关键帧生成截图 ({len(successful_kf_indices)}/{len(selected_keyframes)} 成功)。", infohash_hex)
        self.log.info("[%s] 截图任务成功完成。", infohash_hex)

    def _find_video_file(self, ti: "lt.torrent_info") -> Tuple[int, int, int, Optional[str]]:
        video_file_index, video_file_size, video_file_offset, video_filename = -1, -1, -1, None
        fs = ti.files()
        for i in range(fs.num_files()):
            file_path = fs.file_path(i)
            if file_path.lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)
                video_filename = file_path
        return video_file_index, video_file_size, video_file_offset, video_filename

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length, infohash_hex) -> bytes:
        mdat_info = None
        try:
            head_size = min(self.settings.moov_head_probe_size, video_file_size)
            if head_size > 0:
                head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
                head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=self.settings.moov_probe_timeout)
                head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)
                stream = io.BytesIO(head_data)
                for box_type, partial_box_data, box_offset, declared_size in self._parse_mp4_boxes(stream):
                    if box_type == 'moov':
                        if len(partial_box_data) >= declared_size:
                            return partial_box_data
                        full_moov_offset_in_torrent = video_file_offset + box_offset
                        needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, declared_size, piece_length)
                        moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=self.settings.moov_probe_timeout)
                        return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, declared_size, piece_length)
                    if box_type == 'mdat':
                        mdat_info = {'offset': box_offset, 'size': declared_size}
        except TorrentClientError as e:
            raise MoovFetchError(f"在 moov 头部探测期间获取 piece 失败: {e}", infohash_hex) from e

        if mdat_info:
            try:
                mdat_end_offset_in_file = mdat_info['offset'] + mdat_info['size']
                if mdat_end_offset_in_file >= video_file_size:
                    raise MoovNotFoundError(f"mdat box seems to extend to or past the end of the file.", infohash_hex)
                tail_torrent_offset = video_file_offset + mdat_end_offset_in_file
                tail_size = video_file_size - mdat_end_offset_in_file
                if tail_size > 0:
                    tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)
                    tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=self.settings.moov_probe_timeout)
                    tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)
                    stream = io.BytesIO(tail_data)
                    for box_type, box_data, _, _ in self._parse_mp4_boxes(stream):
                        if box_type == 'moov':
                            return box_data
            except TorrentClientError as e:
                raise MoovFetchError(f"在智能 moov 尾部探测期间失败: {e}", infohash_hex) from e
        raise MoovNotFoundError("无法在文件的头部或尾部定位 'moov' atom。", infohash_hex)

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)
        current_offset = stream.tell()
        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)
            try:
                header_data = stream.read(8)
                if len(header_data) < 8: break
                declared_size, box_type_bytes = struct.unpack('>I4s', header_data)
                box_type = box_type_bytes.decode('ascii', 'ignore')
            except struct.error:
                self.log.warning("在偏移量 %d 处解析 MP4 box 头部时遇到 struct.error。", current_offset)
                break
            box_header_size = 8
            if declared_size == 1:
                if current_offset + 16 > buffer_size: break
                declared_size = struct.unpack('>Q', stream.read(8))[0]
                box_header_size = 16
            elif declared_size == 0:
                declared_size = buffer_size - current_offset
            if declared_size < box_header_size: break
            effective_box_size = declared_size
            if current_offset + declared_size > buffer_size:
                effective_box_size = buffer_size - current_offset
            box_content = stream_buffer[current_offset : current_offset + effective_box_size]
            yield box_type, bytes(box_content), current_offset, declared_size
            current_offset += declared_size

    def _select_keyframes(self, all_keyframes: list[Keyframe], timescale: int, duration_pts: int, samples: list = None) -> list[Keyframe]:
        if not all_keyframes: return []
        trim_percentage = self.settings.keyframe_trim_percentage
        if 0 < trim_percentage < 0.5:
            total_keyframes = len(all_keyframes)
            trim_count = int(total_keyframes * trim_percentage)
            if trim_count > 0 and total_keyframes > trim_count * 2:
                all_keyframes = all_keyframes[trim_count:-trim_count]
        if not all_keyframes: return []
        if duration_pts == 0 and samples:
            duration_pts = samples[-1].pts
        duration_sec = duration_pts / timescale if timescale > 0 else 0
        num_screenshots = self.settings.default_screenshots
        if duration_sec > 0:
            num_screenshots = max(
                self.settings.min_screenshots,
                min(int(duration_sec / self.settings.target_interval_sec), self.settings.max_screenshots)
            )
        if len(all_keyframes) <= num_screenshots:
            return all_keyframes
        target_timestamps_pts = [int(i * duration_pts / num_screenshots) for i in range(num_screenshots)]
        selected_keyframes = []
        for target_pts in target_timestamps_pts:
            closest_keyframe = min(all_keyframes, key=lambda kf: abs(kf.pts - target_pts))
            if closest_keyframe not in selected_keyframes:
                selected_keyframes.append(closest_keyframe)
        selected_keyframes.sort(key=lambda kf: kf.pts)
        return selected_keyframes

    async def _process_keyframe_pieces(self, handle, local_queue, task_state, keyframe_info, piece_to_keyframes, remaining_keyframes) -> Tuple[set, dict]:
        infohash_hex, extractor = task_state['infohash'], task_state['extractor']
        video_file_offset, piece_length = task_state['video_file_offset'], task_state['piece_length']
        processed_this_run, generation_tasks_map = set(), {}

        async def process_and_generate_task(keyframe_index):
            info = keyframe_info.get(keyframe_index)
            if not info: return None
            keyframe = info['keyframe']; sample = extractor.samples[keyframe.sample_index - 1]
            try:
                keyframe_pieces_data = await self.client.fetch_pieces(handle, self._get_pieces_for_range(video_file_offset + sample.offset, sample.size, piece_length), timeout=self.settings.piece_fetch_timeout)
                packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, video_file_offset + sample.offset, sample.size, piece_length)
            except TorrentClientError as e:
                self.log.warning(f"获取关键帧 {keyframe.index} 数据失败: {e}，跳过。"); return None
            if not packet_data_bytes or len(packet_data_bytes) != sample.size:
                return None
            if extractor.mode == 'avc1':
                annexb_data, start_code, cursor = bytearray(), b'\\x00\\x00\\x00\\x01', 0
                while cursor < len(packet_data_bytes):
                    nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + extractor.nal_length_size], 'big'); cursor += extractor.nal_length_size
                    nal_data = packet_data_bytes[cursor : cursor + nal_length]; annexb_data.extend(start_code + nal_data); cursor += nal_length
                packet_data = bytes(annexb_data)
            else: packet_data = packet_data_bytes
            ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
            timestamp_str = str(int(ts_sec))
            return self.loop.create_task(self.generator.generate(extractor.codec_name, extractor.extradata, packet_data, infohash_hex, timestamp_str))

        torrent_is_complete = False
        while len(processed_this_run) < len(remaining_keyframes):
            try:
                finished_piece = await asyncio.wait_for(local_queue.get(), timeout=self.settings.piece_queue_timeout)
                if finished_piece is None: torrent_is_complete = True; break
            except asyncio.TimeoutError: break
            task_state.setdefault('completed_pieces', set()).add(finished_piece)
            if finished_piece not in piece_to_keyframes: continue
            for kf_index in piece_to_keyframes[finished_piece]:
                if kf_index in processed_this_run: continue
                info = keyframe_info.get(kf_index)
                if not info: continue
                info['needed_pieces'].remove(finished_piece)
                if not info['needed_pieces']:
                    task = await process_and_generate_task(kf_index)
                    if task: generation_tasks_map[kf_index] = task
                    processed_this_run.add(kf_index)
        if torrent_is_complete:
            final_try_keyframes = [kf for kf in remaining_keyframes if kf.index not in processed_this_run]
            for kf_index in [kf.index for kf in final_try_keyframes]:
                task = await process_and_generate_task(kf_index)
                if task: generation_tasks_map[kf_index] = task
                processed_this_run.add(kf_index)
        return processed_this_run, generation_tasks_map

    def _get_pieces_for_range(self, offset_in_torrent: int, size: int, piece_length: int) -> list[int]:
        if size <= 0: return []
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data: dict[int, bytes], offset_in_torrent: int, size: int, piece_length: int) -> bytes:
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data:
                self.log.warning("组装数据时缺少 piece #%d，操作中止。", piece_index)
                return b""
        buffer = bytearray(size)
        buffer_offset = 0
        for piece_index in range(start_piece, end_piece + 1):
            piece_data = pieces_data[piece_index]
            copy_from_start = offset_in_torrent % piece_length if piece_index == start_piece else 0
            copy_to_end = (offset_in_torrent + size - 1) % piece_length + 1 if piece_index == end_piece else piece_length
            chunk = piece_data[copy_from_start:copy_to_end]
            bytes_to_copy = min(len(chunk), size - buffer_offset)
            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy
        return bytes(buffer)
