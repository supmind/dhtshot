# -*- coding: utf-8 -*-
"""
This module defines the VideoProcessor, which is responsible for processing a
single torrent to generate screenshots.
"""
import asyncio
import logging
from typing import Tuple, Optional, Callable, Any
from collections import defaultdict

from .client import TorrentClient, TorrentClientError
from .errors import (
    NoVideoFileError, MoovParsingError, FrameDownloadTimeoutError, MP4ParsingError
)
from .extractor import KeyframeExtractor, Keyframe
from .generator import ScreenshotGenerator
from .piece_manager import PieceManager
from .moov_finder import MoovFinder
from .keyframe_selector import select_keyframes
from config import Settings


class VideoProcessor:
    """
    Handles the detailed processing of a single torrent to generate screenshots.
    This class orchestrates getting the moov atom, selecting keyframes,
    and generating screenshots from the required data pieces.
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
        self.piece_manager: Optional[PieceManager] = None

    async def process(self, handle, infohash_hex: str):
        """
        Main processing method for a torrent.
        Orchestrates the entire screenshot generation process for a single torrent.
        """
        ti = handle.get_torrent_info()
        piece_length = ti.piece_length()
        video_file_index, video_file_size, video_file_offset, video_filename = self._find_video_file(ti)
        if video_file_index == -1:
            raise NoVideoFileError("在 torrent 中没有找到 .mp4 文件。", infohash_hex)

        self.piece_manager = PieceManager(self.client, handle, piece_length, infohash_hex)
        moov_finder = MoovFinder(self.piece_manager, video_file_offset, video_file_size, self.settings, infohash_hex)
        moov_data = await moov_finder.find_moov_atom()

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
        selected_keyframes = select_keyframes(
            all_keyframes, extractor.timescale, extractor.duration_pts, self.settings, extractor.samples
        )

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

        task_state = {"infohash": infohash_hex, "video_file_offset": video_file_offset, "extractor": extractor}
        keyframe_info, piece_to_keyframes, all_needed_pieces = {}, defaultdict(list), set()
        for kf in selected_keyframes:
            sample = task_state['extractor'].samples[kf.sample_index - 1]
            offset = task_state['video_file_offset'] + sample.offset
            # We can use the piece manager's internal method here since it's part of the same logical unit
            needed = self.piece_manager._get_pieces_for_range(offset, sample.size)
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
                 raise FrameDownloadTimeoutError(f"没有成功下载任何关键帧的数据 ({len(selected_keyframes)} ailed)。", infohash_hex)
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

    async def _process_keyframe_pieces(self, handle, local_queue, task_state, keyframe_info, piece_to_keyframes, remaining_keyframes) -> Tuple[set, dict]:
        infohash_hex, extractor = task_state['infohash'], task_state['extractor']
        video_file_offset = task_state['video_file_offset']
        processed_this_run, generation_tasks_map = set(), {}

        async def process_and_generate_task(keyframe_index):
            info = keyframe_info.get(keyframe_index)
            if not info: return None
            keyframe = info['keyframe']
            sample = extractor.samples[keyframe.sample_index - 1]
            try:
                packet_data_bytes = await self.piece_manager.fetch_and_assemble_range(
                    video_file_offset + sample.offset, sample.size, self.settings.piece_fetch_timeout
                )
            except TorrentClientError as e:
                self.log.warning(f"获取关键帧 {keyframe.index} 数据失败: {e}，跳过。")
                return None

            if not packet_data_bytes or len(packet_data_bytes) != sample.size:
                return None

            if extractor.mode == 'avc1':
                annexb_data, start_code, cursor = bytearray(), b'\\x00\\x00\\x00\\x01', 0
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
            timestamp_str = str(int(ts_sec))
            return self.loop.create_task(self.generator.generate(extractor.codec_name, extractor.extradata, packet_data, infohash_hex, timestamp_str))

        torrent_is_complete = False
        while len(processed_this_run) < len(remaining_keyframes):
            try:
                finished_piece = await asyncio.wait_for(local_queue.get(), timeout=self.settings.piece_queue_timeout)
                if finished_piece is None:
                    torrent_is_complete = True
                    break
            except asyncio.TimeoutError:
                break

            task_state.setdefault('completed_pieces', set()).add(finished_piece)
            if finished_piece not in piece_to_keyframes:
                continue

            for kf_index in piece_to_keyframes[finished_piece]:
                if kf_index in processed_this_run:
                    continue
                info = keyframe_info.get(kf_index)
                if not info:
                    continue
                info['needed_pieces'].remove(finished_piece)
                if not info['needed_pieces']:
                    task = await process_and_generate_task(kf_index)
                    if task:
                        generation_tasks_map[kf_index] = task
                    processed_this_run.add(kf_index)

        if torrent_is_complete:
            final_try_keyframes = [kf for kf in remaining_keyframes if kf.index not in processed_this_run]
            for kf_index in [kf.index for kf in final_try_keyframes]:
                task = await process_and_generate_task(kf_index)
                if task:
                    generation_tasks_map[kf_index] = task
                processed_this_run.add(kf_index)

        return processed_this_run, generation_tasks_map
