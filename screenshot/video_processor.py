# -*- coding: utf-8 -*-
"""
本模块定义了 VideoProcessor 类，它负责处理单个 torrent 任务以生成截图。
这是单个截图任务的“大脑”，负责协调所有其他组件。
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
    处理单个 torrent 的详细流程以生成截图。
    此类负责协调获取 'moov' atom、选择关键帧以及从所需数据块生成截图的整个过程。
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
        单个 torrent 的主要处理方法。
        协调单个 torrent 的完整截图生成流程。
        """
        # 步骤 1: 查找视频文件并获取基本信息
        ti = handle.get_torrent_info()
        piece_length = ti.piece_length()
        video_file_index, video_file_size, video_file_offset, video_filename = self._find_video_file(ti)
        if video_file_index == -1:
            raise NoVideoFileError("在 torrent 中没有找到 .mp4 文件。", infohash_hex)

        # 步骤 2: 查找并提取 'moov' atom
        self.piece_manager = PieceManager(self.client, handle, piece_length, infohash_hex)
        moov_finder = MoovFinder(self.piece_manager, video_file_offset, video_file_size, self.settings, infohash_hex)
        moov_data = await moov_finder.find_moov_atom()

        # 步骤 3: 解析 'moov' atom 以提取关键帧元数据
        try:
            extractor = KeyframeExtractor(moov_data)
            if not extractor.keyframes:
                raise MoovParsingError("无法从 moov atom 中提取任何关键帧。", infohash_hex)
        except (MP4ParsingError, Exception) as e:
            raise MoovParsingError(f"解析 moov 数据时失败: {e}", infohash_hex) from e

        # 步骤 4: (可选) 回调以上报任务详情
        if self.details_callback:
            duration_sec = extractor.duration_pts / extractor.timescale if extractor.timescale > 0 else 0
            details = {"torrent_name": ti.name(), "video_filename": video_filename, "video_duration_seconds": int(duration_sec)}
            await self.details_callback(infohash_hex, details)

        # 步骤 5: 选择一个代表性的关键帧子集
        all_keyframes = extractor.keyframes
        selected_keyframes = select_keyframes(
            all_keyframes, extractor.timescale, extractor.duration_pts, self.settings, extractor.samples
        )

        # 步骤 6: (可选) 检查是否已有截图存在，并跳过它们
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

        # 步骤 7: 构建处理关键帧所需的数据结构
        task_state = {"infohash": infohash_hex, "video_file_offset": video_file_offset, "extractor": extractor}
        keyframe_info, piece_to_keyframes, all_needed_pieces = {}, defaultdict(list), set()
        for kf in selected_keyframes:
            sample = task_state['extractor'].samples[kf.sample_index - 1]
            offset = task_state['video_file_offset'] + sample.offset
            needed = self.piece_manager._get_pieces_for_range(offset, sample.size)
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes[piece_idx].append(kf.index)
            all_needed_pieces.update(needed)

        # 步骤 8: 订阅 piece 完成事件，并开始处理
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

        # 步骤 9: 检查结果并报告最终状态
        successful_kf_indices = {kf_idx for i, kf_idx in enumerate(generation_tasks_map.keys()) if not isinstance(results[i], Exception)}
        if len(successful_kf_indices) < len(selected_keyframes):
            raise FrameDownloadTimeoutError(f"未能为所有选定的关键帧生成截图 ({len(successful_kf_indices)}/{len(selected_keyframes)} 成功)。", infohash_hex)
        self.log.info("[%s] 截图任务成功完成。", infohash_hex)

    def _find_video_file(self, ti: "lt.torrent_info") -> Tuple[int, int, int, Optional[str]]:
        """在 torrent 文件列表中查找最大的 .mp4 文件。"""
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
        """
        这是一个事件驱动的循环，它等待数据块下载完成的通知，
        并在一个关键帧所需的所有数据块都可用时，触发该关键帧的截图生成。
        """
        infohash_hex, extractor = task_state['infohash'], task_state['extractor']
        video_file_offset = task_state['video_file_offset']
        processed_this_run, generation_tasks_map = set(), {}

        async def process_and_generate_task(keyframe_index):
            """一个辅助函数，负责获取和组装单个关键帧的数据，并交给生成器处理。"""
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

            # 如果视频流格式是 avc1 (MP4)，需要将数据转换为 Annex B 格式才能被解码器接受
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
                # 等待来自 TorrentClient 的数据块完成通知
                finished_piece = await asyncio.wait_for(local_queue.get(), timeout=self.settings.piece_queue_timeout)
                if finished_piece is None: # None 是下载完成的信号
                    torrent_is_complete = True
                    break
            except asyncio.TimeoutError:
                break # 如果长时间没有数据块完成，则超时退出

            task_state.setdefault('completed_pieces', set()).add(finished_piece)
            if finished_piece not in piece_to_keyframes:
                continue

            # 检查这个新完成的数据块是否满足了任何关键帧的所有数据需求
            for kf_index in piece_to_keyframes[finished_piece]:
                if kf_index in processed_this_run:
                    continue
                info = keyframe_info.get(kf_index)
                if not info:
                    continue
                info['needed_pieces'].remove(finished_piece)
                if not info['needed_pieces']: # 如果一个关键帧的所有数据块都已就绪
                    task = await process_and_generate_task(kf_index)
                    if task:
                        generation_tasks_map[kf_index] = task
                    processed_this_run.add(kf_index)

        # 如果整个 torrent 都下载完了，做最后一次尝试处理所有剩余的关键帧
        if torrent_is_complete:
            final_try_keyframes = [kf for kf in remaining_keyframes if kf.index not in processed_this_run]
            for kf_index in [kf.index for kf in final_try_keyframes]:
                task = await process_and_generate_task(kf_index)
                if task:
                    generation_tasks_map[kf_index] = task
                processed_this_run.add(kf_index)

        return processed_this_run, generation_tasks_map
