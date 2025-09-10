# -*- coding: utf-8 -*-
"""
本模块定义了 ScreenshotService，它是协调整个截图生成过程的核心服务。
"""
import asyncio
import logging
import io
import struct
import base64
from typing import Generator, Tuple, Optional, Callable, Awaitable, Any
from collections import defaultdict

import av

from .client import TorrentClient, TorrentClientError
from .errors import (
    TaskError, NoVideoFileError, MoovNotFoundError, MoovFetchError,
    MoovParsingError, MetadataTimeoutError, FrameDownloadTimeoutError, MP4ParsingError
)
from .extractor import MP4Extractor, MKVExtractor, BaseExtractor, Keyframe, SampleInfo
from .generator import ScreenshotGenerator
from config import Settings

# 为 status_callback 定义一个类型签名，以增强可读性和静态检查能力
StatusCallback = Callable[..., Awaitable[None]]

class ScreenshotService:
    """
    协调截图生成过程的核心服务类。
    """
    def __init__(self, settings: Settings, loop=None, client=None, status_callback: Optional[StatusCallback] = None, screenshot_callback: Optional[Callable] = None, details_callback: Optional[Callable] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.settings = settings
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False

        self.client = client or TorrentClient(
            loop=self.loop,
            settings=self.settings
        )
        self.generator = ScreenshotGenerator(
            loop=self.loop,
            output_dir=self.settings.output_dir,
            on_success=screenshot_callback
        )
        self.status_callback = status_callback
        self.details_callback = details_callback
        self.active_tasks = set()
        self._submit_lock = asyncio.Lock()

    def get_queue_size(self) -> int:
        return self.task_queue.qsize()

    async def run(self):
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.settings.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info("ScreenshotService 已启动，拥有 %d 个工作进程。", self.settings.num_workers)

    async def stop(self):
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        await self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, metadata: bytes = None, resume_data: dict = None):
        async with self._submit_lock:
            if infohash in self.active_tasks:
                self.log.warning("任务 %s 已在处理中，本次提交被忽略。", infohash)
                return
            self.active_tasks.add(infohash)

        await self.task_queue.put({'infohash': infohash, 'metadata': metadata, 'resume_data': resume_data})
        log_msg = "为 infohash: %s 重新提交了任务" if resume_data else "为 infohash: %s 提交了新任务"
        self.log.info(log_msg, infohash)

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
                if current_offset + 16 > buffer_size:
                    self.log.warning("Box '%s' 在偏移量 %d 处需要 64 位大小，但数据不足。", box_type, current_offset)
                    break
                declared_size = struct.unpack('>Q', stream.read(8))[0]
                box_header_size = 16
            elif declared_size == 0:
                declared_size = buffer_size - current_offset
            if declared_size < box_header_size:
                self.log.warning("Box '%s' 在偏移量 %d 处声明了无效的大小 %d。", box_type, current_offset, declared_size)
                break
            effective_box_size = declared_size
            if current_offset + declared_size > buffer_size:
                self.log.debug("Box '%s' (大小: %d) 超出了缓冲区大小 %d。将 yield 部分数据。", box_type, declared_size, buffer_size)
                effective_box_size = buffer_size - current_offset
            box_content = stream_buffer[current_offset : current_offset + effective_box_size]
            yield box_type, bytes(box_content), current_offset, declared_size
            if effective_box_size < declared_size:
                break
            current_offset += declared_size

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length, infohash_hex) -> bytes:
        self.log.info("[%s] DEBUG: 进入 _get_moov_atom_data", infohash_hex)
        try:
            head_size = min(256 * 1024, video_file_size)
            if head_size > 0:
                self.log.info("[%s] DEBUG: 正在探测文件头部 (大小: %d)...", infohash_hex, head_size)
                head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
                head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=self.settings.moov_probe_timeout)
                head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)
                stream = io.BytesIO(head_data)
                for box_type, partial_box_data, box_offset, declared_size in self._parse_mp4_boxes(stream):
                    if box_type == 'moov':
                        if len(partial_box_data) >= declared_size:
                            self.log.info("[%s] 在头部探测中找到了完整的 'moov' box。", infohash_hex)
                            return partial_box_data
                        self.log.info("[%s] 在头部探测中找到了一个大小为 %d 的部分 'moov' box。现在将获取完整的 box。", infohash_hex, declared_size)
                        full_moov_offset_in_torrent = video_file_offset + box_offset
                        needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, declared_size, piece_length)
                        moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=self.settings.moov_probe_timeout)
                        return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, declared_size, piece_length)
                    if box_type == 'mdat':
                        self.log.info("[%s] 在文件头部探测到 'mdat'，将转而探测文件尾部。", infohash_hex)
                        break
        except TorrentClientError as e:
            raise MoovFetchError(f"在 moov 头部探测期间获取 piece 失败: {e}", infohash_hex) from e
        try:
            tail_probe_size = 10 * 1024 * 1024
            tail_file_offset = max(0, video_file_size - tail_probe_size)
            tail_torrent_offset = video_file_offset + tail_file_offset
            tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
            if tail_size > 0:
                self.log.info("[%s] DEBUG: 正在探测文件尾部 (大小: %d)...", infohash_hex, tail_size)
                tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)
                tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=self.settings.moov_probe_timeout)
                tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)
                offset_in_buffer = len(tail_data)
                while offset_in_buffer > 4:
                    try:
                        size_bytes = tail_data[offset_in_buffer - 4 : offset_in_buffer]
                        box_size = struct.unpack('>I', size_bytes)[0]
                        if box_size < 8:
                            offset_in_buffer -= 1
                            continue
                        box_start_in_buffer = offset_in_buffer - box_size
                        if box_start_in_buffer < 0: break
                        type_bytes = tail_data[box_start_in_buffer + 4 : box_start_in_buffer + 8]
                        box_type = type_bytes.decode('ascii', 'ignore')
                        if box_type == 'moov':
                            self.log.info("[%s] 在尾部探测中通过结构化解析找到了有效的 'moov' box。", infohash_hex)
                            return tail_data[box_start_in_buffer : box_start_in_buffer + box_size]
                        offset_in_buffer = box_start_in_buffer
                    except (struct.error, IndexError):
                        offset_in_buffer -= 1
        except TorrentClientError as e:
            raise MoovFetchError(f"在 moov 尾部探测期间失败: {e}", infohash_hex) from e
        raise MoovNotFoundError("无法在文件的头部或尾部定位 'moov' atom。", infohash_hex)

    async def _get_mkv_metadata(self, handle, video_file_offset, video_file_size, piece_length, infohash_hex) -> Tuple[bytes, bytes]:
        from ebmlite import loadSchema
        schema = loadSchema('matroska.xml')
        head_size = min(4 * 1024, video_file_size)
        head_data = b''
        if head_size > 0:
            head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
            head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=self.settings.metadata_timeout)
            head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)
        if not head_data:
            raise MoovFetchError("无法下载 MKV 文件头部。", infohash_hex)
        doc = schema.loads(head_data)
        segment = next((el for el in doc if el.name == 'Segment'), None)
        if not segment:
            raise MP4ParsingError("在 MKV 数据中未找到 Segment。", infohash_hex)
        seek_head = next((el for el in segment if el.name == 'SeekHead'), None)
        if not seek_head:
            raise MP4ParsingError("在 MKV 数据中未找到 SeekHead。", infohash_hex)
        cues_offset = -1
        cues_id = schema.get_id('Cues')
        for seek_entry in seek_head:
            if seek_entry.name == 'Seek':
                seek_id_el = next((el for el in seek_entry if el.name == 'SeekID'), None)
                if seek_id_el and seek_id_el.value == cues_id:
                    cues_offset_el = next((el for el in seek_entry if el.name == 'SeekPosition'), None)
                    if cues_offset_el:
                        cues_offset = cues_offset_el.value
                        break
        if cues_offset == -1:
            raise MP4ParsingError("在 MKV SeekHead 中未找到 Cues 的位置信息。", infohash_hex)
        cues_fetch_size = 5 * 1024 * 1024
        cues_torrent_offset = video_file_offset + segment.offset + cues_offset
        cues_pieces = self._get_pieces_for_range(cues_torrent_offset, cues_fetch_size, piece_length)
        cues_data_pieces = await self.client.fetch_pieces(handle, cues_pieces, timeout=self.settings.metadata_timeout)
        cues_data = self._assemble_data_from_pieces(cues_data_pieces, cues_torrent_offset, cues_fetch_size, piece_length)
        if not cues_data:
            raise MoovFetchError("无法下载 MKV Cues 数据。", infohash_hex)
        return head_data, cues_data

    def _find_video_file(self, ti: "lt.torrent_info") -> Tuple[int, int, int, Optional[str], Optional[str]]:
        video_file_index, video_file_size, video_file_offset, video_filename, video_file_ext = -1, -1, -1, None, None
        supported_exts = ('.mp4', '.mkv')
        fs = ti.files()
        for i in range(fs.num_files()):
            file_path = fs.file_path(i).lower()
            if file_path.endswith(supported_exts) and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)
                video_filename = fs.file_path(i)
                video_file_ext = file_path[file_path.rfind('.')+1:]
        return video_file_index, video_file_size, video_file_offset, video_filename, video_file_ext

    def _select_keyframes(self, all_keyframes: list[Keyframe], timescale: int, duration_pts: int, samples: list = None) -> list[Keyframe]:
        if not all_keyframes: return []
        if duration_pts == 0 and samples:
            self.log.warning("duration_pts 为 0，将使用最后一个样本的 PTS 作为估算时长。")
            duration_pts = samples[-1].pts
        duration_sec = duration_pts / timescale if timescale > 0 else 0
        num_screenshots = self.settings.default_screenshots
        if duration_sec > 0:
            num_screenshots = max(self.settings.min_screenshots, min(int(duration_sec / self.settings.target_interval_sec), self.settings.max_screenshots))
        if len(all_keyframes) <= num_screenshots:
            self.log.info("DEBUG: 关键帧数量 (%d) 小于等于目标数量 (%d)，将选择所有关键帧。", len(all_keyframes), num_screenshots)
            return all_keyframes
        target_timestamps_pts = [int(i * duration_pts / num_screenshots) for i in range(num_screenshots)]
        selected_keyframes = []
        for target_pts in target_timestamps_pts:
            closest_keyframe = min(all_keyframes, key=lambda kf: abs(kf.pts - target_pts))
            if closest_keyframe not in selected_keyframes:
                selected_keyframes.append(closest_keyframe)
        selected_keyframes.sort(key=lambda kf: kf.pts)
        self.log.info("DEBUG: 从 %d 个关键帧中选择了 %d 个。", len(all_keyframes), len(selected_keyframes))
        return selected_keyframes

    def _serialize_task_state(self, state: dict) -> dict:
        extractor: BaseExtractor = state.get('extractor')
        if not extractor: return None
        file_type = 'mkv' if isinstance(extractor, MKVExtractor) else 'mp4'
        return {
            "infohash": state['infohash'], "piece_length": state['piece_length'],
            "video_file_offset": state['video_file_offset'], "video_file_size": state['video_file_size'],
            "file_type": file_type,
            "extractor_info": {
                "extradata": extractor.extradata, "codec_name": extractor.codec_name, "mode": extractor.mode,
                "nal_length_size": extractor.nal_length_size, "timescale": extractor.timescale,
                "samples": [s._asdict() for s in extractor.samples],
            },
            "all_keyframes": [k._asdict() for k in state['all_keyframes']],
            "selected_keyframes": [k._asdict() for k in state['selected_keyframes']],
            "completed_pieces": list(state.get('completed_pieces', [])),
            "processed_keyframes": list(state.get('processed_keyframes', [])),
        }

    def _load_state_from_resume_data(self, data: dict) -> dict:
        file_type = data.get('file_type', 'mp4')
        if file_type == 'mkv':
            extractor = MKVExtractor(head_data=b'', cues_data=b'')
        else:
            extractor = MP4Extractor(moov_data=b'')
        ext_info = data['extractor_info']
        extradata = ext_info.get('extradata')
        if extradata and isinstance(extradata, str):
            try: extractor.extradata = base64.b64decode(extradata)
            except (base64.binascii.Error, TypeError) as e:
                self.log.error(f"解码 base64 extradata 失败: {e}"); extractor.extradata = None
        else: extractor.extradata = extradata
        extractor.codec_name = ext_info.get('codec_name')
        extractor.mode = ext_info['mode']
        extractor.nal_length_size = ext_info['nal_length_size']
        extractor.timescale = ext_info['timescale']
        extractor.samples = [SampleInfo(**s) for s in ext_info['samples']]
        all_keyframes = [Keyframe(**k) for k in data['all_keyframes']]
        extractor.keyframes = all_keyframes
        return {
            "infohash": data['infohash'], "piece_length": data['piece_length'],
            "video_file_offset": data['video_file_offset'], "video_file_size": data['video_file_size'],
            "extractor": extractor, "all_keyframes": all_keyframes,
            "selected_keyframes": [Keyframe(**k) for k in data['selected_keyframes']],
            "completed_pieces": set(data['completed_pieces']),
            "processed_keyframes": set(data['processed_keyframes']),
        }

    async def _process_keyframe_pieces(self, handle, local_queue, task_state, keyframe_info, piece_to_keyframes, remaining_keyframes) -> Tuple[set, dict]:
        infohash_hex, extractor = task_state['infohash'], task_state['extractor']
        video_file_offset, piece_length = task_state['video_file_offset'], task_state['piece_length']
        processed_this_run, generation_tasks_map = set(), {}
        async def process_and_generate_task(keyframe_index):
            info = keyframe_info.get(keyframe_index)
            if not info: return None
            keyframe = info['keyframe']; sample = extractor.samples[keyframe.sample_index - 1]
            fetch_size = sample.size if sample.size > 0 else self.settings.mkv_default_fetch_size
            try:
                keyframe_pieces_data = await self.client.fetch_pieces(handle, self._get_pieces_for_range(video_file_offset + sample.offset, fetch_size, piece_length), timeout=self.settings.piece_fetch_timeout)
                packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, video_file_offset + sample.offset, fetch_size, piece_length)
            except TorrentClientError as e:
                self.log.warning(f"获取关键帧 {keyframe.index} 数据失败: {e}，跳过。"); return None
            if not packet_data_bytes:
                self.log.warning(f"关键帧 {keyframe.index} 的数据未能成功组装，跳过。"); return None
            if sample.size > 0 and len(packet_data_bytes) != sample.size:
                self.log.warning(f"关键帧 {keyframe.index} 的数据不完整 ({len(packet_data_bytes)}/{sample.size})，跳过。"); return None
            if extractor.mode == 'avc1':
                packet_data = self._convert_avc1_to_annexb(packet_data_bytes, extractor.nal_length_size)
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
                    self.log.info("[%s] DEBUG: 收到 torrent 完成信号 (None)。", infohash_hex)
                    torrent_is_complete = True; break
            except asyncio.TimeoutError: self.log.warning(f"[{infohash_hex}] 等待 piece 超时。"); break
            task_state.setdefault('completed_pieces', set()).add(finished_piece)
            if finished_piece not in piece_to_keyframes: continue
            for kf_index in piece_to_keyframes[finished_piece]:
                if kf_index in processed_this_run: continue
                info = keyframe_info.get(kf_index);
                if not info: continue
                info['needed_pieces'].remove(finished_piece)
                if not info['needed_pieces']:
                    self.log.info("[%s] DEBUG: 关键帧 %d 的所有 piece 都已完成，正在创建生成任务。", infohash_hex, kf_index)
                    task = await process_and_generate_task(kf_index)
                    if task: generation_tasks_map[kf_index] = task
                    processed_this_run.add(kf_index)
        if torrent_is_complete:
            self.log.info("[%s] DEBUG: Torrent 已完成，最后尝试处理剩余的 %d 个关键帧。", infohash_hex, len(remaining_keyframes) - len(processed_this_run))
            final_try_keyframes = [kf for kf in remaining_keyframes if kf.index not in processed_this_run]
            for kf_index in [kf.index for kf in final_try_keyframes]:
                task = await process_and_generate_task(kf_index)
                if task: generation_tasks_map[kf_index] = task
                processed_this_run.add(kf_index)
        return processed_this_run, generation_tasks_map

    def _convert_avc1_to_annexb(self, packet_data_bytes: bytes, nal_length_size: int) -> bytes:
        if not packet_data_bytes or nal_length_size not in [1, 2, 4]:
            return packet_data_bytes
        annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
        while cursor < len(packet_data_bytes):
            try:
                nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + nal_length_size], 'big')
                cursor += nal_length_size
                if cursor + nal_length > len(packet_data_bytes):
                    self.log.error("NALU length (%d) exceeds remaining packet size.", nal_length)
                    return packet_data_bytes
                nal_data = packet_data_bytes[cursor : cursor + nal_length]
                annexb_data.extend(start_code + nal_data)
                cursor += nal_length
            except Exception:
                self.log.exception("在 NALU 转换期间发生错误")
                return packet_data_bytes
        return bytes(annexb_data)

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex, resume_data=None):
        self.log.info("[%s] DEBUG: 进入 _generate_screenshots_from_torrent", infohash_hex)
        if not handle.has_metadata():
            raise MetadataTimeoutError(f"句柄未能获取元数据，无法继续。", infohash_hex)
        task_state = {}
        if resume_data:
            try:
                self.log.info("[%s] DEBUG: 正在尝试从 resume_data 加载状态。", infohash_hex)
                task_state = self._load_state_from_resume_data(resume_data)
            except (KeyError, TypeError) as e: self.log.error("[%s] 加载恢复数据失败: %s", infohash_hex, e); resume_data = None
        if not resume_data:
            self.log.info("[%s] DEBUG: 无恢复数据，正在开始新任务。", infohash_hex)
            ti = handle.get_torrent_info()
            piece_length = ti.piece_length()
            video_file_index, video_file_size, video_file_offset, video_filename, video_file_ext = self._find_video_file(ti)
            if video_file_index == -1:
                raise NoVideoFileError("在 torrent 中没有找到 .mp4 或 .mkv 文件。", infohash_hex)
            self.log.info("[%s] DEBUG: 找到视频文件: %s", infohash_hex, video_filename)
            extractor: Optional[BaseExtractor] = None
            if video_file_ext == 'mp4':
                moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length, infohash_hex)
                try:
                    extractor = MP4Extractor(moov_data)
                    if not extractor.keyframes: raise MoovParsingError("无法从 moov atom 中提取任何关键帧。", infohash_hex)
                except (MP4ParsingError, Exception) as e:
                    raise MoovParsingError(f"解析 moov 数据时失败: {e}", infohash_hex) from e
            elif video_file_ext == 'mkv':
                head_data, cues_data = await self._get_mkv_metadata(handle, video_file_offset, video_file_size, piece_length, infohash_hex)
                extractor = MKVExtractor(head_data, cues_data)
                extractor.parse()
                if not extractor.keyframes:
                    raise MoovParsingError("无法从 MKV cues 中提取任何关键帧。", infohash_hex)
            if not extractor:
                raise NoVideoFileError(f"不支持的文件类型: {video_file_ext}", infohash_hex)
            self.log.info("[%s] DEBUG: Extractor 创建成功，找到 %d 个关键帧。", infohash_hex, len(extractor.keyframes))
            if self.details_callback:
                duration_sec = extractor.duration_pts / extractor.timescale if extractor.timescale > 0 else 0
                details = {"torrent_name": ti.name(), "video_filename": video_filename, "video_duration_seconds": int(duration_sec)}
                await self.details_callback(infohash_hex, details)
            all_keyframes = extractor.keyframes
            selected_keyframes = self._select_keyframes(all_keyframes, extractor.timescale, extractor.duration_pts, extractor.samples)
            task_state = {
                "infohash": infohash_hex, "piece_length": piece_length, "video_file_offset": video_file_offset,
                "video_file_size": video_file_size, "extractor": extractor, "all_keyframes": all_keyframes,
                "selected_keyframes": selected_keyframes, "completed_pieces": set(), "processed_keyframes": set()
            }
        remaining_keyframes = [kf for kf in task_state['selected_keyframes'] if kf.index not in task_state.get('processed_keyframes', set())]
        self.log.info("[%s] DEBUG: 需要处理 %d 个剩余的关键帧。", infohash_hex, len(remaining_keyframes))
        if not remaining_keyframes: self.log.info("[%s] 所有选定的关键帧都已处理。", infohash_hex); return
        keyframe_info, piece_to_keyframes, all_needed_pieces = {}, defaultdict(list), set()
        for kf in remaining_keyframes:
            sample = task_state['extractor'].samples[kf.sample_index - 1]
            offset = task_state['video_file_offset'] + sample.offset
            needed = self._get_pieces_for_range(offset, sample.size, task_state['piece_length'])
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed: piece_to_keyframes[piece_idx].append(kf.index)
            all_needed_pieces.update(needed)
        pieces_to_request = list(all_needed_pieces - task_state.get('completed_pieces', set()))
        local_queue = asyncio.Queue()
        self.client.subscribe_pieces(infohash_hex, local_queue)
        self.log.info("[%s] DEBUG: 共需请求 %d 个 pieces。", infohash_hex, len(pieces_to_request))
        try:
            self.client.request_pieces(handle, pieces_to_request)
            processed_this_run, generation_tasks_map = await self._process_keyframe_pieces(handle, local_queue, task_state, keyframe_info, piece_to_keyframes, remaining_keyframes)
            self.log.info("[%s] DEBUG: _process_keyframe_pieces 完成，创建了 %d 个生成任务。", infohash_hex, len(generation_tasks_map))
            if not generation_tasks_map and remaining_keyframes:
                 task_state.setdefault('processed_keyframes', set()).update(processed_this_run)
                 raise FrameDownloadTimeoutError("没有成功下载任何关键帧的数据。", infohash_hex, resume_data=self._serialize_task_state(task_state))
            results = await asyncio.gather(*generation_tasks_map.values(), return_exceptions=True)
        finally:
            self.client.unsubscribe_pieces(infohash_hex, local_queue)
        successful_kf_indices = set()
        kf_indices = list(generation_tasks_map.keys())
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.log.error(f"生成截图时发生错误 (关键帧索引: {kf_indices[i]}): {result}", exc_info=result)
            else: successful_kf_indices.add(kf_indices[i])
        self.log.info("[%s] DEBUG: %d/%d 个截图任务成功。", infohash_hex, len(successful_kf_indices), len(remaining_keyframes))
        task_state.setdefault('processed_keyframes', set()).update(successful_kf_indices)
        if len(successful_kf_indices) < len(remaining_keyframes):
            raise FrameDownloadTimeoutError(
                f"未能为所有选定的关键帧生成截图 ({len(successful_kf_indices)}/{len(remaining_keyframes)} 成功)。",
                infohash_hex, resume_data=self._serialize_task_state(task_state)
            )
        self.log.info("[%s] 截图任务成功完成。", infohash_hex)

    async def _send_status_update(self, **kwargs: Any) -> None:
        if self.status_callback:
            await self.status_callback(**kwargs)

    async def _handle_screenshot_task(self, task_info: dict):
        infohash_hex = task_info['infohash']
        log_message = "正在处理任务: %s" + (" (正在恢复)" if task_info.get('resume_data') else "")
        self.log.info(log_message, infohash_hex)
        try:
            async with self.client.get_handle(infohash_hex, metadata=task_info.get('metadata')) as handle:
                await self._generate_screenshots_from_torrent(handle, infohash_hex, task_info.get('resume_data'))
            self.log.info("任务 %s 成功完成。", infohash_hex)
            await self._send_status_update(status='success', infohash=infohash_hex, message='任务成功完成。')
        except (FrameDownloadTimeoutError, MetadataTimeoutError, MoovFetchError, asyncio.TimeoutError) as e:
            self.log.warning("任务 %s 因可恢复的错误而失败: %s", infohash_hex, e)
            await self._send_status_update(status='recoverable_failure', infohash=getattr(e, 'infohash', infohash_hex), message=str(e), error=e, resume_data=getattr(e, 'resume_data', None))
            return
        except TaskError as e:
            self.log.error("任务 %s 因永久性错误而失败: %s", e.infohash, e, exc_info=True)
            await self._send_status_update(status='permanent_failure', infohash=e.infohash, message=str(e), error=e)
            return
        except Exception as e:
            self.log.exception("处理 %s 时发生意外的严重错误。", infohash_hex)
            await self._send_status_update(status='permanent_failure', infohash=infohash_hex, message=f"发生意外错误: {e}", error=e)
            return
        finally:
            self.active_tasks.discard(infohash_hex)

    async def _worker(self):
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作进程中发生未捕获的错误。")
