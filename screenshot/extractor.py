# -*- coding: utf-8 -*-
"""
MP4 H.264 关键帧提取器

功能:
- 从一个 'moov' box 的字节数据中精确解析 MP4 文件结构，并提取 H.264 关键帧元数据。
- 采用健壮的启发式逻辑，以应对现实世界中不完全符合规范的 MP4 文件。

MP4 Box 结构简介:
MP4 文件由一系列称为 "box" 或 "atom" 的数据块组成。每个 box 都有一个大小和一个4字符的类型标识符。
'moov' box 是一个容器 box，它包含了所有关于媒体的元数据，例如视频轨道的编码信息、
帧（样本）的大小、时间戳和在文件中的偏移量。

本提取器的核心任务是深入 'moov' -> 'trak' -> 'mdia' -> 'minf' -> 'stbl' 路径，
解析 'stbl' (Sample Table Box) 中的各种表，以构建一个完整的样本地图。

关键的表包括:
- stsd (Sample Description): 描述编码信息 (如 'avc1')。
- stts (Time-to-Sample): 存储样本的时长，用于计算时间戳 (PTS)。
- stss (Sync Sample): 标记哪些样本是关键帧 (I-frames)。如果此表不存在，则所有样本都是关键帧。
- stsc (Sample-to-Chunk): 将样本组织成块 (chunk)。
- stsz (Sample Size): 定义每个样本的大小。
- stco/co64 (Chunk Offset): 存储每个块在文件中的物理偏移量。
"""
import sys
import struct
import logging
from io import BytesIO
from collections import namedtuple
from typing import BinaryIO, Generator, Tuple, Optional, Any

# --- 配置日志 ---
log = logging.getLogger(__name__)

# --- 数据结构定义 ---
SampleInfo = namedtuple("SampleInfo", ["offset", "size", "is_keyframe", "index", "pts"])
"""
描述单个媒体样本（通常是一帧视频）的信息。
- offset: 样本在文件中的字节偏移量。
- size: 样本的字节大小。
- is_keyframe: 布尔值，如果样本是关键帧则为 True。
- index: 样本的序号 (从1开始)。
- pts: 样本的显示时间戳 (Presentation Time Stamp)。
"""
Keyframe = namedtuple("Keyframe", ["index", "sample_index", "pts", "timescale"])
"""
描述一个关键帧的信息。
- index: 关键帧自身的序号 (在所有关键帧中的索引，从0开始)。
- sample_index: 该关键帧对应的样本序号 (从1开始)。
- pts: 关键帧的显示时间戳。
- timescale: 时间戳的时间单位，用于将 pts 转换为秒 (seconds = pts / timescale)。
"""


class H264KeyframeExtractor:
    """一个健壮的 MP4 H.264 关键帧提取器。"""

    def __init__(self, moov_data: bytes):
        """
        初始化提取器。
        :param moov_data: 包含 'moov' box 完整内容的字节串。
        """
        self.moov_stream = BytesIO(moov_data)
        self.keyframes: list[Keyframe] = []
        self.samples: list[SampleInfo] = []
        self.extradata: Optional[bytes] = None # H.264 解码器需要的额外配置数据 (avcC)
        self.mode: str = "unknown" # 'avc1' (带外) 或 'avc3' (带内)
        self.nal_length_size: int = 4 # NAL 单元长度字段的大小 (通常为4字节)
        self.timescale: int = 1000 # 默认时间单位

        try:
            self._parse_structure()
        except Exception as e:
            log.error(f"解析 'moov' box 时发生严重错误: {e}", exc_info=True)
            # 即使解析失败，也允许对象创建，但其内部状态将是空的，避免崩溃。
            pass

    def _parse_boxes(self, stream: BinaryIO) -> Generator[Tuple[str, BytesIO], None, None]:
        """
        一个生成器，用于解析给定二进制流中的所有 MP4 box。
        它能处理标准的32位大小、64位大小 ('size' == 1) 和直到文件末尾的大小 ('size' == 0)。
        """
        while True:
            current_offset = stream.tell()
            header_data = stream.read(8)
            if not header_data or len(header_data) < 8: break # 数据不足，停止解析

            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii', 'ignore')
            log.debug(f"在偏移量 {current_offset} 处找到 Box '{box_type}'，大小为 {size}")

            header_size = 8
            if size == 1: # 64位大小
                size_64_data = stream.read(8)
                if not size_64_data: break
                size = struct.unpack('>Q', size_64_data)[0]
                header_size = 16
            elif size == 0: # 直到文件末尾
                current_pos = stream.tell()
                stream.seek(0, 2) # 移动到流的末尾
                size = stream.tell() - (current_pos - header_size)
                stream.seek(current_pos) # 恢复原始位置

            payload_size = size - header_size
            if payload_size < 0:
                log.warning(f"在 Box '{box_type}' 中检测到无效的 payload 大小 ({payload_size})，停止解析。")
                break

            payload_content = stream.read(payload_size)
            if len(payload_content) < payload_size:
                log.warning(f"无法读取 Box '{box_type}' 的完整 payload，停止解析。")
                break

            payload = BytesIO(payload_content)
            yield box_type, payload

    def _find_box_payload(self, stream: BinaryIO, box_path: list[str]) -> Optional[BytesIO]:
        """
        从流的当前位置开始，递归地在嵌套的 box 结构中查找目标 box。
        例如 `box_path=['mdia', 'hdlr']` 会查找 'mdia' box，然后在其中查找 'hdlr' box。
        :param stream: 开始搜索的二进制流。
        :param box_path: 一个 box 类型字符串的列表，表示要查找的路径。
        :return: 目标 box 的 payload 流，如果未找到则返回 None。
        """
        if not box_path: return stream
        target_box, remaining_path = box_path[0], box_path[1:]
        for box_type, payload in self._parse_boxes(stream):
            if box_type == target_box:
                return self._find_box_payload(payload, remaining_path)
        return None

    def _parse_structure(self):
        """
        解析 moov box 的核心逻辑。它负责找到视频轨道 ('trak') 并触发后续的样本表解析。
        """
        stream = self.moov_stream
        log.info(f"开始解析 'moov' box。总大小: {len(stream.getbuffer())} 字节。")

        # 有些 MP4 文件可能将 'moov' box 嵌套在另一个同名的 'moov' box 中。
        # 我们需要先“解包”外部的容器，以访问包含 'trak' 的内部 payload。
        stream.seek(0)
        size, box_type = struct.unpack('>I4s', stream.read(8))
        stream.seek(0) # 看完后重置指针

        if box_type == b'moov':
            log.info("检测到 'moov' box 容器，将解析其 payload。")
            found_inner = False
            for b_type, payload in self._parse_boxes(stream):
                if b_type == 'moov':
                    stream = payload # 新的解析流是外部 moov 的 payload。
                    found_inner = True
                    break
            if not found_inner:
                raise ValueError("无法从外部 'moov' box 中提取 payload。")

        moov_payload = stream
        moov_payload.seek(0)

        # 1. 找到视频轨道 ('trak')
        trak_payload = None
        for t_type, t_payload_iter in self._parse_boxes(moov_payload):
            if t_type == 'trak':
                # 通过检查其 handler 类型 ('hdlr' box) 来确认这是否是一个视频轨道。
                current_pos = t_payload_iter.tell()
                hdlr_payload = self._find_box_payload(t_payload_iter, ['mdia', 'hdlr'])
                t_payload_iter.seek(current_pos) # 检查后恢复指针，以备后用

                if hdlr_payload:
                    hdlr_payload.seek(8) # 跳过 version, flags, component type
                    handler_type = hdlr_payload.read(4)
                    if handler_type == b'vide':
                        log.info("找到视频轨道 ('trak' with 'vide' handler)。")
                        trak_payload = t_payload_iter
                        break # 使用找到的第一个视频轨道

        if not trak_payload:
            all_children = [t for t, _ in self._parse_boxes(BytesIO(moov_payload.getvalue()))]
            log.error(f"在 'moov' 的 payload 中未找到视频轨道。找到的 box: {all_children}")
            raise ValueError("在 'moov' Box 中未找到有效的视频轨道。")

        # 2. 从 'mdhd' (Media Header) box 获取 timescale
        mdhd_payload = self._find_box_payload(trak_payload, ['mdia', 'mdhd'])
        if mdhd_payload:
            version = struct.unpack('>B', mdhd_payload.read(1))[0]
            # 根据 MP4 规范，timescale 字段的偏移量取决于 box 的版本
            mdhd_payload.seek(12 if version == 0 else 20)
            self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
        else:
            log.warning("未找到 'mdhd' box，将使用默认 timescale。")
        trak_payload.seek(0) # 重置 trak payload 流

        # 3. 找到包含所有采样信息的 'stbl' (Sample Table) box
        stbl_payload = self._find_box_payload(trak_payload, ['mdia', 'minf', 'stbl'])
        if not stbl_payload:
            raise ValueError("在视频轨道中未找到 'stbl' Box。")

        # 4. 一次性解析 'stbl' 内的所有表，并存入字典以便快速访问
        tables = {b_type: b_payload for b_type, b_payload in self._parse_boxes(stbl_payload)}

        # 5. 构建采样地图并获取解码器配置
        self._build_sample_map_and_config(tables)

    def _build_sample_map_and_config(self, tables: dict):
        """
        解析 stbl 中的各个表，以获取解码器配置 (extradata) 并构建完整的采样地图。
        这是整个提取过程中最复杂的部分。
        """
        # --- 1. 获取解码器配置 (extradata) ---
        # H.264 视频的解码器配置 (SPS/PPS) 可以存储在 'avcC' box 中 (称为 "带外" 或 "out-of-band")，
        # 或者与关键帧数据打包在一起 (称为 "带内" 或 "in-band")。
        # 'avc1' 格式表示带外，'avc3' 表示带内。
        stsd_payload = tables.get('stsd')
        if not stsd_payload: raise ValueError("在 'stbl' Box 中未找到 'stsd' Box。")
        stsd_payload.seek(8) # 跳过 version, flags, entry_count

        avc1_payload = self._find_box_payload(stsd_payload, ['avc1'])
        if avc1_payload:
            log.info("检测到 'avc1' 采样条目，正在检查 'avcC' Box...")
            avc1_payload.seek(78) # 'avc1' 头部固定为78字节，之后是扩展 box
            avcc_payload_stream = self._find_box_payload(avc1_payload, ['avcC'])

            if avcc_payload_stream and len(avcc_payload_stream.getvalue()) > 5:
                # 找到有效的 'avcC' -> 'avc1' 带外模式
                self.mode = 'avc1'
                avcc_payload = avcc_payload_stream.getvalue()
                self.extradata = avcc_payload
                # NALU 长度字段的大小记录在 avcC box 的第5个字节中
                self.nal_length_size = (avcc_payload[4] & 0x03) + 1
                log.info(f"找到有效的 'avcC' Box，将使用 '带外' 模式 (NALU 长度: {self.nal_length_size} 字节)。")
            else:
                # 'avc1' 但无 'avcC' -> 回退到 'avc3' 带内模式
                self.mode = 'avc3'
                log.warning("'avc1' 条目缺少有效 'avcC' Box，将回退到 '带内' (Annex B) 模式。")
        else:
            stsd_payload.seek(8) # 重置以便重新搜索
            avc3_payload = self._find_box_payload(stsd_payload, ['avc3'])
            if avc3_payload:
                self.mode = 'avc3'
                log.info("检测到 'avc3' 采样条目，将使用 '带内' (Annex B) 模式。")
            else:
                raise ValueError("在 'stsd' Box 中既未找到 'avc1' 也未找到 'avc3'。")

        # --- 2. 解析所有必要的表以构建采样地图 ---
        # 获取采样大小 ('stsz')
        stsz_payload = tables.get('stsz'); stsz_payload.seek(4) # 跳过 version, flags
        sample_size, sample_count = struct.unpack('>II', stsz_payload.read(8))
        sample_sizes = []
        if sample_size == 0: # 如果 sample_size 为0, 表示每个样本大小不同，存储在列表中
            if sample_count > 0:
                sample_sizes = struct.unpack(f'>{sample_count}I', stsz_payload.read(sample_count * 4))

        # 获取同步采样 (关键帧) ('stss')
        stss_payload = tables.get('stss'); keyframe_set = set()
        if stss_payload:
            stss_payload.seek(4); entry_count = struct.unpack('>I', stss_payload.read(4))[0]
            if entry_count > 0:
                # stss 表列出了所有关键帧的样本索引 (从1开始)
                keyframe_set = set(struct.unpack(f'>{entry_count}I', stss_payload.read(entry_count * 4)))
        else: # 如果 stss box 缺失，根据规范，所有样本都应被视为关键帧
            keyframe_set = set(range(1, sample_count + 1))

        # 获取块偏移 ('stco' 或 'co64')
        co_box = tables.get('stco') or tables.get('co64')
        if not co_box: raise ValueError("未找到 'stco' 或 'co64' box。")
        co_payload = co_box; co_payload.seek(4)
        entry_count = struct.unpack('>I', co_payload.read(4))[0]
        unpack_char = '>I' if tables.get('stco') else '>Q' # 32位或64位偏移
        chunk_offsets = struct.unpack(f'>{entry_count}{unpack_char[-1]}', co_payload.read(entry_count * struct.calcsize(unpack_char)))

        # 获取采样到块的映射 ('stsc')
        stsc_payload = tables.get('stsc'); stsc_payload.seek(4)
        entry_count = struct.unpack('>I', stsc_payload.read(4))[0]
        stsc_entries = [struct.unpack('>III', stsc_payload.read(12)) for _ in range(entry_count)]

        # 获取采样时间戳 ('stts')
        stts_payload = tables.get('stts'); stts_payload.seek(4)
        entry_count = struct.unpack('>I', stts_payload.read(4))[0]
        stts_entries = [struct.unpack('>II', stts_payload.read(8)) for _ in range(entry_count)]

        # --- 3. 迭代所有块和样本，创建完整的采样列表 ---
        # 这是最复杂的部分，需要同时跟踪多个表的状态来为每个样本计算正确的偏移量和时间戳。
        samples = []
        stsc_idx, sample_idx, current_time = 0, 0, 0
        stts_iter = iter(stts_entries)
        stts_count, stts_duration = next(stts_iter, (0, 0))
        stts_sample_idx_in_entry = 0

        for chunk_idx, chunk_offset in enumerate(chunk_offsets):
            # 确定当前块 (chunk) 使用哪个 stsc 条目。
            # stsc 表的条目是 (first_chunk, samples_per_chunk, sample_description_index)。
            # 我们需要找到最后一个 first_chunk 小于或等于当前 chunk_idx+1 的条目。
            if stsc_idx < len(stsc_entries) - 1 and (chunk_idx + 1) >= stsc_entries[stsc_idx + 1][0]:
                stsc_idx += 1
            _, samples_per_chunk, _ = stsc_entries[stsc_idx]

            current_offset_in_chunk = 0
            for _ in range(samples_per_chunk):
                if sample_idx >= sample_count: break

                # 为当前样本计算时间戳 (pts)。
                # stts 表的条目是 (sample_count, sample_duration)。
                # 我们需要遍历 stts 条目来累积时间。
                while stts_sample_idx_in_entry >= stts_count:
                    stts_sample_idx_in_entry -= stts_count
                    current_time += stts_count * stts_duration
                    stts_count, stts_duration = next(stts_iter, (sample_count, 0))
                pts = current_time + stts_sample_idx_in_entry * stts_duration
                stts_sample_idx_in_entry += 1

                size = sample_sizes[sample_idx] if sample_size == 0 else sample_size
                is_keyframe = (sample_idx + 1) in keyframe_set
                # 将计算出的样本信息添加到列表中
                samples.append(SampleInfo(chunk_offset + current_offset_in_chunk, size, is_keyframe, sample_idx + 1, pts))
                current_offset_in_chunk += size
                sample_idx += 1

        self.samples = samples

        # 从所有样本中筛选出关键帧
        keyframe_samples = [s for s in self.samples if s.is_keyframe]
        self.keyframes = [
            Keyframe(i, s.index, s.pts, self.timescale)
            for i, s in enumerate(keyframe_samples)
        ]

        log.info(f"完成采样地图构建。共找到 {len(self.samples)} 个样本，其中 {len(self.keyframes)} 个是关键帧。")
