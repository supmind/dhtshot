# -*- coding: utf-8 -*-
"""
MP4 H.264 关键帧提取器

功能:
- 从一个 'moov' box 的字节数据中精确解析 MP4 文件结构，并提取 H.264 关键帧元数据。
- 采用健壮的启发式逻辑，以应对现实世界中不完全符合规范的 MP4 文件。
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
Keyframe = namedtuple("Keyframe", ["index", "sample_index", "pts", "timescale"])


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
        self.extradata: Optional[bytes] = None
        self.mode: str = "unknown"
        self.nal_length_size: int = 4
        self.timescale: int = 1000

        try:
            self._parse_structure()
        except Exception as e:
            log.error(f"解析 'moov' box 时发生严重错误: {e}", exc_info=True)
            # 即使解析失败，也允许对象创建，但其内部状态将是空的
            pass

    def _parse_boxes(self, stream: BinaryIO) -> Generator[Tuple[str, BytesIO], None, None]:
        """同步解析一个流中的所有 box。"""
        while True:
            current_offset = stream.tell()
            header_data = stream.read(8)
            if not header_data or len(header_data) < 8: break
            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii', 'ignore')
            log.debug(f"在偏移量 {current_offset} 处找到 Box '{box_type}'，大小为 {size}")
            header_size = 8
            if size == 1:
                size_64_data = stream.read(8)
                if not size_64_data: break
                size = struct.unpack('>Q', size_64_data)[0]
                header_size = 16
            elif size == 0:
                current_pos = stream.tell()
                stream.seek(0, 2)
                size = stream.tell() - (current_pos - header_size)
                stream.seek(current_pos)

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
        """从流的当前位置开始，递归查找嵌套的 Box 并返回其 payload。"""
        if not box_path: return stream
        target_box, remaining_path = box_path[0], box_path[1:]
        for box_type, payload in self._parse_boxes(stream):
            if box_type == target_box:
                return self._find_box_payload(payload, remaining_path)
        return None

    def _parse_structure(self):
        """
        解析 moov box 中的 MP4 结构，以找到视频轨道并构建采样地图。
        """
        stream = self.moov_stream
        log.info(f"开始解析 'moov' box。总大小: {len(stream.getbuffer())} 字节。")

        # 传入提取器的数据是完整的 'moov' atom (头部 + payload)。
        # 有些文件可能有一个嵌套的 'moov' atom。我们需要解开它才能访问到 'trak' atoms。
        stream.seek(0)
        # 偷看一下 box 类型，看是否需要解包。
        size, box_type = struct.unpack('>I4s', stream.read(8))
        stream.seek(0) # 看完后重置。

        if box_type == b'moov':
            log.info("检测到 'moov' box 容器，将解析其 payload。")
            # 流本身就是 moov box。我们需要解析它的 payload。
            # _parse_boxes 将 payload 作为新的 BytesIO 流返回。
            found_inner = False
            for b_type, payload in self._parse_boxes(stream):
                if b_type == 'moov':
                    stream = payload # 新的解析流是外部 moov 的 payload。
                    found_inner = True
                    break
            if not found_inner:
                # 如果偷看是正确的，这不应该发生，但作为安全措施：
                raise ValueError("无法从外部 'moov' box 中提取 payload。")

        # 此时，`stream` 应该包含带有 trak/mvhd 等的实际内容。
        moov_payload = stream
        moov_payload.seek(0)

        # 1. 找到视频轨道 ('trak')
        trak_payload = None
        for t_type, t_payload_iter in self._parse_boxes(moov_payload):
            if t_type == 'trak':
                # 要检查它是否是视频轨道，我们需要检查它的 handler 类型。
                # 需要在不消耗 payload 的情况下进行检查，以备后用。
                current_pos = t_payload_iter.tell()
                hdlr_payload = self._find_box_payload(t_payload_iter, ['mdia', 'hdlr'])
                t_payload_iter.seek(current_pos) # 为下一次检查或最终使用重置

                if hdlr_payload:
                    hdlr_payload.seek(8) # 跳过 version, flags, component type
                    handler_type = hdlr_payload.read(4)
                    if handler_type == b'vide':
                        log.info("找到视频轨道 ('trak' with 'vide' handler)。")
                        trak_payload = t_payload_iter
                        break # 使用找到的第一个视频轨道

        if not trak_payload:
            all_children = [t for t, _ in self._parse_boxes(moov_payload)]
            log.error(f"在 'moov' 的 payload 中未找到视频轨道。找到的 box: {all_children}")
            raise ValueError("在 'moov' Box 中未找到有效的视频轨道。")

        # 2. 从 'mdhd' 获取 timescale
        mdhd_payload = self._find_box_payload(trak_payload, ['mdia', 'mdhd'])
        if mdhd_payload:
            version = struct.unpack('>B', mdhd_payload.read(1))[0]
            # 根据规范，版本 0 的 timescale 偏移量为 12，版本 1 为 20
            mdhd_payload.seek(12 if version == 0 else 20)
            self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
        else:
            log.warning("未找到 'mdhd' box，将使用默认 timescale。")
        trak_payload.seek(0) # 重置 trak payload 流

        # 3. 找到包含所有采样信息的 'stbl' (Sample Table) box
        stbl_payload = self._find_box_payload(trak_payload, ['mdia', 'minf', 'stbl'])
        if not stbl_payload:
            raise ValueError("在视频轨道中未找到 'stbl' Box。")

        # 4. 解析 'stbl' 内的所有表
        tables = {b_type: b_payload for b_type, b_payload in self._parse_boxes(stbl_payload)}

        # 5. 构建采样地图并获取解码器配置
        self._build_sample_map_and_config(tables)

    def _build_sample_map_and_config(self, tables: dict):
        """
        解析 stbl 表以获取解码器配置 (extradata) 并构建完整的采样地图。
        """
        # --- 1. 获取解码器配置 ('avc1'/'avc3' 和 'avcC') ---
        stsd_payload = tables.get('stsd')
        if not stsd_payload: raise ValueError("在 'stbl' Box 中未找到 'stsd' Box。")
        stsd_payload.seek(8) # 跳过 version, flags, entry_count

        # 首先尝试找到 'avc1'
        avc1_payload = self._find_box_payload(stsd_payload, ['avc1'])
        if avc1_payload:
            log.info("检测到 'avc1' 采样条目，正在检查 'avcC' Box...")
            # 'avcC' box 是 'avc1' 的子 box。
            avc1_payload.seek(78) # 到达 avcC 等子 box 开始的固定偏移量
            avcc_payload_stream = self._find_box_payload(avc1_payload, ['avcC'])

            if avcc_payload_stream and len(avcc_payload_stream.getvalue()) > 5:
                # 找到带有有效 'avcC' 的 'avc1' -> 带外模式
                self.mode = 'avc1'
                avcc_payload = avcc_payload_stream.getvalue()
                self.extradata = avcc_payload
                self.nal_length_size = (avcc_payload[4] & 0x03) + 1
                log.info(f"找到有效的 'avcC' Box，将使用 '带外' 模式 (NALU 长度: {self.nal_length_size} 字节)。")
            else:
                # 找到 'avc1' 但 'avcC' 缺失或无效 -> 回退到带内模式
                self.mode = 'avc3'
                log.warning("'avc1' 条目缺少有效 'avcC' Box，将回退到 '带内' (Annex B) 模式。")
        else:
            # 如果未找到 'avc1'，尝试 'avc3'
            stsd_payload.seek(8) # 重置以便重新搜索
            avc3_payload = self._find_box_payload(stsd_payload, ['avc3'])
            if avc3_payload:
                self.mode = 'avc3'
                log.info("检测到 'avc3' 采样条目，将使用 '带内' (Annex B) 模式。")
            else:
                raise ValueError("在 'stsd' Box 中既未找到 'avc1' 也未找到 'avc3'。")

        # --- 2. 构建采样地图 ---
        # 获取采样大小 ('stsz')
        stsz_payload = tables.get('stsz'); stsz_payload.seek(4) # version, flags
        sample_size, sample_count = struct.unpack('>II', stsz_payload.read(8))
        sample_sizes = []
        if sample_size == 0:
            if sample_count > 0:
                sample_sizes = struct.unpack(f'>{sample_count}I', stsz_payload.read(sample_count * 4))

        # 获取同步采样 (关键帧) ('stss')
        stss_payload = tables.get('stss'); keyframe_set = set()
        if stss_payload:
            stss_payload.seek(4); entry_count = struct.unpack('>I', stss_payload.read(4))[0]
            if entry_count > 0:
                keyframe_set = set(struct.unpack(f'>{entry_count}I', stss_payload.read(entry_count * 4)))
        else: # 如果 stss 缺失，所有采样都是关键帧
            keyframe_set = set(range(1, sample_count + 1))

        # 获取块偏移 ('stco' 或 'co64')
        co_box = tables.get('stco') or tables.get('co64')
        if not co_box: raise ValueError("未找到 'stco' 或 'co64' box。")
        co_payload = co_box; co_payload.seek(4)
        entry_count = struct.unpack('>I', co_payload.read(4))[0]
        unpack_char = '>I' if tables.get('stco') else '>Q'
        chunk_offsets = struct.unpack(f'>{entry_count}{unpack_char[-1]}', co_payload.read(entry_count * struct.calcsize(unpack_char)))

        # 获取采样到块的映射 ('stsc')
        stsc_payload = tables.get('stsc'); stsc_payload.seek(4)
        entry_count = struct.unpack('>I', stsc_payload.read(4))[0]
        stsc_entries = [struct.unpack('>III', stsc_payload.read(12)) for _ in range(entry_count)]

        # 获取采样时间戳 ('stts')
        stts_payload = tables.get('stts'); stts_payload.seek(4)
        entry_count = struct.unpack('>I', stts_payload.read(4))[0]
        stts_entries = [struct.unpack('>II', stts_payload.read(8)) for _ in range(entry_count)]

        # --- 3. 迭代并创建完整的采样列表 ---
        samples = []
        stsc_idx, sample_idx, current_time = 0, 0, 0
        stts_iter = iter(stts_entries)
        stts_count, stts_duration = next(stts_iter, (0, 0))
        stts_sample_idx_in_entry = 0

        for chunk_idx, chunk_offset in enumerate(chunk_offsets):
            # 为此块找到正确的 stsc 条目
            if stsc_idx < len(stsc_entries) - 1 and (chunk_idx + 1) >= stsc_entries[stsc_idx + 1][0]:
                stsc_idx += 1
            first_chunk, samples_per_chunk, _ = stsc_entries[stsc_idx]

            current_offset_in_chunk = 0
            for _ in range(samples_per_chunk):
                if sample_idx >= sample_count: break

                # 为此采样计算时间戳 (pts)
                while stts_sample_idx_in_entry >= stts_count:
                    stts_sample_idx_in_entry -= stts_count
                    current_time += stts_count * stts_duration
                    stts_count, stts_duration = next(stts_iter, (sample_count, 0))
                pts = current_time + stts_sample_idx_in_entry * stts_duration
                stts_sample_idx_in_entry += 1

                size = sample_sizes[sample_idx] if sample_size == 0 else sample_size
                is_keyframe = (sample_idx + 1) in keyframe_set
                samples.append(SampleInfo(chunk_offset + current_offset_in_chunk, size, is_keyframe, sample_idx + 1, pts))
                current_offset_in_chunk += size
                sample_idx += 1

        self.samples = samples

        keyframe_samples = [s for s in self.samples if s.is_keyframe]
        self.keyframes = [
            Keyframe(i, s.index, s.pts, self.timescale)
            for i, s in enumerate(keyframe_samples)
        ]

        log.info(f"完成采样地图构建。共找到 {len(self.samples)} 个样本，其中 {len(self.keyframes)} 个是关键帧。")
