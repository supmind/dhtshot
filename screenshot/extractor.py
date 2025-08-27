# -*- coding: utf-8 -*-
"""
MP4 H.264 关键帧提取器 (生产级重构版)

功能:
- 从一个 'moov' box 的字节数据中精确解析 MP4 文件结构，并提取 H.264 关键帧元数据。
- 采用健壮的启发式逻辑，以应对现实世界中不完全符合规范的 MP4 文件。
- 异步地从数据源读取关键帧数据包。
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

    def __init__(self, moov_data: bytes, reader: Any):
        """
        初始化提取器。
        :param moov_data: 包含 'moov' box 完整内容的字节串。
        :param reader: 一个异步的、类文件读取器 (例如 AsyncTorrentReader)，
                       必须实现 async seek() 和 async read()。
        """
        self.reader = reader
        self.moov_stream = BytesIO(moov_data)
        self.keyframes: list[Keyframe] = []
        self.samples: list[SampleInfo] = []
        self.extradata: Optional[bytes] = None
        self.mode: str = "unknown"
        self.nal_length_size: int = 4
        self.timescale: int = 1000

        try:
            self._parse_moov()
        except Exception as e:
            log.error(f"解析 'moov' box 时发生严重错误: {e}", exc_info=True)
            # 即使解析失败，也允许对象创建，但 keyframes 列表将为空。
            pass

    def _parse_boxes(self, stream: BinaryIO) -> Generator[Tuple[str, BytesIO], None, None]:
        """同步解析一个流中的所有 box。"""
        while True:
            header_data = stream.read(8)
            if not header_data or len(header_data) < 8: break
            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii', 'ignore')
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

    def _parse_moov(self):
        """解析 'moov' box 的内容来构建采样地图和解码器配置。"""
        # 从 'moov' box 开始，找到视频轨道
        trak_payload = None
        for t_type, t_payload_iter in self._parse_boxes(self.moov_stream):
            if t_type == 'trak':
                # 检查 hdlr box 确定这是不是视频轨道
                hdlr_payload = self._find_box_payload(t_payload_iter, ['mdia', 'hdlr'])
                if hdlr_payload:
                    hdlr_payload.seek(8) # version, flags, pre_defined
                    handler_type = hdlr_payload.read(4)
                    if handler_type == b'vide':
                        trak_payload = t_payload_iter
                        break # 找到第一个视频轨道就停止

        if not trak_payload: raise ValueError("在 'moov' Box 中未找到视频轨道。")
        trak_payload.seek(0) # 显式重置

        # 获取 timescale
        mdhd_payload = self._find_box_payload(trak_payload, ['mdia', 'mdhd'])
        if mdhd_payload:
            version = struct.unpack('>B', mdhd_payload.read(1))[0]
            mdhd_payload.seek(12 if version == 0 else 20)
            self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
        trak_payload.seek(0)

        # 找到 stbl box
        stbl_payload = self._find_box_payload(trak_payload, ['mdia', 'minf', 'stbl'])
        if not stbl_payload: raise ValueError("在视频轨道中未找到 'stbl' Box。")

        # 将 stbl 中的所有表解析并存入字典
        tables = {b_type: b_payload for b_type, b_payload in self._parse_boxes(stbl_payload)}

        self._build_sample_map_and_config(tables)

    def _build_sample_map_and_config(self, tables: dict):
        # --- 解码模式和 extradata ---
        stsd_payload = tables.get('stsd')
        if not stsd_payload: raise ValueError("在 'stbl' Box 中未找到 'stsd' Box。")
        stsd_payload.seek(8) # version, flags, entry_count

        avc1_payload = self._find_box_payload(stsd_payload, ['avc1'])
        if avc1_payload:
            log.info("检测到 'avc1' 采样条目，正在检查其 'avcC' Box...")
            avc1_payload.seek(78) # Skip reserved, data_ref_index, etc.
            avcc_payload_stream = self._find_box_payload(avc1_payload, ['avcC'])

            if avcc_payload_stream:
                avcc_payload = avcc_payload_stream.getvalue()
                if len(avcc_payload) > 5:
                    self.mode = 'avc1'
                    self.extradata = avcc_payload
                    self.nal_length_size = (avcc_payload[4] & 0x03) + 1
                    log.info(f"找到有效的 'avcC' Box，将使用 '带外' 模式 (NALU 长度: {self.nal_length_size} 字节)。")
                else:
                    self.mode = 'avc3'
                    log.warning("'avc1' 条目中的 'avcC' Box 无效，将回退到 '带内' (Annex B) 模式。")
            else:
                self.mode = 'avc3'
                log.warning("'avc1' 条目缺少 'avcC' Box，将回退到 '带内' (Annex B) 模式。")
        else:
            stsd_payload.seek(8) # 重置以查找 avc3
            avc3_payload = self._find_box_payload(stsd_payload, ['avc3'])
            if avc3_payload:
                self.mode = 'avc3'
                log.info("检测到 'avc3' 采样条目，将使用 '带内' (Annex B) 模式。")
            else:
                raise ValueError("在 'stsd' Box 中既未找到 'avc1' 也未找到 'avc3' 采样条目。")

        # --- 构建采样地图 ---
        stsz_payload = tables.get('stsz'); stsz_payload.seek(4) # version, flags
        sample_size, sample_count = struct.unpack('>II', stsz_payload.read(8))
        sample_sizes = []
        if sample_size == 0: sample_sizes = struct.unpack(f'>{sample_count}I', stsz_payload.read(sample_count * 4))

        stss_payload = tables.get('stss'); keyframe_set = set()
        if stss_payload:
            stss_payload.seek(4); entry_count = struct.unpack('>I', stss_payload.read(4))[0]
            keyframe_set = set(struct.unpack(f'>{entry_count}I', stss_payload.read(entry_count * 4)))

        co_box = tables.get('stco') or tables.get('co64'); co_payload = co_box; co_payload.seek(4)
        entry_count = struct.unpack('>I', co_payload.read(4))[0]
        unpack_char = '>I' if tables.get('stco') else '>Q'
        chunk_offsets = struct.unpack(f'>{entry_count}{unpack_char[-1]}', co_payload.read(entry_count * struct.calcsize(unpack_char)))

        stsc_payload = tables.get('stsc'); stsc_payload.seek(4)
        entry_count = struct.unpack('>I', stsc_payload.read(4))[0]
        stsc_entries = [struct.unpack('>III', stsc_payload.read(12)) for _ in range(entry_count)]

        stts_payload = tables.get('stts'); stts_payload.seek(4)
        entry_count = struct.unpack('>I', stts_payload.read(4))[0]
        stts_entries = [struct.unpack('>II', stts_payload.read(8)) for _ in range(entry_count)]

        # --- 迭代生成所有样本信息 ---
        samples = []; stsc_idx, sample_idx, current_time = 0, 0, 0
        stts_iter = iter(stts_entries)
        stts_count, stts_duration = next(stts_iter, (0, 0))
        stts_sample_idx = 0

        for chunk_idx, chunk_offset in enumerate(chunk_offsets):
            if stsc_idx < len(stsc_entries) - 1 and (chunk_idx + 1) >= stsc_entries[stsc_idx + 1][0]:
                stsc_idx += 1
            samples_per_chunk = stsc_entries[stsc_idx][1]
            current_offset_in_chunk = 0
            for _ in range(samples_per_chunk):
                if sample_idx >= sample_count: break

                # 计算时间戳 (pts)
                while stts_sample_idx >= stts_count:
                    stts_sample_idx -= stts_count
                    current_time += stts_count * stts_duration
                    stts_count, stts_duration = next(stts_iter, (sample_count, 0))
                pts = current_time + stts_sample_idx * stts_duration
                stts_sample_idx += 1

                size = sample_sizes[sample_idx] if sample_size == 0 else sample_size
                is_keyframe = (sample_idx + 1) in keyframe_set
                samples.append(SampleInfo(chunk_offset + current_offset_in_chunk, size, is_keyframe, sample_idx + 1, pts))
                current_offset_in_chunk += size
                sample_idx += 1

        self.samples = samples
        self.keyframes = [
            Keyframe(i, s.index, s.pts, self.timescale)
            for i, s in enumerate(samples) if s.is_keyframe
        ]

    async def get_keyframe_packet(self, keyframe_index: int) -> Tuple[Optional[bytes], bytes]:
        """异步获取并处理一个关键帧的数据包。"""
        if not (0 <= keyframe_index < len(self.keyframes)):
            raise IndexError(f"关键帧索引 {keyframe_index} 越界。")

        target_sample_index = self.keyframes[keyframe_index].sample_index
        target_sample = self.samples[target_sample_index - 1]

        log.info(f"正在提取第 {keyframe_index} 个关键帧: 样本索引={target_sample.index}, 偏移={target_sample.offset}, 大小={target_sample.size}")

        await self.reader.seek(target_sample.offset)
        sample_data = await self.reader.read(target_sample.size)

        if len(sample_data) != target_sample.size:
            raise IOError(f"读取关键帧 {keyframe_index} 的数据不完整。")

        if self.mode == 'avc1':
            annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
            while cursor < len(sample_data):
                try:
                    nal_length = int.from_bytes(sample_data[cursor : cursor + self.nal_length_size], 'big')
                    cursor += self.nal_length_size
                    nal_data = sample_data[cursor : cursor + nal_length]
                    if len(nal_data) < nal_length:
                        log.warning("样本数据不完整，NALU 数据被截断。")
                        break
                    annexb_data.extend(start_code + nal_data)
                    cursor += nal_length
                except Exception:
                    log.error("解析 NALU 长度时出错，样本数据可能已损坏。")
                    break
            return self.extradata, bytes(annexb_data)
        else: # avc3 mode
            return None, sample_data
