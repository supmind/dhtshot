# -*- coding: utf-8 -*-
"""
本模块提供了用于从视频文件中提取关键帧元数据的提取器基类和具体实现。
它负责解析复杂的视频文件内部结构，以定位解码所需的关键信息。
"""
import struct
import logging
from io import BytesIO
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import BinaryIO, Generator, Tuple, Optional, List

# --- 日志配置 ---
log = logging.getLogger(__name__)

# --- 数据结构定义 ---
SampleInfo = namedtuple("SampleInfo", ["offset", "size", "is_keyframe", "index", "pts"])
Keyframe = namedtuple("Keyframe", ["index", "sample_index", "pts", "timescale"])


class BaseExtractor(ABC):
    """
    提取器抽象基类，定义了所有具体格式提取器（如 MP4, MKV）的通用接口。
    """
    def __init__(self):
        self.keyframes: List[Keyframe] = []
        self.samples: List[SampleInfo] = []
        self.extradata: Optional[bytes] = None
        self.codec_name: Optional[str] = None
        self.mode: str = "unknown"
        self.nal_length_size: int = 4
        self.timescale: int = 1000
        self.duration_pts: int = 0

    @abstractmethod
    def parse(self):
        """
        解析元数据。每个子类必须实现此方法。
        """
        raise NotImplementedError


class MP4Extractor(BaseExtractor):
    """
    一个健壮的 MP4 视频关键帧提取器。
    它的核心功能是仅通过解析 'moov' box 的二进制数据，就能提取出视频流中所有
    关键帧的位置、大小、时间戳和解码所需配置等元数据。
    """

    def __init__(self, moov_data: bytes):
        """
        初始化 MP4 提取器。
        :param moov_data: 包含 'moov' box 完整内容的字节串。
        """
        super().__init__()
        self.moov_stream = BytesIO(moov_data)
        if moov_data:
            try:
                self.parse()
            except Exception as e:
                log.error("解析 'moov' box 时发生严重错误: %s", e, exc_info=True)
                raise

    def parse(self):
        """
        解析 'moov' box 的核心逻辑。
        """
        self._parse_structure()

    def _parse_boxes(self, stream: BinaryIO) -> Generator[Tuple[str, BytesIO], None, None]:
        """
        一个 MP4 box 解析器生成器。
        """
        while True:
            current_offset = stream.tell()
            header_data = stream.read(8)
            if not header_data or len(header_data) < 8: break

            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii', 'ignore')
            header_size = 8

            if size == 1: # 64位大小
                size_64_data = stream.read(8)
                if not size_64_data: break
                size = struct.unpack('>Q', size_64_data)[0]
                header_size = 16
            elif size == 0: # Box 延伸到文件末尾
                current_pos = stream.tell()
                stream.seek(0, 2)
                size = stream.tell() - (current_pos - header_size)
                stream.seek(current_pos)

            payload_size = size - header_size
            if payload_size < 0:
                log.warning("在 Box '%s' 中检测到无效的 payload 大小 (%d)，停止解析。", box_type, payload_size)
                break

            payload_content = stream.read(payload_size)
            if len(payload_content) < payload_size:
                log.warning("无法读取 Box '%s' 的完整 payload，停止解析。", box_type)
                break

            yield box_type, BytesIO(payload_content)

    def _find_box_payload(self, stream: BinaryIO, box_path: list[str]) -> Optional[BytesIO]:
        """
        从流的当前位置开始，递归地查找一个按路径指定的嵌套 Box，并返回其 payload。
        """
        if not box_path: return stream
        target_box, remaining_path = box_path[0], box_path[1:]
        for box_type, payload in self._parse_boxes(stream):
            if box_type == target_box:
                return self._find_box_payload(payload, remaining_path)
        return None

    def _parse_structure(self):
        """
        解析 'moov' box 的结构，找到视频轨道并构建采样地图。
        """
        stream = self.moov_stream
        stream.seek(0)
        _, box_type = struct.unpack('>I4s', stream.read(8))
        stream.seek(0)
        if box_type == b'moov':
            for b_type, payload in self._parse_boxes(stream):
                if b_type == 'moov':
                    stream = payload; break
        moov_payload = stream; moov_payload.seek(0)

        trak_payload = None
        for t_type, t_payload_iter in self._parse_boxes(moov_payload):
            if t_type == 'trak':
                current_pos = t_payload_iter.tell()
                hdlr_payload = self._find_box_payload(t_payload_iter, ['mdia', 'hdlr'])
                t_payload_iter.seek(current_pos)
                if hdlr_payload:
                    hdlr_payload.seek(8)
                    if hdlr_payload.read(4) == b'vide':
                        trak_payload = t_payload_iter; break
        if not trak_payload: raise ValueError("在 'moov' Box 中未找到有效的视频轨道。")

        mdhd_payload = self._find_box_payload(trak_payload, ['mdia', 'mdhd'])
        if mdhd_payload:
            version = struct.unpack('>B', mdhd_payload.read(1))[0]
            if version == 0:
                mdhd_payload.seek(12)
                self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
                self.duration_pts = struct.unpack('>I', mdhd_payload.read(4))[0]
            else:
                mdhd_payload.seek(20)
                self.timescale = struct.unpack('>I', mdhd_payload.read(4))[0]
                self.duration_pts = struct.unpack('>Q', mdhd_payload.read(8))[0]
        else:
            log.warning("在视频轨道中未找到 'mdhd' box，无法确定 timescale 和 duration。")
        trak_payload.seek(0)

        stbl_payload = self._find_box_payload(trak_payload, ['mdia', 'minf', 'stbl'])
        if not stbl_payload: raise ValueError("在视频轨道中未找到 'stbl' Box。")

        tables = {b_type: b_payload for b_type, b_payload in self._parse_boxes(stbl_payload)}
        self._build_sample_map_and_config(tables)

    def _build_sample_map_and_config(self, tables: dict):
        """
        解析 stbl 内的各个表，以获取解码器配置 (extradata) 并构建完整的采样地图。
        """
        stsd_payload = tables.get('stsd')
        if not stsd_payload: raise ValueError("在 'stbl' Box 中未找到 'stsd' Box。")
        stsd_payload.seek(8)

        codec_configs = {
            'avc1': {'codec': 'h264', 'config_box': 'avcC'}, 'avc3': {'codec': 'h264'},
            'hvc1': {'codec': 'hevc', 'config_box': 'hvcC'}, 'hev1': {'codec': 'hevc', 'config_box': 'hvcC'},
            'av01': {'codec': 'av1', 'config_box': 'av1C'},
        }
        found_codec = False
        for sample_entry_type, sample_entry_payload in self._parse_boxes(stsd_payload):
            if sample_entry_type in codec_configs:
                config = codec_configs[sample_entry_type]
                self.codec_name = config['codec']
                config_box_name = config.get('config_box')

                if config_box_name:
                    sample_entry_payload.seek(78)
                    config_box_payload = self._find_box_payload(sample_entry_payload, [config_box_name])
                    if config_box_payload and len(config_box_payload.getvalue()) > 5:
                        self.mode = 'avc1'
                        config_data = config_box_payload.getvalue()

                        if self.codec_name == 'h264':
                            self.extradata = config_data
                            self.nal_length_size = (config_data[4] & 0x03) + 1
                        elif self.codec_name == 'hevc':
                            self.extradata = self._parse_hvcC_extradata(config_data)
                            self.nal_length_size = (config_data[21] & 0x03) + 1
                        elif self.codec_name == 'av1':
                            self.extradata = config_data
                    else:
                        self.mode = 'avc3'
                else:
                    self.mode = 'avc3'

                found_codec = True; break
        if not found_codec: raise ValueError("在 'stsd' Box 中未找到任何受支持的视频采样条目 (H.264, H.265, AV1)。")

        stsz_payload = tables.get('stsz'); stsz_payload.seek(4)
        sample_size, sample_count = struct.unpack('>II', stsz_payload.read(8))
        sample_sizes = list(struct.unpack(f'>{sample_count}I', stsz_payload.read(sample_count * 4))) if sample_size == 0 else []

        stss_payload = tables.get('stss'); keyframe_set = set()
        if stss_payload:
            stss_payload.seek(4); entry_count = struct.unpack('>I', stss_payload.read(4))[0]
            if entry_count > 0: keyframe_set = set(struct.unpack(f'>{entry_count}I', stss_payload.read(entry_count * 4)))
        else: keyframe_set = set(range(1, sample_count + 1))

        co_box = tables.get('stco') or tables.get('co64')
        if not co_box: raise ValueError("未找到 'stco' 或 'co64' box。")
        co_payload = co_box; co_payload.seek(4)
        entry_count = struct.unpack('>I', co_payload.read(4))[0]
        unpack_char = '>I' if tables.get('stco') else '>Q'
        chunk_offsets = struct.unpack(f'>{entry_count}{unpack_char[-1]}', co_payload.read(entry_count * struct.calcsize(unpack_char)))

        stsc_payload = tables.get('stsc'); stsc_payload.seek(4)
        entry_count = struct.unpack('>I', stsc_payload.read(4))[0]
        stsc_entries = [struct.unpack('>III', stsc_payload.read(12)) for _ in range(entry_count)]

        stts_payload = tables.get('stts'); stts_payload.seek(4)
        entry_count = struct.unpack('>I', stts_payload.read(4))[0]
        stts_entries = [struct.unpack('>II', stts_payload.read(8)) for _ in range(entry_count)]

        samples, stsc_idx, sample_idx, current_time = [], 0, 0, 0
        stts_iter = iter(stts_entries); stts_count, stts_duration = next(stts_iter, (0, 0)); stts_sample_idx_in_entry = 0

        for chunk_idx, chunk_offset in enumerate(chunk_offsets):
            if stsc_idx < len(stsc_entries) - 1 and (chunk_idx + 1) >= stsc_entries[stsc_idx + 1][0]: stsc_idx += 1
            _, samples_per_chunk, _ = stsc_entries[stsc_idx]
            current_offset_in_chunk = 0
            for _ in range(samples_per_chunk):
                if sample_idx >= sample_count: break
                while stts_sample_idx_in_entry >= stts_count:
                    stts_sample_idx_in_entry -= stts_count; current_time += stts_count * stts_duration
                    stts_count, stts_duration = next(stts_iter, (sample_count, 0))
                pts = current_time + stts_sample_idx_in_entry * stts_duration
                stts_sample_idx_in_entry += 1
                size = sample_sizes[sample_idx] if sample_size == 0 else sample_size
                is_keyframe = (sample_idx + 1) in keyframe_set
                samples.append(SampleInfo(chunk_offset + current_offset_in_chunk, size, is_keyframe, sample_idx + 1, pts))
                current_offset_in_chunk += size; sample_idx += 1

        self.samples = samples
        keyframe_samples = [s for s in self.samples if s.is_keyframe]
        self.keyframes = [Keyframe(i, s.index, s.pts, self.timescale) for i, s in enumerate(keyframe_samples)]
        log.info("完成采样地图构建。共找到 %d 个样本，其中 %d 个是关键帧。", len(self.samples), len(self.keyframes))

    def _parse_hvcC_extradata(self, config_data: bytes) -> bytes:
        """
        解析 hvcC box，提取 VPS, SPS, PPS 等参数集，并将其格式化为
        解码器期望的 Annex B 格式的 extradata。
        """
        stream = BytesIO(config_data)
        stream.seek(22)
        num_of_arrays = struct.unpack('>B', stream.read(1))[0]

        annexb_extradata = bytearray()
        start_code = b'\x00\x00\x00\x01'

        for _ in range(num_of_arrays):
            stream.read(1)
            num_nalus = struct.unpack('>H', stream.read(2))[0]
            for _ in range(num_nalus):
                nalu_len = struct.unpack('>H', stream.read(2))[0]
                nalu_data = stream.read(nalu_len)
                annexb_extradata.extend(start_code)
                annexb_extradata.extend(nalu_data)

        return bytes(annexb_extradata)


class MKVExtractor(BaseExtractor):
    """
    一个 MKV 视频关键帧提取器。
    此类将负责解析 MKV (Matroska) 文件格式，特别是 'Cues' (线索点)
    部分，以提取关键帧的元数据。
    """
    def __init__(self, head_data: bytes, cues_data: bytes):
        super().__init__()
        self.head_stream = BytesIO(head_data)
        self.cues_stream = BytesIO(cues_data)
        self.schema = None

    def parse(self):
        """
        解析 MKV 元数据。
        这需要一个分阶段的方法，因为元数据（如 Cues）可能不在文件的开头。
        此方法首先解析头部和轨道信息。
        """
        try:
            from ebmlite import loadSchema
            self.schema = loadSchema('matroska.xml')
        except ImportError:
            log.error("未能导入 ebmlite 库。请确保它已正确安装。")
            raise
        except FileNotFoundError:
            log.error("未能找到 matroska.xml schema 文件。")
            raise

        head_doc = self.schema.loads(self.head_stream.getvalue())
        self._parse_tracks(head_doc)

        cues_doc = self.schema.loads(self.cues_stream.getvalue())
        self._parse_cues(cues_doc)

    def _parse_tracks(self, doc):
        """ 从 MKV 文档中解析轨道信息，找到视频轨道并提取编解码器数据。 """
        segment = next((el for el in doc if el.name == 'Segment'), None)
        if not segment: return

        ts_element = next((el for el in segment if el.name == 'TimecodeScale'), None)
        if ts_element:
            self.timescale = ts_element.value

        tracks = next((el for el in segment if el.name == 'Tracks'), None)
        if not tracks: return

        for track_entry in (el for el in tracks if el.name == 'TrackEntry'):
            children = {el.name: el for el in track_entry}
            track_type = children.get('TrackType')

            if track_type and track_type.value == 1: # 1 = video track
                codec_id_el = children.get('CodecID')
                if codec_id_el:
                    self.codec_name = self._map_codec_id(codec_id_el.value)

                private_data_el = children.get('CodecPrivate')
                if private_data_el:
                    self.extradata = private_data_el.value

                return

    def _parse_cues(self, doc):
        """ 从 MKV 文档中解析 Cues (线索点) 以构建关键帧列表。 """
        segment = next((el for el in doc if el.name == 'Segment'), None)
        if not segment: return

        cues = next((el for el in segment if el.name == 'Cues'), None)
        if not cues:
            log.info("在提供的 MKV 数据中未找到 Cues 元素。")
            return

        samples = []
        timescale_ns = self.timescale

        for i, cue_point in enumerate(el for el in cues if el.name == 'CuePoint'):
            cue_children = {el.name: el for el in cue_point}
            cue_time_el = cue_children.get('CueTime')
            if not cue_time_el: continue

            track_pos = cue_children.get('CueTrackPositions')
            if not track_pos: continue

            track_pos_children = {el.name: el for el in track_pos}
            cluster_offset_el = track_pos_children.get('CueClusterPosition')
            if not cluster_offset_el: continue

            cue_time_ns = cue_time_el.value
            cluster_offset = cluster_offset_el.value

            # Note: MKV Cues do not provide sample sizes. We use 0 as a placeholder.
            # The service layer will need to handle fetching potentially more data than needed.
            sample = SampleInfo(
                offset=cluster_offset,
                size=0, # Placeholder
                is_keyframe=True,
                index=i + 1, # Sample indices are 1-based
                pts=int(cue_time_ns / (timescale_ns / self.timescale))
            )
            samples.append(sample)

        self.samples = samples
        self.keyframes = [Keyframe(i, s.index, s.pts, self.timescale) for i, s in enumerate(self.samples)]
        log.info("从 Cues 构建了 %d 个样本和 %d 个关键帧。", len(self.samples), len(self.keyframes))


    def _map_codec_id(self, codec_id: str) -> Optional[str]:
        """ 将 MKV 的 CodecID 映射到我们内部使用的编解码器名称。 """
        mapping = {
            "V_MPEG4/ISO/AVC": "h264",
            "V_MPEGH/ISO/HEVC": "hevc",
            "V_AV1": "av1",
        }
        return mapping.get(codec_id)
