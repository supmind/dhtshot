# -*- coding: utf-8 -*-
"""
本模块提供了 VideoMetadataExtractor 类，其核心功能是使用 PyAV (FFmpeg)
从视频文件中提取关键帧元数据。它取代了之前仅支持 MP4 的手动解析器，
能够处理 FFmpeg 支持的多种视频容器格式（如 MP4, MKV, AVI 等）。
"""
import logging
import io
import struct
from collections import namedtuple
from typing import Optional

import av

# --- 日志配置 ---
log = logging.getLogger(__name__)

# --- 数据结构定义 (保持与旧版兼容) ---
# SampleInfo: 代表一个媒体样本（通常是一帧视频或音频）的详细信息。
# 在 PyAV 的上下文中，这通常对应一个 Packet。
SampleInfo = namedtuple("SampleInfo", ["offset", "size", "is_keyframe", "index", "pts"])
# Keyframe: 代表一个视频关键帧（或同步点）的简化信息。
Keyframe = namedtuple("Keyframe", ["index", "sample_index", "pts", "timescale"])


class VideoMetadataExtractor:
    """
    一个基于 PyAV 的、支持多种格式的视频元数据提取器。

    它的核心功能是打开一个视频文件（以字节形式提供），并解析出解码和定位
    关键帧所需的所有元数据。

    主要工作流程：
    1.  使用 `av.open()` 打开内存中的视频数据。
    2.  自动检测视频流和编解码器信息。
    3.  获取解码器配置 (`extradata`)、时间尺度 (`timescale`) 等参数。
    4.  通过 demuxing 遍历视频的所有数据包 (packets)。
    5.  为每个数据包记录其位置、大小、时间戳和是否为关键帧，构建 `samples` 列表。
    6.  从 `samples` 列表中筛选出关键帧，生成 `keyframes` 列表。
    """

    def __init__(self, video_data: Optional[bytes], resume_info: Optional[dict] = None):
        """
        初始化提取器。

        :param video_data: 包含视频文件头部内容的字节串。
                           如果提供了此参数，将立即开始解析。
        :param resume_info: 一个包含之前序列化状态的字典。如果提供此参数，
                            将从该状态恢复，而不是解析 `video_data`。
        """
        self.samples: list[SampleInfo] = []
        self.keyframes: list[Keyframe] = []
        self.extradata: Optional[bytes] = None
        self.codec_name: Optional[str] = None
        self.container_format: Optional[str] = None
        self.timescale: int = 1
        self.duration_pts: int = 0
        self.nal_length_size: int = 0

        # 为了兼容性，保留这些字段，但它们在新逻辑中可能不再需要
        self.mode: str = "pyav"

        if resume_info:
            self._load_from_resume_info(resume_info)
        elif video_data:
            try:
                self._parse_stream(video_data)
            except Exception as e:
                log.error("使用 PyAV 解析视频流时发生严重错误: %s", e, exc_info=True)
                raise
        else:
            log.info("创建了一个空的 VideoMetadataExtractor 实例。")

    def _load_from_resume_info(self, resume_info: dict):
        """从反序列化的字典中加载提取器状态。"""
        log.info("正在从 resume_info 恢复 VideoMetadataExtractor 状态...")
        self.extradata = resume_info.get('extradata')
        self.codec_name = resume_info.get('codec_name')
        self.container_format = resume_info.get('container_format')
        self.timescale = resume_info.get('timescale', 1)
        self.duration_pts = resume_info.get('duration_pts', 0)
        self.nal_length_size = resume_info.get('nal_length_size', 0)
        self.samples = [SampleInfo(**s) for s in resume_info.get('samples', [])]
        self.keyframes = [Keyframe(**k) for k in resume_info.get('keyframes', [])]
        log.info("成功从 resume_info 恢复状态。")


    def _parse_stream(self, video_data: bytes):
        """
        使用 PyAV 解析视频流的核心逻辑。
        """
        log.info("开始使用 PyAV 解析视频流 (数据大小: %d 字节)...", len(video_data))
        try:
            with av.open(io.BytesIO(video_data)) as container:
                self.container_format = container.format.name
                # 1. 查找第一个视频流
                video_stream = next((s for s in container.streams if s.type == 'video'), None)
                if not video_stream:
                    raise ValueError("在提供的视频数据中未找到视频流。")

                log.info("找到视频流: codec=%s, profile=%s", video_stream.codec_context.name, video_stream.profile)

                # 2. 提取解码器和流信息
                codec_context = video_stream.codec_context
                self.codec_name = codec_context.name
                self.extradata = codec_context.extradata
                self.timescale = video_stream.time_base.denominator
                self.duration_pts = video_stream.duration
                self.nal_length_size = 0  # 默认值

                # 对于 H.264/HEVC，需要从 extradata (avcC/hvcC box) 中手动解析 NAL 长度
                if self.codec_name == 'h264' and self.extradata and len(self.extradata) > 4:
                    self.nal_length_size = (self.extradata[4] & 0x03) + 1
                elif self.codec_name == 'hevc' and self.extradata:
                    if len(self.extradata) > 21:
                        self.nal_length_size = (self.extradata[21] & 0x03) + 1
                    # For HEVC, the extradata (hvcC) also needs to be converted to Annex B
                    self.extradata = self._parse_hvcC_extradata(self.extradata)


                # 3. 遍历数据包以构建样本列表
                sample_idx = 1
                for packet in container.demux(video_stream):
                    # packet.pos 是 packet 在文件中的字节偏移量
                    # 这是替换旧的 'stco'/'co64' 解析的关键
                    if packet.pos is None:
                        # demuxer 可能不总是报告位置，对于我们的用例，这是至关重要的
                        log.warning("数据包 (pts=%s) 缺少位置信息 (pos)，跳过...", packet.pts)
                        continue

                    sample = SampleInfo(
                        offset=packet.pos,
                        size=packet.size,
                        is_keyframe=packet.is_keyframe,
                        index=sample_idx,
                        pts=packet.pts
                    )
                    self.samples.append(sample)
                    sample_idx += 1

                if not self.samples:
                    log.warning("从视频流中未能解析出任何有效的样本/数据包。")
                    return

                # 4. 从样本列表中筛选出关键帧
                keyframe_samples = [s for s in self.samples if s.is_keyframe]
                self.keyframes = [
                    Keyframe(
                        index=i,
                        sample_index=s.index,
                        pts=s.pts,
                        timescale=self.timescale
                    ) for i, s in enumerate(keyframe_samples)
                ]

                log.info("完成视频流解析。共找到 %d 个样本，其中 %d 个是关键帧。", len(self.samples), len(self.keyframes))

        except av.AVError as e:
            log.error("PyAV 在处理视频数据时遇到错误: %s", e)
            # 将其重新包装为更具体的异常，以便上层可以决定是否可恢复
            raise ValueError(f"PyAV error: {e}") from e

    def _parse_hvcC_extradata(self, config_data: bytes) -> bytes:
        """
        解析 hvcC box，提取 VPS, SPS, PPS 等参数集，并将其格式化为
        解码器期望的 Annex B 格式的 extradata。
        """
        stream = io.BytesIO(config_data)
        # hvcC 格式参考: ISO/IEC 14496-15, section 8.3.3.1
        stream.seek(22)  # 跳转到 numOfArrays 字段
        num_of_arrays = struct.unpack('>B', stream.read(1))[0]

        annexb_extradata = bytearray()
        start_code = b'\x00\x00\x00\x01'

        for _ in range(num_of_arrays):
            # 读取数组信息
            stream.read(1)  # array_completeness, reserved, NAL_unit_type
            num_nalus = struct.unpack('>H', stream.read(2))[0]
            for _ in range(num_nalus):
                nalu_len = struct.unpack('>H', stream.read(2))[0]
                nalu_data = stream.read(nalu_len)
                annexb_extradata.extend(start_code)
                annexb_extradata.extend(nalu_data)

        return bytes(annexb_extradata)
