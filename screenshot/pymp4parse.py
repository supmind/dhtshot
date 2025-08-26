# -*- coding: utf-8 -*-
"""
一个轻量级的 MP4/F4V 文件格式解析器。
主要基于 Adob​​e 的视频文件格式规范 V10.1 版本。
http://download.macromedia.com/f4v/video_file_format_spec_v10_1.pdf

该解析器的核心功能是解析 MP4 文件中的 box（或称 atom），特别是 'moov' box 及其子 box，
因为它们包含了定位关键帧所需的元数据。

@author: Alastair McCormack
@license: MIT License
"""

import bitstring
from collections import namedtuple
import logging
import struct

# 设置日志记录器
log = logging.getLogger(__name__)
log.setLevel(logging.WARN)

# ==============================================================================
# Box 类定义
# ==============================================================================

class MixinDictRepr(object):
    """
    一个简单的 mixin 类，用于提供一个更具可读性的 __repr__ 方法，
    它会以字典形式显示 box 实例的属性。
    """
    def __repr__(self, *args, **kwargs):
        return f"{self.__class__.__name__}: {self.__dict__}"

class GenericContainerBox(MixinDictRepr):
    """
    通用容器 Box 类。这类 Box 本身不包含具体数据，而是作为其他 Box 的父容器。
    例如 'moov', 'trak' 等。
    """
    def __init__(self, header):
        self.header = header
        self.children = []

    def add_child(self, box):
        """
        向容器中添加一个子 Box。
        为了方便访问，如果子 Box 的类型字符串是合法的 Python 标识符，
        会尝试将其作为一个属性直接添加到实例上。
        例如，一个 'moov' Box 添加了一个 'trak' 子 Box 后，可以通过 `moov_box.trak` 来访问。
        如果存在多个同类型的子 Box，则会将它们存储在一个列表中。
        """
        self.children.append(box)
        # 将 box_type 从字节解码为字符串以便处理
        box_type_str = box.header.box_type.decode('utf-8') if isinstance(box.header.box_type, bytes) else box.header.box_type
        if box_type_str.isidentifier():
            if hasattr(self, box_type_str):
                # 如果已存在同名属性
                existing = getattr(self, box_type_str)
                if isinstance(existing, list):
                    existing.append(box)  # 如果是列表，直接追加
                else:
                    setattr(self, box_type_str, [existing, box])  # 否则，创建一个新列表
            else:
                setattr(self, box_type_str, box)

class UnImplementedBox(MixinDictRepr):
    """
    用于表示解析器尚未实现或不需要解析其具体内容的 Box。
    解析器在遇到未知类型的 Box 时会使用此类，并跳过其内容。
    """
    def __init__(self, header):
        self.header = header
    type = "unimplemented"

class FullBox(MixinDictRepr):
    """
    代表一个 "Full Box"。根据 MP4 规范，Full Box 是标准 Box 的扩展，
    在其内容开始处包含一个8位的版本号（version）和24位的标志（flags）。
    """
    def __init__(self, header):
        self.header = header
        self.version = 0
        self.flags = 0

# --- 以下是各种具体的 Box 类型的定义 ---

class TrackHeaderBox(FullBox): type = "tkhd"
class MediaHeaderBox(FullBox): type = "mdhd"
class SyncSampleBox(FullBox): type = "stss"
class TimeToSampleBox(FullBox): type = "stts"
class SampleToChunkBox(FullBox): type = "stsc"
class SampleSizeBox(FullBox): type = "stsz"
class ChunkOffsetBox(FullBox): type = "stco"
class ChunkLargeOffsetBox(FullBox): type = "co64"

class AvcConfigurationBox(MixinDictRepr): # avcC
    type = "avcC"
    def __init__(self, header):
        self.header = header
        self.sps_list = []
        self.pps_list = []

class SampleDescriptionBox(FullBox): # stsd
    type = "stsd"
    def __init__(self, header):
        super().__init__(header)
        self.children = []

    def add_child(self, box):
        self.children.append(box)

# --- 容器 Box 的具体实现 ---
class MovieBox(GenericContainerBox): type = "moov"
class TrackBox(GenericContainerBox): type = "trak"
class MediaBox(GenericContainerBox): type = "mdia"
class MediaInformationBox(GenericContainerBox): type = "minf"
class SampleTableBox(GenericContainerBox): type = "stbl"

# 使用 namedtuple 定义一个清晰的 BoxHeader 结构
BoxHeader = namedtuple("BoxHeader", [
    "box_size", "box_type", "header_size", "start_offset"
])

# ==============================================================================
# 解析器主类
# ==============================================================================

class F4VParser(object):
    @classmethod
    def parse(cls, filename=None, bytes_input=None, file_input=None, offset_bytes=0):
        """
        MP4 Box 解析器的核心方法。这是一个生成器。
        """
        box_lookup = {
            "moov": cls._parse_container_box, "trak": cls._parse_container_box,
            "mdia": cls._parse_container_box, "minf": cls._parse_container_box,
            "stbl": cls._parse_container_box,
            "tkhd": cls._parse_tkhd, "mdhd": cls._parse_mdhd,
            "stss": cls._parse_stss, "stts": cls._parse_stts,
            "stsc": cls._parse_stsc, "stsz": cls._parse_stsz,
            "stco": cls._parse_stco, "co64": cls._parse_co64,
            "stsd": cls._parse_stsd, "avc1": cls._parse_container_box,
            "avcC": cls._parse_avcC,
        }

        if filename: bs = bitstring.ConstBitStream(filename=filename, offset=offset_bytes * 8)
        elif bytes_input: bs = bitstring.ConstBitStream(bytes=bytes_input, offset=offset_bytes * 8)
        else: bs = bitstring.ConstBitStream(auto=file_input, offset=offset_bytes * 8)

        while bs.pos < bs.len:
            try:
                header = cls._read_box_header(bs)
            except bitstring.ReadError:
                break

            box_type_str = header.box_type
            parse_function = box_lookup.get(box_type_str, cls._parse_unimplemented)

            try:
                yield parse_function(bs, header)
            except (bitstring.ReadError, ValueError) as e:
                log.warning(f"解析 Box '{box_type_str}' 失败: {e}。正在跳过。")
                next_box_pos = header.start_offset + header.header_size + header.box_size
                if next_box_pos * 8 < bs.len:
                    bs.bytepos = next_box_pos
                else:
                    log.warning(f"无法安全地恢复解析，因为下一个 box 的位置 ({next_box_pos}) 超出范围。")
                    break
                continue

    @staticmethod
    def _read_box_header(bs):
        header_start_pos = bs.bytepos
        size, box_type_bytes = bs.readlist("uint:32, bytes:4")
        box_type = box_type_bytes.decode('ascii', errors='ignore')
        header_size = 8
        if size == 1:
            size = bs.read("uint:64")
            header_size = 16
        content_size = size - header_size if size > header_size else 0
        return BoxHeader(box_size=content_size, box_type=box_type, header_size=header_size, start_offset=header_start_pos)

    @classmethod
    def _parse_container_box(cls, bs, header):
        box_type_map = {
            "moov": MovieBox, "trak": TrackBox, "mdia": MediaBox,
            "minf": MediaInformationBox, "stbl": SampleTableBox,
            "avc1": GenericContainerBox,
        }
        box = box_type_map.get(header.box_type, GenericContainerBox)(header)
        content_bs = bs.read(header.box_size * 8)
        for child in cls.parse(bytes_input=content_bs.bytes):
            box.add_child(child)
        return box

    @staticmethod
    def _parse_full_box_header(bs):
        version = bs.read("uint:8")
        flags = bs.read("uint:24")
        return version, flags

    @classmethod
    def _parse_tkhd(cls, bs, header):
        box = TrackHeaderBox(header)
        content_bs = bs.read(header.box_size * 8)
        box.version, box.flags = cls._parse_full_box_header(content_bs)
        if box.version == 1:
            content_bs.pos += 8 * (8 + 8 + 4 + 4)
            box.duration = content_bs.read("uint:64")
        else:
            content_bs.pos += 4 * (4 + 4 + 4 + 4)
            box.duration = content_bs.read("uint:32")
        return box

    @classmethod
    def _parse_mdhd(cls, bs, header):
        box = MediaHeaderBox(header)
        content_bs = bs.read(header.box_size * 8)
        box.version, box.flags = cls._parse_full_box_header(content_bs)
        if box.version == 1:
            content_bs.pos += 8 * (8 + 8)
            box.timescale = content_bs.read("uint:32")
            box.duration = content_bs.read("uint:64")
        else:
            content_bs.pos += 4 * (4 + 4)
            box.timescale = content_bs.read("uint:32")
            box.duration = content_bs.read("uint:32")
        return box

    @classmethod
    def _parse_stsd(cls, bs, header):
        box = SampleDescriptionBox(header)
        content_bs = bs.read(header.box_size * 8)
        box.version, box.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        for _ in range(entry_count):
            for child in cls.parse(bytes_input=content_bs.bytes[content_bs.bytepos:]):
                box.add_child(child)
        return box

    @classmethod
    def _parse_avcC(cls, bs, header):
        box = AvcConfigurationBox(header)
        content_bs = bs.read(header.box_size * 8)
        content_bs.pos += 8 * 5 # Skip version, profile, compatibility, level, lengthSizeMinusOne
        num_sps = content_bs.read("uint:8") & 0x1F
        for _ in range(num_sps):
            sps_size = content_bs.read("uint:16")
            box.sps_list.append(content_bs.read(sps_size * 8).bytes)
        num_pps = content_bs.read("uint:8")
        for _ in range(num_pps):
            pps_size = content_bs.read("uint:16")
            box.pps_list.append(content_bs.read(pps_size * 8).bytes)
        return box

    @classmethod
    def _parse_stss(cls, bs, header):
        stss = SyncSampleBox(header)
        content_bs = bs.read(header.box_size * 8)
        stss.version, stss.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stss.entries = [content_bs.read("uint:32") for _ in range(entry_count)]
        return stss

    @classmethod
    def _parse_stts(cls, bs, header):
        stts = TimeToSampleBox(header)
        content_bs = bs.read(header.box_size * 8)
        stts.version, stts.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stts.entries = [(content_bs.read("uint:32"), content_bs.read("uint:32")) for _ in range(entry_count)]
        return stts

    @classmethod
    def _parse_stsc(cls, bs, header):
        stsc = SampleToChunkBox(header)
        content_bs = bs.read(header.box_size * 8)
        stsc.version, stsc.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stsc.entries = [(content_bs.read("uint:32"), content_bs.read("uint:32"), content_bs.read("uint:32")) for _ in range(entry_count)]
        return stsc

    @classmethod
    def _parse_stsz(cls, bs, header):
        stsz = SampleSizeBox(header)
        content_bs = bs.read(header.box_size * 8)
        stsz.version, stsz.flags = cls._parse_full_box_header(content_bs)
        stsz.sample_size = content_bs.read("uint:32")
        sample_count = content_bs.read("uint:32")
        if stsz.sample_size == 0:
            stsz.entries = [content_bs.read("uint:32") for _ in range(sample_count)]
        return stsz

    @classmethod
    def _parse_stco(cls, bs, header):
        stco = ChunkOffsetBox(header)
        content_bs = bs.read(header.box_size * 8)
        stco.version, stco.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        stco.entries = [content_bs.read("uint:32") for _ in range(entry_count)]
        return stco

    @classmethod
    def _parse_co64(cls, bs, header):
        co64 = ChunkLargeOffsetBox(header)
        content_bs = bs.read(header.box_size * 8)
        co64.version, co64.flags = cls._parse_full_box_header(content_bs)
        entry_count = content_bs.read("uint:32")
        co64.entries = [content_bs.read("uint:64") for _ in range(entry_count)]
        return co64

    @staticmethod
    def _parse_unimplemented(bs, header):
        ui = UnImplementedBox(header)
        bs.pos += header.box_size * 8
        return ui

    @staticmethod
    def find_child_box(box, path):
        current_box = box
        for box_type in path:
            found_child = None
            if hasattr(current_box, 'children'):
                for child in current_box.children:
                    if child.header.box_type == box_type:
                        found_child = child
                        break

            if found_child is None and hasattr(current_box, box_type):
                child = getattr(current_box, box_type)
                found_child = child[0] if isinstance(child, list) else child

            if found_child:
                current_box = found_child
            else:
                return None
        return current_box
