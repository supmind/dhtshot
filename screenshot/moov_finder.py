# -*- coding: utf-8 -*-
"""
本模块定义了 MoovFinder 类，它负责以智能方式从 torrent 中的视频文件里
查找并提取 'moov' atom。'moov' atom 对于解码视频至关重要。
"""
import io
import logging
import struct
from typing import Generator, Tuple

from .errors import MoovFetchError, MoovNotFoundError, MP4ParsingError
from .piece_manager import PieceManager
from config import Settings


class MoovFinder:
    """
    使用智能探测策略来查找和提取视频文件中的 'moov' atom，以最小化下载量。

    工作原理：
    MP4 文件中的 'moov' atom 通常位于文件的开头或结尾。此查找器利用这一特性：
    1.  首先探测文件头部的一小部分数据。
    2.  如果在头部找到 'moov'，则任务完成。
    3.  如果在头部找到 'mdat' (媒体数据) atom，则可以推断 'moov' atom
        很可能在 'mdat' atom 之后，即文件的尾部。然后探测器会下载并检查尾部数据。
    4.  如果两种策略都失败，则认为无法找到 'moov' atom。
    """
    def __init__(self, piece_manager: PieceManager, video_file_offset: int, video_file_size: int, settings: Settings, infohash_hex: str):
        self.piece_manager = piece_manager
        self.video_file_offset = video_file_offset
        self.video_file_size = video_file_size
        self.settings = settings
        self.infohash_hex = infohash_hex
        self.log = logging.getLogger("MoovFinder")

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        """
        一个健壮的 MP4 box 解析器，可以跳过无效或垃圾数据。
        这对于处理来自 torrent 的、可能不完整的数据流至关重要。
        """
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)
        current_offset = stream.tell()

        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)
            try:
                header_data = stream.read(8)
                if len(header_data) < 8:
                    break

                declared_size, box_type_bytes = struct.unpack('>I4s', header_data)

                # --- 增强的验证逻辑 ---
                # box 的声明大小不能小于其头部大小，也不能大于缓冲区剩余大小。
                if declared_size < 8 or declared_size > (buffer_size - current_offset):
                    current_offset += 1
                    continue

                # box 类型应由可打印的 ASCII 字符组成。
                if not all(32 <= c < 127 for c in box_type_bytes):
                    current_offset += 1
                    continue
                # --- 验证结束 ---

                box_type = box_type_bytes.decode('ascii')

            except (struct.error, UnicodeDecodeError):
                # 如果解包或解码失败，说明这不是一个有效的 box 头部。
                current_offset += 1
                continue

            box_header_size = 8
            if declared_size == 1: # 64位大小
                if current_offset + 16 > buffer_size:
                    break # 数据不足以构成一个完整的 64位头部
                declared_size = struct.unpack('>Q', stream.read(8))[0]
                box_header_size = 16
            elif declared_size == 0: # box 延伸至文件末尾
                declared_size = buffer_size - current_offset

            if declared_size < box_header_size:
                current_offset += 1
                continue

            effective_box_size = declared_size
            if current_offset + declared_size > buffer_size:
                effective_box_size = buffer_size - current_offset

            box_content = stream_buffer[current_offset: current_offset + effective_box_size]

            self.log.debug("在偏移量 %d 处找到 box '%s'，大小为 %d", current_offset, box_type, declared_size)
            yield box_type, bytes(box_content), current_offset, declared_size

            # 前进到下一个 box 的起始位置
            current_offset += declared_size

    async def find_moov_atom(self) -> bytes:
        """
        智能地查找并返回完整的 'moov' atom 数据。
        它首先探测文件的头部，如果未找到，则探测尾部。
        """
        mdat_info = None

        # 步骤 1: 探测文件头部
        try:
            head_size = min(self.settings.moov_head_probe_size, self.video_file_size)
            if head_size > 0:
                head_data = await self.piece_manager.fetch_and_assemble_range(
                    self.video_file_offset, head_size, self.settings.moov_probe_timeout
                )
                stream = io.BytesIO(head_data)
                for box_type, partial_box_data, box_offset, declared_size in self._parse_mp4_boxes(stream):
                    if box_type == 'moov':
                        # 如果在头部探测中获取了完整的 box，直接返回它
                        if len(partial_box_data) >= declared_size:
                            return partial_box_data

                        # 如果只找到了部分的 moov box，则根据其声明的大小去获取完整数据
                        self.log.info("[%s] 在文件头部找到部分 'moov' box，正在获取完整数据...", self.infohash_hex)
                        full_moov_offset_in_torrent = self.video_file_offset + box_offset
                        return await self.piece_manager.fetch_and_assemble_range(
                            full_moov_offset_in_torrent, declared_size, self.settings.moov_probe_timeout
                        )

                    if box_type == 'mdat':
                        # 记录 mdat 的位置和大小，供后续的尾部探测使用
                        mdat_info = {'offset': box_offset, 'size': declared_size}
        except Exception as e:
            raise MoovFetchError(f"在探测 moov 头部时获取数据块失败: {e}", self.infohash_hex) from e

        # 步骤 2: 如果在头部未找到 moov，并且已找到 mdat，则尝试探测尾部
        if mdat_info:
            try:
                mdat_end_offset_in_file = mdat_info['offset'] + mdat_info['size']
                if mdat_end_offset_in_file >= self.video_file_size:
                    raise MoovNotFoundError(f"'mdat' box 似乎延伸到或超过了文件末尾。", self.infohash_hex)

                # 计算尾部数据在整个 torrent 中的起始偏移量和大小
                tail_torrent_offset = self.video_file_offset + mdat_end_offset_in_file
                tail_size = self.video_file_size - mdat_end_offset_in_file

                if tail_size > 0:
                    self.log.info("[%s] 'moov' 未在头部找到，正在探测尾部...", self.infohash_hex)
                    tail_data = await self.piece_manager.fetch_and_assemble_range(
                        tail_torrent_offset, tail_size, self.settings.moov_probe_timeout
                    )
                    stream = io.BytesIO(tail_data)
                    for box_type, box_data, _, _ in self._parse_mp4_boxes(stream):
                        if box_type == 'moov':
                            return box_data
            except Exception as e:
                raise MoovFetchError(f"在智能探测 moov 尾部时失败: {e}", self.infohash_hex) from e

        raise MoovNotFoundError("未能在文件的头部或尾部定位到 'moov' atom。", self.infohash_hex)
