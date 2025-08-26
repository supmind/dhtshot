# -*- coding: utf-8 -*-
"""
该模块包含 VideoFile 类，负责处理与视频相关的操作，
如查找关键帧和下载帧数据。它还包含 AsyncTorrentReader 类。
"""
import asyncio
import io
import logging
import struct
from collections import OrderedDict

from .pymp4parse import F4VParser

class AsyncTorrentReader:
    """
    一个用于 Torrent 中文件的异步、类文件读取器。
    它通过向 TorrentClient 请求 piece 来按需读取数据。
    """
    def __init__(self, client, handle, file_index):
        self.client = client
        self.handle = handle
        self.ti = handle.torrent_file()
        self.file_index = file_index
        file_storage = self.ti.files()
        self.file_size = file_storage.file_size(self.file_index)
        self.pos = 0
        self.piece_cache = OrderedDict()
        self.PIECE_CACHE_SIZE = 32  # 缓存的 piece 数量
        self.log = logging.getLogger("AsyncTorrentReader")

    def seek(self, offset, whence=io.SEEK_SET):
        """在流中移动到新的位置。"""
        if whence == io.SEEK_SET: self.pos = offset
        elif whence == io.SEEK_CUR: self.pos += offset
        elif whence == io.SEEK_END: self.pos = self.file_size + offset
        return self.pos

    def tell(self):
        """返回当前流的位置。"""
        return self.pos

    async def read(self, size=-1):
        """从当前位置异步读取指定大小的数据。"""
        if size == -1: size = self.file_size - self.pos
        size = min(size, self.file_size - self.pos)
        if size <= 0: return b''

        result_buffer = bytearray(size)
        buffer_offset, bytes_to_go, current_file_pos = 0, size, self.pos
        piece_size = self.ti.piece_length()

        while bytes_to_go > 0:
            req = self.ti.map_file(self.file_index, current_file_pos, 1)
            piece_index, piece_offset = req.piece, req.start
            read_len = min(bytes_to_go, piece_size - piece_offset)

            if piece_index in self.piece_cache:
                piece_data = self.piece_cache[piece_index]
                self.piece_cache.move_to_end(piece_index)  # 更新为最近使用
            else:
                try:
                    piece_data = await self.client.download_and_read_piece(self.handle, piece_index)
                except Exception as e:
                    self.log.error(f"读取 piece {piece_index} 时出错: {e}")
                    raise IOError(f"获取 piece {piece_index} 失败") from e

                self.piece_cache[piece_index] = piece_data
                if len(self.piece_cache) > self.PIECE_CACHE_SIZE:
                    self.piece_cache.popitem(last=False)  # 移除最久未使用的项

            if piece_data is None:
                raise IOError(f"读取 piece {piece_index} 失败：未收到数据。")

            chunk = piece_data[piece_offset : piece_offset + read_len]
            result_buffer[buffer_offset : buffer_offset + len(chunk)] = chunk

            buffer_offset += read_len
            bytes_to_go -= read_len
            current_file_pos += read_len

        self.pos += size
        return bytes(result_buffer)

class VideoFile:
    """代表 Torrent 中一个可被处理的视频文件。"""
    def __init__(self, client, handle):
        self.client = client
        self.handle = handle
        self.log = logging.getLogger("VideoFile")
        self.file_index = self._find_largest_video_file_index()

    def _find_largest_video_file_index(self):
        """在 Torrent 中查找最大的视频文件的索引。"""
        files = self.handle.torrent_file().files()
        video_file_index, max_size = -1, -1
        for i in range(files.num_files()):
            if files.file_path(i).lower().endswith((".mp4", ".mkv")) and files.file_size(i) > max_size:
                max_size = files.file_size(i)
                video_file_index = i
        return video_file_index

    async def get_keyframes_and_moov(self):
        """
        通过下载文件的头部和尾部来查找 'moov' box，
        然后返回关键帧信息和原始的 'moov' box 数据。
        """
        if self.file_index == -1:
            self.log.warning("在 torrent 中未找到视频文件。")
            return None, None

        reader = AsyncTorrentReader(self.client, self.handle, self.file_index)
        PROBE_SIZE = 4 * 1024 * 1024  # 探测大小为 4MB
        file_size = reader.file_size

        def find_moov_in_data(data):
            """一个在字节数据中查找 'moov' box 的辅助函数。"""
            stream = io.BytesIO(data)
            while True:
                pos = stream.tell()
                header_bytes = stream.read(8)
                if not header_bytes: break
                size, box_type_bytes = struct.unpack('>I4s', header_bytes)
                box_type = box_type_bytes.decode('ascii', errors='ignore')
                if size == 1: size = struct.unpack('>Q', stream.read(8))[0]
                if box_type == 'moov':
                    stream.seek(pos)
                    return stream.read(size)
                stream.seek(pos + size)
            return None

        self.log.debug(f"下载文件头部 ({PROBE_SIZE} 字节) 以查找 'moov' box...")
        reader.seek(0)
        head_data = await reader.read(min(PROBE_SIZE, file_size))
        moov_data = await asyncio.get_running_loop().run_in_executor(None, find_moov_in_data, head_data)

        if not moov_data and file_size > PROBE_SIZE:
            self.log.debug(f"下载文件尾部 ({PROBE_SIZE} 字节) 以查找 'moov' box...")
            seek_pos = max(0, file_size - PROBE_SIZE)
            reader.seek(seek_pos)
            tail_data = await reader.read(PROBE_SIZE)
            moov_data = await asyncio.get_running_loop().run_in_executor(None, find_moov_in_data, tail_data)

        if moov_data:
            self.log.info("已找到 'moov' box，正在解析关键帧...")
            try:
                moov_box_parsed = next(F4VParser.parse(bytes_input=moov_data))
                keyframe_infos = self._parse_keyframes_from_stbl(moov_box_parsed)
                if not keyframe_infos:
                    self.log.error("无法从 'moov' box 中提取关键帧。")
                    return None, None
                return keyframe_infos, moov_data
            except Exception as e:
                self.log.exception(f"解析找到的 'moov' box 时出错: {e}")
                return None, None
        else:
            self.log.error("在文件的头部或尾部未找到 'moov' box。")
            return None, None

    async def download_keyframe_data(self, keyframe_info):
        """下载单个关键帧的数据。"""
        self.log.debug(f"正在下载关键帧数据 (PTS: {keyframe_info.pts}), 位置: {keyframe_info.pos}, 大小: {keyframe_info.size}")
        try:
            reader = AsyncTorrentReader(self.client, self.handle, self.file_index)
            reader.seek(keyframe_info.pos)
            keyframe_data = await reader.read(keyframe_info.size)
            if len(keyframe_data) != keyframe_info.size:
                self.log.error(f"关键帧数据下载不完整。预期: {keyframe_info.size}, 得到: {len(keyframe_data)}")
                return None
            return keyframe_data
        except Exception as e:
            self.log.exception(f"下载关键帧数据时出错 (PTS: {keyframe_info.pts}): {e}")
            return None

    def _parse_keyframes_from_stbl(self, moov_box):
        """从 'moov' box 中解析关键帧信息。"""
        # 这个逻辑是 CPU 密集型的同步操作
        from screenshot.service import KeyframeInfo
        stbl_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl'])
        if not stbl_box: return []

        stss, stts, stsc, stsz, stco, co64 = (getattr(stbl_box, attr, None) for attr in
                                             ['stss', 'stts', 'stsc', 'stsz', 'stco', 'co64'])
        if not all([stss, stts, stsc, stsz, (stco or co64)]): return []

        keyframe_samples = stss.entries
        chunk_offsets = stco.entries if stco else co64.entries
        sample_sizes = stsz.entries if stsz.sample_size == 0 else [stsz.sample_size] * stsz.sample_count

        sample_timestamps = []
        current_time = 0
        for count, duration in stts.entries:
            for _ in range(count):
                sample_timestamps.append(current_time)
                current_time += duration

        sample_offsets = []
        stsc_entries_iter = iter(stsc.entries)
        chunk_offsets_iter = iter(chunk_offsets)
        current_stsc = next(stsc_entries_iter, None)
        next_stsc = next(stsc_entries_iter, None)
        current_chunk_num = 1
        current_sample_in_chunk = 0
        try:
            current_chunk_offset = next(chunk_offsets_iter)
            for sample_size in sample_sizes:
                if current_stsc and (next_stsc is None or current_chunk_num < next_stsc[0]):
                    if current_sample_in_chunk >= current_stsc[1]:
                        current_chunk_num += 1
                        current_sample_in_chunk = 0
                        current_chunk_offset = next(chunk_offsets_iter)
                elif current_stsc and next_stsc and current_chunk_num >= next_stsc[0]:
                    current_stsc = next_stsc
                    next_stsc = next(stsc_entries_iter, None)
                sample_offsets.append(current_chunk_offset)
                current_chunk_offset += sample_size
                current_sample_in_chunk += 1
        except StopIteration:
            self.log.warning("在计算样本偏移量时，块偏移或 STSC 数据提前结束。")
            pass

        tkhd = F4VParser.find_child_box(moov_box, ['trak', 'tkhd'])
        mdhd = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'mdhd'])
        timescale = mdhd.timescale if mdhd and mdhd.timescale > 0 else 1000

        all_keyframes = []
        for s_num in keyframe_samples:
            idx = s_num - 1
            if idx < len(sample_offsets) and idx < len(sample_timestamps) and idx < len(sample_sizes):
                all_keyframes.append(KeyframeInfo(
                    sample_timestamps[idx], sample_offsets[idx], sample_sizes[idx], timescale
                ))

        if not all_keyframes: return []

        duration = tkhd.duration if tkhd else sum(c * d for c, d in stts.entries)
        duration_sec = duration / timescale

        # 根据视频时长决定截图数量
        num_screenshots = 20 if duration_sec <= 3600 else int(duration_sec / 180)
        # 从所有关键帧中均匀选取指定数量的样本
        selected_keyframes = all_keyframes if len(all_keyframes) <= num_screenshots else \
                             [all_keyframes[int(i * len(all_keyframes) / num_screenshots)] for i in range(num_screenshots)]

        return selected_keyframes
