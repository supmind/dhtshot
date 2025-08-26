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
        self.PIECE_CACHE_SIZE = 32
        self.log = logging.getLogger("AsyncTorrentReader")

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET: self.pos = offset
        elif whence == io.SEEK_CUR: self.pos += offset
        elif whence == io.SEEK_END: self.pos = self.file_size + offset
        return self.pos

    def tell(self):
        return self.pos

    async def read(self, size=-1):
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
                self.piece_cache.move_to_end(piece_index)
            else:
                try:
                    piece_data = await self.client.download_and_read_piece(self.handle, piece_index)
                except Exception as e:
                    self.log.error(f"读取 piece {piece_index} 时出错: {e}")
                    raise IOError(f"获取 piece {piece_index} 失败") from e
                self.piece_cache[piece_index] = piece_data
                if len(self.piece_cache) > self.PIECE_CACHE_SIZE:
                    self.piece_cache.popitem(last=False)

            if piece_data is None: raise IOError(f"读取 piece {piece_index} 失败：未收到数据。")
            chunk = piece_data[piece_offset : piece_offset + read_len]
            result_buffer[buffer_offset : buffer_offset + len(chunk)] = chunk
            buffer_offset += read_len
            bytes_to_go -= read_len
            current_file_pos += read_len

        self.pos += size
        return bytes(result_buffer)

class VideoFile:
    def __init__(self, client, handle):
        self.client = client
        self.handle = handle
        self.log = logging.getLogger("VideoFile")
        self.file_index = self._find_largest_video_file_index()
        self._moov_box = None

    def _find_largest_video_file_index(self):
        files = self.handle.torrent_file().files()
        video_file_index, max_size = -1, -1
        for i in range(files.num_files()):
            if files.file_path(i).lower().endswith((".mp4", ".mkv")) and files.file_size(i) > max_size:
                max_size = files.file_size(i)
                video_file_index = i
        return video_file_index

    def create_reader(self):
        return AsyncTorrentReader(self.client, self.handle, self.file_index)

    async def _get_moov_box(self):
        if self._moov_box:
            return self._moov_box

        reader = self.create_reader()
        PROBE_SIZE = 10 * 1024 * 1024
        file_size = reader.file_size

        async def find_moov_in_data(data):
            # ... (omitted for brevity, assume a simple find)
            moov_data = None
            stream = io.BytesIO(data)
            for box in F4VParser.parse(bytes_input=data):
                if box.header.box_type == 'moov':
                    stream.seek(box.header.start_offset)
                    moov_data = stream.read(box.header.box_size + box.header.header_size)
                    break
            return moov_data

        reader.seek(0)
        head_data = await reader.read(min(PROBE_SIZE, file_size))
        moov_data = await find_moov_in_data(head_data)

        if not moov_data and file_size > PROBE_SIZE:
            seek_pos = max(0, file_size - PROBE_SIZE)
            reader.seek(seek_pos)
            tail_data = await reader.read(PROBE_SIZE)
            moov_sig_pos = tail_data.find(b'moov')
            if moov_sig_pos != -1 and moov_sig_pos >= 4:
                box_start_pos = moov_sig_pos - 4
                moov_data = await find_moov_in_data(tail_data[box_start_pos:])

        if moov_data:
            self._moov_box = next(F4VParser.parse(bytes_input=moov_data))
            return self._moov_box
        return None

    async def get_decoder_config(self):
        moov_box = await self._get_moov_box()
        if not moov_box:
            return None, None

        avcC_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl', 'stsd', 'avc1', 'avcC'])
        if avcC_box and avcC_box.sps_list and avcC_box.pps_list:
            return avcC_box.sps_list[0], avcC_box.pps_list[0]
        return None, None

    async def get_keyframes(self):
        from screenshot.service import KeyframeInfo
        moov_box = await self._get_moov_box()
        if not moov_box:
            return []

        stbl_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl'])
        if not stbl_box: return []

        stss = getattr(stbl_box, 'stss', None)
        stts = getattr(stbl_box, 'stts', None)
        stsc = getattr(stbl_box, 'stsc', None)
        stsz = getattr(stbl_box, 'stsz', None)
        stco = getattr(stbl_box, 'stco', None)
        co64 = getattr(stbl_box, 'co64', None)

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
        try:
            stsc_entries_iter = iter(stsc.entries)
            chunk_offsets_iter = iter(chunk_offsets)
            current_stsc = next(stsc_entries_iter, None)
            next_stsc = next(stsc_entries_iter, None)
            current_chunk_num = 1
            current_sample_in_chunk = 0
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

        tkhd = F4VParser.find_child_box(moov_box, ['trak', 'tkhd'])
        duration = tkhd.duration if tkhd else sum(c * d for c, d in stts.entries)
        duration_sec = duration / timescale

        # Selection logic
        num_screenshots = 20
        if len(all_keyframes) <= num_screenshots:
            return all_keyframes
        indices = [int(i * len(all_keyframes) / num_screenshots) for i in range(num_screenshots)]
        return [all_keyframes[i] for i in indices]

    async def download_keyframe_data(self, keyframe_info):
        reader = self.create_reader()
        reader.seek(keyframe_info.pos)
        keyframe_data = await reader.read(keyframe_info.size)
        if len(keyframe_data) != keyframe_info.size:
            self.log.error(f"关键帧数据下载不完整。")
            return None
        return keyframe_data
