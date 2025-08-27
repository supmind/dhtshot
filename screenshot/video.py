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

        self.log.debug(f"Probing for 'moov' box in the first {PROBE_SIZE} bytes.")
        reader.seek(0)
        head_data = await reader.read(min(PROBE_SIZE, file_size))
        moov_data = await find_moov_in_data(head_data)

        if not moov_data and file_size > PROBE_SIZE:
            seek_pos = max(0, file_size - PROBE_SIZE)
            self.log.debug(f"Probing for 'moov' box in the last {PROBE_SIZE} bytes (from position {seek_pos}).")
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

    def _calculate_sample_info(self, sample_to_find, stts, stsc, chunk_offsets, stsz):
        """
        为一个样本（sample）计算其时间戳（timestamp）和文件偏移量（file offset）。
        这是一个复杂的查找过程，需要解析多个MP4元数据表来精确计算一个样本的位置和时间。
        这个函数是性能优化的核心，它避免了一次性加载所有样本信息。

        :param sample_to_find: 要查找的样本序号（从1开始计数）。
        :param stts: 解析后的 Time-to-Sample box ('stts')。
        :param stsc: 解析后的 Sample-to-Chunk box ('stsc')。
        :param chunk_offsets: 解析后的 Chunk-Offset box ('stco' 或 'co64') 的条目列表。
        :param stsz: 解析后的 Sample-Size box ('stsz')。
        :return: 一个包含 (timestamp, offset, size) 的元组，如果找不到则返回 (None, None, None)。
        """
        # 1. 计算时间戳 (pts)
        sample_time = None
        sample_count_so_far = 0
        current_time = 0
        for count, duration in stts.entries:
            if sample_count_so_far + count >= sample_to_find:
                # 目标样本在此时间段内
                sample_time = current_time + (sample_to_find - sample_count_so_far - 1) * duration
                break
            sample_count_so_far += count
            current_time += count * duration
        if sample_time is None:
            self.log.warning(f"无法为样本 {sample_to_find} 找到时间戳。")
            return None, None, None

        # 2. 计算文件偏移量 (pos)
        sample_size = stsz.entries[sample_to_find - 1] if stsz.sample_size == 0 else stsz.sample_size

        # 确定目标样本所在的区块 (chunk)
        target_chunk_index = -1
        samples_in_chunk = -1
        first_sample_in_chunk = -1

        stsc_entries = stsc.entries
        current_sample = 1
        for i, (first_chunk, num_samples, desc_id) in enumerate(stsc_entries):
            is_last_stsc_entry = (i + 1 == len(stsc_entries))

            if is_last_stsc_entry:
                # 如果是最后一个stsc条目，它适用于所有剩余的区块
                chunk_group_len = len(chunk_offsets) - first_chunk + 1
            else:
                next_first_chunk = stsc_entries[i+1][0]
                chunk_group_len = next_first_chunk - first_chunk

            num_samples_in_group = chunk_group_len * num_samples

            if current_sample + num_samples_in_group > sample_to_find:
                # 目标样本在此区块组中
                chunk_offset_in_group = (sample_to_find - current_sample) // num_samples
                target_chunk_index = first_chunk + chunk_offset_in_group - 1
                samples_in_chunk = num_samples

                # 计算此区块的第一个样本的序号
                samples_before_this_group = current_sample - 1
                samples_in_prev_chunks_of_group = chunk_offset_in_group * num_samples
                first_sample_in_chunk = samples_before_this_group + samples_in_prev_chunks_of_group + 1
                break

            current_sample += num_samples_in_group

        if target_chunk_index == -1:
            self.log.warning(f"无法为样本 {sample_to_find} 找到对应的区块。")
            return None, None, None

        # 计算从区块开始到目标样本的偏移量
        chunk_start_offset = chunk_offsets[target_chunk_index]
        offset_within_chunk = 0

        # 使用 stsz box 来累加此区块内、目标样本之前所有样本的大小
        if stsz.sample_size == 0: # 如果每个样本大小不同
            start_idx = first_sample_in_chunk - 1
            end_idx = sample_to_find - 1
            offset_within_chunk = sum(stsz.entries[i] for i in range(start_idx, end_idx))
        else: # 如果所有样本大小相同
            num_samples_before = sample_to_find - first_sample_in_chunk
            offset_within_chunk = num_samples_before * stsz.sample_size

        sample_offset = chunk_start_offset + offset_within_chunk

        return sample_time, sample_offset, sample_size

    async def get_keyframes(self):
        """
        从视频文件中提取并筛选用于截图的关键帧信息。
        此方法实现了性能优化的流程：
        1. 解析 'moov' box 获取元数据。
        2. 获取所有关键帧的样本序号。
        3. 根据视频时长和设定的数量范围，对序号列表进行筛选。
        4. 只为筛选后的关键帧，按需计算其精确的时间戳和文件偏移。
        """
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

        # 1. 获取所有关键帧的样本序号
        all_keyframe_samples = stss.entries
        if not all_keyframe_samples:
            return []

        # 2. 应用筛选逻辑来选择要处理的样本序号
        mdhd = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'mdhd'])
        timescale = mdhd.timescale if mdhd and mdhd.timescale > 0 else 1000
        tkhd = F4VParser.find_child_box(moov_box, ['trak', 'tkhd'])
        duration = tkhd.duration if tkhd else sum(c * d for c, d in stts.entries)
        duration_sec = duration / timescale

        MIN_SCREENSHOTS = 5
        MAX_SCREENSHOTS = 50
        TARGET_INTERVAL_SEC = 180

        if duration_sec > 0:
            num_screenshots = int(duration_sec / TARGET_INTERVAL_SEC)
            num_screenshots = max(MIN_SCREENSHOTS, min(num_screenshots, MAX_SCREENSHOTS))
        else:
            num_screenshots = 20 # 对于未知时长的视频，使用默认值

        if len(all_keyframe_samples) <= num_screenshots:
            selectable_samples = all_keyframe_samples
        else:
            # 在视频的 10% 到 90% 区间内选择
            start_idx = int(len(all_keyframe_samples) * 0.1)
            end_idx = int(len(all_keyframe_samples) * 0.9)

            if start_idx >= end_idx:
                # 区间无效时，回退到使用所有关键帧（除去首尾）
                selectable_samples = all_keyframe_samples[1:-1] if len(all_keyframe_samples) > 2 else all_keyframe_samples
            else:
                # 在有效区间内，均匀选取
                candidate_samples = all_keyframe_samples[start_idx:end_idx]
                if len(candidate_samples) <= num_screenshots:
                    selectable_samples = candidate_samples
                else:
                    indices = [int(i * len(candidate_samples) / num_screenshots) for i in range(num_screenshots)]
                    selectable_samples = [candidate_samples[i] for i in sorted(list(set(indices)))]

        if not selectable_samples:
            return []

        # 3. 只为筛选后的关键帧计算详细信息
        selected_keyframes = []
        chunk_offsets = stco.entries if stco else co64.entries
        for s_num in selectable_samples:
            pts, pos, size = self._calculate_sample_info(s_num, stts, stsc, chunk_offsets, stsz)
            if all(v is not None for v in [pts, pos, size]):
                selected_keyframes.append(KeyframeInfo(pts, pos, size, timescale))
            else:
                self.log.warning(f"无法计算样本 {s_num} 的完整信息，已跳过。")

        return selected_keyframes

    async def download_keyframe_data(self, keyframe_info):
        reader = self.create_reader()
        reader.seek(keyframe_info.pos)
        keyframe_data = await reader.read(keyframe_info.size)
        if len(keyframe_data) != keyframe_info.size:
            self.log.error(f"关键帧数据下载不完整。")
            return None
        return keyframe_data
