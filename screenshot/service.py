# -*- coding: utf-8 -*-
"""
该模块提供了 ScreenshotService，它是协调整个截图生成过程的核心业务逻辑。
"""
import asyncio
import logging
import io
import struct
import base64
from dataclasses import dataclass, field
from typing import Generator, Tuple, Optional, Dict, Any, Callable
from .client import TorrentClient, LibtorrentError
from .extractor import H264KeyframeExtractor, Keyframe
from .generator import ScreenshotGenerator


# --- 结果数据类 ---
# 使用 dataclass 可以方便地创建结构化的结果对象，用于在任务完成时返回信息。

@dataclass
class FatalErrorResult:
    """代表一个最终的、不可恢复的错误。"""
    infohash: str
    reason: str

@dataclass
class AllSuccessResult:
    """代表任务成功完成。"""
    infohash: str
    screenshots_count: int

@dataclass
class PartialSuccessResult:
    """
    代表一个可恢复的错误。任务被中断但可以恢复。
    这用于任何可恢复的错误，例如下载超时。它包含 `resume_data`，
    允许用户从中断的地方重新提交任务。
    """
    infohash: str
    screenshots_count: int
    reason: str
    resume_data: Dict[str, Any] = field(default_factory=dict)


class ScreenshotService:
    """
    协调截图生成过程的服务。
    这是一个高级服务，封装了 TorrentClient、H264KeyframeExtractor 和 ScreenshotGenerator
    的所有交互逻辑。
    """
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output', torrent_save_path='/dev/shm'):
        """
        初始化服务。
        :param loop: asyncio 事件循环。
        :param num_workers: 并行处理任务的工作线程数量。
        :param output_dir: 保存截图的目标目录。
        :param torrent_save_path: libtorrent 用于保存临时文件的路径。
        """
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False
        self.client = TorrentClient(loop=self.loop, save_path=torrent_save_path)
        self.generator = ScreenshotGenerator(loop=self.loop, output_dir=self.output_dir)

    async def run(self):
        """启动服务，包括底层的 torrent 客户端和截图工作线程。"""
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info(f"ScreenshotService 已启动，拥有 {self.num_workers} 个工作线程。")

    def stop(self):
        """停止服务，取消所有正在运行的工作线程并停止 torrent 客户端。"""
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, resume_data: Optional[Dict[str, Any]] = None, on_complete: Optional[Callable] = None):
        """
        向服务提交一个新的截图任务。
        :param infohash: torrent 的 infohash (十六进制字符串)。
        :param resume_data: (可选) 用于从上次中断处恢复任务的数据。
        :param on_complete: (可选) 任务完成时调用的回调函数。回调函数将接收一个结果对象
                            (AllSuccessResult, PartialSuccessResult, or FatalErrorResult) 作为其唯一参数。
        """
        task = {
            'infohash': infohash,
            'resume_data': resume_data,
            'on_complete': on_complete
        }
        await self.task_queue.put(task)
        self.log.info(f"已为 infohash 提交新任务: {infohash}")

    def _get_pieces_for_range(self, offset_in_torrent: int, size: int, piece_length: int) -> list[int]:
        """一个工具函数，用于根据文件内的字节偏移量和大小，计算出需要下载的 torrent piece 索引列表。"""
        if size <= 0: return []
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data: dict[int, bytes], offset_in_torrent: int, size: int, piece_length: int) -> bytes:
        """
        一个工具函数，用于从一个包含多个 piece 数据的字典中，根据精确的偏移量和大小，
        拼接出所需的连续字节数据。
        """
        buffer = bytearray(size)
        buffer_offset = 0

        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length

        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data: continue

            piece_data = pieces_data[piece_index]

            # 计算需要从当前 piece 复制的起始位置
            copy_from_start = 0
            if piece_index == start_piece:
                copy_from_start = offset_in_torrent % piece_length

            # 计算需要从当前 piece 复制的结束位置
            copy_to_end = piece_length
            if piece_index == end_piece:
                copy_to_end = (offset_in_torrent + size - 1) % piece_length + 1

            chunk = piece_data[copy_from_start:copy_to_end]

            # 确保不会写入超出目标缓冲区范围的数据
            bytes_to_copy = len(chunk)
            if (buffer_offset + bytes_to_copy) > size:
                bytes_to_copy = size - buffer_offset

            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy

        return bytes(buffer)

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        """一个健壮的 MP4 box 解析器，生成 box 类型、其完整数据、偏移量和大小。"""
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)

        current_offset = stream.tell()
        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)

            try:
                header_data = stream.read(8)
                if not header_data or len(header_data) < 8: break
                size, box_type_bytes = struct.unpack('>I4s', header_data)
                box_type = box_type_bytes.decode('ascii', 'ignore')
            except struct.error:
                self.log.warning(f"无法在偏移量 {current_offset} 处解包 box 头部。数据不完整。")
                break

            box_header_size = 8
            if size == 1: # 64位大小
                if current_offset + 16 > buffer_size:
                    self.log.warning(f"Box '{box_type}' 有一个64位大小，但头部数据不足。")
                    break
                size_64_data = stream.read(8)
                size = struct.unpack('>Q', size_64_data)[0]
                box_header_size = 16
            elif size == 0: # 直到文件末尾
                size = buffer_size - current_offset

            if size < box_header_size:
                self.log.warning(f"Box '{box_type}' 的大小无效 {size}。停止解析。")
                break

            if current_offset + size > buffer_size:
                self.log.warning(f"Box '{box_type}' 的大小 {size} 超出了可用数据。无法完全解析。")
                full_box_data = stream_buffer[current_offset:buffer_size]
                yield box_type, bytes(full_box_data), current_offset, size
                break

            full_box_data = stream_buffer[current_offset:current_offset + size]
            yield box_type, bytes(full_box_data), current_offset, size
            current_offset += size

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length) -> bytes | None:
        """
        智能地查找并获取 moov atom 数据。
        'moov' atom 对于解析视频至关重要，但它可能位于文件的开头或结尾。
        此方法首先探测文件的头部。如果发现一个巨大的 'mdat' (媒体数据) box，
        它会假设 'moov' 在尾部并探测尾部。
        """
        self.log.info("智能搜索 moov atom...")
        mdat_size_from_head = 0

        # --- 阶段 1: 探测文件头部 ---
        self.log.info("阶段 1: 探测文件头部以查找 moov atom...")
        initial_probe_size = 256 * 1024  # 先下载 256KB
        head_size = min(initial_probe_size, video_file_size)
        head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)

        moov_timeout = getattr(self, 'MOOV_TIMEOUT_FOR_TESTING', 120)
        head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=moov_timeout)
        head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)

        stream = io.BytesIO(head_data)
        for box_type, partial_box_data, box_offset, box_size in self._parse_mp4_boxes(stream):
            self.log.info(f"在头部找到 box '{box_type}'，偏移量 {box_offset}，大小 {box_size}。")
            if box_type == 'moov':
                self.log.info("在头部探测中找到 'moov' atom。")
                # 如果我们只下载了部分 moov atom，则计算并下载剩余部分
                if len(partial_box_data) < box_size:
                    self.log.info(f"初始探测获取了 moov 的 {len(partial_box_data)}/{box_size} 字节。正在获取剩余部分。")
                    full_moov_offset_in_torrent = video_file_offset + box_offset
                    needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, box_size, piece_length)
                    moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=moov_timeout)
                    return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, box_size, piece_length)
                else:
                    return partial_box_data

            if box_type == 'mdat':
                # 如果在头部发现一个巨大的 mdat，可以安全地假设 moov 在尾部
                if box_size > video_file_size * 0.8:
                        self.log.info("在头部找到大的 'mdat' box。假设 'moov' 在尾部。")
                        mdat_size_from_head = box_size
                        break

        # --- 阶段 2: 尾部探测 (如果头部未找到) ---
        self.log.info("阶段 2: Moov 不在头部，探测文件尾部。")
        if mdat_size_from_head > 0:
            tail_probe_size = video_file_size - mdat_size_from_head
            self.log.info(f"基于 mdat 使用动态尾部探测大小: {tail_probe_size} 字节。")
        else:
            tail_probe_size = 10 * 1024 * 1024 # 默认探测尾部 10MB
            self.log.info(f"使用固定尾部探测大小: {tail_probe_size} 字节。")

        tail_file_offset = max(0, video_file_size - tail_probe_size)
        tail_torrent_offset = video_file_offset + tail_file_offset
        tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
        tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)

        moov_timeout = getattr(self, 'MOOV_TIMEOUT_FOR_TESTING', 180)
        tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=moov_timeout)
        tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)

        # 从后向前搜索 'moov' box 的签名
        search_pos = len(tail_data)
        while search_pos > 4:
            found_pos = tail_data.rfind(b'moov', 0, search_pos)
            if found_pos == -1: break

            potential_start_pos = found_pos - 4
            if potential_start_pos < 0:
                search_pos = found_pos; continue

            try: # 尝试从找到的位置解析 box
                stream = io.BytesIO(tail_data); stream.seek(potential_start_pos)
                box_type, full_box_data, _, _ = next(self._parse_mp4_boxes(stream), (None, None, None, None))
                if box_type == 'moov':
                    self.log.info(f"从尾部成功解析 'moov' atom，大小 {len(full_box_data)}。")
                    return full_box_data
            except Exception: pass # 忽略解析错误并继续搜索
            search_pos = found_pos
        return None

    async def _process_and_generate_screenshot(self, keyframe, extractor, handle, video_file_offset, piece_length, infohash_hex):
        """辅助函数，用于下载、组装和生成单个关键帧的截图。"""
        try:
            sample = extractor.samples[keyframe.sample_index - 1]
            keyframe_torrent_offset = video_file_offset + sample.offset
            keyframe_piece_indices = self._get_pieces_for_range(keyframe_torrent_offset, sample.size, piece_length)

            # 获取 piece (此调用应该很快，因为 piece 应该已经被预先下载了)
            keyframe_pieces_data = await self.client.fetch_pieces(handle, keyframe_piece_indices, timeout=60)
            packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, keyframe_torrent_offset, sample.size, piece_length)

            if len(packet_data_bytes) != sample.size:
                self.log.warning(f"关键帧 {keyframe.index} 的数据不完整，跳过。")
                return False

            # 如果视频是 'avc1' 格式，需要将 NALU 长度前缀转换成 Annex B 的起始码 (0001)
            if extractor.mode == 'avc1':
                annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
                while cursor < len(packet_data_bytes):
                    nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + extractor.nal_length_size], 'big')
                    cursor += extractor.nal_length_size
                    nal_data = packet_data_bytes[cursor : cursor + nal_length]
                    annexb_data.extend(start_code + nal_data)
                    cursor += nal_length
                packet_data = bytes(annexb_data)
            else: # 'avc3' 格式已经是 Annex B
                packet_data = packet_data_bytes

            # 将 PTS 时间戳转换为 HH-MM-SS 格式的字符串
            ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
            m, s = divmod(ts_sec, 60); h, m = divmod(m, 60)
            timestamp_str = f"{int(h):02d}-{int(m):02d}-{int(s):02d}"

            # 调用生成器来解码并保存截图
            await self.generator.generate(
                extradata=extractor.extradata,
                packet_data=packet_data,
                infohash_hex=infohash_hex,
                timestamp_str=timestamp_str
            )
            return True
        except Exception as e:
            self.log.error(f"处理关键帧 {keyframe.index} 失败: {e}", exc_info=True)
            return False

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex, resume_data: Optional[Dict[str, Any]] = None):
        """
        处理为给定 torrent 生成截图的核心业务逻辑。
        返回一个结果对象 (AllSuccessResult, FatalErrorResult, or PartialSuccessResult)。
        """
        ti = handle.get_torrent_info()
        piece_length = ti.piece_length()

        # --- 阶段 1: 恢复或重新开始 ---
        is_resume = resume_data and resume_data.get('moov_data_b64')
        screenshots_generated_so_far = 0 # 初始化截至目前生成的截图总数

        if is_resume: # 如果是恢复任务
            self.log.info(f"使用丰富的 resume_data 恢复任务 {infohash_hex}。")
            try:
                moov_data = base64.b64decode(resume_data['moov_data_b64'])
                video_file_offset = resume_data['video_file_offset']
                all_kf_indices = resume_data['all_kf_indices']
                already_processed_indices = set(resume_data.get('processed_kf_indices', []))
                screenshots_generated_so_far = resume_data.get('screenshots_generated_so_far', 0)

                extractor = H264KeyframeExtractor(moov_data)
                all_keyframes = extractor.keyframes
                keyframes_to_process = [kf for kf in all_keyframes if kf.index in all_kf_indices and kf.index not in already_processed_indices]
            except Exception as e:
                self.log.error(f"解析 {infohash_hex} 的 rich resume_data 失败: {e}", exc_info=True)
                return FatalErrorResult(infohash=infohash_hex, reason=f"解析 resume_data 失败: {e}")
        else: # 如果是新任务
            self.log.info(f"为 {infohash_hex} 开始新任务。")
            already_processed_indices = set()

            # --- 阶段 1a: 在 torrent 中查找最大的 .mp4 文件 ---
            video_file_index, video_file_size, video_file_offset = -1, -1, -1
            fs = ti.files()
            for i in range(fs.num_files()):
                if fs.file_path(i).lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                    video_file_size, video_file_index, video_file_offset = fs.file_size(i), i, fs.file_offset(i)

            if video_file_index == -1:
                return FatalErrorResult(infohash=infohash_hex, reason="在 torrent 中未找到 .mp4 视频文件")

            # --- 阶段 1b: 获取 moov atom ---
            try:
                moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length)
                if not moov_data:
                    return FatalErrorResult(infohash=infohash_hex, reason="无法找到或解析 moov atom")
            except (asyncio.TimeoutError, LibtorrentError):
                return PartialSuccessResult(infohash=infohash_hex, screenshots_count=0, reason="获取 moov atom 超时或出错", resume_data={})
            except Exception as e:
                self.log.error(f"获取 {infohash_hex} 的 moov atom 时发生致命错误: {e}", exc_info=True)
                return FatalErrorResult(infohash=infohash_hex, reason=f"获取 moov atom 时出错: {e}")

            # --- 阶段 1c: 提取并选择要截图的关键帧 ---
            try:
                extractor = H264KeyframeExtractor(moov_data)
                all_keyframes = extractor.keyframes
                if not all_keyframes:
                    return FatalErrorResult(infohash=infohash_hex, reason="无法提取任何关键帧")

                # 根据视频时长动态确定要生成的截图数量
                MIN_SCREENSHOTS, MAX_SCREENSHOTS, TARGET_INTERVAL_SEC = 5, 50, 180
                duration_sec = extractor.samples[-1].pts / extractor.timescale if extractor.timescale > 0 else 0
                num_screenshots = max(MIN_SCREENSHOTS, min(int(duration_sec / TARGET_INTERVAL_SEC), MAX_SCREENSHOTS)) if duration_sec > 0 else 20

                if len(all_keyframes) <= num_screenshots:
                    keyframes_to_process = all_keyframes
                else: # 如果关键帧太多，则均匀选择
                    indices = [int(i * len(all_keyframes) / num_screenshots) for i in range(num_screenshots)]
                    keyframes_to_process = [all_keyframes[i] for i in sorted(list(set(indices)))]
                all_kf_indices = [kf.index for kf in keyframes_to_process]
            except Exception as e:
                return FatalErrorResult(infohash=infohash_hex, reason=f"解析视频元数据失败: {e}")

        if not keyframes_to_process:
            self.log.info(f"没有剩余的关键帧需要为 {infohash_hex} 处理。")
            return AllSuccessResult(infohash=infohash_hex, screenshots_count=screenshots_generated_so_far)

        self.log.info(f"准备为 {infohash_hex} 处理 {len(keyframes_to_process)} 个关键帧。")

        # --- 阶段 2: 下载并处理关键帧 ---
        keyframe_info, piece_to_keyframes = {}, {}
        all_needed_pieces = set()
        for kf in keyframes_to_process:
            sample = extractor.samples[kf.sample_index - 1]
            needed = self._get_pieces_for_range(video_file_offset + sample.offset, sample.size, piece_length)
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes.setdefault(piece_idx, []).append(kf.index)
            all_needed_pieces.update(needed)

        self.client.request_pieces(handle, list(all_needed_pieces))

        newly_processed_indices, generation_tasks = [], []
        timeout = getattr(self, 'TIMEOUT_FOR_TESTING', 300)
        try:
            while len(newly_processed_indices) < len(keyframes_to_process):
                finished_piece = await asyncio.wait_for(self.client.finished_piece_queue.get(), timeout=timeout)
                if finished_piece not in piece_to_keyframes: continue

                # 检查此 piece 完成后，哪些关键帧的所有 piece 都已就绪
                for kf_index in piece_to_keyframes.pop(finished_piece, []):
                    info = keyframe_info.get(kf_index)
                    if not info or kf_index in newly_processed_indices: continue
                    info['needed_pieces'].remove(finished_piece)

                    if not info['needed_pieces']: # 如果一个关键帧的所有 piece 都已下载完毕
                        # 测试钩子：允许在测试中模拟超时
                        fail_after = getattr(self, 'FAIL_AFTER_N_KEYFRAMES', None)
                        if fail_after is not None and len(newly_processed_indices) >= fail_after:
                            raise asyncio.TimeoutError("为测试模拟的失败")

                        task = asyncio.create_task(self._process_and_generate_screenshot(info['keyframe'], extractor, handle, video_file_offset, piece_length, infohash_hex))
                        generation_tasks.append(task)
                        newly_processed_indices.append(kf_index)
                self.client.finished_piece_queue.task_done()
        except asyncio.TimeoutError:
            self.log.error(f"等待 {infohash_hex} 的 pieces 超时。本次运行处理了 {len(newly_processed_indices)} 个新关键帧。")

            # 为下次恢复创建丰富的 resume data
            final_processed_indices = sorted(list(already_processed_indices.union(newly_processed_indices)))
            new_screenshots_this_run = sum(1 for t in generation_tasks if not t.done() or (t.done() and t.result() is True))
            total_screenshots_so_far = screenshots_generated_so_far + new_screenshots_this_run

            rich_resume_data = {
                "moov_data_b64": base64.b64encode(moov_data).decode('ascii'),
                "video_file_offset": video_file_offset,
                "piece_length": piece_length,
                "all_kf_indices": all_kf_indices,
                "processed_kf_indices": final_processed_indices,
                "screenshots_generated_so_far": total_screenshots_so_far,
            }
            return PartialSuccessResult(
                infohash=infohash_hex,
                screenshots_count=new_screenshots_this_run,
                reason=f"等待 pieces 超时。本次运行处理了 {len(keyframes_to_process)} 帧中的 {new_screenshots_this_run} 帧。",
                resume_data=rich_resume_data
            )

        screenshots_generated_in_this_run = sum(1 for r in await asyncio.gather(*generation_tasks) if r is True)
        total_screenshots = screenshots_generated_so_far + screenshots_generated_in_this_run
        self.log.info(f"{infohash_hex} 的截图任务完成。本次运行生成了 {screenshots_generated_in_this_run} 张截图，总计 {total_screenshots} 张。")
        return AllSuccessResult(infohash=infohash_hex, screenshots_count=total_screenshots)

    async def _handle_screenshot_task(self, task_info: dict):
        """截图任务的完整生命周期，包括 torrent 句柄管理和错误处理。"""
        infohash, resume_data, on_complete = task_info['infohash'], task_info['resume_data'], task_info['on_complete']
        self.log.info(f"正在处理 infohash 的任务: {infohash}")
        handle, result = None, None
        try:
            handle = await self.client.add_torrent(infohash)
            if not handle or not handle.is_valid():
                result = FatalErrorResult(infohash=infohash, reason="未能获取有效的 torrent 句柄")
            else:
                result = await self._generate_screenshots_from_torrent(handle, infohash, resume_data)
        except Exception as e:
            self.log.exception(f"处理 {infohash} 时发生意外错误。")
            result = FatalErrorResult(infohash=infohash, reason=f"意外的工作线程错误: {str(e)}")
        finally:
            # 根据设计，torrent 句柄在客户端中缓存，任务完成后不在此处移除，
            # 以便后续的恢复任务可以重用它。LRU 缓存将在需要时驱逐旧句柄。
            if on_complete:
                try:
                    if asyncio.iscoroutinefunction(on_complete): await on_complete(result)
                    else: on_complete(result)
                except Exception:
                    self.log.exception(f"为 {infohash} 执行 on_complete 回调时出错")

    async def _worker(self):
        """工作协程，从队列中循环拉取并处理任务。"""
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                self.log.info("工作线程已取消。")
                break
            except Exception:
                # 这是一个最后的安全网，理想情况下不应到达此块。
                self.log.exception("截图工作循环中发生严重未处理错误。")
