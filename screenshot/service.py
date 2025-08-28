# -*- coding: utf-8 -*-
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
    这用于任何可恢复的错误，无论成功生成了多少截图。
    """
    infohash: str
    screenshots_count: int
    reason: str
    resume_data: Dict[str, Any] = field(default_factory=dict)


class ScreenshotService:
    """
    协调截图生成过程的服务。
    """
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output', torrent_save_path='/dev/shm'):
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
        """启动服务，包括 torrent 客户端和工作线程。"""
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info(f"ScreenshotService 已启动，拥有 {self.num_workers} 个工作线程。")

    def stop(self):
        """停止服务，包括 torrent 客户端和工作线程。"""
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, resume_data: Optional[Dict[str, Any]] = None, on_complete: Optional[Callable] = None):
        """
        提交一个新的截图任务。

        参数:
            infohash: torrent 的 infohash。
            resume_data: 用于恢复部分完成任务的可选数据。
            on_complete: 任务完成时调用的回调函数。
                         回调函数将接收一个结果对象作为其唯一参数。
        """
        task = {
            'infohash': infohash,
            'resume_data': resume_data,
            'on_complete': on_complete
        }
        await self.task_queue.put(task)
        self.log.info(f"已为 infohash 提交新任务: {infohash}")

    def _get_pieces_for_range(self, offset_in_torrent, size, piece_length):
        """为 torrent 中的给定字节范围计算 piece 索引。"""
        if size <= 0: return []
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data, offset_in_torrent, size, piece_length):
        """从 piece 数据字典中组装一个字节范围。"""
        buffer = bytearray(size)
        buffer_offset = 0

        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length

        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data: continue

            piece_data = pieces_data[piece_index]

            # 确定要使用的 piece 切片
            copy_from_start = 0
            if piece_index == start_piece:
                copy_from_start = offset_in_torrent % piece_length

            copy_to_end = piece_length
            if piece_index == end_piece:
                copy_to_end = (offset_in_torrent + size - 1) % piece_length + 1

            chunk = piece_data[copy_from_start:copy_to_end]

            # 确定将块放置在缓冲区中的位置
            bytes_to_copy = len(chunk)
            if (buffer_offset + bytes_to_copy) > size:
                bytes_to_copy = size - buffer_offset

            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy

        return bytes(buffer)

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        """一个健壮的 MP4 box 解析器，生成 box 类型、其完整数据、偏移量和大小。"""
        # 使用 getbuffer() 可以高效地查看整个字节数组。
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)

        current_offset = stream.tell()
        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)

            try:
                header_data = stream.read(8)
                if not header_data or len(header_data) < 8:
                    break
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

            # 检查完整的 box 是否在缓冲区中可用。
            if current_offset + size > buffer_size:
                self.log.warning(f"Box '{box_type}' 的大小 {size} 超出了可用数据。无法完全解析。")
                # 我们仍然可以生成我们拥有的部分数据
                full_box_data = stream_buffer[current_offset:buffer_size]
                yield box_type, bytes(full_box_data), current_offset, size
                break

            full_box_data = stream_buffer[current_offset:current_offset + size]
            yield box_type, bytes(full_box_data), current_offset, size

            # 移动到下一个 box。
            current_offset += size

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length) -> bytes | None:
        """
        智能地查找并获取 moov atom 数据。
        它首先探测文件的头部，解析 box 以精确定位 moov atom。
        如果找不到，则回退到探测尾部。
        """
        self.log.info("智能搜索 moov atom...")
        mdat_size_from_head = 0

        # --- 头部探测 ---
        self.log.info("阶段 1: 探测文件头部以查找 moov atom...")
        initial_probe_size = 256 * 1024  # 256KB
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
                if len(partial_box_data) < box_size:
                    self.log.info(f"初始探测获取了 moov 的 {len(partial_box_data)}/{box_size} 字节。正在获取剩余部分。")
                    full_moov_offset_in_torrent = video_file_offset + box_offset
                    needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, box_size, piece_length)

                    moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=moov_timeout)
                    return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, box_size, piece_length)
                else:
                    return partial_box_data

            if box_type == 'mdat':
                if box_size > video_file_size * 0.8:
                        self.log.info("在头部找到大的 'mdat' box。假设 'moov' 在尾部。")
                        mdat_size_from_head = box_size
                        break

        # --- 尾部探测 (回退) ---
        self.log.info("阶段 2: Moov 不在头部，探测文件尾部。")

        if mdat_size_from_head > 0:
            tail_probe_size = video_file_size - mdat_size_from_head
            self.log.info(f"基于 mdat 使用动态尾部探测大小: {tail_probe_size} 字节。")
        else:
            tail_probe_size = 10 * 1024 * 1024 # 10MB
            self.log.info(f"使用固定尾部探测大小: {tail_probe_size} 字节。")

        tail_file_offset = max(0, video_file_size - tail_probe_size)
        tail_torrent_offset = video_file_offset + tail_file_offset
        tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
        tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)

        moov_timeout = getattr(self, 'MOOV_TIMEOUT_FOR_TESTING', 180)
        tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=moov_timeout)
        tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)

        # 从后向前搜索 'moov'
        search_pos = len(tail_data)
        while search_pos > 4:
            found_pos = tail_data.rfind(b'moov', 0, search_pos)
            if found_pos == -1:
                break
            potential_start_pos = found_pos - 4
            if potential_start_pos < 0:
                search_pos = found_pos
                continue
            try:
                stream = io.BytesIO(tail_data)
                stream.seek(potential_start_pos)
                box_type, full_box_data, _, _ = next(self._parse_mp4_boxes(stream), (None, None, None, None))

                if box_type == 'moov':
                    self.log.info(f"从尾部成功解析 'moov' atom，大小 {len(full_box_data)}。")
                    return full_box_data
            except Exception:
                pass # 忽略解析错误并继续搜索
            search_pos = found_pos

        return None

    async def _process_and_generate_screenshot(self, keyframe, extractor, handle, video_file_offset, piece_length, infohash_hex):
        """辅助函数，用于在下载完 piece 后处理单个关键帧。"""
        try:
            sample = extractor.samples[keyframe.sample_index - 1]
            keyframe_torrent_offset = video_file_offset + sample.offset
            keyframe_piece_indices = self._get_pieces_for_range(keyframe_torrent_offset, sample.size, piece_length)

            # 获取 piece (应该很快，因为它们已经被下载了)
            keyframe_pieces_data = await self.client.fetch_pieces(handle, keyframe_piece_indices, timeout=60)
            packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, keyframe_torrent_offset, sample.size, piece_length)

            if len(packet_data_bytes) != sample.size:
                self.log.warning(f"关键帧 {keyframe.index} 的数据不完整，跳过。")
                return False

            if extractor.mode == 'avc1':
                # 将 avc1 格式转换为 Annex B 格式
                annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
                while cursor < len(packet_data_bytes):
                    nal_length = int.from_bytes(packet_data_bytes[cursor : cursor + extractor.nal_length_size], 'big')
                    cursor += extractor.nal_length_size
                    nal_data = packet_data_bytes[cursor : cursor + nal_length]
                    annexb_data.extend(start_code + nal_data)
                    cursor += nal_length
                packet_data = bytes(annexb_data)
            else:
                packet_data = packet_data_bytes

            # 生成时间戳字符串
            ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
            m, s = divmod(ts_sec, 60)
            h, m = divmod(m, 60)
            timestamp_str = f"{int(h):02d}-{int(m):02d}-{int(s):02d}"

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
        处理为给定 torrent 生成截图的详细逻辑。
        返回一个结果对象 (AllSuccess, FatalError, or PartialSuccess)。
        """
        ti = handle.get_torrent_info()
        piece_length = ti.piece_length()

        # --- 阶段 1: 恢复或重新开始 ---
        is_resume = resume_data and resume_data.get('moov_data_b64')

        screenshots_generated_so_far = 0 # 初始化截至目前生成的截图总数
        if is_resume:
            self.log.info(f"使用丰富的 resume_data 恢复任务 {infohash_hex}。")
            try:
                moov_data = base64.b64decode(resume_data['moov_data_b64'])
                video_file_offset = resume_data['video_file_offset']
                all_kf_indices = resume_data['all_kf_indices']
                already_processed_indices = set(resume_data.get('processed_kf_indices', []))
                # 恢复之前运行中已生成的截图数量
                screenshots_generated_so_far = resume_data.get('screenshots_generated_so_far', 0)

                extractor = H264KeyframeExtractor(moov_data)
                all_keyframes = extractor.keyframes

                # 筛选出仍需处理的关键帧
                keyframes_to_process = [kf for kf in all_keyframes if kf.index in all_kf_indices and kf.index not in already_processed_indices]

            except Exception as e:
                self.log.error(f"解析 {infohash_hex} 的 rich resume_data 失败: {e}", exc_info=True)
                return FatalErrorResult(infohash=infohash_hex, reason=f"解析 resume_data 失败: {e}")
        else:
            self.log.info(f"为 {infohash_hex} 开始新任务。")
            already_processed_indices = set()

            # --- 阶段 1a: 查找视频文件 ---
            video_file_index, video_file_size, video_file_offset = -1, -1, -1
            fs = ti.files()
            for i in range(fs.num_files()):
                if fs.file_path(i).lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                    video_file_size = fs.file_size(i)
                    video_file_index = i
                    video_file_offset = fs.file_offset(i)

            if video_file_index == -1:
                self.log.warning(f"在 torrent {infohash_hex} 中未找到 .mp4 视频文件。")
                return FatalErrorResult(infohash=infohash_hex, reason="在 torrent 中未找到 .mp4 视频文件")

            # --- 阶段 1b: 获取 moov atom ---
            try:
                moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length)
                if not moov_data:
                    return FatalErrorResult(infohash=infohash_hex, reason="无法找到或解析 moov atom")
            except (asyncio.TimeoutError, LibtorrentError):
                self.log.warning(f"获取 {infohash_hex} 的 moov atom 超时或发生 LibtorrentError。任务可从头恢复。")
                return PartialSuccessResult(infohash=infohash_hex, screenshots_count=0, reason="获取 moov atom 超时或出错", resume_data={})
            except Exception as e:
                self.log.error(f"获取 {infohash_hex} 的 moov atom 时发生致命错误: {e}", exc_info=True)
                return FatalErrorResult(infohash=infohash_hex, reason=f"获取 moov atom 时出错: {e}")

            # --- 阶段 1c: 提取并选择关键帧 ---
            try:
                extractor = H264KeyframeExtractor(moov_data)
                all_keyframes = extractor.keyframes
                if not all_keyframes:
                    return FatalErrorResult(infohash=infohash_hex, reason="无法提取任何关键帧")

                # 定义截图数量的参数
                MIN_SCREENSHOTS, MAX_SCREENSHOTS, TARGET_INTERVAL_SEC = 5, 50, 180
                duration_sec = extractor.samples[-1].pts / extractor.timescale if extractor.timescale > 0 else 0
                num_screenshots = max(MIN_SCREENSHOTS, min(int(duration_sec / TARGET_INTERVAL_SEC), MAX_SCREENSHOTS)) if duration_sec > 0 else 20
                if len(all_keyframes) <= num_screenshots:
                    keyframes_to_process = all_keyframes
                else:
                    # 均匀选择关键帧
                    indices = [int(i * len(all_keyframes) / num_screenshots) for i in range(num_screenshots)]
                    keyframes_to_process = [all_keyframes[i] for i in sorted(list(set(indices)))]

                all_kf_indices = [kf.index for kf in keyframes_to_process]

            except Exception as e:
                return FatalErrorResult(infohash=infohash_hex, reason=f"解析视频元数据失败: {e}")

        if not keyframes_to_process:
            self.log.info(f"没有剩余的关键帧需要为 {infohash_hex} 处理。")
            return AllSuccessResult(infohash=infohash_hex, screenshots_count=0)

        self.log.info(f"准备为 {infohash_hex} 处理 {len(keyframes_to_process)} 个关键帧。")

        # --- 阶段 2: 下载并处理关键帧 ---
        keyframe_info = {}
        piece_to_keyframes = {}
        for kf in keyframes_to_process:
            sample = extractor.samples[kf.sample_index - 1]
            keyframe_torrent_offset = video_file_offset + sample.offset
            needed = self._get_pieces_for_range(keyframe_torrent_offset, sample.size, piece_length)
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes.setdefault(piece_idx, []).append(kf.index)

        self.client.request_pieces(handle, list(piece_to_keyframes.keys()))

        newly_processed_indices = []
        generation_tasks = []
        timeout = getattr(self, 'TIMEOUT_FOR_TESTING', 300)
        try:
            while len(newly_processed_indices) < len(keyframes_to_process):
                finished_piece = await asyncio.wait_for(self.client.finished_piece_queue.get(), timeout=timeout)
                if finished_piece not in piece_to_keyframes: continue

                for kf_index in piece_to_keyframes.pop(finished_piece):
                    info = keyframe_info.get(kf_index)
                    if not info or not info.get('needed_pieces'): continue
                    info['needed_pieces'].remove(finished_piece)
                    if not info['needed_pieces']: # 所有需要的 piece 都已下载完毕
                        # --- 仅供测试的钩子，用于在 N 个关键帧后模拟超时 ---
                        fail_after = getattr(self, 'FAIL_AFTER_N_KEYFRAMES', None)
                        if fail_after is not None and len(newly_processed_indices) >= fail_after:
                            self.log.warning(f"为测试目的，在 {fail_after} 个关键帧后模拟超时。")
                            raise asyncio.TimeoutError("为测试模拟的失败")
                        # --- 测试钩子结束 ---

                        task = asyncio.create_task(self._process_and_generate_screenshot(info['keyframe'], extractor, handle, video_file_offset, piece_length, infohash_hex))
                        generation_tasks.append(task)
                        newly_processed_indices.append(kf_index)
                self.client.finished_piece_queue.task_done()
        except asyncio.TimeoutError:
            self.log.error(f"等待 {infohash_hex} 的 pieces 超时。本次运行处理了 {len(newly_processed_indices)} 个新关键帧。")

            # 为下次恢复创建丰富的 resume data
            final_processed_indices = sorted(list(already_processed_indices.union(newly_processed_indices)))
            new_screenshots_this_run = len(newly_processed_indices)
            total_screenshots_so_far = screenshots_generated_so_far + new_screenshots_this_run

            rich_resume_data = {
                "moov_data_b64": base64.b64encode(moov_data).decode('ascii'),
                "video_file_offset": video_file_offset,
                "piece_length": piece_length,
                "all_kf_indices": all_kf_indices,
                "processed_kf_indices": final_processed_indices,
                "screenshots_generated_so_far": total_screenshots_so_far, # 保存累积总数以供下次恢复
            }
            return PartialSuccessResult(
                infohash=infohash_hex,
                screenshots_count=new_screenshots_this_run, # 本次运行生成的数量
                reason=f"等待 pieces 超时。本次运行处理了 {len(keyframes_to_process)} 帧中的 {new_screenshots_this_run} 帧。",
                resume_data=rich_resume_data
            )

        screenshots_generated_in_this_run = 0
        if generation_tasks:
            results = await asyncio.gather(*generation_tasks)
            screenshots_generated_in_this_run = sum(1 for r in results if r is True)

        total_screenshots = screenshots_generated_so_far + screenshots_generated_in_this_run
        self.log.info(f"{infohash_hex} 的截图任务完成。本次运行生成了 {screenshots_generated_in_this_run} 张截图，总计 {total_screenshots} 张。")
        return AllSuccessResult(infohash=infohash_hex, screenshots_count=total_screenshots)

    async def _handle_screenshot_task(self, task_info: dict):
        """截图任务的完整生命周期，包括 torrent 句柄管理。"""
        infohash = task_info['infohash']
        resume_data = task_info['resume_data']
        on_complete = task_info['on_complete']

        self.log.info(f"正在处理 infohash 的任务: {infohash}")
        handle = None
        result = None
        try:
            handle = await self.client.add_torrent(infohash)
            if not handle or not handle.is_valid():
                self.log.error(f"无法为 {infohash} 获取有效的 torrent 句柄。")
                result = FatalErrorResult(infohash=infohash, reason="未能获取有效的 torrent 句柄")
            else:
                result = await self._generate_screenshots_from_torrent(handle, infohash, resume_data)

        except Exception as e:
            self.log.exception(f"处理 {infohash} 时发生意外错误。")
            result = FatalErrorResult(infohash=infohash, reason=f"意外的工作线程错误: {str(e)}")
        finally:
            # torrent 句柄现在缓存在客户端中，不应在每个任务后移除，
            # 因为后续恢复可能需要它。
            # if handle:
            #     self.client.remove_torrent(handle)

            if on_complete:
                try:
                    # 允许回调是常规函数或协程
                    if asyncio.iscoroutinefunction(on_complete):
                        await on_complete(result)
                    else:
                        on_complete(result)
                except Exception:
                    self.log.exception(f"为 {infohash} 执行 on_complete 回调时出错")

    async def _worker(self):
        """工作协程，从队列中拉取任务。"""
        while self._running:
            try:
                task_info = await self.task_queue.get()
                # _handle_screenshot_task 现在负责所有错误处理
                # 并确保 on_complete 回调被调用。
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                self.log.info("工作线程已取消。")
                break
            except Exception:
                # 如果 _handle_screenshot_task 足够健壮，理想情况下不应到达此块。
                # 这是一个最后的安全网。
                self.log.exception("截图工作循环中发生严重未处理错误。")
