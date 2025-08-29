# -*- coding: utf-8 -*-
import asyncio
import logging
import io
import struct
from typing import Generator, Tuple, Optional, Callable, Awaitable, Any
from collections import defaultdict

import av

from .client import TorrentClient, TorrentClientError
from .errors import (
    TaskError,
    NoVideoFileError,
    MoovNotFoundError,
    MoovFetchError,
    MoovParsingError,
    MetadataTimeoutError,
    FrameDownloadTimeoutError,
    FrameDecodeError
)
from .extractor import H264KeyframeExtractor, Keyframe, SampleInfo
from .generator import ScreenshotGenerator


StatusCallback = Callable[..., Awaitable[None]]


class ScreenshotService:
    def __init__(self, loop=None, num_workers=10, output_dir='./screenshots_output', torrent_save_path='/dev/shm', client=None, status_callback: Optional[StatusCallback] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.num_workers = num_workers
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotService")
        self.task_queue = asyncio.Queue()
        self.workers = []
        self._running = False
        self.client = client or TorrentClient(loop=self.loop, save_path=torrent_save_path)
        self.generator = ScreenshotGenerator(loop=self.loop, output_dir=self.output_dir)
        self.status_callback = status_callback
        self.active_tasks = set()

    async def run(self):
        self.log.info("正在启动 ScreenshotService...")
        self._running = True
        await self.client.start()
        for _ in range(self.num_workers):
            self.workers.append(self.loop.create_task(self._worker()))
        self.log.info(f"ScreenshotService 已启动，拥有 {self.num_workers} 个工作进程。")

    def stop(self):
        self.log.info("正在停止 ScreenshotService...")
        self._running = False
        self.client.stop()
        for worker in self.workers:
            worker.cancel()
        self.log.info("ScreenshotService 已停止。")

    async def submit_task(self, infohash: str, resume_data: dict = None):
        if infohash in self.active_tasks:
            self.log.warning(f"任务 {infohash} 已在处理中，本次提交被忽略。")
            return
        self.active_tasks.add(infohash)
        await self.task_queue.put({'infohash': infohash, 'resume_data': resume_data})
        if resume_data:
            self.log.info(f"为 infohash: {infohash} 重新提交了任务")
        else:
            self.log.info(f"为 infohash: {infohash} 提交了新任务")

    def _get_pieces_for_range(self, offset_in_torrent, size, piece_length):
        if size <= 0: return []
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data, offset_in_torrent, size, piece_length):
        buffer = bytearray(size)
        buffer_offset = 0
        start_piece = offset_in_torrent // piece_length
        end_piece = (offset_in_torrent + size - 1) // piece_length
        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data: continue
            piece_data = pieces_data[piece_index]
            copy_from_start = 0
            if piece_index == start_piece:
                copy_from_start = offset_in_torrent % piece_length
            copy_to_end = piece_length
            if piece_index == end_piece:
                copy_to_end = (offset_in_torrent + size - 1) % piece_length + 1
            chunk = piece_data[copy_from_start:copy_to_end]
            bytes_to_copy = len(chunk)
            if (buffer_offset + bytes_to_copy) > size:
                bytes_to_copy = size - buffer_offset
            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy
        return bytes(buffer)

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
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
            if size == 1:
                if current_offset + 16 > buffer_size:
                    self.log.warning(f"Box '{box_type}' 有一个 64 位的大小，但没有足够的数据用于头部。")
                    break
                size_64_data = stream.read(8)
                size = struct.unpack('>Q', size_64_data)[0]
                box_header_size = 16
            elif size == 0:
                size = buffer_size - current_offset
            if size < box_header_size:
                self.log.warning(f"Box '{box_type}' 的大小无效 {size}。停止解析。")
                break
            if current_offset + size > buffer_size:
                self.log.warning(f"大小为 {size} 的 Box '{box_type}' 超出了可用数据范围。无法完全解析。")
                full_box_data = stream_buffer[current_offset:buffer_size]
                yield box_type, bytes(full_box_data), current_offset, size
                break
            full_box_data = stream_buffer[current_offset:current_offset + size]
            yield box_type, bytes(full_box_data), current_offset, size
            current_offset += size

    async def _get_moov_atom_data(self, handle, video_file_offset, video_file_size, piece_length, infohash_hex) -> bytes:
        self.log.info("正在智能搜索 moov atom...")
        mdat_size_from_head = 0
        try:
            self.log.info("阶段 1: 探测文件头部以查找 moov atom...")
            initial_probe_size = 256 * 1024
            head_size = min(initial_probe_size, video_file_size)
            head_pieces = self._get_pieces_for_range(video_file_offset, head_size, piece_length)
            head_data_pieces = await self.client.fetch_pieces(handle, head_pieces, timeout=120)
            head_data = self._assemble_data_from_pieces(head_data_pieces, video_file_offset, head_size, piece_length)
            stream = io.BytesIO(head_data)
            for box_type, partial_box_data, box_offset, box_size in self._parse_mp4_boxes(stream):
                self.log.info(f"在头部发现 box '{box_type}'，偏移量 {box_offset}，大小 {box_size}。")
                if box_type == 'moov':
                    self.log.info("在头部探测中发现 'moov' atom。")
                    if len(partial_box_data) < box_size:
                        self.log.info(f"探测获得了部分 moov ({len(partial_box_data)}/{box_size} 字节)。正在获取完整数据。")
                        full_moov_offset_in_torrent = video_file_offset + box_offset
                        needed_pieces = self._get_pieces_for_range(full_moov_offset_in_torrent, box_size, piece_length)
                        moov_data_pieces = await self.client.fetch_pieces(handle, needed_pieces, timeout=120)
                        return self._assemble_data_from_pieces(moov_data_pieces, full_moov_offset_in_torrent, box_size, piece_length)
                    else:
                        return partial_box_data
                if box_type == 'mdat' and box_size > video_file_size * 0.8:
                    self.log.info("在头部发现大的 'mdat' box。假设 'moov' 在尾部。")
                    mdat_size_from_head = box_size
                    break
        except TorrentClientError as e:
            self.log.error(f"在头部探测期间发生 torrent 客户端错误: {e}")
            raise MoovFetchError(f"在 moov 头部探测期间发生 torrent 客户端错误: {e}", infohash_hex) from e
        try:
            self.log.info("阶段 2: Moov 不在头部，探测文件尾部。")
            tail_probe_size = (video_file_size - mdat_size_from_head) if mdat_size_from_head > 0 else (10 * 1024 * 1024)
            tail_file_offset = max(0, video_file_size - tail_probe_size)
            tail_torrent_offset = video_file_offset + tail_file_offset
            tail_size = min(tail_probe_size, video_file_size - tail_file_offset)
            tail_pieces = self._get_pieces_for_range(tail_torrent_offset, tail_size, piece_length)
            tail_data_pieces = await self.client.fetch_pieces(handle, tail_pieces, timeout=180)
            tail_data = self._assemble_data_from_pieces(tail_data_pieces, tail_torrent_offset, tail_size, piece_length)
            search_pos = len(tail_data)
            while search_pos > 4:
                found_pos = tail_data.rfind(b'moov', 0, search_pos)
                if found_pos == -1: break
                potential_start_pos = found_pos - 4
                if potential_start_pos < 0:
                    search_pos = found_pos
                    continue
                stream = io.BytesIO(tail_data)
                stream.seek(potential_start_pos)
                box_type, full_box_data, _, _ = next(self._parse_mp4_boxes(stream), (None, None, None, None))
                if box_type == 'moov':
                    self.log.info(f"成功从尾部解析 'moov' atom，大小为 {len(full_box_data)}。")
                    return full_box_data
                search_pos = found_pos
        except TorrentClientError as e:
            self.log.error(f"在尾部探测期间发生 torrent 客户端错误: {e}")
            raise MoovFetchError(f"在 moov 尾部探测期间发生 torrent 客户端错误: {e}", infohash_hex) from e
        raise MoovNotFoundError("无法在文件的第一部分/最后一部分定位 'moov' atom。", infohash_hex)

    def _find_video_file(self, ti: "lt.torrent_info") -> Tuple[int, int, int]:
        video_file_index, video_file_size, video_file_offset = -1, -1, -1
        fs = ti.files()
        for i in range(fs.num_files()):
            if fs.file_path(i).lower().endswith('.mp4') and fs.file_size(i) > video_file_size:
                video_file_size = fs.file_size(i)
                video_file_index = i
                video_file_offset = fs.file_offset(i)
        return video_file_index, video_file_size, video_file_offset

    def _select_keyframes(self, all_keyframes: list, timescale: int, samples: list) -> list:
        if not all_keyframes: return []
        MIN_SCREENSHOTS, MAX_SCREENSHOTS, TARGET_INTERVAL_SEC = 5, 50, 180
        duration_sec = samples[-1].pts / timescale if timescale > 0 else 0
        num_screenshots = max(MIN_SCREENSHOTS, min(int(duration_sec / TARGET_INTERVAL_SEC), MAX_SCREENSHOTS)) if duration_sec > 0 else 20
        if len(all_keyframes) <= num_screenshots:
            return all_keyframes
        else:
            indices = [int(i * len(all_keyframes) / num_screenshots) for i in range(num_screenshots)]
            return [all_keyframes[i] for i in sorted(list(set(indices)))]

    def _serialize_task_state(self, state: dict) -> dict:
        extractor = state['extractor']
        if not extractor: return None
        return {"infohash": state['infohash'],"piece_length": state['piece_length'],"video_file_offset": state['video_file_offset'],"video_file_size": state['video_file_size'],"extractor_info": {"extradata": extractor.extradata,"mode": extractor.mode,"nal_length_size": extractor.nal_length_size,"timescale": extractor.timescale,"samples": [s._asdict() for s in extractor.samples],},"all_keyframes": [k._asdict() for k in state['all_keyframes']],"selected_keyframes": [k._asdict() for k in state['selected_keyframes']],"completed_pieces": list(state['completed_pieces']),"processed_keyframes": list(state['processed_keyframes']),}

    def _load_state_from_resume_data(self, data: dict) -> dict:
        extractor = H264KeyframeExtractor(moov_data=None)
        ext_info = data['extractor_info']
        extractor.extradata = ext_info['extradata']
        extractor.mode = ext_info['mode']
        extractor.nal_length_size = ext_info['nal_length_size']
        extractor.timescale = ext_info['timescale']
        extractor.samples = [SampleInfo(**s) for s in ext_info['samples']]
        all_keyframes = [Keyframe(**k) for k in data['all_keyframes']]
        extractor.keyframes = all_keyframes
        return {"infohash": data['infohash'],"piece_length": data['piece_length'],"video_file_offset": data['video_file_offset'],"video_file_size": data['video_file_size'],"extractor": extractor,"all_keyframes": all_keyframes,"selected_keyframes": [Keyframe(**k) for k in data['selected_keyframes']],"completed_pieces": set(data['completed_pieces']),"processed_keyframes": set(data['processed_keyframes']),}

    async def _generate_screenshots_from_torrent(self, handle, infohash_hex, resume_data=None):
        task_state = {}
        if resume_data:
            self.log.info(f"[{infohash_hex}] 正在从提供的数据恢复任务。")
            try:
                task_state = self._load_state_from_resume_data(resume_data)
            except (KeyError, TypeError) as e:
                self.log.error(f"[{infohash_hex}] 加载恢复数据失败，将重新开始。错误: {e}")
                resume_data = None
        if not resume_data:
            self.log.info(f"[{infohash_hex}] 正在开始新的截图任务分析。")
            ti = handle.get_torrent_info()
            piece_length = ti.piece_length()
            video_file_index, video_file_size, video_file_offset = self._find_video_file(ti)
            if video_file_index == -1:
                raise NoVideoFileError("在 torrent 中没有找到 .mp4 文件。", infohash_hex)
            video_file_path = ti.files().file_path(video_file_index)
            self.log.info(f"[{infohash_hex}] 找到视频文件: '{video_file_path}' ({video_file_size} 字节)。")
            moov_data = await self._get_moov_atom_data(handle, video_file_offset, video_file_size, piece_length, infohash_hex)
            try:
                extractor = H264KeyframeExtractor(moov_data)
                if not extractor.keyframes:
                    raise MoovParsingError("无法从 moov atom 中提取任何关键帧。", infohash_hex)
            except Exception as e:
                raise MoovParsingError(f"解析 moov 数据失败: {e}", infohash_hex) from e
            all_keyframes = extractor.keyframes
            selected_keyframes = self._select_keyframes(all_keyframes, extractor.timescale, extractor.samples)
            self.log.info(f"[{infohash_hex}] 从总共 {len(all_keyframes)} 个关键帧中选择了 {len(selected_keyframes)} 个。")
            task_state = {"infohash": infohash_hex, "piece_length": piece_length,"video_file_offset": video_file_offset, "video_file_size": video_file_size,"extractor": extractor, "all_keyframes": all_keyframes,"selected_keyframes": selected_keyframes, "completed_pieces": set(),"processed_keyframes": set()}

        extractor = task_state['extractor']
        selected_keyframes = task_state['selected_keyframes']
        video_file_offset = task_state['video_file_offset']
        piece_length = task_state['piece_length']

        keyframe_info = {}
        piece_to_keyframes = defaultdict(list)
        all_needed_pieces = set()
        remaining_keyframes = [kf for kf in selected_keyframes if kf.index not in task_state.get('processed_keyframes', set())]

        self.log.info(f"[{infohash_hex}] 需要处理 {len(remaining_keyframes)} 个关键帧。")
        if not remaining_keyframes:
            self.log.info(f"[{infohash_hex}] 所有选定的关键帧都已处理。")
            return

        for kf in remaining_keyframes:
            sample = extractor.samples[kf.sample_index - 1]
            offset = video_file_offset + sample.offset
            needed = self._get_pieces_for_range(offset, sample.size, piece_length)
            keyframe_info[kf.index] = {'keyframe': kf, 'needed_pieces': set(needed)}
            for piece_idx in needed:
                piece_to_keyframes[piece_idx].append(kf.index)
            all_needed_pieces.update(needed)

        pieces_to_request = list(all_needed_pieces - task_state.get('completed_pieces', set()))
        self.log.info(f"[{infohash_hex}] 正在从 torrent 客户端请求 {len(pieces_to_request)} 个新 piece。")

        local_queue = asyncio.Queue()
        self.client.subscribe_pieces(infohash_hex, local_queue)

        try:
            self.client.request_pieces(handle, pieces_to_request)
            processed_this_run = set()
            generation_tasks = []

            while len(processed_this_run) < len(remaining_keyframes):
                try:
                    finished_piece = await asyncio.wait_for(local_queue.get(), timeout=300)
                    if finished_piece is None: # Sentinel value indicates torrent is finished
                        self.log.warning(f"[{infohash_hex}] Torrent 完成但仍有 piece 未下载，提前终止。")
                        break
                except asyncio.TimeoutError:
                    self.log.warning(f"[{infohash_hex}] 等待 piece 超时。")
                    break

                task_state.setdefault('completed_pieces', set()).add(finished_piece)

                if finished_piece not in piece_to_keyframes: continue

                for kf_index in piece_to_keyframes[finished_piece]:
                    info = keyframe_info.get(kf_index)
                    if not info or kf_index in processed_this_run: continue

                    info['needed_pieces'].remove(finished_piece)
                    if not info['needed_pieces']:
                        self.log.info(f"[{infohash_hex}] 关键帧 {kf_index} 的所有 piece 都已准备好。正在获取数据并生成截图任务。")

                        keyframe = info['keyframe']
                        sample = extractor.samples[keyframe.sample_index - 1]

                        try:
                            keyframe_pieces_data = await self.client.fetch_pieces(handle, self._get_pieces_for_range(video_file_offset + sample.offset, sample.size, piece_length), timeout=60)
                            packet_data_bytes = self._assemble_data_from_pieces(keyframe_pieces_data, video_file_offset + sample.offset, sample.size, piece_length)
                        except TorrentClientError as e:
                            self.log.warning(f"获取关键帧 {keyframe.index} 数据失败: {e}，跳过。")
                            processed_this_run.add(kf_index)
                            continue

                        if len(packet_data_bytes) != sample.size:
                            self.log.warning(f"关键帧 {keyframe.index} 的数据不完整，跳过。")
                            processed_this_run.add(kf_index)
                            continue

                        if extractor.mode == 'avc1':
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

                        ts_sec = keyframe.pts / keyframe.timescale if keyframe.timescale > 0 else keyframe.index
                        m, s = divmod(ts_sec, 60)
                        h, m = divmod(m, 60)
                        timestamp_str = f"{int(h):02d}-{int(m):02d}-{int(s):02d}"

                        task = self.loop.create_task(self.generator.generate(
                            extradata=extractor.extradata,
                            packet_data=packet_data,
                            infohash_hex=infohash_hex,
                            timestamp_str=timestamp_str
                        ))
                        generation_tasks.append(task)
                        processed_this_run.add(kf_index)

            if not generation_tasks and len(remaining_keyframes) > 0:
                 raise FrameDownloadTimeoutError(f"没有成功下载任何关键帧的数据。", infohash_hex, resume_data=self._serialize_task_state(task_state))

            results = await asyncio.gather(*generation_tasks, return_exceptions=True)
        finally:
            self.client.unsubscribe_pieces(infohash_hex, local_queue)

        for result in results:
            if isinstance(result, Exception):
                raise result # Let the main handler catch it

        # Check if all were processed
        if len(processed_this_run) < len(remaining_keyframes):
            self.log.warning(f"[{infohash_hex}] 未能为所有选定的关键帧生成截图。")
        else:
            self.log.info(f"[{infohash_hex}] 截图任务成功完成。")

    async def _send_status_update(self, **kwargs: Any) -> None:
        if self.status_callback:
            loop = asyncio.get_running_loop()
            loop.create_task(self.status_callback(**kwargs))

    async def _handle_screenshot_task(self, task_info: dict):
        infohash_hex = task_info['infohash']
        resume_data = task_info.get('resume_data')
        task_succeeded = False
        log_message = f"正在处理任务: {infohash_hex}"
        if resume_data:
            log_message += " (正在恢复)"
        self.log.info(log_message)
        handle = None
        try:
            handle = await self.client.add_torrent(infohash_hex)
            if not handle or not handle.is_valid():
                raise TorrentClientError(f"无法为 {infohash_hex} 获取有效的 torrent handle。")
            await self._generate_screenshots_from_torrent(handle, infohash_hex, resume_data)
            task_succeeded = True
        except (FrameDownloadTimeoutError, MetadataTimeoutError, MoovFetchError, asyncio.TimeoutError) as e:
            self.log.warning(f"任务 {infohash_hex} 因可恢复的错误而失败: {e}")
            infohash = getattr(e, 'infohash', infohash_hex)
            await self._send_status_update(status='recoverable_failure',infohash=infohash,message=str(e),resume_data=getattr(e, 'resume_data', None))
        except TaskError as e:
            self.log.error(f"任务 {infohash_hex} 因永久性错误而失败: {e}")
            await self._send_status_update(status='permanent_failure',infohash=e.infohash,message=str(e))
        except TorrentClientError as e:
            self.log.error(f"任务 {infohash_hex} 因 torrent 客户端错误而失败: {e}")
            await self._send_status_update(status='permanent_failure',infohash=infohash_hex,message=str(e))
        except Exception as e:
            self.log.exception(f"处理 {infohash_hex} 时发生意外的严重错误。")
            await self._send_status_update(status='permanent_failure',infohash=infohash_hex,message=f"发生意外错误: {e}")
        finally:
            if task_succeeded:
                self.log.info(f"任务 {infohash_hex} 成功完成。")
                await self._send_status_update(status='success',infohash=infohash_hex,message='任务成功完成。')
            if handle and handle.is_valid():
                await self.client.remove_torrent(handle)
            self.active_tasks.discard(infohash_hex)

    async def _worker(self):
        while self._running:
            try:
                task_info = await self.task_queue.get()
                await self._handle_screenshot_task(task_info)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("截图工作进程中发生错误。")
