# -*- coding: utf-8 -*-
"""
该模块包含 TorrentClient 类，它负责所有与 libtorrent 库的直接交互。
"""
import asyncio
import logging
import os
import time
import libtorrent as lt
import threading
from collections import defaultdict
from concurrent.futures import Future

class LibtorrentError(Exception):
    """自定义异常，用于清晰地传递来自 libtorrent 核心的特定错误。"""
    def __init__(self, error_code_or_message):
        if hasattr(error_code_or_message, 'message'):
            self.error_code = error_code_or_message
            message = self.error_code.message()
        else:
            self.error_code = None
            message = str(error_code_or_message)
        super().__init__(f"Libtorrent 错误: {message}")

class TorrentClient:
    """一个 libtorrent 会话的包装器，用于处理 torrent 相关操作。"""
    def __init__(self, loop=None, save_path='/dev/shm'):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        self.save_path = save_path
        self.ses = lt.session()
        settings = {
            'listen_interfaces': '0.0.0.0:6881',
            'enable_dht': True,
            'alert_mask': (
                lt.alert_category.error |
                lt.alert_category.status |
                lt.alert_category.storage |
                lt.alert_category.piece_progress
            ),
            'dht_bootstrap_nodes': 'dht.libtorrent.org:25401,router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881,router.bt.ouinet.work:6881',
            'user_agent': 'qBittorrent/4.5.2',
            'peer_fingerprint': 'qB4520',
        }
        self.ses.apply_settings(settings)
        self.alert_task = None
        self._running = False
        self.dht_ready = asyncio.Event()

        # --- 缓存与状态管理 ---
        self.handles = {}  # torrent 句柄的缓存 {infohash: handle}
        self.pending_metadata = {}  # 等待元数据的 Future {infohash: Future}
        self.pending_reads = defaultdict(list) # 等待读取 piece 的 Future
        self.pending_reads_lock = threading.Lock() # pending_reads 的锁
        self.pending_fetches = {} # 等待下载 piece 的 Future
        self.fetch_lock = threading.Lock() # pending_fetches 的锁
        self.next_fetch_id = 0 # 下一个 fetch 请求的 ID

        self.last_dht_log_time = 0
        self.finished_piece_queue = asyncio.Queue() # 已完成 piece 的队列

    async def start(self):
        """启动 torrent 客户端的警报循环。"""
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self.alert_task = self.loop.create_task(self._alert_loop())
        self.log.info("TorrentClient 已启动。")

    def stop(self):
        """停止 torrent 客户端的警报循环。"""
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self.alert_task:
            self.alert_task.cancel()
        self.log.info("TorrentClient 已停止。")

    async def add_torrent(self, infohash: str):
        """
        通过 infohash 添加 torrent 或获取现有 torrent 的句柄。
        在收到元数据后返回句柄。
        """
        # 首先检查缓存
        if infohash in self.handles and self.handles[infohash].is_valid():
            self.log.info(f"从缓存返回 {infohash} 的现有句柄。")
            return self.handles[infohash]

        # 如果不在缓存中或无效，则添加它
        self.log.info(f"为 infohash 添加新 torrent: {infohash}")
        save_dir = os.path.join(self.save_path, infohash)

        meta_future = self.loop.create_future()
        self.pending_metadata[infohash] = meta_future

        params = lt.parse_magnet_uri(f"magnet:?xt=urn:btih:{infohash}")
        params.save_path = save_dir
        handle = self.ses.add_torrent(params)
        self.handles[infohash] = handle  # 立即添加到缓存

        self.log.debug(f"正在等待 {infohash} 的元数据...")
        try:
            # 等待元数据，设置超时
            await asyncio.wait_for(meta_future, timeout=180)
        except asyncio.TimeoutError:
            self.log.error(f"为 {infohash} 获取元数据超时。")
            # 超时后清理
            self.handles.pop(infohash, None)
            self.pending_metadata.pop(infohash, None)
            raise LibtorrentError(f"为 {infohash} 获取元数据超时。")

        # 成功获取元数据后，暂停 torrent 并将所有 piece 优先级设为 0，
        # 这样我们就只下载明确请求的 piece。
        self.log.info(f"成功获取 {infohash} 的元数据。暂停 torrent。")
        handle.pause()

        ti = handle.get_torrent_info()
        if ti:
            self.log.info(f"为 {infohash} 设置所有 piece 优先级为 0。")
            for i in range(ti.num_pieces()):
                handle.piece_priority(i, 0)

        return handle

    def remove_torrent(self, handle):
        """从会话和缓存中移除一个 torrent。"""
        if handle and handle.is_valid():
            infohash = str(handle.info_hash())
            self.handles.pop(infohash, None)
            self.pending_metadata.pop(infohash, None)
            self.ses.remove_torrent(handle, lt.session.delete_files)
            self.log.info(f"已移除 torrent: {infohash}")

    def request_pieces(self, handle, piece_indices: list[int]):
        """按需请求一组特定的 piece，但不等待它们完成。"""
        if not handle.is_valid():
            self.log.warning("请求 pieces 时使用了无效的句柄。")
            return
        if not piece_indices:
            return

        unique_indices = sorted(list(set(piece_indices)))
        # 只请求尚未拥有的 piece
        pieces_to_request = [p for p in unique_indices if not handle.have_piece(p)]

        if pieces_to_request:
            # 将需要的 piece 优先级设为最高 (7)
            for p in pieces_to_request:
                handle.piece_priority(p, 7)
            # 恢复 torrent 开始下载
            handle.resume()

    async def fetch_pieces(self, handle, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        """按需下载、读取并返回一组特定的 piece。"""
        if not handle.is_valid():
            raise LibtorrentError("获取 pieces 时使用了无效的句柄。")
        if not piece_indices:
            return {}

        unique_indices = sorted(list(set(piece_indices)))
        pieces_to_download = [p for p in unique_indices if not handle.have_piece(p)]

        if pieces_to_download:
            self.log.info(f"需要下载 {len(pieces_to_download)} 个 pieces: {pieces_to_download}")
            for p in pieces_to_download:
                handle.piece_priority(p, 7)
            handle.resume()

            request_future = self.loop.create_future()
            with self.fetch_lock:
                fetch_id = self.next_fetch_id
                self.next_fetch_id += 1
                self.pending_fetches[fetch_id] = {
                    'future': request_future,
                    'remaining': set(pieces_to_download)
                }

            try:
                await asyncio.wait_for(request_future, timeout=timeout)
            except asyncio.TimeoutError:
                self.log.error(f"等待 pieces {pieces_to_download} 超时。")
                with self.fetch_lock: self.pending_fetches.pop(fetch_id, None)
                raise LibtorrentError(f"下载 pieces {pieces_to_download} 超时。")

        async def _read_piece(piece_index):
            """辅助函数，用于异步读取单个 piece。"""
            read_future = self.loop.create_future()
            with self.pending_reads_lock:
                self.pending_reads[piece_index].append(read_future)
            handle.read_piece(piece_index)
            return await read_future

        try:
            tasks = [_read_piece(p) for p in unique_indices]
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
            return dict(zip(unique_indices, results))
        except asyncio.TimeoutError:
            raise LibtorrentError(f"读取 pieces {unique_indices} 超时。")
        except Exception as e:
            if "invalid torrent handle" in str(e):
                 raise LibtorrentError(f"读取 pieces 时句柄失效: {unique_indices}")
            raise


    def _handle_metadata_received(self, alert):
        """处理接收到元数据的警报。"""
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata and not self.pending_metadata[infohash_str].done():
            self.pending_metadata[infohash_str].set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        """处理 piece 下载完成的警报。"""
        piece_index = alert.piece_index
        self.finished_piece_queue.put_nowait(piece_index)

        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if piece_index in request['remaining']:
                    request['remaining'].remove(piece_index)
                    if not request['remaining']:
                        if not request['future'].done():
                            request['future'].set_result(True)
                        self.pending_fetches.pop(fetch_id, None)

    def _handle_dht_bootstrap(self, alert):
        """处理 DHT 引导完成的警报。"""
        if not self.dht_ready.is_set(): self.dht_ready.set()

    def _handle_read_piece(self, alert):
        """处理 piece 读取完成的警报。"""
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])

        error = None
        if alert.error and alert.error.value() != 0:
            # 忽略非错误的 "success" 消息
            if "success" not in alert.error.message().lower():
                error = LibtorrentError(alert.error)

        data = bytes(alert.buffer)
        for future in futures:
            if not future.done():
                if error:
                    future.set_exception(error)
                else:
                    future.set_result(data)

    async def _alert_loop(self):
        """libtorrent 会话的主警报处理循环。"""
        while self._running:
            try:
                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    # 记录所有错误警报
                    if alert.category() & lt.alert_category.error:
                        self.log.error(f"Libtorrent 警报: {alert}")

                    alert_type = type(alert)
                    if alert_type == lt.metadata_received_alert:
                        self._handle_metadata_received(alert)
                    elif alert_type == lt.piece_finished_alert:
                        self._handle_piece_finished(alert)
                    elif alert_type == lt.read_piece_alert:
                        self._handle_read_piece(alert)
                    elif alert_type == lt.dht_bootstrap_alert:
                        self._handle_dht_bootstrap(alert)

                now = time.time()
                # 每隔 10 秒记录一次 DHT 状态
                if now - self.last_dht_log_time > 10:
                    status = self.ses.status()
                    self.log.info(f"DHT 状态: {status.dht_nodes} 个节点。")
                    self.last_dht_log_time = now

                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("libtorrent 警报循环中发生错误。")
