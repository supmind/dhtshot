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

from .errors import TorrentClientError, MetadataTimeoutError


class TorrentClient:
    """一个 libtorrent 会话的包装器，用于处理 torrent 相关操作。"""
    def __init__(self, loop=None, save_path='/dev/shm'):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        self.save_path = save_path

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
        # 会话及其对象不是线程安全的。
        self._ses = lt.session(settings)
        self._ses_lock = threading.Lock()

        self._thread = None
        self._running = False

        self.dht_ready = asyncio.Event()
        self.pending_metadata = {} # infohash -> future
        self.pending_reads = defaultdict(list)
        self.pending_reads_lock = threading.Lock()

        self.pending_fetches = {} # fetch_id -> {'future': future, 'remaining': set}
        self.fetch_lock = threading.Lock()
        self.next_fetch_id = 0

        self.last_dht_log_time = 0
        self.finished_piece_queue = asyncio.Queue()

    async def _execute_sync(self, func, *args, **kwargs):
        """以线程安全的方式执行接触 libtorrent 对象的功能。"""
        return await self.loop.run_in_executor(None, self._sync_wrapper, func, *args, **kwargs)

    def _sync_wrapper(self, func, *args, **kwargs):
        """在执行函数前获取锁的内部包装器。"""
        with self._ses_lock:
            return func(*args, **kwargs)

    def _execute_sync_nowait(self, func, *args, **kwargs):
        """执行一个接触 libtorrent 对象的函数，但不等待结果。"""
        self.loop.run_in_executor(None, self._sync_wrapper, func, *args, **kwargs)

    async def start(self):
        """启动 torrent 客户端的警报循环。"""
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self._thread = threading.Thread(target=self._alert_loop, daemon=True)
        self._thread.start()
        self.log.info("TorrentClient 已启动。")

    def stop(self):
        """停止 torrent 客户端的警报循环。"""
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self._thread and self._thread.is_alive():
            # 这个调用会唤醒 self._ses.wait_for_alert()
            self._execute_sync_nowait(self._ses.post_dht_stats)
            self._thread.join()
        self.log.info("TorrentClient 已停止。")

    async def add_torrent(self, infohash: str):
        """通过 infohash 添加 torrent，并在收到元数据后返回句柄。"""
        self.log.info(f"正在为 infohash 添加 torrent: {infohash}")
        save_dir = os.path.join(self.save_path, infohash)
        meta_future = self.loop.create_future()
        self.pending_metadata[infohash] = meta_future

        trackers = [
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://open.demonii.com:1337/announce",
            "udp://open.stealth.si:80/announce",
            "udp://exodus.desync.com:6969/announce",
            "udp://tracker.bittor.pw:1337/announce",
            "http://sukebei.tracker.wf:8888/announce",
            "udp://tracker.torrent.eu.org:451/announce",
        ]
        magnet_uri = f"magnet:?xt=urn:btih:{infohash}&{'&'.join(['tr=' + t for t in trackers])}"

        params = lt.parse_magnet_uri(magnet_uri)
        params.save_path = save_dir
        params.flags |= lt.torrent_flags.paused

        handle = await self._execute_sync(self._ses.add_torrent, params)

        self.log.debug(f"正在等待 {infohash} 的元数据...")
        try:
            handle = await asyncio.wait_for(meta_future, timeout=180)
        except asyncio.TimeoutError:
            self.log.error(f"为 {infohash} 获取元数据超时。")
            self.pending_metadata.pop(infohash, None)
            # 引发特定的、结构化的错误
            raise MetadataTimeoutError(f"获取元数据超时", infohash=infohash)

        ti = await self._execute_sync(handle.get_torrent_info)
        if ti:
            self.log.info(f"为 {infohash} 设置所有 piece 优先级为 0。")
            priorities = [0] * ti.num_pieces()
            await self._execute_sync(handle.prioritize_pieces, priorities)

        return handle

    async def remove_torrent(self, handle):
        """从会话中移除一个 torrent。"""
        if handle and handle.is_valid():
            infohash = str(await self._execute_sync(handle.info_hash))
            self.pending_metadata.pop(infohash, None)
            await self._execute_sync(self._ses.remove_torrent, handle, lt.session.delete_files)
            self.log.info(f"已移除 torrent: {infohash}")

    def request_pieces(self, handle, piece_indices: list[int]):
        """按需请求一组特定的 piece，但不等待它们完成。"""
        if not piece_indices:
            return

        unique_indices = sorted(list(set(piece_indices)))

        def _request_sync():
            # 如果 have_piece 在调用之间发生变化，这并非完全安全，但对于此目的已足够好。
            pieces_to_request = [p for p in unique_indices if not handle.have_piece(p)]
            if pieces_to_request:
                self.log.info(f"为 pieces {pieces_to_request} 设置高优先级。")
                priorities = [(p, 7) for p in pieces_to_request]
                # prioritize_pieces 比逐个设置更高效
                handle.prioritize_pieces(priorities)
                handle.resume()

        self._execute_sync_nowait(_request_sync)

    async def fetch_pieces(self, handle, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        """按需下载、读取并返回一组特定的 piece。"""
        if not piece_indices: return {}

        unique_indices = sorted(list(set(piece_indices)))

        pieces_to_download = await self._execute_sync(lambda: [p for p in unique_indices if not handle.have_piece(p)])

        if pieces_to_download:
            self.log.info(f"需要下载 {len(pieces_to_download)} 个 pieces: {pieces_to_download}")

            def _set_priorities_sync():
                priorities = [(p, 7) for p in pieces_to_download]
                handle.prioritize_pieces(priorities)
                handle.resume()
            await self._execute_sync(_set_priorities_sync)

            request_future = self.loop.create_future()
            with self.fetch_lock:
                fetch_id = self.next_fetch_id
                self.next_fetch_id += 1
                self.pending_fetches[fetch_id] = {'future': request_future, 'remaining': set(pieces_to_download)}

            try:
                await asyncio.wait_for(request_future, timeout=timeout)
                self.log.info(f"成功等到 pieces: {pieces_to_download}")
            except asyncio.TimeoutError:
                with self.fetch_lock: self.pending_fetches.pop(fetch_id, None)
                raise TorrentClientError(f"下载 pieces {pieces_to_download} 超时。")

        self.log.info(f"所有需要的 pieces ({unique_indices}) 均已就绪，开始读取。")

        async def _read_piece(piece_index):
            read_future = self.loop.create_future()
            with self.pending_reads_lock:
                self.pending_reads[piece_index].append(read_future)
            self._execute_sync_nowait(handle.read_piece, piece_index)
            return await read_future

        try:
            tasks = [_read_piece(p) for p in unique_indices]
            results = await asyncio.gather(*tasks)
            return dict(zip(unique_indices, results))
        except asyncio.TimeoutError:
            raise TorrentClientError(f"读取 pieces {unique_indices} 超时。")

    def _handle_metadata_received(self, alert):
        infohash_str = str(alert.handle.info_hash())
        future = self.pending_metadata.get(infohash_str)
        if future and not future.done():
            self.loop.call_soon_threadsafe(future.set_result, alert.handle)

    def _handle_piece_finished(self, alert):
        piece_index = alert.piece_index
        self.log.info(f"收到 piece 下载完成通知: piece #{piece_index}")
        self.loop.call_soon_threadsafe(self.finished_piece_queue.put_nowait, piece_index)
        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if piece_index in request['remaining']:
                    request['remaining'].remove(piece_index)
                    if not request['remaining']:
                        future = request['future']
                        self.loop.call_soon_threadsafe(future.set_result, True)
                        self.pending_fetches.pop(fetch_id, None)

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set():
            self.loop.call_soon_threadsafe(self.dht_ready.set)

    def _handle_read_piece(self, alert):
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])
        error = None
        if alert.error and alert.error.value() != 0:
            if "success" not in alert.error.message().lower():
                error = TorrentClientError(alert.error.message())
        data = bytes(alert.buffer)
        for future in futures:
            if not future.done():
                if error:
                    self.loop.call_soon_threadsafe(future.set_exception, error)
                else:
                    self.loop.call_soon_threadsafe(future.set_result, data)

    def _alert_loop(self):
        """libtorrent 会话的主警报处理循环。在专用线程中运行。"""
        while self._running:
            alerts = []
            with self._ses_lock:
                if not self._running: break
                # 等待最多 1 秒，直到有警报
                alert = self._ses.wait_for_alert(1000)
                if alert:
                    alerts = self._ses.pop_alerts()

            for alert in alerts:
                if alert.category() & lt.alert_category.error:
                    self.log.error(f"Libtorrent 警报: {alert}")
                else:
                    self.log.debug(f"Libtorrent 警报: {alert}")

                alert_map = {
                    lt.metadata_received_alert: self._handle_metadata_received,
                    lt.piece_finished_alert: self._handle_piece_finished,
                    lt.read_piece_alert: self._handle_read_piece,
                    lt.dht_bootstrap_alert: self._handle_dht_bootstrap,
                }
                handler = alert_map.get(type(alert))
                if handler:
                    handler(alert)
                elif type(alert) == lt.state_update_alert:
                    with self._ses_lock:
                        for s in alert.status:
                            if s.state != lt.torrent_status.states.seeding:
                                self.log.info(
                                    f"  状态更新 - {s.name}: {s.state_str} {s.progress * 100:.2f}% "
                                    f"| 下载速度: {s.download_rate / 1000:.1f} kB/s "
                                    f"| 节点: {s.num_peers} ({s.num_seeds} 种子)"
                                )

            now = time.time()
            if now - self.last_dht_log_time > 10:
                with self._ses_lock:
                    status = self._ses.status()
                    self.log.info(f"DHT 状态: {status.dht_nodes} 个节点。")
                self.last_dht_log_time = now
