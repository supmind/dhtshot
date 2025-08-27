# -*- coding: utf-8 -*-
"""
该模块包含 TorrentClient 类，它负责所有与 libtorrent 库的直接交互。
"""
import asyncio
import logging
import libtorrent as lt
import threading
from collections import defaultdict
from concurrent.futures import Future

class LibtorrentError(Exception):
    """自定义异常，用于清晰地传递来自 libtorrent 核心的特定错误。"""
    def __init__(self, error_code_or_message):
        if hasattr(error_code_or_message, 'message'):
            # It's a libtorrent error code object
            self.error_code = error_code_or_message
            message = self.error_code.message()
        else:
            # It's a simple string message
            self.error_code = None
            message = str(error_code_or_message)
        super().__init__(f"Libtorrent 错误: {message}")

class TorrentClient:
    """一个 libtorrent 会话的包装器，用于处理 torrent 相关操作。"""
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        settings = {
            'listen_interfaces': '0.0.0.0:6881', 'enable_dht': True,
            'alert_mask': lt.alert_category.error | lt.alert_category.status | lt.alert_category.storage,
            'dht_bootstrap_nodes': 'dht.libtorrent.org:25401,router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881,router.bt.ouinet.work:6881',
        }
        self.ses = lt.session(settings)
        self.alert_task = None
        self._running = False
        self.dht_ready = asyncio.Event()
        self.pending_metadata = {}
        self.pending_reads = defaultdict(list)
        self.pending_reads_lock = threading.Lock()
        self.piece_download_futures = defaultdict(list)

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
        """通过 infohash 添加 torrent，并在收到元数据后返回句柄。"""
        self.log.info(f"正在为 infohash 添加 torrent: {infohash}")
        save_dir = f"/dev/shm/{infohash}"

        meta_future = self.loop.create_future()
        self.pending_metadata[infohash] = meta_future
        trackers = [
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://open.demonii.com:1337/announce",
            "udp://open.stealth.si:80/announce",
            "udp://exodus.desync.com:6969/announce",
            "udp://tracker.bittor.pw:1337/announce",
        ]
        magnet_uri = f"magnet:?xt=urn:btih:{infohash}&{'&'.join(['tr=' + t for t in trackers])}"
        params = lt.parse_magnet_uri(magnet_uri)
        # 为 add_torrent_params 对象设置 save_path 属性，而不是通过字典赋值
        params.save_path = save_dir

        handle = self.ses.add_torrent(params)

        # Wait for DHT to bootstrap, but with a timeout. This helps with trackerless torrents
        # without blocking indefinitely.
        self.log.debug("Waiting for DHT bootstrap...")
        try:
            await asyncio.wait_for(self.dht_ready.wait(), timeout=30)
            self.log.info("DHT bootstrap successful.")
        except asyncio.TimeoutError:
            self.log.warning("DHT bootstrap timed out, proceeding without it.")

        self.log.debug(f"正在等待 {infohash} 的元数据...")
        try:
            handle = await asyncio.wait_for(meta_future, timeout=180)
        except asyncio.TimeoutError:
            self.log.error(f"为 {infohash} 获取元数据超时。")
            self.pending_metadata.pop(infohash, None)
            raise LibtorrentError(f"为 {infohash} 获取元数据超时。")

        return handle

    def remove_torrent(self, handle):
        """从会话中移除一个 torrent。"""
        if handle and handle.is_valid():
            infohash = str(handle.info_hash())
            self.pending_metadata.pop(infohash, None)
            self.ses.remove_torrent(handle, lt.session.delete_files)
            self.log.info(f"已移除 torrent: {infohash}")

    async def download_and_read_piece(self, handle, piece_index, timeout=240.0):
        """
        Asynchronously downloads and reads a single piece with a total timeout.
        """
        async def _download_and_read():
            if not handle.have_piece(piece_index):
                self.log.debug(f"Piece {piece_index}: Not available, setting high priority and waiting for download.")
                handle.piece_priority(piece_index, 7)
                download_future = self.loop.create_future()
                self.piece_download_futures[piece_index].append(download_future)
                await download_future
                # No need to check have_piece again, if the future resolved, it's downloaded.

            read_future = self.loop.create_future()
            with self.pending_reads_lock:
                self.pending_reads[piece_index].append(read_future)
            handle.read_piece(piece_index)
            return await read_future

        try:
            return await asyncio.wait_for(_download_and_read(), timeout=timeout)
        except asyncio.TimeoutError:
            # Clean up futures to prevent memory leaks
            self.piece_download_futures.pop(piece_index, None)
            with self.pending_reads_lock:
                self.pending_reads.pop(piece_index, None)

            error_msg = f"Timeout occurred while downloading or reading piece {piece_index}."
            self.log.error(error_msg)
            raise LibtorrentError(error_msg)

    def _handle_metadata_received(self, alert):
        """处理接收到元数据的警报。"""
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata and not self.pending_metadata[infohash_str].done():
            self.pending_metadata[infohash_str].set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        """处理 piece 下载完成的警报。"""
        futures = self.piece_download_futures.pop(alert.piece_index, [])
        for future in futures:
            if not future.done(): future.set_result(True)

    def _handle_dht_bootstrap(self, alert):
        """处理 DHT 引导完成的警报。"""
        if not self.dht_ready.is_set(): self.dht_ready.set()

    def _handle_read_piece(self, alert):
        """处理 piece 读取完成的警报。"""
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])
        if alert.error and alert.error.value() != 0:
            # 在某些情况下，即使出现非零错误码，libtorrent 也会报告“成功”
            if "success" not in alert.error.message().lower():
                error = LibtorrentError(alert.error)
                for future in futures:
                    if not future.done(): future.set_exception(error)
                return
        data = bytes(alert.buffer)
        for future in futures:
            if not future.done(): future.set_result(data)

    async def _alert_loop(self):
        """libtorrent 会话的主警报处理循环。"""
        while self._running:
            try:
                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    if alert.category() & lt.alert_category.error: self.log.error(f"Libtorrent 警报: {alert}")
                    else: self.log.debug(f"Libtorrent 警报: {alert}")
                    if isinstance(alert, lt.metadata_received_alert): self._handle_metadata_received(alert)
                    elif isinstance(alert, lt.piece_finished_alert): self._handle_piece_finished(alert)
                    elif isinstance(alert, lt.read_piece_alert): self._handle_read_piece(alert)
                    elif isinstance(alert, lt.dht_bootstrap_alert): self._handle_dht_bootstrap(alert)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("libtorrent 警报循环中发生错误。")
