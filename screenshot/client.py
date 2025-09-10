# -*- coding: utf-8 -*-
"""
该模块包含 TorrentClient 类，它是一个围绕 libtorrent 库的异步包装器，
负责所有与 BitTorrent 网络的直接交互。
"""
import asyncio
import logging
import os
import time
import libtorrent as lt
import threading
import queue
from collections import defaultdict
from contextlib import asynccontextmanager
import tempfile

from .errors import TorrentClientError, MetadataTimeoutError
from config import Settings


class TorrentClient:
    """
    一个 libtorrent 会话的异步包装器，用于处理 torrent 相关操作。
    """
    def __init__(self, loop=None, settings: Settings = None):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        app_settings = settings or Settings()
        self.save_path = app_settings.torrent_save_path
        self.metadata_timeout = app_settings.metadata_timeout
        settings_pack = {
            'listen_interfaces': app_settings.lt_listen_interfaces,
            'user_agent': 'qBittorrent/4.5.2', 'peer_fingerprint': 'qB4520',
            'dht_bootstrap_nodes': 'dht.libtorrent.org:25401,router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881,router.bt.ouinet.work:6881',
            'enable_dht': True, 'active_limit': app_settings.lt_active_limit,
            'active_downloads': app_settings.lt_active_downloads, 'connections_limit': app_settings.lt_connections_limit,
            'upload_rate_limit': app_settings.lt_upload_rate_limit, 'download_rate_limit': app_settings.lt_download_rate_limit,
            'peer_connect_timeout': app_settings.lt_peer_connect_timeout, 'cache_size': app_settings.lt_cache_size,
            'alert_mask': (lt.alert_category.error | lt.alert_category.status | lt.alert_category.storage | lt.alert_category.piece_progress),
        }
        self._ses = lt.session(settings_pack)
        self._cmd_queue = queue.Queue()
        self._thread = None
        self._running = False
        self.dht_ready = asyncio.Event()
        self.pending_metadata = {}
        self.pending_reads = {}
        self.pending_reads_lock = threading.Lock()
        self.pending_fetches = {}
        self.fetch_lock = threading.Lock()
        self.next_fetch_id = 0
        self.last_dht_log_time = 0
        self.piece_subscribers = defaultdict(list)
        self.subscribers_lock = threading.Lock()

    async def _execute_sync(self, func, *args, **kwargs):
        future = self.loop.create_future()
        self._cmd_queue.put((future, func, args, kwargs))
        self._ses.post_session_stats()
        return await future

    def _execute_sync_nowait(self, func, *args, **kwargs):
        self._cmd_queue.put((None, func, args, kwargs))
        self._ses.post_session_stats()

    async def start(self):
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self._thread = threading.Thread(target=self._alert_loop, daemon=True)
        self._thread.start()
        self.log.info("TorrentClient 已启动。")

    async def stop(self):
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self._thread and self._thread.is_alive():
            self._ses.post_dht_stats()
            await self.loop.run_in_executor(None, self._thread.join)
        self.log.info("TorrentClient 已停止。")

    def subscribe_pieces(self, infohash: str, queue: asyncio.Queue):
        with self.subscribers_lock:
            self.piece_subscribers[infohash].append(queue)

    def unsubscribe_pieces(self, infohash: str, queue: asyncio.Queue):
        with self.subscribers_lock:
            try:
                self.piece_subscribers[infohash].remove(queue)
                if not self.piece_subscribers[infohash]:
                    del self.piece_subscribers[infohash]
            except (ValueError, KeyError):
                pass

    @asynccontextmanager
    async def get_handle(self, infohash: str, metadata: bytes = None):
        infohash_hex = None
        try:
            infohash_hex = await self.add_torrent(infohash, metadata=metadata)
            if not infohash_hex:
                raise TorrentClientError(f"无法为 {infohash} 添加 torrent。")
            yield infohash_hex
        finally:
            if infohash_hex:
                await self.remove_torrent(infohash_hex)

    async def add_torrent(self, infohash_hex: str, metadata: bytes = None) -> str:
        self.log.info("正在为 infohash 添加 torrent: %s", infohash_hex)
        save_dir = os.path.join(self.save_path, infohash_hex)

        if metadata:
            self.log.info("正在使用提供的元数据为 %s 添加 torrent。", infohash_hex)
            try:
                ti = lt.torrent_info(metadata)
                if str(ti.info_hash()) != infohash_hex:
                    raise TorrentClientError(f"提供的元数据 infohash ({ti.info_hash()}) 与指定的 infohash ({infohash_hex}) 不匹配。")
            except RuntimeError as e:
                raise TorrentClientError(f"无法解析元数据: {e}")
            params = lt.add_torrent_params()
            params.ti = ti
            params.save_path = save_dir
        else:
            self.log.info("没有提供元数据。正在为 %s 使用磁力链接。", infohash_hex)
            meta_future = self.loop.create_future()
            self.pending_metadata[infohash_hex] = meta_future
            trackers = [
                "udp://tracker.opentrackr.org:1337/announce", "udp://open.demonii.com:1337/announce",
                "udp://open.stealth.si:80/announce", "udp://exodus.desync.com:6969/announce",
                "udp://tracker.bittor.pw:1337/announce", "http://sukebei.tracker.wf:8888/announce",
                "udp://tracker.torrent.eu.org:451/announce",
            ]
            magnet_uri = f"magnet:?xt=urn:btih:{infohash_hex}&{'&'.join(['tr=' + t for t in trackers])}"
            params = lt.parse_magnet_uri(magnet_uri)
            params.save_path = save_dir

        def add_and_resume_if_needed():
            # This function runs entirely in the libtorrent thread
            if metadata:
                params.flags |= lt.torrent_flags.paused
                params.flags &= ~lt.torrent_flags.auto_managed
                handle = self._ses.add_torrent(params)
            else:
                params.flags &= ~lt.torrent_flags.auto_managed
                handle = self._ses.add_torrent(params)
                if handle.is_valid():
                    handle.resume()
            return handle

        handle = await self._execute_sync(add_and_resume_if_needed)
        if not handle.is_valid():
            raise TorrentClientError(f"为 {infohash_hex} 添加 torrent 后未能获取有效句柄。")

        if not metadata:
            self.log.debug("正在等待 %s 的元数据... (超时: %ss)", infohash_hex, self.metadata_timeout)
            try:
                await asyncio.wait_for(meta_future, timeout=self.metadata_timeout)
                self.log.info("为 %s 成功接收元数据。", infohash_hex)
            except asyncio.TimeoutError:
                raise MetadataTimeoutError(f"获取元数据超时", infohash=infohash_hex)
            finally:
                self.pending_metadata.pop(infohash_hex, None)

        def set_piece_priorities_to_zero(h):
            if h and h.is_valid() and h.has_metadata():
                self.log.info("为 %s 设置所有 piece 优先级为 0。", str(h.info_hash()))
                priorities = [0] * h.status().num_pieces
                h.prioritize_pieces(priorities)

        # We need to use the handle on the libtorrent thread
        await self._execute_sync(lambda: set_piece_priorities_to_zero(self._ses.find_torrent(infohash_hex)))

        return infohash_hex

    async def get_torrent_info(self, infohash: str):
        def _sync_get():
            handle = self._ses.find_torrent(infohash)
            if handle and handle.is_valid() and handle.has_metadata():
                return handle.get_torrent_info()
            return None
        return await self._execute_sync(_sync_get)

    async def remove_torrent(self, infohash: str):
        self.log.info("正在移除 torrent: %s", infohash)
        meta_future = self.pending_metadata.pop(infohash, None)
        if meta_future and not meta_future.done():
            self.loop.call_soon_threadsafe(meta_future.cancel)
        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if request['infohash'] == infohash:
                    future = request['future']
                    if not future.done():
                        self.loop.call_soon_threadsafe(future.cancel)
                    self.pending_fetches.pop(fetch_id, None)
        with self.pending_reads_lock:
            for read_key, pending_info in list(self.pending_reads.items()):
                if read_key[0] == infohash:
                    future = pending_info['future']
                    if not future.done():
                        self.loop.call_soon_threadsafe(future.cancel)
                    self.pending_reads.pop(read_key, None)
        with self.subscribers_lock:
            self.piece_subscribers.pop(infohash, None)
        await self._execute_sync(self._ses.remove_torrent, handle, lt.session.delete_files)
        self.log.info("已移除 torrent: %s", infohash)

    def request_pieces(self, infohash: str, piece_indices: list[int]):
        if not piece_indices: return
        unique_indices = sorted(list(set(piece_indices)))
        def _request_sync():
            handle = self._ses.find_torrent(infohash)
            if not handle or not handle.is_valid(): return
            pieces_to_request = [p for p in unique_indices if not handle.have_piece(p)]
            if pieces_to_request:
                priorities = [(p, 7) for p in pieces_to_request]
                handle.prioritize_pieces(priorities)
                handle.resume()
        self._execute_sync_nowait(_request_sync)

    async def fetch_pieces(self, infohash_hex: str, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        if not piece_indices: return {}
        unique_indices = sorted(list(set(piece_indices)))

        def get_pieces_to_download_sync():
            handle = self._ses.find_torrent(infohash_hex)
            if not handle or not handle.is_valid(): return []
            return [p for p in unique_indices if not handle.have_piece(p)]
        pieces_to_download = await self._execute_sync(get_pieces_to_download_sync)

        if pieces_to_download:
            def _set_priorities_sync():
                handle = self._ses.find_torrent(infohash_hex)
                if not handle or not handle.is_valid(): return
                priorities = [(p, 7) for p in pieces_to_download]
                handle.prioritize_pieces(priorities)
                handle.resume()
            await self._execute_sync(_set_priorities_sync)
            request_future = self.loop.create_future()
            with self.fetch_lock:
                fetch_id = self.next_fetch_id
                self.next_fetch_id += 1
                self.pending_fetches[fetch_id] = {'future': request_future, 'remaining': set(pieces_to_download), 'infohash': infohash_hex}
            try:
                await asyncio.wait_for(request_future, timeout=timeout)
            except asyncio.TimeoutError:
                raise TorrentClientError(f"下载 pieces {pieces_to_download} 超时。")
            finally:
                with self.fetch_lock: self.pending_fetches.pop(fetch_id, None)

        futures_to_await, read_keys_to_await = [], []
        with self.pending_reads_lock:
            for piece_index in unique_indices:
                read_key = (infohash_hex, piece_index)
                if read_key in self.pending_reads:
                    future = self.pending_reads[read_key]['future']
                else:
                    future = self.loop.create_future()
                    self.pending_reads[read_key] = {'future': future, 'retries': 0}
                    def _read_piece_sync(ih, pi):
                        h = self._ses.find_torrent(ih)
                        if h and h.is_valid():
                            h.read_piece(pi)
                    self._execute_sync_nowait(lambda: _read_piece_sync(infohash_hex, piece_index))
                futures_to_await.append(future)
                read_keys_to_await.append(read_key)
        try:
            gathered_results = await asyncio.gather(*futures_to_await)
            return dict(zip([key[1] for key in read_keys_to_await], gathered_results))
        except (TorrentClientError, asyncio.TimeoutError) as e:
            with self.pending_reads_lock:
                for key in read_keys_to_await:
                    if key in self.pending_reads and not self.pending_reads[key]['future'].done():
                        self.pending_reads.pop(key, None)
            raise TorrentClientError(f"读取 pieces {unique_indices} 超时或失败: {e}") from e
        except asyncio.CancelledError:
            with self.pending_reads_lock:
                for i, f in enumerate(futures_to_await):
                    f.cancel()
                    self.pending_reads.pop(read_keys_to_await[i], None)
            raise

    def _handle_metadata_received(self, alert):
        infohash_str = str(alert.handle.info_hash())
        future = self.pending_metadata.get(infohash_str)
        if future and not future.done():
            self.loop.call_soon_threadsafe(future.set_result, True)

    def _handle_piece_finished(self, alert):
        if not alert.handle.is_valid(): return
        piece_index, infohash_hex = alert.piece_index, str(alert.handle.info_hash())
        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if request['infohash'] == infohash_hex and piece_index in request['remaining']:
                    request['remaining'].remove(piece_index)
                    if not request['remaining']:
                        future = request['future']
                        if not future.done():
                            self.loop.call_soon_threadsafe(future.set_result, True)
                        self.pending_fetches.pop(fetch_id, None)
        with self.subscribers_lock:
            if infohash_hex in self.piece_subscribers:
                for queue in self.piece_subscribers[infohash_hex]:
                    self.loop.call_soon_threadsafe(queue.put_nowait, piece_index)

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set():
            self.loop.call_soon_threadsafe(self.dht_ready.set)

    def _handle_torrent_finished(self, alert):
        if not alert.handle.is_valid(): return
        infohash_hex = str(alert.handle.info_hash())
        with self.subscribers_lock:
            if infohash_hex in self.piece_subscribers:
                for queue in self.piece_subscribers[infohash_hex]:
                    self.loop.call_soon_threadsafe(queue.put_nowait, None)

    async def _retry_read_piece(self, read_key):
        await asyncio.sleep(0.2)
        infohash_hex, piece_index = read_key
        def _read_piece_sync():
            handle = self._ses.find_torrent(infohash_hex)
            if handle and handle.is_valid():
                handle.read_piece(piece_index)
        self._execute_sync_nowait(_read_piece_sync)

    def _handle_read_piece(self, alert):
        if not alert.handle.is_valid(): return
        infohash_hex, piece_index = str(alert.handle.info_hash()), alert.piece
        read_key = (infohash_hex, piece_index)
        with self.pending_reads_lock:
            pending_info = self.pending_reads.get(read_key)
        if not pending_info or pending_info['future'].done(): return
        error = None
        if alert.error and alert.error.value() != 0:
            error_message = alert.error.message()
            if "invalid piece index" in error_message and pending_info['retries'] < 5:
                pending_info['retries'] += 1
                self.loop.create_task(self._retry_read_piece(read_key))
                return
            if "success" not in error_message.lower():
                error = TorrentClientError(error_message)
        with self.pending_reads_lock:
            self.pending_reads.pop(read_key, None)
        if error:
            self.loop.call_soon_threadsafe(pending_info['future'].set_exception, error)
        else:
            data = bytes(alert.buffer)
            self.loop.call_soon_threadsafe(pending_info['future'].set_result, data)

    def _alert_loop(self):
        while self._running:
            try:
                future, func, args, kwargs = self._cmd_queue.get_nowait()
                try:
                    result = func(*args, **kwargs)
                    if future and not future.done():
                        self.loop.call_soon_threadsafe(future.set_result, result)
                except Exception as e:
                    if future and not future.done():
                        self.loop.call_soon_threadsafe(future.set_exception, e)
            except queue.Empty:
                pass
            if not self._running:
                break
            alert = self._ses.wait_for_alert(200)
            if not alert:
                continue
            alerts = self._ses.pop_alerts()
            for alert in alerts:
                try:
                    if hasattr(alert, 'handle') and not alert.handle.is_valid():
                        continue
                    if alert.category() & lt.alert_category.error:
                        self.log.error("Libtorrent 警报: %s", alert)
                    else:
                        self.log.debug("Libtorrent 警报: %s", alert)
                    alert_map = {
                        lt.metadata_received_alert: self._handle_metadata_received,
                        lt.piece_finished_alert: self._handle_piece_finished,
                        lt.read_piece_alert: self._handle_read_piece,
                        lt.dht_bootstrap_alert: self._handle_dht_bootstrap,
                        lt.torrent_finished_alert: self._handle_torrent_finished,
                    }
                    handler = alert_map.get(type(alert))
                    if handler:
                        handler(alert)
                    elif type(alert) == lt.state_update_alert:
                        for s in alert.status:
                            if s.state != lt.torrent_status.states.seeding:
                                self.log.info(
                                    "  状态更新 - %s: %s %.2f%% | 下载速度: %.1f kB/s | 节点: %d (%d 种子)",
                                    s.name, s.state_str, s.progress * 100,
                                    s.download_rate / 1000, s.num_peers, s.num_seeds
                                )
                except Exception:
                    self.log.exception("处理 libtorrent 警报时发生未知错误，但工作线程将继续运行。")

            now = time.time()
            if now - self.last_dht_log_time > 10:
                status = self._ses.status()
                self.log.info("DHT 状态: %d 个节点。", status.dht_nodes)
                self.last_dht_log_time = now
