# -*- coding: utf-8 -*-
"""
本模块包含 TorrentClient 类，它负责所有与 libtorrent 库的直接交互。
其设计目标是为上层业务（如 ScreenshotService）提供一个简洁、异步的接口，
同时封装 libtorrent 的复杂性。
"""
import asyncio
import logging
import os
import time
import libtorrent as lt
import threading
from collections import defaultdict, OrderedDict
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
    """
    一个 libtorrent 会话的包装器，用于处理 torrent 相关操作。

    架构和线程模型:
    - `TorrentClient` 在 asyncio 事件循环中运行其主要方法（`add_torrent`, `fetch_pieces` 等）。
    - `libtorrent` 的 `session` 对象本身不是线程安全的，并且其警报（alerts）需要在自己的循环中处理。
    - `_alert_loop` 作为一个后台 asyncio 任务运行，它定期调用 `ses.pop_alerts()`，这是一个阻塞调用。
      为了不阻塞整个事件循环，`_alert_loop` 在一个独立的线程中运行 `pop_alerts`，或者（如此处实现的）
      依赖于 `pop_alerts` 的超时机制并配合 `asyncio.sleep` 来避免长时间阻塞。
    - **通信**:
      - 从 `async` 方法到 `libtorrent` 的调用是直接的。
      - 从 `_alert_loop`（处理 libtorrent 的响应）到 `async` 方法的通信是通过 `asyncio.Future` 对象实现的。
        例如，`add_torrent` 创建一个 `Future` 并将其存储在 `self.pending_metadata` 中；当 `_alert_loop`
        收到 `metadata_received_alert` 时，它会找到对应的 `Future` 并设置其结果，从而唤醒等待中的 `add_torrent` 调用。
    - **锁**:
      - `threading.Lock` 用于保护被 `_alert_loop` 和其他 `async` 方法共享的数据结构（如 `pending_fetches`）。
        因为 `_alert_loop` 和 `fetch_pieces` 可能在不同的线程上下文中修改这些共享状态，所以需要线程锁。
    """
    def __init__(self, loop=None, save_path='/dev/shm', max_cache_size=50):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        self.save_path = save_path
        self.max_cache_size = max_cache_size # LRU 缓存的最大大小
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

        self.trackers = [
            # Tracker 列表保持不变
            "http://1337.abcvg.info:80/announce", "http://ipv4.rer.lol:2710/announce", "http://ipv6.rer.lol:6969/announce",
            "http://lucke.fenesisu.moe:6969/announce", "http://nyaa.tracker.wf:7777/announce", "http://open.trackerlist.xyz:80/announce",
            "http://shubt.net:2710/announce", "http://tk.greedland.net:80/announce", "http://torrent.hificode.in:6969/announce",
            "http://torrentsmd.com:8080/announce", "http://tracker.beeimg.com:6969/announce", "http://tracker.bt4g.com:2095/announce",
            "http://tracker.ipv6tracker.org:80/announce", "http://tracker.ipv6tracker.ru:80/announce", "http.tracker.lintk.me:2710/announce",
            "http://tracker.moxing.party:6969/announce", "http://tracker.mywaifu.best:6969/announce", "http://tracker.renfei.net:8080/announce",
            "http://tracker.sbsub.com:2710/announce", "http://tracker.xiaoduola.xyz:6969/announce", "http://tracker810.xyz:11450/announce",
            "http://www.all4nothin.net:80/announce.php", "http://www.torrentsnipe.info:2701/announce", "https://1337.abcvg.info:443/announce",
            "https://2.tracker.eu.org:443/announce", "https://3.tracker.eu.org:443/announce", "https://4.tracker.eu.org:443/announce",
            "https://shahidrazi.online:443/announce", "https://tr.abiir.top:443/announce", "https://tr.abir.ga:443/announce",
            "https://tr.nyacat.pw:443/announce", "https://tracker.alaskantf.com:443/announce", "https://tracker.cutie.dating:443/announce",
            "https://tracker.expli.top:443/announce", "https://tracker.foreverpirates.co:443/announce", "https://tracker.jdx3.org:443/announce",
            "https://tracker.kuroy.me:443/announce", "https://tracker.moeblog.cn:443/announce", "https://tracker.yemekyedim.com:443/announce",
            "https://tracker.zhuqiy.top:443/announce", "https://tracker1.520.jp:443/announce", "udp://1c.premierzal.ru:6969/announce",
            "udp://6ahddutb1ucc3cp.ru:6969/announce", "udp://bt.bontal.net:6969/announce", "udp://d40969.acod.regrucolo.ru:6969/announce",
            "udp://discord.heihachi.pw:6969/announce", "udp://evan.im:6969/announce", "udp://exodus.desync.com:6969/announce",
            "udp://extracker.dahrkael.net:6969/announce", "udp://hificode.in:6969/announce", "udp://isk.richardsw.club:6969/announce",
            "udp://leet-tracker.moe:1337/announce", "udp://martin-gebhardt.eu:25/announce", "udp://open.demonii.com:1337/announce",
            "udp://open.free-tracker.ga:6969/announce", "udp://open.stealth.si:80/announce", "udp://opentor.org:2710/announce",
            "udp://opentracker.io:6969/announce", "udp://p4p.arenabg.com:1337/announce", "udp://retracker.hotplug.ru:2710/announce",
            "udp://retracker.lanta.me:2710/announce", "udp://retracker01-msk-virt.corbina.net:80/announce", "udp://tr4ck3r.duckdns.org:6969/announce",
            "udp://tracker.bitcoinindia.space:6969/announce", "udp://tracker.bittor.pw:1337/announce", "udp://tracker.cloudbase.store:1333/announce",
            "udp://tracker.dler.com:6969/announce", "udp://tracker.ducks.party:1984/announce", "udp://tracker.fnix.net:6969/announce",
            "udp://tracker.gigantino.net:6969/announce", "udp://tracker.hifitechindia.com:6969/announce", "udp://tracker.kmzs123.cn:17272/announce",
            "udp://tracker.opentrackr.org:1337/announce", "udp://tracker.plx.im:6969/announce", "udp://tracker.rescuecrew7.com:1337/announce",
            "udp://tracker.skillindia.site:6969/announce", "udp://tracker.srv00.com:6969/announce", "udp://tracker.startwork.cv:1337/announce",
            "udp://tracker.therarbg.to:6969/announce", "udp://tracker.torrent.eu.org:451/announce", "udp://tracker.torrust-demo.com:6969/announce",
            "udp://tracker.tvunderground.org.ru:3218/announce", "udp://tracker.valete.tf:9999/announce", "udp://tracker.zupix.online:1333/announce",
            "udp://ttk2.nbaonlineservice.com:6969/announce", "udp://www.torrent.eu.org:451/announce", "wss://tracker.openwebtorrent.com:443/announce",
        ]

        # --- 缓存与状态管理 ---
        self.handles = OrderedDict()
        self.pending_metadata = {}
        self.pending_reads = defaultdict(list)
        self.pending_reads_lock = threading.Lock()
        self.pending_fetches = {}
        self.fetch_lock = threading.Lock()
        self.next_fetch_id = 0

        self.last_dht_log_time = 0
        self.finished_piece_queue = asyncio.Queue()

    async def start(self):
        """启动 torrent 客户端的后台警报循环。"""
        self.log.info("正在启动 TorrentClient...")
        self._running = True
        self.alert_task = self.loop.create_task(self._alert_loop())
        self.log.info("TorrentClient 已启动。")

    def stop(self):
        """停止 torrent 客户端的后台警报循环。"""
        self.log.info("正在停止 TorrentClient...")
        self._running = False
        if self.alert_task:
            self.alert_task.cancel()
        self.log.info("TorrentClient 已停止。")

    async def add_torrent(self, infohash: str, timeout: int = 180):
        """
        业务流程: 通过 infohash 添加 torrent 或从缓存中获取现有句柄。
        1.  **缓存检查**: 首先检查 `self.handles` (一个 OrderedDict, 作为 LRU 缓存) 中是否已存在有效的句柄。
        2.  **LRU 驱逐**: 如果缓存已满，移除最久未使用的条目。
        3.  **添加 Torrent**: 使用磁力链接 (magnet URI) 将新的 torrent 添加到 libtorrent 会话中。
        4.  **等待元数据**: 异步等待，直到 `_alert_loop` 收到 `metadata_received_alert` 并通过 Future 发出信号。
        5.  **初始化设置**: 收到元数据后，暂停 torrent 并将所有 piece 的优先级设为0，以便我们只下载明确请求的数据。
        """
        if infohash in self.handles and self.handles[infohash].is_valid():
            self.log.info(f"从缓存返回 {infohash} 的现有句柄。")
            self.handles.move_to_end(infohash)
            return self.handles[infohash]

        self.log.info(f"为 infohash 添加新 torrent: {infohash}")

        # --- LRU 缓存管理与死锁修复 ---
        if len(self.handles) >= self.max_cache_size:
            old_infohash, old_handle = self.handles.popitem(last=False)
            self.log.info(f"缓存已满。正在移除最久未使用的句柄: {old_infohash}")
            # **死锁修复**: `remove_torrent` 是一个同步、可能阻塞的方法，因为它内部有锁和 I/O 操作。
            # 直接在 `add_torrent` (一个 async 函数) 中调用它会导致死锁风险：
            # 一个协程可能持有 `fetch_lock` 并等待 I/O，而此处的 `add_torrent` 调用 `remove_torrent`
            # 并试图获取同一个 `fetch_lock`。
            # 解决方案是使用 `run_in_executor` 将 `remove_torrent` 的执行推迟到一个独立的线程中，
            # 从而断开调用链，避免死锁。
            self.loop.run_in_executor(None, self.remove_torrent, old_handle)

        save_dir = os.path.join(self.save_path, infohash)
        meta_future = self.loop.create_future()
        self.pending_metadata[infohash] = meta_future
        tracker_urls = "".join([f"&tr={t}" for t in self.trackers])
        magnet_uri = f"magnet:?xt=urn:btih:{infohash}{tracker_urls}"
        params = lt.parse_magnet_uri(magnet_uri)
        params.save_path = save_dir
        handle = self.ses.add_torrent(params)
        self.handles[infohash] = handle

        self.log.debug(f"正在等待 {infohash} 的元数据...")
        try:
            await asyncio.wait_for(meta_future, timeout=timeout)
        except asyncio.TimeoutError:
            self.log.error(f"为 {infohash} 获取元数据超时。")
            self.handles.pop(infohash, None)
            self.pending_metadata.pop(infohash, None)
            raise LibtorrentError(f"为 {infohash} 获取元数据超时。")

        self.log.info(f"成功获取 {infohash} 的元数据。暂停 torrent。")
        handle.pause()
        ti = handle.get_torrent_info()
        if ti:
            self.log.info(f"为 {infohash} 设置所有 piece 优先级为 0。")
            for i in range(ti.num_pieces()):
                handle.piece_priority(i, 0)
        return handle

    def remove_torrent(self, handle):
        """从会话和缓存中移除一个 torrent。这是一个同步、阻塞的方法。"""
        if not (handle and handle.is_valid()):
            return

        infohash = str(handle.info_hash())
        self.log.info(f"开始移除 torrent: {infohash}")

        # 从我们的内部状态管理中清理
        self.handles.pop(infohash, None)
        self.pending_metadata.pop(infohash, None)

        # 清理与此 torrent 相关的、正在进行的 piece fetch 请求。
        # 这是线程安全的操作，因为我们使用了 `fetch_lock`。
        with self.fetch_lock:
            # 创建一个列表来存储要移除的 fetch_id，以避免在迭代时修改字典
            fetches_to_remove = []
            for fetch_id, request in self.pending_fetches.items():
                # 这是一个简化的检查。在更复杂的系统中，可能需要更精确的句柄匹配。
                if infohash == str(request.get('handle').info_hash()):
                     fetches_to_remove.append(fetch_id)
                     # 给等待的 future 发送一个异常，告知它们操作已被取消
                     if not request['future'].done():
                         exc = LibtorrentError(f"Torrent {infohash} 在 fetch 完成前被移除。")
                         # 需要在事件循环线程中设置异常
                         self.loop.call_soon_threadsafe(request['future'].set_exception, exc)

            for fetch_id in fetches_to_remove:
                self.pending_fetches.pop(fetch_id, None)

        # 指示 libtorrent 移除 torrent 及其文件
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
        pieces_to_request = [p for p in unique_indices if not handle.have_piece(p)]

        if pieces_to_request:
            for p in pieces_to_request:
                handle.piece_priority(p, 7)
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
                    'remaining': set(pieces_to_download),
                    'handle': handle  # 存储句柄引用以便在清理时进行匹配
                }

            try:
                await asyncio.wait_for(request_future, timeout=timeout)
            except asyncio.TimeoutError:
                self.log.error(f"等待 pieces {pieces_to_download} 超时。")
                with self.fetch_lock: self.pending_fetches.pop(fetch_id, None)
                raise LibtorrentError(f"下载 pieces {pieces_to_download} 超时。")

        async def _read_piece(piece_index):
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
        """处理来自 libtorrent 的 `metadata_received_alert` 警报。"""
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata and not self.pending_metadata[infohash_str].done():
            self.pending_metadata[infohash_str].set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        """处理来自 libtorrent 的 `piece_finished_alert` 警报。"""
        piece_index = alert.piece_index
        self.finished_piece_queue.put_nowait(piece_index)

        with self.fetch_lock:
            # 检查此 piece 是否是某个 fetch 请求的一部分
            fetches_to_resolve = []
            for fetch_id, request in self.pending_fetches.items():
                if piece_index in request['remaining']:
                    request['remaining'].remove(piece_index)
                    if not request['remaining']:
                        fetches_to_resolve.append(fetch_id)

            # 如果一个 fetch 请求的所有 piece 都已完成，则设置其 future 的结果
            for fetch_id in fetches_to_resolve:
                request = self.pending_fetches.pop(fetch_id, None)
                if request and not request['future'].done():
                    # 在事件循环中安全地设置 future 的结果
                    self.loop.call_soon_threadsafe(request['future'].set_result, True)

    def _handle_dht_bootstrap(self, alert):
        """处理 DHT 引导完成的警报，表示 DHT 网络已准备就绪。"""
        if not self.dht_ready.is_set(): self.dht_ready.set()

    def _handle_read_piece(self, alert):
        """处理来自 libtorrent 的 `read_piece_alert` 警报。"""
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])

        error = None
        if alert.error and alert.error.value() != 0:
            if "success" not in alert.error.message().lower():
                error = LibtorrentError(alert.error)

        data = bytes(alert.buffer)
        for future in futures:
            if not future.done():
                # 在事件循环中安全地设置 future 的结果或异常
                if error:
                    self.loop.call_soon_threadsafe(future.set_exception, error)
                else:
                    self.loop.call_soon_threadsafe(future.set_result, data)

    async def _alert_loop(self):
        """
        libtorrent 会话的主警报处理循环。
        它在一个后台任务中运行，不断地从 libtorrent 获取警报并分派给相应的处理函数。
        通过在执行器中运行阻塞的 libtorrent 调用，可以避免阻塞事件循环。
        """
        while self._running:
            try:
                # 在执行器线程中运行阻塞调用
                await self.loop.run_in_executor(None, self.ses.wait_for_alert, 500)
                alerts = await self.loop.run_in_executor(None, self.ses.pop_alerts)

                for alert in alerts:
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
                if now - self.last_dht_log_time > 10:
                    status = self.ses.status()
                    self.log.info(f"DHT 状态: {status.dht_nodes} 个节点。")
                    self.last_dht_log_time = now
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("libtorrent 警报循环中发生错误。")
