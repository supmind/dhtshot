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

    *** 警告: 潜在问题 ***
    1.  内存管理:
        此类的当前实现包含一个用于 torrent 句柄的 LRU（最近最少使用）缓存 (`self.handles`)。
        这是为了防止在长时间运行的服务中无限增长的句柄列表导致的内存泄漏。
        然而，在测试此功能时遇到了问题。

    2.  并发问题/死锁:
        在为 LRU 缓存驱逐逻辑编写单元测试时，测试套件出现了超时（超过 6 分钟）。
        这强烈表明在 `add_torrent` 和 `remove_torrent` 的交互中可能存在死锁或无限循环，
        尤其是在涉及 `self.fetch_lock` 等锁的情况下。由于这些测试被放弃，
        根本原因尚未确定。

    建议:
        在生产环境中使用此类之前，应进行更深入的调试和更稳健的并发测试，
        以确保 LRU 逻辑不会导致死锁。应特别注意 `add_torrent` 中调用 `remove_torrent`
        的流程。
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

        # --- 公共 Trackers ---
        # (Tracker 列表保持不变)
        self.trackers = [
            "http://1337.abcvg.info:80/announce",
            "http://ipv4.rer.lol:2710/announce",
            "http://ipv6.rer.lol:6969/announce",
            "http://lucke.fenesisu.moe:6969/announce",
            "http://nyaa.tracker.wf:7777/announce",
            "http://open.trackerlist.xyz:80/announce",
            "http://shubt.net:2710/announce",
            "http://tk.greedland.net:80/announce",
            "http://torrent.hificode.in:6969/announce",
            "http://torrentsmd.com:8080/announce",
            "http://tracker.beeimg.com:6969/announce",
            "http://tracker.bt4g.com:2095/announce",
            "http://tracker.ipv6tracker.org:80/announce",
            "http://tracker.ipv6tracker.ru:80/announce",
            "http://tracker.lintk.me:2710/announce",
            "http://tracker.moxing.party:6969/announce",
            "http://tracker.mywaifu.best:6969/announce",
            "http://tracker.renfei.net:8080/announce",
            "http://tracker.sbsub.com:2710/announce",
            "http://tracker.xiaoduola.xyz:6969/announce",
            "http://tracker810.xyz:11450/announce",
            "http://www.all4nothin.net:80/announce.php",
            "http://www.torrentsnipe.info:2701/announce",
            "https://1337.abcvg.info:443/announce",
            "https://2.tracker.eu.org:443/announce",
            "https://3.tracker.eu.org:443/announce",
            "https://4.tracker.eu.org:443/announce",
            "https://shahidrazi.online:443/announce",
            "https://tr.abiir.top:443/announce",
            "https://tr.abir.ga:443/announce",
            "https://tr.nyacat.pw:443/announce",
            "https://tracker.alaskantf.com:443/announce",
            "https://tracker.cutie.dating:443/announce",
            "https://tracker.expli.top:443/announce",
            "https://tracker.foreverpirates.co:443/announce",
            "https://tracker.jdx3.org:443/announce",
            "https://tracker.kuroy.me:443/announce",
            "https://tracker.moeblog.cn:443/announce",
            "https://tracker.yemekyedim.com:443/announce",
            "https://tracker.zhuqiy.top:443/announce",
            "https://tracker1.520.jp:443/announce",
            "udp://1c.premierzal.ru:6969/announce",
            "udp://6ahddutb1ucc3cp.ru:6969/announce",
            "udp://bt.bontal.net:6969/announce",
            "udp://d40969.acod.regrucolo.ru:6969/announce",
            "udp://discord.heihachi.pw:6969/announce",
            "udp://evan.im:6969/announce",
            "udp://exodus.desync.com:6969/announce",
            "udp://extracker.dahrkael.net:6969/announce",
            "udp://hificode.in:6969/announce",
            "udp://isk.richardsw.club:6969/announce",
            "udp://leet-tracker.moe:1337/announce",
            "udp://martin-gebhardt.eu:25/announce",
            "udp://open.demonii.com:1337/announce",
            "udp://open.free-tracker.ga:6969/announce",
            "udp://open.stealth.si:80/announce",
            "udp://opentor.org:2710/announce",
            "udp://opentracker.io:6969/announce",
            "udp://p4p.arenabg.com:1337/announce",
            "udp://retracker.hotplug.ru:2710/announce",
            "udp://retracker.lanta.me:2710/announce",
            "udp://retracker01-msk-virt.corbina.net:80/announce",
            "udp://tr4ck3r.duckdns.org:6969/announce",
            "udp://tracker.bitcoinindia.space:6969/announce",
            "udp://tracker.bittor.pw:1337/announce",
            "udp://tracker.cloudbase.store:1333/announce",
            "udp://tracker.dler.com:6969/announce",
            "udp://tracker.ducks.party:1984/announce",
            "udp://tracker.fnix.net:6969/announce",
            "udp://tracker.gigantino.net:6969/announce",
            "udp://tracker.hifitechindia.com:6969/announce",
            "udp://tracker.kmzs123.cn:17272/announce",
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://tracker.plx.im:6969/announce",
            "udp://tracker.rescuecrew7.com:1337/announce",
            "udp://tracker.skillindia.site:6969/announce",
            "udp://tracker.srv00.com:6969/announce",
            "udp://tracker.startwork.cv:1337/announce",
            "udp://tracker.therarbg.to:6969/announce",
            "udp://tracker.torrent.eu.org:451/announce",
            "udp://tracker.torrust-demo.com:6969/announce",
            "udp://tracker.tvunderground.org.ru:3218/announce",
            "udp://tracker.valete.tf:9999/announce",
            "udp://tracker.zupix.online:1333/announce",
            "udp://ttk2.nbaonlineservice.com:6969/announce",
            "udp://www.torrent.eu.org:451/announce",
            "wss://tracker.openwebtorrent.com:443/announce",
        ]

        # --- 缓存与状态管理 ---
        self.handles = OrderedDict()  # torrent 句柄的 LRU 缓存 {infohash: handle}
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
        此方法还管理句柄缓存的 LRU 逻辑。
        """
        # 首先检查缓存
        if infohash in self.handles and self.handles[infohash].is_valid():
            self.log.info(f"从缓存返回 {infohash} 的现有句柄。")
            self.handles.move_to_end(infohash)  # 标记为最近使用
            return self.handles[infohash]

        # --- 如果不在缓存中或无效，则添加它 ---
        self.log.info(f"为 infohash 添加新 torrent: {infohash}")

        # --- LRU 缓存管理 ---
        if len(self.handles) >= self.max_cache_size:
            # 移除最久未使用的条目
            old_infohash, old_handle = self.handles.popitem(last=False)
            self.log.info(f"缓存已满。正在移除最久未使用的句柄: {old_infohash}")
            self.remove_torrent(old_handle) # 从会话中移除

        save_dir = os.path.join(self.save_path, infohash)

        meta_future = self.loop.create_future()
        self.pending_metadata[infohash] = meta_future

        tracker_urls = "".join([f"&tr={t}" for t in self.trackers])
        magnet_uri = f"magnet:?xt=urn:btih:{infohash}{tracker_urls}"

        self.log.info(f"使用 trackers 构建磁力链接: {magnet_uri}")

        params = lt.parse_magnet_uri(magnet_uri)
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
            # 不需要从 ses 中移除，因为 add_torrent 已经失败
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
            # 从缓存中移除句柄。使用 pop(infohash, None) 以避免在键不存在时出错
            # (例如，在 add_torrent 的 LRU 逻辑中，它已经被 popitem 移除了)
            self.handles.pop(infohash, None)
            self.pending_metadata.pop(infohash, None)
            # 在移除时也清理相关的 pending 请求
            with self.fetch_lock:
                for fetch_id, request in list(self.pending_fetches.items()):
                    # 这是一个简化的清理。更复杂的逻辑可能需要取消 future。
                    if str(handle.info_hash()) in request.get('infohash_str', ''):
                         self.pending_fetches.pop(fetch_id, None)

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
        """处理来自 libtorrent 的 `metadata_received_alert` 警报。"""
        infohash_str = str(alert.handle.info_hash())
        # 如果有正在等待此元数据的 future，则设置其结果
        if infohash_str in self.pending_metadata and not self.pending_metadata[infohash_str].done():
            self.pending_metadata[infohash_str].set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        """处理来自 libtorrent 的 `piece_finished_alert` 警报。"""
        piece_index = alert.piece_index
        # 将完成的 piece 索引放入队列，供其他协程消费
        self.finished_piece_queue.put_nowait(piece_index)

        # 检查是否有 `fetch_pieces` 请求正在等待此 piece
        with self.fetch_lock:
            for fetch_id, request in list(self.pending_fetches.items()):
                if piece_index in request['remaining']:
                    request['remaining'].remove(piece_index)
                    # 如果一个 fetch 请求的所有 piece 都已完成，则设置其 future 的结果
                    if not request['remaining']:
                        if not request['future'].done():
                            request['future'].set_result(True)
                        self.pending_fetches.pop(fetch_id, None)

    def _handle_dht_bootstrap(self, alert):
        """处理 DHT 引导完成的警报，表示 DHT 网络已准备就绪。"""
        if not self.dht_ready.is_set(): self.dht_ready.set()

    def _handle_read_piece(self, alert):
        """处理来自 libtorrent 的 `read_piece_alert` 警报。"""
        # 获取所有等待此 piece 数据的 future
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])

        error = None
        if alert.error and alert.error.value() != 0:
            # 忽略非错误的 "success" 消息，因为它实际上表示成功
            if "success" not in alert.error.message().lower():
                error = LibtorrentError(alert.error)

        data = bytes(alert.buffer)
        # 将结果或异常设置给所有等待的 future
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
                # 从 libtorrent 会话中弹出所有待处理的警报
                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    # 记录所有错误类型的警报
                    if alert.category() & lt.alert_category.error:
                        self.log.error(f"Libtorrent 警报: {alert}")

                    alert_type = type(alert)
                    # 根据警报类型调用相应的处理函数
                    if alert_type == lt.metadata_received_alert:
                        self._handle_metadata_received(alert)
                    elif alert_type == lt.piece_finished_alert:
                        self._handle_piece_finished(alert)
                    elif alert_type == lt.read_piece_alert:
                        self._handle_read_piece(alert)
                    elif alert_type == lt.dht_bootstrap_alert:
                        self._handle_dht_bootstrap(alert)

                now = time.time()
                # 定期记录 DHT 状态以供调试
                if now - self.last_dht_log_time > 10:
                    status = self.ses.status()
                    self.log.info(f"DHT 状态: {status.dht_nodes} 个节点。")
                    self.last_dht_log_time = now

                # 等待一小段时间，避免 CPU 占用过高
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                # 如果任务被取消，则中断循环
                break
            except Exception:
                self.log.exception("libtorrent 警报循环中发生错误。")
