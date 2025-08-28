# -*- coding: utf-8 -*-
"""
对 screenshot.client.TorrentClient 的健壮性测试。

这些测试旨在验证并发场景、资源管理（LRU缓存）和错误处理，
确保客户端在面对现实世界的复杂情况时不会死锁、超时或行为不当。
"""
import pytest
import pytest_asyncio
import asyncio
import libtorrent as lt
import threading
from unittest.mock import MagicMock, patch

from screenshot.client import TorrentClient, LibtorrentError

# --- 模拟 Libtorrent 对象 ---

def create_mock_handle(infohash: str, ti_files=None):
    """创建一个模拟的 torrent_handle 对象。"""
    handle = MagicMock(spec=lt.torrent_handle)
    handle.is_valid.return_value = True
    # 修复: lt.sha1_hash 没有 from_hex 方法，应直接使用构造函数。
    handle.info_hash.return_value = lt.sha1_hash(infohash)

    # 模拟 get_torrent_info()
    ti = MagicMock(spec=lt.torrent_info)
    if ti_files:
        fs = MagicMock(spec=lt.file_storage)
        fs.files.return_value = ti_files
        ti.files.return_value = fs
    else:
        ti.files.return_value = None

    handle.get_torrent_info.return_value = ti

    return handle

def create_mock_alert(alert_type, **kwargs):
    """创建一个模拟的 libtorrent alert 对象。"""
    alert = MagicMock(spec=alert_type)
    alert.category.return_value = 0 # 默认为非错误
    for key, value in kwargs.items():
        setattr(alert, key, value)
    return alert

# --- Pytest Fixtures ---

@pytest_asyncio.fixture
async def mock_lt_session():
    """
    模拟 libtorrent.session，并提供一个通过 threading.Event 实现同步的警报队列。
    这允许测试精确地控制在执行器线程中运行的 _alert_loop 的执行时机。
    """
    with patch('libtorrent.session') as mock_session_class:
        mock_ses_instance = mock_session_class.return_value

        alert_queue = asyncio.Queue()
        alert_posted_event = threading.Event()

        def pop_alerts():
            alerts = []
            while not alert_queue.empty():
                alerts.append(alert_queue.get_nowait())
            return alerts
        mock_ses_instance.pop_alerts.side_effect = pop_alerts

        # 模拟 wait_for_alert，这是一个阻塞函数
        def wait_for_alert_mock(*args, **kwargs):
            # run_in_executor 会在一个单独的线程中调用此函数，
            # 因此使用同步的 event.wait() 是安全的。
            alert_posted_event.wait()
            alert_posted_event.clear()
        mock_ses_instance.wait_for_alert.side_effect = wait_for_alert_mock

        # 暴露一个 post_alert 方法给测试，用于添加警报并触发事件
        async def post_alert(alert):
            await alert_queue.put(alert)
            alert_posted_event.set()
        mock_ses_instance.post_alert = post_alert

        yield mock_ses_instance

@pytest_asyncio.fixture
async def client(mock_lt_session):
    """提供一个已启动并正在运行的 TorrentClient 实例及其模拟会话。"""
    # 使用一个小的缓存大小来方便测试 LRU 逻辑
    # 使用 asyncio.get_running_loop() 是获取当前事件循环的现代方法
    torrent_client = TorrentClient(loop=asyncio.get_running_loop(), max_cache_size=2)

    # 在后台启动客户端的警报循环
    await torrent_client.start()

    yield torrent_client, mock_lt_session

    # --- 清理 ---
    # 确保在测试结束后停止客户端，以取消后台任务
    torrent_client.stop()
    # 等待任务实际完成取消
    await asyncio.sleep(0.01)

# --- 测试用例 ---

@pytest.mark.asyncio
class TestTorrentClient:
    """TorrentClient 的核心功能测试套件。"""

    async def test_add_torrent_success(self, client):
        """测试：成功添加一个 torrent 并获取元数据。"""
        torrent_client, mock_ses = client
        infohash = "a" * 40

        add_task = asyncio.create_task(torrent_client.add_torrent(infohash))

        # 模拟 libtorrent 异步返回元数据
        mock_handle = create_mock_handle(infohash)
        alert = create_mock_alert(lt.metadata_received_alert, handle=mock_handle)
        mock_ses.post_alert(alert)

        # 等待 add_torrent 完成
        handle = await asyncio.wait_for(add_task, timeout=1)

        assert handle is not None
        assert str(handle.info_hash()) == infohash
        handle.pause.assert_called_once()
        assert infohash in torrent_client.handles

    async def test_add_torrent_timeout(self, client):
        """测试：当元数据在超时时间内未到达时，add_torrent 应失败。"""
        torrent_client, mock_ses = client
        infohash = "b" * 40

        with pytest.raises(LibtorrentError, match="获取元数据超时"):
            await torrent_client.add_torrent(infohash, timeout=0.1)

        assert infohash not in torrent_client.handles
        assert infohash not in torrent_client.pending_metadata

    async def test_fetch_pieces_success(self, client):
        """测试：成功下载并读取 piece。"""
        torrent_client, mock_ses = client
        infohash = "c" * 40
        mock_handle = create_mock_handle(infohash)
        mock_handle.have_piece.return_value = False

        torrent_client.handles[infohash] = mock_handle

        fetch_task = asyncio.create_task(torrent_client.fetch_pieces(mock_handle, [0, 1]))

        mock_ses.post_alert(create_mock_alert(lt.piece_finished_alert, piece_index=0))
        mock_ses.post_alert(create_mock_alert(lt.piece_finished_alert, piece_index=1))

        mock_ses.post_alert(create_mock_alert(lt.read_piece_alert, piece=0, buffer=b'piece0_data', error=None))
        mock_ses.post_alert(create_mock_alert(lt.read_piece_alert, piece=1, buffer=b'piece1_data', error=None))

        result = await asyncio.wait_for(fetch_task, timeout=1)

        assert result == {0: b'piece0_data', 1: b'piece1_data'}

    async def test_fetch_pieces_timeout(self, client):
        """测试：当 piece 在超时时间内未完成下载时，fetch_pieces 应失败。"""
        torrent_client, mock_ses = client
        infohash = "d" * 40
        mock_handle = create_mock_handle(infohash)
        mock_handle.have_piece.return_value = False # 确保会触发下载
        torrent_client.handles[infohash] = mock_handle

        with pytest.raises(LibtorrentError, match="下载 pieces .* 超时"):
            await torrent_client.fetch_pieces(mock_handle, [5, 6], timeout=0.1)

        assert not torrent_client.pending_fetches

    async def test_lru_eviction_no_deadlock(self, client):
        """
        关键测试: 验证当 LRU 缓存驱逐被触发时，不会与正在进行的 piece 获取操作发生死锁。
        """
        torrent_client, mock_ses = client

        h1_infohash, h2_infohash = "h1" * 20, "h2" * 20
        h1_handle = create_mock_handle(h1_infohash)
        h1_handle.have_piece.return_value = False
        h2_handle = create_mock_handle(h2_infohash)

        torrent_client.handles[h1_infohash] = h1_handle
        torrent_client.handles.move_to_end(h1_infohash)
        torrent_client.handles[h2_infohash] = h2_handle
        torrent_client.handles.move_to_end(h2_infohash)

        fetch_task = asyncio.create_task(
            torrent_client.fetch_pieces(h1_handle, [0])
        )
        # 让 fetch_task 有机会运行并获取锁
        await asyncio.sleep(0)

        h3_infohash = "h3" * 20
        add_task = asyncio.create_task(torrent_client.add_torrent(h3_infohash))

        h3_handle = create_mock_handle(h3_infohash)
        mock_ses.post_alert(create_mock_alert(lt.metadata_received_alert, handle=h3_handle))

        await asyncio.wait_for(add_task, timeout=1)

        assert list(torrent_client.handles.keys()) == [h2_infohash, h3_infohash]

        mock_ses.post_alert(create_mock_alert(lt.piece_finished_alert, piece_index=0))

        with pytest.raises(LibtorrentError, match="在 fetch 完成前被移除"):
            await fetch_task

    async def test_remove_torrent_cleans_pending_fetches(self, client):
        """测试: 当一个 torrent 被移除时，其相关的 pending fetch 请求应被取消。"""
        torrent_client, mock_ses = client
        infohash = "e" * 40
        mock_handle = create_mock_handle(infohash)
        mock_handle.have_piece.return_value = False # 确保会触发下载
        torrent_client.handles[infohash] = mock_handle

        fetch_task = asyncio.create_task(
            torrent_client.fetch_pieces(mock_handle, [10])
        )
        await asyncio.sleep(0)
        assert len(torrent_client.pending_fetches) == 1

        torrent_client.remove_torrent(mock_handle)

        with pytest.raises(LibtorrentError, match="在 fetch 完成前被移除"):
            await asyncio.wait_for(fetch_task, timeout=1)

        assert not torrent_client.pending_fetches
