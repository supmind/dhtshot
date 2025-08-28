# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, call, patch
from screenshot.client import TorrentClient, LibtorrentError

# 这个 fixture 现在是一个“工厂”，它返回一个函数
# 用于在测试函数内部创建 client 实例。
@pytest.fixture
def client_factory(monkeypatch):
    """提供一个用于创建带有模拟依赖的 TorrentClient 的工厂函数。"""

    # 创建一个模拟的 session 类，并预先配置好它
    mock_session_class = MagicMock()
    mock_session_class.delete_files = 1 # 修复 AttributeError

    # 在工厂作用域内 patch，这样所有创建的 client 都会使用这些 mock
    monkeypatch.setattr("screenshot.client.lt.session", mock_session_class)
    # 模拟 parse_magnet_uri 以避免真实的 libtorrent 调用
    monkeypatch.setattr("screenshot.client.lt.parse_magnet_uri", MagicMock)

    def _factory():
        """这个内部函数在测试运行时被调用。"""
        # 调用 TorrentClient() 时，它会通过 asyncio.get_event_loop()
        # 获取由 pytest-asyncio 提供的、正在运行的事件循环。
        client = TorrentClient()
        client.log = MagicMock()
        return client

    return _factory


@pytest.mark.asyncio
async def test_add_torrent_success(client_factory):
    """测试成功添加 torrent。"""
    client = client_factory() # 在测试开始时创建 client
    infohash = "test_infohash"
    mock_handle = MagicMock()
    mock_handle.info_hash.return_value = infohash
    client.ses.add_torrent.return_value = mock_handle

    add_task = asyncio.create_task(client.add_torrent(infohash))
    await asyncio.sleep(0)

    # 直接调用 handler 来模拟警报循环的效果
    client._handle_metadata_received(MagicMock(handle=mock_handle))

    handle = await add_task
    assert handle == mock_handle
    handle.pause.assert_called_once()

@pytest.mark.asyncio
async def test_fetch_pieces_success(client_factory):
    """测试 fetch_pieces 的成功流程。"""
    client = client_factory()
    mock_handle = MagicMock()
    mock_handle.have_piece.return_value = False

    fetch_task = asyncio.create_task(client.fetch_pieces(mock_handle, [0, 1]))
    await asyncio.sleep(0)

    # 模拟 piece 完成警报
    client._handle_piece_finished(MagicMock(piece_index=0))
    client._handle_piece_finished(MagicMock(piece_index=1))
    await asyncio.sleep(0)

    # 模拟 piece 读取警报
    client._handle_read_piece(MagicMock(piece=0, buffer=b'data0', error=None))
    client._handle_read_piece(MagicMock(piece=1, buffer=b'data1', error=None))

    result = await fetch_task
    assert result == {0: b'data0', 1: b'data1'}

@pytest.mark.asyncio
async def test_fetch_pieces_read_error(client_factory):
    """测试当 read_piece 警报带有错误时，fetch_pieces 会失败。"""
    client = client_factory()
    mock_handle = MagicMock()
    mock_handle.have_piece.return_value = True

    fetch_task = asyncio.create_task(client.fetch_pieces(mock_handle, [0]))
    await asyncio.sleep(0)

    mock_error = MagicMock()
    mock_error.value.return_value = 1
    mock_error.message.return_value = "模拟读取错误"
    client._handle_read_piece(MagicMock(piece=0, buffer=b'', error=mock_error))

    with pytest.raises(LibtorrentError, match="模拟读取错误"):
        await fetch_task

def test_request_pieces(client_factory):
    """测试 request_pieces 是否正确设置优先级并恢复。"""
    client = client_factory()
    mock_handle = MagicMock()
    mock_handle.have_piece.side_effect = lambda p: p == 2

    client.request_pieces(mock_handle, [0, 1, 2])

    calls = [call(0, 7), call(1, 7)]
    mock_handle.piece_priority.assert_has_calls(calls, any_order=True)
    assert mock_handle.piece_priority.call_count == 2
    mock_handle.resume.assert_called_once()

def test_remove_torrent(client_factory):
    """测试 remove_torrent 是否能从会话和缓存中正确移除。"""
    client = client_factory()
    infohash = "infohash_to_remove"
    mock_handle = MagicMock(name="TorrentHandle")
    mock_handle.is_valid.return_value = True
    mock_handle.info_hash.return_value = infohash
    client.handles[infohash] = mock_handle

    client.remove_torrent(mock_handle)

    # 访问 lt.session.delete_files
    client.ses.remove_torrent.assert_called_once_with(mock_handle, client.ses.delete_files)
    assert infohash not in client.handles
