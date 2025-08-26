# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch

# Mock the libtorrent library before it's imported by other modules
lt_mock = MagicMock()
lt_mock.session.delete_files = 1 # Define the constant used in the code
with patch.dict('sys.modules', {'libtorrent': lt_mock}):
    from screenshot.client import TorrentClient, LibtorrentError

def setup_client(loop):
    """A helper function to create and configure a client for tests."""
    with patch('libtorrent.session', return_value=MagicMock()):
        client = TorrentClient(loop=loop)
        client.ses = MagicMock()
        client.dht_ready.set()
        return client

@pytest.mark.asyncio
async def test_add_torrent_success():
    """
    Tests the successful flow of adding a torrent and receiving metadata.
    """
    loop = asyncio.get_running_loop()
    client = setup_client(loop)
    fake_infohash = "a" * 40
    mock_handle = MagicMock()
    mock_handle.info_hash.return_value = fake_infohash
    client.ses.add_torrent.return_value = mock_handle

    add_torrent_task = asyncio.create_task(client.add_torrent(fake_infohash))
    await asyncio.sleep(0.01)

    assert fake_infohash in client.pending_metadata

    alert = MagicMock()
    alert.handle = mock_handle
    type(alert.handle).info_hash = MagicMock(return_value=fake_infohash)
    client._handle_metadata_received(alert)

    returned_handle = await add_torrent_task

    client.ses.add_torrent.assert_called_once()
    assert returned_handle == mock_handle

@pytest.mark.asyncio
async def test_add_torrent_dht_timeout():
    """
    Tests that add_torrent logs a warning on DHT timeout but continues.
    """
    loop = asyncio.get_running_loop()
    client = setup_client(loop)
    client.dht_ready.clear()
    fake_infohash = "c" * 40

    original_wait_for = asyncio.wait_for

    async def mock_wait_for(awaitable, timeout):
        # We can't compare coroutine objects directly, so we check the timeout
        if timeout == 30: # This is the DHT timeout
            raise asyncio.TimeoutError
        return await original_wait_for(awaitable, timeout)

    with patch('screenshot.client.asyncio.wait_for', side_effect=mock_wait_for):
        with patch.object(client.log, 'warning') as mock_log_warning:
            with pytest.raises(LibtorrentError, match="获取元数据超时"):
                await client.add_torrent(fake_infohash)
            mock_log_warning.assert_called_once_with("DHT bootstrap timed out, proceeding without it.")

@pytest.mark.asyncio
async def test_handle_piece_finished():
    """
    Tests that the piece finished handler correctly notifies futures.
    """
    loop = asyncio.get_running_loop()
    client = setup_client(loop)
    piece_index = 5
    future = loop.create_future()
    client.piece_download_futures[piece_index].append(future)

    alert = MagicMock()
    alert.piece_index = piece_index

    client._handle_piece_finished(alert)

    await asyncio.sleep(0)
    assert future.done()
    assert future.result() is True
    assert piece_index not in client.piece_download_futures

class TestDownloadAndReadPiece:
    @pytest.mark.asyncio
    async def test_already_have_piece(self):
        loop = asyncio.get_running_loop()
        client = setup_client(loop)
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = True

        async def mock_read_handler():
            await asyncio.sleep(0.01)
            client._handle_read_piece(MagicMock(piece=10, buffer=b"piece_data", error=None))

        handler_task = loop.create_task(mock_read_handler())
        data = await client.download_and_read_piece(mock_handle, 10)

        assert data == b"piece_data"
        mock_handle.piece_priority.assert_not_called()
        mock_handle.read_piece.assert_called_once_with(10)
        await handler_task

    @pytest.mark.asyncio
    async def test_download_and_read_full_flow(self):
        loop = asyncio.get_running_loop()
        client = setup_client(loop)
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = False

        async def mock_alert_flow():
            await asyncio.sleep(0.01)
            assert 10 in client.piece_download_futures
            client._handle_piece_finished(MagicMock(piece_index=10))
            await asyncio.sleep(0.01)
            mock_handle.read_piece.assert_called_once_with(10)
            client._handle_read_piece(MagicMock(piece=10, buffer=b"piece_data", error=None))

        flow_task = loop.create_task(mock_alert_flow())
        data = await client.download_and_read_piece(mock_handle, 10)

        assert data == b"piece_data"
        mock_handle.piece_priority.assert_called_once_with(10, 7)
        await flow_task

    @pytest.mark.asyncio
    async def test_timeout_occurs(self):
        loop = asyncio.get_running_loop()
        client = setup_client(loop)
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = False

        with pytest.raises(LibtorrentError, match="Timeout occurred"):
            await client.download_and_read_piece(mock_handle, 10, timeout=0.01)

        assert 10 not in client.piece_download_futures
        assert 10 not in client.pending_reads

def test_remove_torrent():
    loop = asyncio.get_event_loop()
    client = setup_client(loop)
    mock_handle = MagicMock()
    mock_handle.is_valid.return_value = True
    mock_handle.info_hash.return_value = "a" * 40
    client.remove_torrent(mock_handle)
    client.ses.remove_torrent.assert_called_once_with(mock_handle, lt_mock.session.delete_files)
