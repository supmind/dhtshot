# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch

# Mock the libtorrent library before it's imported by the client
lt_mock = MagicMock()
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
    # --- Setup ---
    loop = asyncio.get_running_loop()
    client = setup_client(loop)
    fake_infohash = "a" * 40
    mock_handle = MagicMock()
    mock_handle.info_hash.return_value = fake_infohash
    client.ses.add_torrent.return_value = mock_handle

    # --- Execute ---
    add_torrent_task = asyncio.create_task(client.add_torrent(fake_infohash))
    await asyncio.sleep(0.01)

    assert fake_infohash in client.pending_metadata

    # Simulate the metadata_received alert
    alert = MagicMock()
    alert.handle = mock_handle
    type(alert.handle).info_hash = MagicMock(return_value=fake_infohash)
    client._handle_metadata_received(alert)

    returned_handle = await add_torrent_task

    # --- Assert ---
    client.ses.add_torrent.assert_called_once()
    assert returned_handle == mock_handle

@pytest.mark.asyncio
@patch('screenshot.client.asyncio.wait_for')
async def test_add_torrent_dht_timeout(mock_wait_for):
    """
    Tests that add_torrent correctly raises LibtorrentError on DHT timeout.
    """
    # --- Setup ---
    loop = asyncio.get_running_loop()
    client = setup_client(loop)
    client.dht_ready.clear() # Ensure DHT is not ready initially
    fake_infohash = "b" * 40

    # The first wait_for is for the DHT, which we want to time out.
    # The second is for metadata, which should not be reached.
    mock_wait_for.side_effect = asyncio.TimeoutError

    # --- Execute & Assert ---
    # We expect our custom LibtorrentError, not the raw asyncio.TimeoutError
    with pytest.raises(LibtorrentError, match="DHT bootstrap timed out"):
        await client.add_torrent(fake_infohash)

    # The metadata future should have been created but not resolved.
    # The error handling in add_torrent should not clean it up because it happens
    # before the metadata wait.
    assert fake_infohash in client.pending_metadata

@pytest.mark.asyncio
async def test_handle_piece_finished():
    """
    Tests that the piece finished handler correctly notifies futures.
    """
    # --- Setup ---
    loop = asyncio.get_running_loop()
    client = setup_client(loop)
    piece_index = 5
    future = loop.create_future()
    client.piece_download_futures[piece_index].append(future)

    alert = MagicMock()
    alert.piece_index = piece_index

    # --- Execute ---
    client._handle_piece_finished(alert)

    # --- Assert ---
    await asyncio.sleep(0) # Allow the loop to process the future result
    assert future.done()
    assert future.result() is True
    assert piece_index not in client.piece_download_futures

class TestTorrentClientExtended:

    def test_remove_torrent(self):
        loop = asyncio.get_event_loop()
        client = setup_client(loop)
        mock_handle = MagicMock()
        mock_handle.is_valid.return_value = True
        mock_handle.info_hash.return_value = "a" * 40

        client.remove_torrent(mock_handle)

        client.ses.remove_torrent.assert_called_once_with(mock_handle, lt_mock.session.delete_files)

    @pytest.mark.asyncio
    async def test_download_and_read_piece_already_have(self):
        loop = asyncio.get_running_loop()
        client = setup_client(loop)
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = True

        # Simulate a successful read alert happening later
        async def mock_read_handler():
            await asyncio.sleep(0.01)
            read_alert = MagicMock()
            read_alert.piece = 10
            read_alert.buffer = b"piece_data"
            read_alert.error = None
            client._handle_read_piece(read_alert)

        handler_task = loop.create_task(mock_read_handler())
        data = await client.download_and_read_piece(mock_handle, 10)

        assert data == b"piece_data"
        mock_handle.read_piece.assert_called_once_with(10)
        await handler_task

    @pytest.mark.asyncio
    async def test_download_and_read_piece_full_flow(self):
        loop = asyncio.get_running_loop()
        client = setup_client(loop)
        mock_handle = MagicMock()
        # Piece is not available initially
        mock_handle.have_piece.return_value = False

        async def mock_alert_flow():
            # Wait for download_and_read_piece to set priority and future
            await asyncio.sleep(0.01)
            assert 10 in client.piece_download_futures

            # Simulate piece finishing download
            finish_alert = MagicMock(piece_index=10)
            client._handle_piece_finished(finish_alert)

            # Wait for the download future to resolve and read_piece to be called
            await asyncio.sleep(0.01)
            mock_handle.read_piece.assert_called_once_with(10)

            # Simulate read finishing
            read_alert = MagicMock(piece=10, buffer=b"piece_data", error=None)
            client._handle_read_piece(read_alert)

        flow_task = loop.create_task(mock_alert_flow())
        data = await client.download_and_read_piece(mock_handle, 10)

        assert data == b"piece_data"
        mock_handle.piece_priority.assert_called_once_with(10, 7)
        await flow_task

    def test_handle_read_piece_error(self):
        loop = asyncio.get_event_loop()
        client = setup_client(loop)
        future = loop.create_future()
        client.pending_reads[10].append(future)

        alert = MagicMock()
        alert.piece = 10
        alert.error = MagicMock()
        alert.error.value.return_value = 1 # Non-zero error code
        alert.error.message.return_value = "A read error"

        client._handle_read_piece(alert)

        assert future.done()
        with pytest.raises(Exception, match="A read error"):
            future.result()
