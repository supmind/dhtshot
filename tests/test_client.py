# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

# Mock the libtorrent library before it's imported by the client
lt_mock = MagicMock()
with patch.dict('sys.modules', {'libtorrent': lt_mock}):
    from screenshot.client import TorrentClient, LibtorrentError

@pytest.fixture
def client():
    """Provides a TorrentClient instance with a mock loop."""
    with patch('libtorrent.session', return_value=MagicMock()) as mock_session:
        client = TorrentClient(loop=MagicMock())
        # Replace the real session with a mock for testing
        client.ses = mock_session.return_value
        yield client

@pytest.mark.asyncio
async def test_add_torrent_success(client):
    """
    Tests the successful flow of adding a torrent and receiving metadata.
    """
    # --- Setup ---
    fake_infohash = "a" * 40
    mock_handle = MagicMock()

    # Mock the libtorrent functions
    lt_mock.parse_magnet_uri.return_value = {'save_path': '/dev/shm/...' }
    client.ses.add_torrent.return_value = mock_handle

    # --- Execute ---
    # We run the add_torrent task and the alert handler task concurrently
    # to simulate the real application flow.

    async def mock_alert_handler():
        # Simulate the DHT becoming ready
        client.dht_ready.set()
        await asyncio.sleep(0.01) # give add_torrent time to run
        # Simulate receiving the metadata alert
        metadata_alert = MagicMock()
        metadata_alert.handle = mock_handle
        # To access the protected method for testing, we get it from the instance's __dict__
        client._handle_metadata_received(metadata_alert)

    add_torrent_task = asyncio.create_task(client.add_torrent(fake_infohash))
    handler_task = asyncio.create_task(mock_alert_handler())

    # Wait for the add_torrent method to complete
    returned_handle = await add_torrent_task

    # --- Assert ---
    lt_mock.parse_magnet_uri.assert_called_once()
    client.ses.add_torrent.assert_called_once()
    assert returned_handle == mock_handle
    handler_task.cancel() # Clean up the handler task

@pytest.mark.asyncio
async def test_add_torrent_timeout(client):
    """
    Tests that add_torrent correctly times out if metadata is not received.
    """
    # --- Setup ---
    fake_infohash = "b" * 40

    # --- Execute & Assert ---
    with pytest.raises(asyncio.TimeoutError):
        # We patch wait_for to have a very short timeout for testing purposes
        with patch('asyncio.wait_for') as mock_wait_for:
            # Configure the mock to raise a TimeoutError
            mock_wait_for.side_effect = asyncio.TimeoutError
            await client.add_torrent(fake_infohash)

def test_handle_piece_finished(client):
    """
    Tests that the piece finished handler correctly notifies futures.
    """
    # --- Setup ---
    piece_index = 5
    future = asyncio.Future()
    client.piece_download_futures[piece_index].append(future)

    alert = MagicMock()
    alert.piece_index = piece_index

    # --- Execute ---
    client._handle_piece_finished(alert)

    # --- Assert ---
    assert future.done()
    assert future.result() is True
    assert piece_index not in client.piece_download_futures
