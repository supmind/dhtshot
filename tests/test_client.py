import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from screenshot.client import TorrentClient, LibtorrentError

@pytest.fixture
def mock_loop():
    return MagicMock(spec=asyncio.AbstractEventLoop)

@pytest.fixture
def client(mock_loop):
    # We patch the session to avoid real network/disk IO
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("libtorrent.session", MagicMock())
        client = TorrentClient(loop=mock_loop)
        client.log = MagicMock()
        yield client

class TestFetchPieces:
    @pytest.mark.asyncio
    async def test_fetch_pieces_already_present(self, client, mock_loop):
        """Tests fetching pieces that are already downloaded."""
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = True

        # Mock the reading part
        async def mock_read_flow():
            await asyncio.sleep(0.01)
            # Simulate read_piece alerts for all requested pieces
            client._handle_read_piece(MagicMock(piece=0, buffer=b'data0', error=None))
            client._handle_read_piece(MagicMock(piece=1, buffer=b'data1', error=None))

        mock_loop.create_task.side_effect = lambda coro: asyncio.create_task(coro)
        mock_loop.create_future.side_effect = lambda: asyncio.get_running_loop().create_future()

        asyncio.create_task(mock_read_flow())

        result = await client.fetch_pieces(mock_handle, [0, 1])

        mock_handle.prioritize_pieces.assert_not_called()
        assert result == {0: b'data0', 1: b'data1'}

    @pytest.mark.asyncio
    async def test_fetch_pieces_download_flow(self, client, mock_loop):
        """Tests the full download->read flow for fetch_pieces."""
        mock_handle = MagicMock()
        mock_handle.have_piece.side_effect = lambda idx: idx in [2] # Piece 2 is present, 0 and 1 need download
        ti = MagicMock()
        ti.num_pieces.return_value = 3
        mock_handle.get_torrent_info.return_value = ti

        async def mock_alert_flow():
            # Wait for priorities to be set
            await asyncio.sleep(0.01)
            # Simulate piece downloads
            client._handle_piece_finished(MagicMock(piece_index=0))
            client._handle_piece_finished(MagicMock(piece_index=1))
            # Wait for read requests
            await asyncio.sleep(0.01)
            # Simulate read completions
            client._handle_read_piece(MagicMock(piece=0, buffer=b'data0', error=None))
            client._handle_read_piece(MagicMock(piece=1, buffer=b'data1', error=None))
            client._handle_read_piece(MagicMock(piece=2, buffer=b'data2', error=None))

        mock_loop.create_task.side_effect = lambda coro: asyncio.create_task(coro)
        mock_loop.create_future.side_effect = lambda: asyncio.get_running_loop().create_future()

        asyncio.create_task(mock_alert_flow())

        result = await client.fetch_pieces(mock_handle, [0, 1, 2])

        from unittest.mock import call
        # Check that priorities were set correctly for the pieces to be downloaded
        calls = [call(0, 7), call(1, 7)]
        mock_handle.piece_priority.assert_has_calls(calls, any_order=True)
        assert mock_handle.piece_priority.call_count == 2
        assert result == {0: b'data0', 1: b'data1', 2: b'data2'}

    @pytest.mark.asyncio
    async def test_fetch_pieces_download_timeout(self, client, mock_loop):
        """Tests that a timeout during download is handled correctly."""
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = False
        ti = MagicMock()
        ti.num_pieces.return_value = 1
        mock_handle.get_torrent_info.return_value = ti

        mock_loop.create_future.side_effect = lambda: asyncio.get_running_loop().create_future()

        with pytest.raises(LibtorrentError, match="下载 pieces \\[0\\] 超时。"):
            await client.fetch_pieces(mock_handle, [0], timeout=0.01)

        # Ensure the pending fetch request is cleaned up
        assert not client.pending_fetches
