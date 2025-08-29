import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from screenshot.client import TorrentClient
from screenshot.errors import TorrentClientError

@pytest.fixture
def client():
    # We patch the session to avoid real network/disk IO
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("screenshot.client.lt.session", MagicMock())
        # Let the test set the loop.
        client = TorrentClient(loop=None)
        client.log = MagicMock()
        yield client

class TestFetchPieces:
    @pytest.mark.asyncio
    async def test_fetch_pieces_already_present(self, client):
        """Tests fetching pieces that are already downloaded."""
        client.loop = asyncio.get_running_loop()
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = True

        # We need to mock the underlying session calls that happen in the executor
        client._ses.add_torrent = MagicMock()
        # We need to mock read_piece on the handle
        mock_handle.read_piece = MagicMock()

        async def mock_read_flow():
            await asyncio.sleep(0.01)
            client._handle_read_piece(MagicMock(piece=0, buffer=b'data0', error=None))
            client._handle_read_piece(MagicMock(piece=1, buffer=b'data1', error=None))

        asyncio.create_task(mock_read_flow())
        result = await client.fetch_pieces(mock_handle, [0, 1])

        # Assert that no download was triggered
        mock_handle.prioritize_pieces.assert_not_called()
        # Assert that read_piece was called for both pieces
        assert mock_handle.read_piece.call_count == 2
        assert result == {0: b'data0', 1: b'data1'}

    @pytest.mark.asyncio
    async def test_fetch_pieces_download_flow(self, client):
        """Tests the full download->read flow for fetch_pieces."""
        client.loop = asyncio.get_running_loop()
        mock_handle = MagicMock()
        mock_handle.have_piece.side_effect = lambda idx: idx in [2] # Piece 2 is present, 0 and 1 need download
        mock_handle.read_piece = MagicMock()

        async def mock_alert_flow():
            await asyncio.sleep(0.01)
            client._handle_piece_finished(MagicMock(piece_index=0))
            client._handle_piece_finished(MagicMock(piece_index=1))
            await asyncio.sleep(0.01)
            client._handle_read_piece(MagicMock(piece=0, buffer=b'data0', error=None))
            client._handle_read_piece(MagicMock(piece=1, buffer=b'data1', error=None))
            client._handle_read_piece(MagicMock(piece=2, buffer=b'data2', error=None))

        asyncio.create_task(mock_alert_flow())
        result = await client.fetch_pieces(mock_handle, [0, 1, 2])

        # Check that priorities were set for the pieces to be downloaded
        mock_handle.prioritize_pieces.assert_called_once_with([(0, 7), (1, 7)])
        # Check that all 3 pieces were read
        assert mock_handle.read_piece.call_count == 3
        assert result == {0: b'data0', 1: b'data1', 2: b'data2'}

    @pytest.mark.asyncio
    async def test_fetch_pieces_download_timeout(self, client):
        """Tests that a timeout during download is handled correctly."""
        client.loop = asyncio.get_running_loop()
        mock_handle = MagicMock()
        mock_handle.have_piece.return_value = False

        with pytest.raises(TorrentClientError, match="下载 pieces \\[0\\] 超时。"):
            await client.fetch_pieces(mock_handle, [0], timeout=0.01)

        # Ensure the pending fetch request is cleaned up
        assert not client.pending_fetches
