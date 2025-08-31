import asyncio
import pytest
from unittest.mock import MagicMock, patch

from screenshot.client import TorrentClient
from screenshot.errors import TorrentClientError

# Mark all tests in this file as async, since the client is async
pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_lt_session():
    """Fixture to provide a mocked libtorrent session."""
    with patch('libtorrent.session') as mock_session_constructor:
        mock_session = MagicMock()
        mock_session_constructor.return_value = mock_session
        yield mock_session

async def test_fetch_pieces_cancellation_cleans_up_pending_reads(mock_lt_session):
    """
    Validates the fix for the task cancellation resource leak.
    It checks if cancelling a `fetch_pieces` call correctly removes
    the corresponding futures from the `pending_reads` dictionary.
    """
    loop = asyncio.get_running_loop()
    client = TorrentClient(loop=loop)
    client._ses = mock_lt_session  # Inject the mock session

    # Mock the torrent handle to be valid
    mock_handle = MagicMock()
    mock_handle.is_valid.return_value = True
    mock_handle.have_piece.return_value = True
    # The key for pending_reads is a tuple of (infohash, piece_index)
    mock_handle.info_hash.return_value = "mockinfohash12345678901234567890"

    piece_indices = [0, 1, 2]
    infohash_hex = str(mock_handle.info_hash())

    # --- Create and run a task that we will then cancel ---
    fetch_task = asyncio.create_task(client.fetch_pieces(mock_handle, piece_indices))

    # Let the task run just long enough to create the futures in pending_reads
    await asyncio.sleep(0.01)

    # At this point, futures for the read operations should be in the dictionary
    assert len(client.pending_reads) == 3
    assert (infohash_hex, 0) in client.pending_reads
    assert (infohash_hex, 1) in client.pending_reads
    assert (infohash_hex, 2) in client.pending_reads

    # Get a reference to one of the futures to check its state later
    future_for_piece_0 = client.pending_reads[(infohash_hex, 0)]['future']

    # --- Now, cancel the task ---
    fetch_task.cancel()

    # Awaiting the cancelled task should raise CancelledError
    with pytest.raises(asyncio.CancelledError):
        await fetch_task

    # --- Assertions ---
    # After cancellation, the pending_reads dict should have been cleared
    assert len(client.pending_reads) == 0

    # And the future itself should be marked as cancelled
    assert future_for_piece_0.cancelled()
