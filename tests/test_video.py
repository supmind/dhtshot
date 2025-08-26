# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

# Mock the libtorrent library before it's imported by other modules
lt_mock = MagicMock()
with patch.dict('sys.modules', {'libtorrent': lt_mock}):
    from screenshot.video import VideoFile, AsyncTorrentReader
    from screenshot.client import TorrentClient
    from screenshot.service import KeyframeInfo

@pytest.fixture
def mock_client():
    """Provides a mock TorrentClient."""
    return MagicMock(spec=TorrentClient)

@pytest.fixture
def mock_handle():
    """Provides a mock libtorrent handle."""
    handle = MagicMock()
    files = MagicMock()

    # Setup multiple files to test the selection logic
    files.num_files.return_value = 3
    files.file_path.side_effect = ["video1.mkv", "video2.mp4", "other.txt"]
    files.file_size.side_effect = [100, 200, 50] # video2.mp4 is the largest

    handle.torrent_file.return_value.files.return_value = files
    return handle

def test_find_largest_video_file_index(mock_client, mock_handle):
    """
    Tests that the VideoFile class correctly identifies the index of the
    largest video file in the torrent.
    """
    video_file = VideoFile(mock_client, mock_handle)
    # video2.mp4 at index 1 is the largest video file
    assert video_file.file_index == 1

@pytest.mark.asyncio
async def test_download_keyframe_data(mock_client, mock_handle):
    """
    Tests that download_keyframe_data correctly uses the AsyncTorrentReader
    to seek and read the right data.
    """
    video_file = VideoFile(mock_client, mock_handle)
    keyframe_info = KeyframeInfo(pts=1, pos=123, size=456, timescale=1000)

    # We need to patch the AsyncTorrentReader used by the method
    with patch('screenshot.video.AsyncTorrentReader', autospec=True) as MockReader:
        mock_reader_instance = MockReader.return_value
        # Make the async read method an awaitable
        mock_reader_instance.read = AsyncMock(return_value=b"fake_data")

        # Execute
        data = await video_file.download_keyframe_data(keyframe_info)

        # Assert
        MockReader.assert_called_once_with(mock_client, mock_handle, video_file.file_index)
        mock_reader_instance.seek.assert_called_once_with(123)
        mock_reader_instance.read.assert_awaited_once_with(456)
        assert data == b"fake_data"

@pytest.mark.asyncio
@patch('screenshot.video.F4VParser')
@patch('screenshot.video.asyncio.get_running_loop')
async def test_get_keyframes_and_moov_success(mock_loop, mock_parser, mock_client, mock_handle):
    """
    Tests the successful flow of getting keyframes and moov data.
    """
    # --- Setup ---
    video_file = VideoFile(mock_client, mock_handle)
    fake_moov_data = b"moov_data"
    fake_keyframe_infos = [KeyframeInfo(1,2,3,4)]

    # Mock the helper function that finds the moov box inside the downloaded data
    # We need to mock the run_in_executor call to return our fake data
    mock_executor = AsyncMock(return_value=fake_moov_data)
    mock_loop.return_value.run_in_executor = mock_executor

    # Mock the internal parsing method
    video_file._parse_keyframes_from_stbl = MagicMock(return_value=fake_keyframe_infos)

    # Mock the reader
    with patch('screenshot.video.AsyncTorrentReader', autospec=True) as MockReader:
        mock_reader_instance = MockReader.return_value
        mock_reader_instance.read = AsyncMock(return_value=b"head_data")
        mock_reader_instance.file_size = 5 * 1024 * 1024 # 5MB

        # --- Execute ---
        keyframes, moov_data = await video_file.get_keyframes_and_moov()

        # --- Assert ---
        # Assert that it tried to read the head of the file
        mock_reader_instance.read.assert_awaited_once()
        # Assert that the moov finding function was called via the executor
        mock_executor.assert_called_once()
        # Assert that the keyframe parsing function was called
        video_file._parse_keyframes_from_stbl.assert_called_once()
        # Assert the correct data was returned
        assert keyframes == fake_keyframe_infos
        assert moov_data == fake_moov_data
