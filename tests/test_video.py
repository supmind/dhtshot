# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.video import VideoFile, AsyncTorrentReader
from screenshot.client import TorrentClient
from screenshot.service import KeyframeInfo
from screenshot.pymp4parse import F4VParser

@pytest.fixture
def mock_client():
    """Provides a mock TorrentClient."""
    return MagicMock(spec=TorrentClient)

@pytest.fixture
def mock_handle():
    """
    Provides a mock libtorrent handle with an explicitly mocked file list.
    This avoids the ambiguity of using side_effect in a loop.
    """
    handle = MagicMock()
    files_mock = MagicMock()

    # Create individual mock file entries
    file1 = MagicMock()
    file1.path = "video1.mkv"
    file1.size = 100

    file2 = MagicMock()
    file2.path = "video2.mp4"
    file2.size = 200

    file3 = MagicMock()
    file3.path = "other.txt"
    file3.size = 50

    # Configure the 'files' mock to behave like a list-like object
    files_mock.num_files.return_value = 3
    # The 'file_path' and 'file_size' methods of the lt.file_storage object
    # take an index as an argument. We mock this behavior.
    def file_path_side_effect(index):
        return [file1.path, file2.path, file3.path][index]
    def file_size_side_effect(index):
        return [file1.size, file2.size, file3.size][index]

    files_mock.file_path.side_effect = file_path_side_effect
    files_mock.file_size.side_effect = file_size_side_effect

    handle.torrent_file.return_value.files.return_value = files_mock
    return handle

def test_find_largest_video_file_index(mock_client, mock_handle):
    """
    Tests that the VideoFile class correctly identifies the index of the
    largest video file in the torrent.
    """
    # We patch libtorrent here to avoid loading the actual library
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        video_file = VideoFile(mock_client, mock_handle)
        # video2.mp4 at index 1 is the largest video file
        assert video_file.file_index == 1

@pytest.mark.asyncio
async def test_download_keyframe_data(mock_client, mock_handle):
    """
    Tests that download_keyframe_data correctly uses the AsyncTorrentReader
    to seek and read the right data.
    """
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        video_file = VideoFile(mock_client, mock_handle)
    keyframe_info = KeyframeInfo(pts=1, pos=123, size=456, timescale=1000)

    # We need to patch the AsyncTorrentReader used by the method
    with patch('screenshot.video.AsyncTorrentReader', autospec=True) as MockReader:
        mock_reader_instance = MockReader.return_value
        # Make the async read method an awaitable that returns the correct amount of data
        mock_reader_instance.read = AsyncMock(return_value=b"a" * 456)

        # Execute
        data = await video_file.download_keyframe_data(keyframe_info)

        # Assert
        MockReader.assert_called_once_with(mock_client, mock_handle, video_file.file_index)
        mock_reader_instance.seek.assert_called_once_with(123)
        mock_reader_instance.read.assert_awaited_once_with(456)
        assert data == b"a" * 456

@pytest.mark.asyncio
@patch('screenshot.video.F4VParser')
@patch('screenshot.video.asyncio.get_running_loop')
async def test_get_keyframes_and_moov_success(mock_loop, mock_parser, mock_client, mock_handle):
    """
    Tests the successful flow of getting keyframes and moov data.
    """
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
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

class TestAsyncTorrentReader:
    """
    Tests for the AsyncTorrentReader class.
    """
    @pytest.mark.asyncio
    async def test_read_logic(self, mock_client):
        """
        Tests the core read logic of the AsyncTorrentReader.
        """
        # --- Setup ---
        mock_handle = MagicMock()
        ti = mock_handle.torrent_file.return_value
        ti.piece_length.return_value = 16384  # 16KB piece size
        ti.map_file.return_value = MagicMock(piece=10, start=100)

        file_storage = ti.files.return_value
        file_storage.file_size.return_value = 50000 # 50KB file size

        # Mock the client's download method
        mock_client.download_and_read_piece = AsyncMock(return_value=b'a' * 16384)

        # --- Execute ---
        with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
            reader = AsyncTorrentReader(mock_client, mock_handle, file_index=0)

            # Test reading a small chunk
            data = await reader.read(100)
            assert len(data) == 100

            # Test reading across piece boundary (this is simplified, full test would be complex)
            reader.seek(16300)
            ti.map_file.side_effect = [
                MagicMock(piece=10, start=16300), # First piece
                MagicMock(piece=11, start=0)      # Second piece
            ]
            data_across_boundary = await reader.read(100)
            assert len(data_across_boundary) == 100
            # Should have called download for two different pieces
            assert mock_client.download_and_read_piece.call_count == 2

    def test_seek_and_tell(self, mock_client, mock_handle):
        """
        Tests the seek and tell methods.
        """
        with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
            reader = AsyncTorrentReader(mock_client, mock_handle, file_index=0)

            reader.seek(123)
            assert reader.tell() == 123

            reader.seek(50, 1) # SEEK_CUR
            assert reader.tell() == 173

            reader.file_size = 1000
            reader.seek(-10, 2) # SEEK_END
            assert reader.tell() == 990

def test_parse_keyframes_from_stbl(moov_data, mock_client, mock_handle):
    """
    Tests the _parse_keyframes_from_stbl method with a real 'moov' box.
    This ensures the complex keyframe calculation logic is correct.
    """
    # Parse the real 'moov' data to get a box object
    moov_box = next(F4VParser.parse(bytes_input=moov_data))

    # Create a VideoFile instance to call the method on
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        video_file = VideoFile(mock_client, mock_handle)

    # Execute the method under test
    keyframes = video_file._parse_keyframes_from_stbl(moov_box)

    # Assert the results
    assert keyframes is not None
    assert isinstance(keyframes, list)
    assert len(keyframes) > 0

    # Check the content of the keyframes
    last_pts = -1
    for kf in keyframes:
        assert isinstance(kf, KeyframeInfo)
        assert kf.pts >= 0
        assert kf.pos > 0
        assert kf.size > 0
        # Timestamps should be monotonically increasing
        assert kf.pts > last_pts
        last_pts = kf.pts

@pytest.mark.asyncio
@patch('screenshot.video.asyncio.get_running_loop')
async def test_get_keyframes_and_moov_not_found(mock_loop, mock_client, mock_handle):
    """
    Tests that get_keyframes_and_moov returns (None, None) when a moov box
    cannot be found in the video file.
    """
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        video_file = VideoFile(mock_client, mock_handle)

        # Mock the executor to simulate the 'find_moov_in_data' helper
        # returning None, as if no moov box was found.
        mock_executor = AsyncMock(return_value=None)
        mock_loop.return_value.run_in_executor = mock_executor

        with patch('screenshot.video.AsyncTorrentReader', autospec=True) as MockReader:
            mock_reader_instance = MockReader.return_value
            # Return some dummy data that doesn't contain 'moov'
            mock_reader_instance.read = AsyncMock(return_value=b"not_a_moov_box")
            # Set a file size larger than the probe size to trigger both head and tail reads
            mock_reader_instance.file_size = 5 * 1024 * 1024

            keyframes, moov_data = await video_file.get_keyframes_and_moov()

            # Assert that the method returns None, None
            assert keyframes is None
            assert moov_data is None
            # Assert that it tried reading both the head and the tail
            assert mock_reader_instance.read.call_count == 2

    @pytest.mark.asyncio
    async def test_read_error(self, mock_client):
        """
        Tests that an error during piece download is handled correctly.
        """
        mock_handle = MagicMock()
        ti = mock_handle.torrent_file.return_value
        ti.piece_length.return_value = 16384
        ti.map_file.return_value = MagicMock(piece=10, start=100)
        file_storage = ti.files.return_value
        file_storage.file_size.return_value = 50000

        # Mock the client's download method to raise an error
        mock_client.download_and_read_piece.side_effect = IOError("Piece download failed")

        with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
            reader = AsyncTorrentReader(mock_client, mock_handle, file_index=0)
            with pytest.raises(IOError, match="Piece download failed"):
                await reader.read(100)

def test_no_video_file_found(mock_client):
    """
    Tests the behavior when no video file is found in the torrent.
    """
    handle = MagicMock()
    files_mock = handle.torrent_file.return_value.files.return_value
    files_mock.num_files.return_value = 1
    files_mock.file_path.return_value = "archive.zip" # No video files
    files_mock.file_size.return_value = 100

    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        video_file = VideoFile(mock_client, handle)
        assert video_file.file_index == -1

@pytest.mark.asyncio
async def test_download_keyframe_incomplete_read(mock_client, mock_handle):
    """
    Tests that download_keyframe_data returns None if the read is incomplete.
    """
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        video_file = VideoFile(mock_client, mock_handle)
    keyframe_info = KeyframeInfo(pts=1, pos=123, size=456, timescale=1000)

    with patch('screenshot.video.AsyncTorrentReader', autospec=True) as MockReader:
        mock_reader_instance = MockReader.return_value
        # Simulate reading less data than requested
        mock_reader_instance.read = AsyncMock(return_value=b"a" * 100)

        data = await video_file.download_keyframe_data(keyframe_info)

        assert data is None

@pytest.mark.asyncio
@patch('screenshot.video.asyncio.get_running_loop')
async def test_get_keyframes_parsing_fails(mock_loop, mock_client, mock_handle):
    """
    Tests get_keyframes_and_moov when keyframe parsing returns no keyframes.
    """
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        video_file = VideoFile(mock_client, mock_handle)
        fake_moov_data = b"moov_data"

        # Simulate finding the moov box
        mock_executor = AsyncMock(return_value=fake_moov_data)
        mock_loop.return_value.run_in_executor = mock_executor

        # Simulate _parse_keyframes_from_stbl returning an empty list
        video_file._parse_keyframes_from_stbl = MagicMock(return_value=[])

        with patch('screenshot.video.AsyncTorrentReader', autospec=True) as MockReader:
            mock_reader_instance = MockReader.return_value
            mock_reader_instance.read = AsyncMock(return_value=b"head_data")
            mock_reader_instance.file_size = 1024

            keyframes, moov_data = await video_file.get_keyframes_and_moov()

            assert keyframes is None
            assert moov_data is None
