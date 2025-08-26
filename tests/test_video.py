# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.video import VideoFile, AsyncTorrentReader
from screenshot.client import TorrentClient
from screenshot.service import KeyframeInfo
from screenshot.pymp4parse import F4VParser

@pytest.fixture
def video_file(mock_client, mock_handle):
    """Provides a VideoFile instance with libtorrent patched."""
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        return VideoFile(mock_client, mock_handle)

def test_find_largest_video_file_index(video_file):
    """
    Tests that the VideoFile class correctly identifies the index of the
    largest video file in the torrent.
    """
    # video2.mp4 at index 1 is the largest video file
    assert video_file.file_index == 1

@pytest.mark.asyncio
async def test_download_keyframe_data(video_file, mock_client, mock_handle):
    """
    Tests that download_keyframe_data correctly uses the AsyncTorrentReader
    to seek and read the right data.
    """
    keyframe_info = KeyframeInfo(pts=1, pos=123, size=456, timescale=1000)

    with patch('screenshot.video.AsyncTorrentReader', autospec=True) as MockReader:
        mock_reader_instance = MockReader.return_value
        mock_reader_instance.read = AsyncMock(return_value=b"a" * 456)

        data = await video_file.download_keyframe_data(keyframe_info)

        MockReader.assert_called_once_with(mock_client, mock_handle, video_file.file_index)
        mock_reader_instance.seek.assert_called_once_with(123)
        mock_reader_instance.read.assert_awaited_once_with(456)
        assert data == b"a" * 456

@pytest.mark.asyncio
async def test_get_decoder_config_success(video_file):
    """
    Tests that get_decoder_config successfully extracts SPS and PPS.
    """
    # Create a mock moov box with the necessary structure
    mock_moov = MagicMock()
    mock_avcC = MagicMock()
    mock_avcC.sps_list = [b'sps_data']
    mock_avcC.pps_list = [b'pps_data']

    with patch('screenshot.video.F4VParser.find_child_box', return_value=mock_avcC) as mock_find:
        video_file._get_moov_box = AsyncMock(return_value=mock_moov)
        sps, pps = await video_file.get_decoder_config()

        # Assertions
        mock_find.assert_called_once_with(mock_moov, ['trak', 'mdia', 'minf', 'stbl', 'stsd', 'avc1', 'avcC'])
        assert sps == b'sps_data'
        assert pps == b'pps_data'

@pytest.mark.asyncio
async def test_get_decoder_config_no_moov(video_file):
    """
    Tests that get_decoder_config returns None when no moov box is found.
    """
    video_file._get_moov_box = AsyncMock(return_value=None)
    sps, pps = await video_file.get_decoder_config()
    assert sps is None
    assert pps is None

@pytest.mark.asyncio
async def test_get_keyframes_integration(video_file, moov_data):
    """
    Tests the entire get_keyframes logic using a real moov box.
    This is an integration test for the parsing and new selection logic.
    """
    moov_box = next(F4VParser.parse(bytes_input=moov_data))
    video_file._get_moov_box = AsyncMock(return_value=moov_box)

    keyframes = await video_file.get_keyframes()

    assert keyframes is not None
    assert isinstance(keyframes, list)
    # Based on the fixture's duration and the new logic, we expect a specific count.
    # The moov.dat fixture has 11 keyframes.
    # The selectable range is from index 1 to 9 (8 keyframes).
    # The duration is short, so we want 5 screenshots.
    # Since 8 > 5, we select 5 keyframes.
    assert len(keyframes) == 5

    last_pts = -1
    for kf in keyframes:
        assert isinstance(kf, KeyframeInfo)
        assert kf.pts > last_pts
        last_pts = kf.pts

@pytest.mark.asyncio
async def test_get_keyframes_no_moov(video_file):
    """
    Tests that get_keyframes returns an empty list if moov is not found.
    """
    video_file._get_moov_box = AsyncMock(return_value=None)
    keyframes = await video_file.get_keyframes()
    assert keyframes == []

@pytest.mark.asyncio
async def test_get_keyframes_no_stbl(video_file, moov_data):
    """
    Tests that get_keyframes returns an empty list if stbl is missing.
    """
    moov_box = next(F4VParser.parse(bytes_input=moov_data))
    # Corrupt the moov_box by removing the 'stbl' attribute
    stbl_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl'])
    minf_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf'])
    del minf_box.stbl
    minf_box.children = [c for c in minf_box.children if c != stbl_box]

    video_file._get_moov_box = AsyncMock(return_value=moov_box)
    keyframes = await video_file.get_keyframes()
    assert keyframes == []

class TestAsyncTorrentReader:
    @pytest.mark.asyncio
    async def test_read_logic(self, mock_client):
        mock_handle = MagicMock()
        ti = mock_handle.torrent_file.return_value
        ti.piece_length.return_value = 16384
        ti.map_file.return_value = MagicMock(piece=10, start=100)
        file_storage = ti.files.return_value
        file_storage.file_size.return_value = 50000
        mock_client.download_and_read_piece = AsyncMock(return_value=b'a' * 16384)

        with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
            reader = AsyncTorrentReader(mock_client, mock_handle, file_index=0)
            data = await reader.read(100)
            assert len(data) == 100

            reader.seek(16300)
            ti.map_file.side_effect = [
                MagicMock(piece=10, start=16300),
                MagicMock(piece=11, start=0)
            ]
            data_across_boundary = await reader.read(100)
            assert len(data_across_boundary) == 100
            assert mock_client.download_and_read_piece.call_count == 2

    def test_seek_and_tell(self, mock_client, mock_handle):
        with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
            reader = AsyncTorrentReader(mock_client, mock_handle, file_index=0)
            reader.file_size = 1000

            reader.seek(123)
            assert reader.tell() == 123
            reader.seek(50, 1)
            assert reader.tell() == 173
            reader.seek(-10, 2)
            assert reader.tell() == 990

    @pytest.mark.asyncio
    async def test_read_error(self, mock_client):
        mock_handle = MagicMock()
        ti = mock_handle.torrent_file.return_value
        ti.piece_length.return_value = 16384
        ti.map_file.return_value = MagicMock(piece=10, start=100)
        file_storage = ti.files.return_value
        file_storage.file_size.return_value = 50000
        mock_client.download_and_read_piece.side_effect = IOError("Piece download failed")

        with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
            reader = AsyncTorrentReader(mock_client, mock_handle, file_index=0)
            with pytest.raises(IOError, match="获取 piece 10 失败"):
                await reader.read(100)
