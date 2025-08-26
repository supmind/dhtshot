# -*- coding: utf-8 -*-
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from screenshot.video import VideoFile
from screenshot.service import KeyframeInfo

@pytest.fixture(autouse=True)
def mock_libtorrent():
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        yield

@pytest.fixture
def mock_client():
    return MagicMock(spec_set=['download_and_read_piece'])

@pytest.fixture
def mock_handle():
    mock = MagicMock()
    mock_files = mock.torrent_file.return_value.files
    mock_files.num_files.return_value = 1
    mock_files.file_path.return_value = "video.mp4"
    mock_files.file_size.return_value = 1024
    return mock

def test_find_largest_video_file_standard(mock_client, mock_handle):
    """Tests finding the largest file in a standard case."""
    mock_files = mock_handle.torrent_file.return_value.files
    mock_files.num_files.return_value = 4
    mock_files.file_path.side_effect = ["movie.mp4", "trailer.mkv", "subtitle.srt", "sample.mp4"]
    mock_files.file_size.side_effect = [1000, 200, 5, 500]
    video_file = VideoFile(mock_client, mock_handle)
    assert video_file.file_index == 0

def test_find_largest_video_file_no_supported_files(mock_client, mock_handle):
    """Tests when no supported video files are present."""
    mock_files = mock_handle.torrent_file.return_value.files
    mock_files.num_files.return_value = 2
    mock_files.file_path.side_effect = ["image.jpg", "document.txt"]
    mock_files.file_size.side_effect = [100, 200]
    video_file = VideoFile(mock_client, mock_handle)
    assert video_file.file_index == -1

@pytest.mark.asyncio
async def test_get_decoder_config_success(mock_client, mock_handle):
    """Tests that get_decoder_config successfully finds and returns SPS/PPS."""
    video_file = VideoFile(mock_client, mock_handle)

    mock_avcC = MagicMock()
    mock_avcC.sps_list = [b'sps']
    mock_avcC.pps_list = [b'pps']

    with patch.object(video_file, '_get_moov_box', new_callable=AsyncMock) as mock_get_moov:
        mock_moov = MagicMock()
        mock_get_moov.return_value = mock_moov
        with patch('screenshot.video.F4VParser.find_child_box', return_value=mock_avcC) as mock_find:
            sps, pps = await video_file.get_decoder_config()
            assert sps == b'sps'
            assert pps == b'pps'
            mock_find.assert_called_once_with(mock_moov, ['trak', 'mdia', 'minf', 'stbl', 'stsd', 'avc1', 'avcC'])

@pytest.mark.asyncio
async def test_get_keyframes_success(mock_client, mock_handle):
    """Tests that get_keyframes successfully parses and returns keyframe info."""
    video_file = VideoFile(mock_client, mock_handle)

    mock_stbl = MagicMock()
    mock_stbl.stss.entries = [1]
    mock_stbl.stts.entries = [(10, 100)]
    mock_stbl.stsc.entries = [(1, 10, 1)]
    mock_stbl.stsz.sample_size = 0  # Set to 0 to test the entries list
    mock_stbl.stsz.entries = [1024] * 10
    mock_stbl.stco.entries = [12345]
    mock_stbl.co64 = None

    mock_moov = MagicMock()
    mock_moov.trak.mdia.mdhd.timescale = 1000
    mock_moov.trak.tkhd.duration = 1000

    with patch.object(video_file, '_get_moov_box', new_callable=AsyncMock, return_value=mock_moov):
        with patch('screenshot.video.F4VParser.find_child_box', return_value=mock_stbl):
            keyframes = await video_file.get_keyframes()
            assert len(keyframes) == 1
            kf = keyframes[0]
            assert isinstance(kf, KeyframeInfo)
            assert kf.pos == 12345
            assert kf.pts == 0
