# -*- coding: utf-8 -*-
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.video import VideoFile, AsyncTorrentReader
from screenshot.extractor import H264KeyframeExtractor, Keyframe


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
async def test_download_keyframe_data(video_file):
    """
    Tests that download_keyframe_data correctly calls the extractor.
    """
    mock_extractor = MagicMock(spec=H264KeyframeExtractor)
    mock_extractor.get_keyframe_packet = AsyncMock(return_value=(b'extradata', b'packet'))
    video_file._get_extractor = AsyncMock(return_value=mock_extractor)

    keyframe_index_to_download = 5
    result = await video_file.download_keyframe_data(keyframe_index_to_download)

    video_file._get_extractor.assert_awaited_once()
    mock_extractor.get_keyframe_packet.assert_awaited_once_with(keyframe_index_to_download)
    assert result == (b'extradata', b'packet')

@pytest.mark.asyncio
async def test_get_keyframes_selection_logic(video_file):
    """
    Tests the keyframe selection logic in get_keyframes.
    """
    mock_extractor = MagicMock(spec=H264KeyframeExtractor)
    # Create 100 mock keyframes
    mock_extractor.keyframes = [Keyframe(i, i, i*100, 1000) for i in range(100)]
    # Mock a duration that will trigger the selection logic
    mock_extractor.samples = [MagicMock(pts=100 * 1000)] # 100 seconds
    mock_extractor.timescale = 1000

    video_file._get_extractor = AsyncMock(return_value=mock_extractor)

    keyframes = await video_file.get_keyframes()

    # Duration is 100s, target interval is 180s.
    # num_screenshots = int(100 / 180) -> 0.
    # num_screenshots becomes min(MIN_SCREENSHOTS, ...) which is 5.
    assert len(keyframes) == 5

    # Check that keyframes are selected from the 10% to 90% range
    # Range is from index 10 to 89.
    for kf in keyframes:
        assert 10 <= kf.index < 90

@pytest.mark.asyncio
async def test_get_keyframes_no_extractor(video_file):
    """
    Tests that get_keyframes returns an empty list if extractor is not created.
    """
    video_file._get_extractor = AsyncMock(return_value=None)
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
