# -*- coding: utf-8 -*-
"""
对 screenshot/video_processor.py 视频处理协调逻辑的单元测试。
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from screenshot.video_processor import VideoProcessor
from config import Settings
from screenshot.errors import NoVideoFileError, MoovParsingError, MP4ParsingError
from screenshot.extractor import Keyframe, KeyframeExtractor
from screenshot.client import TorrentClient


@pytest.fixture
def settings():
    return Settings()

@pytest.fixture
def processor(settings):
    """Provides a VideoProcessor instance with mocked dependencies."""
    loop = asyncio.get_event_loop()
    return VideoProcessor(
        settings=settings,
        client=AsyncMock(spec=TorrentClient),
        generator=AsyncMock(),
        loop=loop,
        details_callback=AsyncMock(),
        screenshot_check_callback=AsyncMock(return_value=[])
    )

@pytest.mark.asyncio
@patch('screenshot.video_processor.MoovFinder', autospec=True)
@patch('screenshot.video_processor.KeyframeExtractor', autospec=True)
@patch('screenshot.video_processor.select_keyframes', autospec=True)
@patch('screenshot.video_processor.PieceManager', autospec=True)
async def test_process_full_success_flow(
    MockPieceManager, mock_select_keyframes, MockKeyframeExtractor, MockMoovFinder, processor
):
    """
    Tests the high-level orchestration of the process method, ensuring all
    components are called correctly in a successful run.
    """
    # --- Setup Mocks ---
    mock_handle = MagicMock()
    mock_ti = MagicMock()
    mock_handle.get_torrent_info.return_value = mock_ti
    mock_ti.piece_length.return_value = 16384
    mock_ti.name.return_value = "Test Torrent"
    processor._find_video_file = MagicMock(return_value=(0, 100000, 0, 'video.mp4'))

    mock_moov_finder_instance = MockMoovFinder.return_value
    mock_moov_finder_instance.find_moov_atom = AsyncMock(return_value=b'moov_data')

    mock_extractor_instance = MockKeyframeExtractor.return_value
    mock_extractor_instance.keyframes = [Keyframe(0,0,0,0)]
    mock_extractor_instance.duration_pts = 180 * 90000
    mock_extractor_instance.timescale = 90000
    mock_extractor_instance.samples = [MagicMock()]

    mock_selected_keyframes = [Keyframe(index=0, sample_index=0, pts=0, timescale=90000)]
    mock_select_keyframes.return_value = mock_selected_keyframes

    # Mock _process_keyframe_pieces to simulate success
    mock_success_kf_indices = {kf.index for kf in mock_selected_keyframes}
    # The generation map should contain awaitables (like Futures or Tasks)
    future = asyncio.Future()
    future.set_result("screenshot_path.jpg") # Simulate a successful screenshot generation
    generation_tasks_map = {0: future}
    processor._process_keyframe_pieces = AsyncMock(return_value=(mock_success_kf_indices, generation_tasks_map))

    # --- Run Test ---
    infohash_hex = "success_hash"
    await processor.process(mock_handle, infohash_hex)

    # --- Assertions ---
    MockPieceManager.assert_called_once()
    MockMoovFinder.assert_called_once()
    mock_moov_finder_instance.find_moov_atom.assert_awaited_once()
    MockKeyframeExtractor.assert_called_once_with(b'moov_data')
    processor.details_callback.assert_awaited_once()
    mock_select_keyframes.assert_called_once()
    processor.screenshot_check_callback.assert_awaited_once_with(infohash=infohash_hex)
    processor._process_keyframe_pieces.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_no_video_file(processor):
    mock_handle = MagicMock()
    mock_ti = MagicMock()
    mock_handle.get_torrent_info.return_value = mock_ti
    processor._find_video_file = MagicMock(return_value=(-1, -1, -1, None))

    with pytest.raises(NoVideoFileError):
        await processor.process(mock_handle, "no_video_hash")

    processor.details_callback.assert_not_called()


@pytest.mark.asyncio
@patch('screenshot.video_processor.MoovFinder', autospec=True)
@patch('screenshot.video_processor.KeyframeExtractor', autospec=True)
async def test_process_moov_parsing_error(MockKeyframeExtractor, MockMoovFinder, processor):
    mock_handle = MagicMock()
    mock_handle.get_torrent_info.return_value = MagicMock()
    processor._find_video_file = MagicMock(return_value=(0, 100000, 0, 'video.mp4'))
    MockMoovFinder.return_value.find_moov_atom = AsyncMock(return_value=b'moov_data')

    MockKeyframeExtractor.side_effect = MP4ParsingError("test error", "parse_error_hash")

    with pytest.raises(MoovParsingError):
        await processor.process(mock_handle, "parse_error_hash")

    processor.details_callback.assert_not_called()
    processor.screenshot_check_callback.assert_not_called()
