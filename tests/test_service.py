# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.service import ScreenshotService, KeyframeInfo
from screenshot.client import TorrentClient
from screenshot.video import VideoFile
from screenshot.generator import ScreenshotGenerator

@pytest.fixture
def service_instance():
    """Provides a ScreenshotService instance with a mock loop."""
    return ScreenshotService(loop=MagicMock())

@pytest.mark.asyncio
async def test_service_orchestration_and_exception_flow(service_instance):
    """
    Tests the main orchestration flow and that cleanup happens on exception.
    """
    # --- Success Case ---
    mock_handle = MagicMock()
    service_instance.client.add_torrent = AsyncMock(return_value=mock_handle)
    service_instance.client.remove_torrent = MagicMock()
    service_instance._generate_screenshots_from_torrent = AsyncMock()

    await service_instance._handle_screenshot_task({'infohash': 'a' * 40})

    service_instance.client.add_torrent.assert_awaited_once_with('a' * 40)
    service_instance._generate_screenshots_from_torrent.assert_awaited_once_with(mock_handle, 'a' * 40)
    service_instance.client.remove_torrent.assert_called_once_with(mock_handle)

    # --- Exception Case ---
    service_instance.client.add_torrent.reset_mock()
    service_instance.client.remove_torrent.reset_mock()
    service_instance._generate_screenshots_from_torrent.side_effect = ValueError("Test Failure")

    await service_instance._handle_screenshot_task({'infohash': 'b' * 40})

    service_instance.client.add_torrent.assert_awaited_once_with('b' * 40)
    service_instance.client.remove_torrent.assert_called_once_with(mock_handle)

@pytest.mark.asyncio
async def test_handle_task_invalid_handle(service_instance):
    """
    Tests that the service correctly handles a case where it receives an
    invalid torrent handle from the client.
    """
    fake_infohash = "b" * 40
    # Mock the client to return an invalid handle
    service_instance.client.add_torrent = AsyncMock(return_value=None)

    # Mock the logger to check if an error was logged
    with patch.object(service_instance.log, 'error') as mock_log_error, \
         patch.object(service_instance.generator, 'generate') as mock_generate:
        await service_instance._handle_screenshot_task({'infohash': fake_infohash})

        # Assert that an error was logged and the function exited early
        mock_log_error.assert_called_once_with(f"Could not get a valid torrent handle for {fake_infohash}.")
        # The generator's generate method should not have been called
        mock_generate.assert_not_called()

def test_stop_service(service_instance):
    """
    Tests that the stop method correctly cancels worker tasks.
    """
    # Create mock workers
    mock_worker1 = MagicMock(spec=asyncio.Task)
    mock_worker2 = MagicMock(spec=asyncio.Task)
    service_instance.workers = [mock_worker1, mock_worker2]
    service_instance.client = MagicMock()

    service_instance.stop()

    assert service_instance._running is False
    mock_worker1.cancel.assert_called_once()
    mock_worker2.cancel.assert_called_once()
    service_instance.client.stop.assert_called_once()

@pytest.mark.asyncio
@patch('screenshot.service.VideoFile')
async def test_generate_screenshots_no_video_file(MockVideoFile, service_instance):
    """
    Tests handling of a torrent with no video file.
    """
    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = -1
    mock_handle = MagicMock()
    infohash_hex = 'c' * 40

    with patch.object(service_instance.log, 'warning') as mock_log_warning:
        await service_instance._generate_screenshots_from_torrent(mock_handle, infohash_hex)
        mock_log_warning.assert_called_once_with(f"No video file found in torrent {infohash_hex}.")

@pytest.mark.asyncio
@patch('screenshot.service.VideoFile')
async def test_generate_screenshots_no_decoder_config(MockVideoFile, service_instance):
    """
    Tests handling of a video file where SPS/PPS cannot be extracted.
    """
    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = 0
    mock_video_file_instance.get_decoder_config = AsyncMock(return_value=(None, None))
    mock_handle = MagicMock()
    infohash_hex = 'd' * 40

    with patch.object(service_instance.log, 'error') as mock_log_error:
        await service_instance._generate_screenshots_from_torrent(mock_handle, infohash_hex)
        mock_log_error.assert_called_once_with(f"Could not extract SPS/PPS from {infohash_hex}. Assuming not H.264 or file is corrupt.")

@pytest.mark.asyncio
@patch('screenshot.service.VideoFile')
async def test_generate_screenshots_no_keyframes(MockVideoFile, service_instance):
    """
    Tests handling of a video file where keyframes cannot be extracted.
    """
    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = 0
    mock_video_file_instance.get_decoder_config = AsyncMock(return_value=(b'sps', b'pps'))
    mock_video_file_instance.get_keyframes = AsyncMock(return_value=[]) # No keyframes
    mock_handle = MagicMock()
    infohash_hex = 'e' * 40

    with patch.object(service_instance.log, 'error') as mock_log_error:
        await service_instance._generate_screenshots_from_torrent(mock_handle, infohash_hex)
        mock_log_error.assert_called_once_with(f"Could not extract keyframes from {infohash_hex}.")

@pytest.mark.asyncio
@patch('screenshot.service.VideoFile')
async def test_generate_screenshots_keyframe_download_fails(MockVideoFile, service_instance):
    """
    Tests the case where a single keyframe download fails.
    The service should log a warning and continue without calling the generator for that frame.
    """
    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = 0
    mock_video_file_instance.get_decoder_config = AsyncMock(return_value=(b'sps', b'pps'))
    kf_info = KeyframeInfo(pts=123, pos=1, size=2, timescale=1000)
    mock_video_file_instance.get_keyframes = AsyncMock(return_value=[kf_info])
    mock_video_file_instance.download_keyframe_data = AsyncMock(return_value=None) # Download fails
    mock_handle = MagicMock()
    infohash_hex = 'f' * 40

    service_instance.generator.generate = AsyncMock()
    with patch.object(service_instance.log, 'warning') as mock_log_warning:
        await service_instance._generate_screenshots_from_torrent(mock_handle, infohash_hex)
        mock_log_warning.assert_called_once_with(f"Skipping frame (PTS: {kf_info.pts}) due to download failure.")
        service_instance.generator.generate.assert_not_called()
