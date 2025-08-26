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
async def test_service_orchestration_flow(service_instance):
    """
    Tests the main orchestration flow of the refactored ScreenshotService.
    This test uses AsyncMock for robust mocking of async methods.
    """
    # 1. Prepare mock data
    fake_infohash = "a" * 40
    mock_handle = MagicMock()
    mock_handle.is_valid.return_value = True
    fake_keyframe_infos = [KeyframeInfo(pts=1, pos=100, size=10, timescale=1000)]
    fake_moov_data = b"moov_data"
    fake_keyframe_data = b"keyframe_data"

    # 2. Setup mocks with correct async behavior
    with patch('screenshot.service.TorrentClient', autospec=True) as MockTorrentClient, \
         patch('screenshot.service.VideoFile', autospec=True) as MockVideoFile, \
         patch('screenshot.service.ScreenshotGenerator', autospec=True) as MockGenerator:

        # Re-initialize the service so it uses our Mock classes during its __init__
        service = ScreenshotService(loop=MagicMock())

        # Get the instances that were created inside the service
        mock_client_instance = service.client
        mock_generator_instance = service.generator

        # Configure the async methods using AsyncMock
        mock_client_instance.add_torrent = AsyncMock(return_value=mock_handle)
        mock_client_instance.remove_torrent = MagicMock() # This is not async

        mock_video_file_instance = MockVideoFile.return_value
        mock_video_file_instance.file_index = 0
        mock_video_file_instance.get_keyframes_and_moov = AsyncMock(return_value=(fake_keyframe_infos, fake_moov_data))
        mock_video_file_instance.download_keyframe_data = AsyncMock(return_value=fake_keyframe_data)

        mock_generator_instance.generate = AsyncMock(return_value=None)

        # 3. Execute the task
        await service._handle_screenshot_task({'infohash': fake_infohash})

        # 4. Assert the flow using await-aware assertions
        mock_client_instance.add_torrent.assert_awaited_once_with(fake_infohash)
        MockVideoFile.assert_called_once_with(mock_client_instance, mock_handle)
        mock_video_file_instance.get_keyframes_and_moov.assert_awaited_once()
        mock_video_file_instance.download_keyframe_data.assert_awaited_once_with(fake_keyframe_infos[0])

        mock_generator_instance.generate.assert_awaited_once()
        _, kwargs = mock_generator_instance.generate.call_args
        assert kwargs['moov_data'] == fake_moov_data
        assert kwargs['keyframe_data'] == fake_keyframe_data

        mock_client_instance.remove_torrent.assert_called_once_with(mock_handle)

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
        mock_log_error.assert_called_once_with(f"无法为 {fake_infohash} 获取有效的 torrent 句柄。")
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
async def test_handle_task_no_video_file():
    """
    Tests handling of a torrent with no video file.
    """
    with patch('screenshot.service.TorrentClient', autospec=True) as MockClient, \
         patch('screenshot.service.VideoFile', autospec=True) as MockVideoFile, \
         patch('screenshot.service.ScreenshotGenerator', autospec=True):

        service = ScreenshotService(loop=MagicMock())
        service.client.add_torrent = AsyncMock(return_value=MagicMock())

        # Simulate no video file found
        mock_video_file_instance = MockVideoFile.return_value
        mock_video_file_instance.file_index = -1

        with patch.object(service.log, 'warning') as mock_log_warning:
            await service._handle_screenshot_task({'infohash': 'c' * 40})
            mock_log_warning.assert_called_once()
            args, _ = mock_log_warning.call_args
            assert "中未找到视频文件" in args[0]

@pytest.mark.asyncio
async def test_handle_task_no_keyframes():
    """
    Tests handling of a video file where keyframes cannot be extracted.
    """
    with patch('screenshot.service.TorrentClient', autospec=True) as MockClient, \
         patch('screenshot.service.VideoFile', autospec=True) as MockVideoFile, \
         patch('screenshot.service.ScreenshotGenerator', autospec=True):

        service = ScreenshotService(loop=MagicMock())
        service.client.add_torrent = AsyncMock(return_value=MagicMock())

        mock_video_file_instance = MockVideoFile.return_value
        mock_video_file_instance.file_index = 0
        # Simulate failure to get keyframes
        mock_video_file_instance.get_keyframes_and_moov = AsyncMock(return_value=(None, None))

        with patch.object(service.log, 'error') as mock_log_error:
            await service._handle_screenshot_task({'infohash': 'd' * 40})
            mock_log_error.assert_called_once_with("无法为 dddddddddddddddddddddddddddddddddddddddd 提取关键帧或 moov_data。")

@pytest.mark.asyncio
async def test_handle_task_keyframe_download_fails():
    """
    Tests the case where a single keyframe download fails.
    The service should log a warning and continue.
    """
    with patch('screenshot.service.TorrentClient', autospec=True), \
         patch('screenshot.service.VideoFile', autospec=True) as MockVideoFile, \
         patch('screenshot.service.ScreenshotGenerator', autospec=True) as MockGenerator:

        service = ScreenshotService(loop=MagicMock())
        service.client.add_torrent = AsyncMock(return_value=MagicMock())

        mock_video_file_instance = MockVideoFile.return_value
        mock_video_file_instance.file_index = 0
        kf_info = KeyframeInfo(1,2,3,1000)
        mock_video_file_instance.get_keyframes_and_moov = AsyncMock(return_value=([kf_info], b'moov'))
        # Simulate download failure
        mock_video_file_instance.download_keyframe_data = AsyncMock(return_value=None)

        with patch.object(service.log, 'warning') as mock_log_warning:
            await service._handle_screenshot_task({'infohash': 'e' * 40})
            mock_log_warning.assert_called_once_with(f"因下载失败，跳过帧 (PTS: {kf_info.pts})。")
            service.generator.generate.assert_not_called()
