# -*- coding: utf-8 -*-
import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from screenshot.service import ScreenshotService, KeyframeInfo

@pytest.fixture
def mock_deps():
    """Fixture to mock all major dependencies of ScreenshotService."""
    with patch('screenshot.service.TorrentClient', autospec=True) as MockClient, \
         patch('screenshot.service.VideoFile', autospec=True) as MockVideoFile, \
         patch('screenshot.service.ScreenshotGenerator', autospec=True) as MockGenerator:
        yield MockClient, MockVideoFile, MockGenerator

@pytest.mark.asyncio
async def test_submit_task_and_worker_flow(mock_deps):
    """
    High-level test to ensure a task is submitted, picked up by a worker,
    and the main screenshot generation logic is called.
    """
    _, _, _ = mock_deps
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop)

    await service.run()

    with patch.object(service, '_handle_screenshot_task', new_callable=AsyncMock) as mock_handler:
        test_infohash = "a" * 40
        await service.submit_task(test_infohash)
        await asyncio.sleep(0.1)

        mock_handler.assert_called_once()
        called_infohash = mock_handler.call_args[0][0]['infohash']
        assert called_infohash == test_infohash

    service.stop()

@pytest.mark.asyncio
async def test_handle_task_torrent_add_fails(mock_deps):
    """
    Tests that if adding the torrent fails, an error is logged.
    """
    _, _, _ = mock_deps
    service = ScreenshotService(loop=MagicMock())
    service.client.add_torrent = AsyncMock(return_value=None)

    with patch.object(service.log, 'error') as mock_log_error:
        await service._handle_screenshot_task({'infohash': 'b' * 40})
        mock_log_error.assert_called_once_with("Could not get a valid torrent handle for bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.")

@pytest.mark.asyncio
async def test_handle_task_no_video_file(mock_deps):
    """
    Tests handling of a torrent that contains no video files.
    """
    _, MockVideoFile, _ = mock_deps
    service = ScreenshotService(loop=MagicMock())
    service.client.add_torrent = AsyncMock(return_value=MagicMock())

    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = -1

    with patch.object(service.log, 'warning') as mock_log_warning:
        await service._handle_screenshot_task({'infohash': 'c' * 40})
        mock_log_warning.assert_called_once_with("No video file found in torrent cccccccccccccccccccccccccccccccccccccccc.")

@pytest.mark.asyncio
async def test_generate_screenshots_no_config(mock_deps):
    """
    Tests the case where decoder config (SPS/PPS) cannot be extracted.
    """
    _, MockVideoFile, _ = mock_deps
    service = ScreenshotService(loop=MagicMock())
    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = 0
    mock_video_file_instance.get_decoder_config = AsyncMock(return_value=(None, None))

    with patch.object(service.log, 'error') as mock_log_error:
        await service._generate_screenshots_from_torrent(MagicMock(), 'd' * 40)
        mock_log_error.assert_called_once()
        assert "Could not extract SPS/PPS" in mock_log_error.call_args[0][0]

@pytest.mark.asyncio
async def test_generate_screenshots_no_keyframes(mock_deps):
    """
    Tests the case where keyframes cannot be extracted.
    """
    _, MockVideoFile, _ = mock_deps
    service = ScreenshotService(loop=MagicMock())
    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = 0
    mock_video_file_instance.get_decoder_config = AsyncMock(return_value=(b'sps', b'pps'))
    mock_video_file_instance.get_keyframes = AsyncMock(return_value=[])

    with patch.object(service.log, 'error') as mock_log_error:
        await service._generate_screenshots_from_torrent(MagicMock(), 'e' * 40)
        mock_log_error.assert_called_once_with("Could not extract keyframes from eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee.")

@pytest.mark.asyncio
async def test_generate_screenshots_success(mock_deps):
    """
    Tests the successful path of screenshot generation.
    """
    _, MockVideoFile, MockGenerator = mock_deps
    service = ScreenshotService(loop=MagicMock())
    mock_generator_instance = MockGenerator.return_value
    mock_generator_instance.generate = AsyncMock()

    mock_video_file_instance = MockVideoFile.return_value
    mock_video_file_instance.file_index = 0
    mock_video_file_instance.get_decoder_config = AsyncMock(return_value=(b'sps', b'pps'))
    keyframes = [KeyframeInfo(1000, 2, 3, 1000), KeyframeInfo(2000, 5, 6, 1000)]
    mock_video_file_instance.get_keyframes = AsyncMock(return_value=keyframes)
    mock_video_file_instance.download_keyframe_data = AsyncMock(return_value=b'kf_data')

    await service._generate_screenshots_from_torrent(MagicMock(), 'f' * 40)

    assert mock_generator_instance.generate.call_count == 2
    first_call_args = mock_generator_instance.generate.call_args_list[0].kwargs
    assert first_call_args['sps'] == b'sps'
    assert first_call_args['pps'] == b'pps'
    assert first_call_args['keyframe_data'] == b'kf_data'
    assert first_call_args['infohash_hex'] == 'f' * 40
    assert first_call_args['timestamp_str'] == '00-00-01'
