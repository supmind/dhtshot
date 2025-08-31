import pytest
import asyncio
from unittest.mock import MagicMock, call, AsyncMock

from screenshot.service import ScreenshotService
from screenshot.errors import (
    MetadataTimeoutError, NoVideoFileError, MoovNotFoundError,
    MoovParsingError, FrameDownloadTimeoutError, TorrentClientError
)

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio

async def test_run_task_success_path(mock_service_dependencies, status_callback):
    """
    Tests the full, successful lifecycle of a screenshot task.
    """
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop, status_callback=status_callback)
    # Mock the main screenshot generation logic, as it's very complex.
    # We trust that its internal logic is correct and just check that it's called.
    service._generate_screenshots_from_torrent = AsyncMock()

    infohash = "test_hash_success"
    await service.run()
    await service._handle_screenshot_task({'infohash': infohash})
    await service.stop()

    client = mock_service_dependencies['client']

    # Check that the core sequence of events happened
    client.add_torrent.assert_awaited_once_with(infohash)
    service._generate_screenshots_from_torrent.assert_awaited_once()
    client.remove_torrent.assert_awaited_once()

    # Check that the final status was 'success'
    status_callback.assert_awaited_once()
    _, kwargs = status_callback.call_args
    assert kwargs['status'] == 'success'
    assert kwargs['infohash'] == infohash

async def test_run_task_add_torrent_fails(mock_service_dependencies, status_callback):
    """Tests failure when the client can't add the torrent."""
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop, status_callback=status_callback)
    client = mock_service_dependencies['client']
    client.add_torrent.side_effect = TorrentClientError("Failed to add")

    service._generate_screenshots_from_torrent = AsyncMock()
    infohash = "test_hash_add_fails"

    await service.run()
    await service._handle_screenshot_task({'infohash': infohash})
    await service.stop()

    # The generation logic should not be called if adding the torrent fails
    service._generate_screenshots_from_torrent.assert_not_called()

    status_callback.assert_awaited_once()
    _, kwargs = status_callback.call_args
    assert kwargs['status'] == 'permanent_failure'
    assert isinstance(kwargs['error'], TorrentClientError)
    # remove_torrent should not be called if the handle was never acquired
    client.remove_torrent.assert_not_called()

async def test_run_task_generation_raises_no_video_file(mock_service_dependencies, status_callback):
    """Tests a permanent failure if no video file is found inside the generation logic."""
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop, status_callback=status_callback)
    service._generate_screenshots_from_torrent = AsyncMock(side_effect=NoVideoFileError("No MP4s", "hash"))
    infohash = "test_hash_no_video"

    await service.run()
    await service._handle_screenshot_task({'infohash': infohash})
    await service.stop()

    status_callback.assert_awaited_once()
    _, kwargs = status_callback.call_args
    assert kwargs['status'] == 'permanent_failure'
    assert isinstance(kwargs['error'], NoVideoFileError)
    mock_service_dependencies['client'].remove_torrent.assert_awaited_once()

async def test_run_task_generation_raises_moov_not_found(mock_service_dependencies, status_callback):
    """Tests a permanent failure if the moov atom isn't found."""
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop, status_callback=status_callback)
    service._generate_screenshots_from_torrent = AsyncMock(side_effect=MoovNotFoundError("Not found", "hash"))
    infohash = "test_hash_moov_not_found"

    await service.run()
    await service._handle_screenshot_task({'infohash': infohash})
    await service.stop()

    status_callback.assert_awaited_once()
    _, kwargs = status_callback.call_args
    assert kwargs['status'] == 'permanent_failure'
    assert isinstance(kwargs['error'], MoovNotFoundError)
    mock_service_dependencies['client'].remove_torrent.assert_awaited_once()

async def test_run_task_generation_raises_parsing_error(mock_service_dependencies, status_callback):
    """Tests a permanent failure on a moov parsing error."""
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop, status_callback=status_callback)
    service._generate_screenshots_from_torrent = AsyncMock(side_effect=MoovParsingError("Corrupt", "hash"))
    infohash = "test_hash_bad_moov"

    await service.run()
    await service._handle_screenshot_task({'infohash': infohash})
    await service.stop()

    status_callback.assert_awaited_once()
    _, kwargs = status_callback.call_args
    assert kwargs['status'] == 'permanent_failure'
    assert isinstance(kwargs['error'], MoovParsingError)
    mock_service_dependencies['client'].remove_torrent.assert_awaited_once()

async def test_run_task_generation_raises_recoverable_error(mock_service_dependencies, status_callback):
    """Tests a recoverable failure during screenshot generation."""
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop, status_callback=status_callback)
    service._generate_screenshots_from_torrent = AsyncMock(side_effect=FrameDownloadTimeoutError("Timeout!", "hash"))
    infohash = "test_hash_frame_timeout"

    await service.run()
    await service._handle_screenshot_task({'infohash': infohash})
    await service.stop()

    status_callback.assert_awaited_once()
    _, kwargs = status_callback.call_args
    assert kwargs['status'] == 'recoverable_failure'
    assert isinstance(kwargs['error'], FrameDownloadTimeoutError)
    mock_service_dependencies['client'].remove_torrent.assert_awaited_once()
