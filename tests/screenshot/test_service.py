# -*- coding: utf-8 -*-
import pytest
import io
import struct
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from screenshot.service import ScreenshotService
from screenshot.config import Settings
from screenshot.errors import MP4ParsingError, NoVideoFileError, FrameDownloadTimeoutError
from screenshot.extractor import Keyframe, SampleInfo, KeyframeExtractor
from screenshot.client import TorrentClient

# --- Fixtures ---

@pytest.fixture
def settings():
    """Provides a default Settings object for tests."""
    return Settings()

@pytest.fixture
def mock_callbacks():
    """Provides mock status and screenshot callbacks."""
    return {
        "status_callback": AsyncMock(),
        "screenshot_callback": AsyncMock()
    }

@pytest.fixture
def mock_torrent_client():
    """Provides a mock TorrentClient."""
    client = MagicMock(spec=TorrentClient)
    client.start = AsyncMock()
    client.stop = AsyncMock()
    client.get_handle = MagicMock()
    # get_handle is an async context manager, so its __aenter__ needs to be an AsyncMock
    client.get_handle.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
    client.get_handle.return_value.__aexit__ = AsyncMock()
    return client

@pytest.fixture
def service(settings, mock_torrent_client, mock_callbacks):
    """Provides a ScreenshotService instance with mocked dependencies."""
    # We need a real event loop for some of the service's internal workings
    loop = asyncio.get_event_loop()

    # Patch the ScreenshotGenerator within the service module
    with patch('screenshot.service.ScreenshotGenerator') as MockGenerator:
        # The service instantiates its own generator, so we patch the class
        # and then can access the instance it creates.
        service_instance = ScreenshotService(
            settings=settings,
            loop=loop,
            client=mock_torrent_client,
            **mock_callbacks
        )
        # Store the mock instance for later assertions
        service_instance.generator = MockGenerator.return_value
        service_instance.generator.generate = AsyncMock()
        yield service_instance


# --- Test Cases ---

def test_parse_mp4_boxes_raises_error_on_invalid_size(service):
    """
    Tests that the MP4 box parser raises an MP4ParsingError if a box declares
    a size that is larger than the available data buffer.
    This test was existing and is preserved.
    """
    header = b'\x00\x00\x00\x18ftypiso5\x00\x00\x00\x01isomiso5'
    malformed_box_header = b'\x00\x00\x00\x64moov' # Size 100 (0x64)
    data = header + malformed_box_header
    stream = io.BytesIO(data)

    with pytest.raises(MP4ParsingError, match="超出了可用数据的范围"):
        list(service._parse_mp4_boxes(stream))

@pytest.mark.asyncio
async def test_handle_task_no_video_file(service, mock_torrent_client, mock_callbacks):
    """
    Tests that a task fails permanently if no video file is found in the torrent.
    """
    infohash = "no_video_hash"
    task_info = {"infohash": infohash}

    # Mock the torrent_info to have no .mp4 files
    mock_ti = MagicMock()
    mock_ti.files.return_value.num_files.return_value = 0
    mock_torrent_client.get_handle.return_value.__aenter__.return_value.get_torrent_info.return_value = mock_ti

    await service._handle_screenshot_task(task_info)

    # Verify that the status callback was called with a permanent failure
    mock_callbacks["status_callback"].assert_called_once()
    args, kwargs = mock_callbacks["status_callback"].call_args
    assert kwargs.get("status") == "permanent_failure"
    assert isinstance(kwargs.get("error"), NoVideoFileError)
    assert kwargs.get("infohash") == infohash

@pytest.mark.asyncio
@patch('screenshot.service.KeyframeExtractor')
async def test_handle_task_successful_run(MockKeyframeExtractor, service, mock_torrent_client, mock_callbacks):
    """
    Tests the full, successful execution path of a screenshot task.
    """
    infohash = "success_hash"
    task_info = {"infohash": infohash}

    # --- Mock Setup ---
    # Mock torrent_info
    mock_ti = MagicMock()
    mock_ti.files.return_value.num_files.return_value = 1
    mock_ti.files.return_value.file_path.return_value = "video.mp4"
    mock_ti.files.return_value.file_size.return_value = 1000
    mock_ti.files.return_value.file_offset.return_value = 0
    mock_torrent_client.get_handle.return_value.__aenter__.return_value.get_torrent_info.return_value = mock_ti

    # Mock _get_moov_atom_data to return some dummy data
    service._get_moov_atom_data = AsyncMock(return_value=b'moov_data')

    # Mock KeyframeExtractor instance
    mock_extractor = MockKeyframeExtractor.return_value
    mock_extractor.keyframes = [Keyframe(index=0, sample_index=1, pts=0, timescale=90000)]
    mock_extractor.samples = [SampleInfo(offset=100, size=50, is_keyframe=True, index=1, pts=0)]
    mock_extractor.timescale = 90000

    # Mock _process_keyframe_pieces to simulate successful generation
    mock_generation_task = asyncio.create_task(asyncio.sleep(0))
    service._process_keyframe_pieces = AsyncMock(return_value=({0}, {0: mock_generation_task}))

    # --- Execution ---
    await service._handle_screenshot_task(task_info)
    await asyncio.sleep(0.01) # allow gather to complete

    # --- Assertions ---
    # Assert _get_moov_atom_data was called
    service._get_moov_atom_data.assert_called_once()

    # Assert KeyframeExtractor was instantiated with moov data
    MockKeyframeExtractor.assert_called_with(b'moov_data')

    # Assert _process_keyframe_pieces was called
    service._process_keyframe_pieces.assert_called_once()

    # Assert the final status was 'success'
    mock_callbacks["status_callback"].assert_called_once_with(
        status='success',
        infohash=infohash,
        message='任务成功完成。'
    )

@pytest.mark.asyncio
async def test_handle_task_recoverable_failure(service, mock_torrent_client, mock_callbacks):
    """
    Tests that a recoverable failure is reported correctly, with resume_data.
    """
    infohash = "recoverable_hash"
    task_info = {"infohash": infohash}

    # Make a part of the process fail with a recoverable error
    service._generate_screenshots_from_torrent = AsyncMock(
        side_effect=FrameDownloadTimeoutError("Timeout", infohash, resume_data={"some": "data"})
    )

    await service._handle_screenshot_task(task_info)

    mock_callbacks["status_callback"].assert_called_once()
    args, kwargs = mock_callbacks["status_callback"].call_args
    assert kwargs.get("status") == "recoverable_failure"
    assert kwargs.get("infohash") == infohash
    assert kwargs.get("resume_data") == {"some": "data"}
    assert isinstance(kwargs.get("error"), FrameDownloadTimeoutError)
