import pytest
import asyncio
from unittest.mock import patch, AsyncMock

from screenshot.service import ScreenshotService, Keyframe, SampleInfo
from screenshot.errors import FrameDownloadTimeoutError
from screenshot.config import Settings

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio

async def test_task_recovery_after_recoverable_failure(status_callback, mock_service_dependencies):
    """
    Tests that if a task fails with a FrameDownloadTimeoutError, it can be
    resumed and will process all keyframes correctly on the second attempt.
    """
    # --- Test Setup ---
    loop = asyncio.get_running_loop()
    settings = Settings(
        min_screenshots=10,
        max_screenshots=10,
        default_screenshots=10
    )
    service = ScreenshotService(
        settings=settings,
        loop=loop,
        status_callback=status_callback
    )
    client = mock_service_dependencies['client']
    handle = mock_service_dependencies['handle']
    generator = mock_service_dependencies['generator']
    service.client = client
    service.generator = generator
    mock_extractor_class = mock_service_dependencies['extractor']

    with patch('screenshot.service.ScreenshotService._get_moov_atom_data', new_callable=AsyncMock, return_value=b'mock_moov_data'), \
         patch('screenshot.service.KeyframeExtractor', mock_extractor_class):

        infohash = "test_hash_recovery"
        num_keyframes = 10

        # --- Common Mock Configuration ---
        # The handle is now provided by the fixture, so we configure it directly.
        mock_ti = handle.get_torrent_info.return_value
        mock_ti.files.return_value.file_size.return_value = 16384 * num_keyframes

        mock_keyframes = [Keyframe(index=i, sample_index=i+1, pts=i*1000, timescale=1000) for i in range(num_keyframes)]
        mock_samples = [SampleInfo(offset=i*16384, size=16384, is_keyframe=True, index=i+1, pts=i*1000) for i in range(num_keyframes)]

        extractor_instance = mock_extractor_class.return_value
        extractor_instance.keyframes = mock_keyframes
        extractor_instance.samples = mock_samples
        extractor_instance.timescale = 1000

        client.fetch_pieces.return_value = {i: f"piece_data_{i}".encode() for i in range(num_keyframes)}

        # 1. --- First Run: Simulate Immediate Timeout ---

        # This will be passed to the service's internal queue's `get` method
        # It will immediately raise a timeout, causing the processing loop to break
        # before any screenshots can be generated.
        client.subscribe_pieces.side_effect = lambda _, q: setattr(q, 'get', AsyncMock(side_effect=asyncio.TimeoutError("Simulated immediate timeout")))

        await service._handle_screenshot_task({'infohash': infohash})

        # 2. --- Assertions for First Run ---
        assert generator.generate.call_count == 0, "No screenshots should be generated on the first failing run"

        status_callback.assert_called_once()
        _, kwargs = status_callback.call_args
        assert kwargs['status'] == 'recoverable_failure'
        assert isinstance(kwargs['error'], FrameDownloadTimeoutError)

        resume_data = kwargs['resume_data']
        assert resume_data is not None, "Resume data must be provided for recoverable failures"
        # Since no progress was made, processed_keyframes should be empty
        assert len(resume_data['processed_keyframes']) == 0

        # 3. --- Second Run: Successful Recovery ---
        status_callback.reset_mock()
        generator.generate.reset_mock()

        # Configure the mock queue to deliver all 10 pieces successfully
        mock_queue_run2 = asyncio.Queue()
        for i in range(num_keyframes):
            await mock_queue_run2.put(i)

        client.subscribe_pieces.side_effect = lambda _, q: setattr(q, 'get', mock_queue_run2.get)

        await service._handle_screenshot_task({'infohash': infohash, 'resume_data': resume_data})

        # 4. --- Assertions for Second Run ---
        assert generator.generate.call_count == 10, "Should generate all 10 screenshots on the second run"

        status_callback.assert_called_once()
        _, kwargs = status_callback.call_args
        assert kwargs['status'] == 'success'

async def test_task_with_metadata_calls_get_handle_with_metadata(status_callback, mock_service_dependencies):
    """
    Tests that if a task is submitted with metadata, the service calls the
    client's get_handle method with that metadata.
    """
    # --- Test Setup ---
    loop = asyncio.get_running_loop()
    settings = Settings()
    service = ScreenshotService(
        settings=settings,
        loop=loop,
        status_callback=status_callback
    )
    client = mock_service_dependencies['client']
    handle = mock_service_dependencies['handle']
    generator = mock_service_dependencies['generator']
    service.client = client
    service.generator = generator
    mock_extractor_class = mock_service_dependencies['extractor']

    # We need to mock the torrent_info's info_hash to match our test data
    # The mock_basetorrent_info is part of the handle mock
    mock_ti = handle.get_torrent_info.return_value
    mock_ti.info_hash.return_value = "c824d516243886576b5151b148ef8648344e433a"

    # Configure the mock extractor instance
    extractor_instance = mock_extractor_class.return_value
    extractor_instance.timescale = 1000
    extractor_instance.samples = [SampleInfo(offset=0, size=100, is_keyframe=True, index=1, pts=1000)]
    extractor_instance.keyframes = [Keyframe(index=0, sample_index=1, pts=1000, timescale=1000)]

    # Mock the piece download queue
    mock_queue = asyncio.Queue()
    await mock_queue.put(0)  # The piece needed for the keyframe
    client.subscribe_pieces.side_effect = lambda _, q: setattr(q, 'get', mock_queue.get)
    client.fetch_pieces.return_value = {0: b'some_piece_data' * 1000} # Make it large enough


    with patch('screenshot.service.ScreenshotService._get_moov_atom_data', new_callable=AsyncMock, return_value=b'mock_moov_data'), \
         patch('screenshot.service.KeyframeExtractor', mock_extractor_class):

        infohash = "c824d516243886576b5151b148ef8648344e433a"
        # A minimal, valid bencoded dictionary for a torrent file
        metadata = b'd4:infod4:name4:test12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee'

        # --- Run Task ---
        # We use submit_task and then run the worker task manually
        await service.submit_task(infohash=infohash, metadata=metadata)

        # Get the task from the queue and execute it
        task_info = await service.task_queue.get()
        await service._handle_screenshot_task(task_info)

        # --- Assertions ---
        # The key assertion: was get_handle called with the correct arguments?
        client.get_handle.assert_called_once_with(infohash, metadata=metadata)

        # Also check that the task completes successfully
        status_callback.assert_called_once()
        _, kwargs = status_callback.call_args
        assert kwargs['status'] == 'success'
