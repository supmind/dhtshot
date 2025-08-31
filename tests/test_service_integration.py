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
