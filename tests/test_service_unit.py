import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from screenshot.service import ScreenshotService, Keyframe, SampleInfo
from screenshot.config import Settings

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio

async def test_process_keyframe_pieces_success_path():
    """
    Tests that _process_keyframe_pieces correctly processes all keyframes
    when all dependencies work as expected.
    """
    # --- Test Setup ---
    loop = asyncio.get_running_loop()
    service = ScreenshotService(settings=Settings(), loop=loop)
    service.client = MagicMock()
    service.generator = MagicMock()

    # 1. --- Mock Inputs ---
    mock_handle = MagicMock()
    mock_queue = asyncio.Queue()

    num_keyframes = 3
    # Pre-fill the queue with piece indices
    for i in range(num_keyframes):
        await mock_queue.put(i)

    # Mock the complex dictionary inputs for the method
    mock_keyframes = [Keyframe(index=i, sample_index=i, pts=i*1000, timescale=1000) for i in range(num_keyframes)]
    mock_samples = [SampleInfo(offset=i*100, size=10, is_keyframe=True, index=i, pts=i*1000) for i in range(num_keyframes)]

    mock_extractor = MagicMock()
    mock_extractor.samples = mock_samples
    mock_extractor.mode = 'avc1'
    mock_extractor.nal_length_size = 4
    mock_extractor.extradata = b'extradata'

    task_state = {
        'infohash': 'test_hash',
        'extractor': mock_extractor,
        'video_file_offset': 0,
        'piece_length': 128,
        'completed_pieces': set()
    }

    # Map pieces to the keyframes they belong to
    piece_to_keyframes = {i: [i] for i in range(num_keyframes)}
    keyframe_info = {
        i: {'keyframe': mock_keyframes[i], 'needed_pieces': {i}}
        for i in range(num_keyframes)
    }

    # Configure mock return values for external calls
    service.client.fetch_pieces = AsyncMock(return_value={i: f"piece_data_{i}".encode() for i in range(num_keyframes)})
    service.generator.generate = AsyncMock()

    # 2. --- Execution ---
    processed_this_run, generation_tasks = await service._process_keyframe_pieces(
        mock_handle,
        mock_queue,
        task_state,
        keyframe_info,
        piece_to_keyframes,
        mock_keyframes  # remaining_keyframes
    )

    # 3. --- Assertions ---
    assert len(processed_this_run) == num_keyframes, "Should have processed all keyframes"
    assert len(generation_tasks) == num_keyframes, "Should have created a task for each keyframe"
    assert service.generator.generate.call_count == num_keyframes, "Generator should have been called for each keyframe"

    # Check that the 'completed_pieces' in the state was updated
    assert task_state['completed_pieces'] == set(range(num_keyframes))


async def test_process_keyframe_pieces_timeout_path():
    """
    Tests that _process_keyframe_pieces correctly handles a timeout from the
    queue and returns the partial progress.
    """
    # --- Test Setup ---
    loop = asyncio.get_running_loop()
    service = ScreenshotService(settings=Settings(), loop=loop)
    service.client = MagicMock()
    service.generator = MagicMock()

    # 1. --- Mock Inputs ---
    mock_handle = MagicMock()
    mock_queue = asyncio.Queue()

    num_keyframes = 5
    keyframes_to_succeed = 2

    # Simulate a timeout after 2 successful pieces
    for i in range(keyframes_to_succeed):
        await mock_queue.put(i)

    async def queue_get_side_effect(*args, **kwargs):
        if not mock_queue.empty():
            return mock_queue.get_nowait()  # Consume from the pre-filled queue
        raise asyncio.TimeoutError("Simulated timeout")

    # The method under test will call our mock `get`
    mock_queue.get = AsyncMock(side_effect=queue_get_side_effect)

    # Mock the complex dictionary inputs for the method
    mock_keyframes = [Keyframe(index=i, sample_index=i, pts=i*1000, timescale=1000) for i in range(num_keyframes)]
    mock_samples = [SampleInfo(offset=i*100, size=10, is_keyframe=True, index=i, pts=i*1000) for i in range(num_keyframes)]

    mock_extractor = MagicMock()
    mock_extractor.samples = mock_samples
    mock_extractor.mode = 'avc1'
    mock_extractor.nal_length_size = 4
    mock_extractor.extradata = b'extradata'

    task_state = {
        'infohash': 'test_hash_timeout',
        'extractor': mock_extractor,
        'video_file_offset': 0,
        'piece_length': 128,
        'completed_pieces': set()
    }

    piece_to_keyframes = {i: [i] for i in range(num_keyframes)}
    keyframe_info = {
        i: {'keyframe': mock_keyframes[i], 'needed_pieces': {i}}
        for i in range(num_keyframes)
    }

    service.client.fetch_pieces = AsyncMock(return_value={i: f"piece_data_{i}".encode() for i in range(num_keyframes)})
    service.generator.generate = AsyncMock()

    # 2. --- Execution ---
    processed_this_run, generation_tasks = await service._process_keyframe_pieces(
        mock_handle,
        mock_queue,
        task_state,
        keyframe_info,
        piece_to_keyframes,
        mock_keyframes
    )

    # 3. --- Assertions ---
    assert len(processed_this_run) == keyframes_to_succeed, "Should have processed only the keyframes before timeout"
    assert len(generation_tasks) == keyframes_to_succeed, "Should have created tasks only for the successful keyframes"
    assert service.generator.generate.call_count == keyframes_to_succeed


async def test_process_keyframe_pieces_data_corruption():
    """
    Tests that _process_keyframe_pieces correctly skips a keyframe if its
    data is found to be corrupt (_assemble_data_from_pieces returns empty).
    """
    # --- Test Setup ---
    loop = asyncio.get_running_loop()
    service = ScreenshotService(settings=Settings(), loop=loop)
    service.client = MagicMock()
    service.generator = MagicMock()

    # 1. --- Mock Inputs ---
    mock_handle = MagicMock()
    mock_queue = asyncio.Queue()

    num_keyframes = 3
    corrupted_kf_index = 1  # We'll make the middle keyframe corrupt
    for i in range(num_keyframes):
        await mock_queue.put(i)

    mock_keyframes = [Keyframe(index=i, sample_index=i, pts=i*1000, timescale=1000) for i in range(num_keyframes)]
    mock_samples = [SampleInfo(offset=i*100, size=10, is_keyframe=True, index=i, pts=i*1000) for i in range(num_keyframes)]

    mock_extractor = MagicMock()
    mock_extractor.samples = mock_samples
    mock_extractor.mode = 'avc1'
    mock_extractor.nal_length_size = 4
    mock_extractor.extradata = b'extradata'

    task_state = {
        'infohash': 'test_hash_corruption',
        'extractor': mock_extractor,
        'video_file_offset': 0,
        'piece_length': 128,
        'completed_pieces': set()
    }

    piece_to_keyframes = {i: [i] for i in range(num_keyframes)}
    keyframe_info = {
        i: {'keyframe': mock_keyframes[i], 'needed_pieces': {i}}
        for i in range(num_keyframes)
    }

    # --- Mock the data corruption ---
    # --- Mock the data corruption ---
    # We mock _assemble_data_from_pieces to return a sequence of values.
    # The second value is empty bytes, simulating a failure for that keyframe.
    service._assemble_data_from_pieces = MagicMock(side_effect=[
        b'a' * 10,  # Corresponds to kf 0
        b"",        # Corresponds to kf 1 (the corrupted one)
        b'a' * 10,  # Corresponds to kf 2
    ])
    service.client.fetch_pieces = AsyncMock(return_value={}) # Return value doesn't matter as assemble is mocked
    service.generator.generate = AsyncMock()

    # 2. --- Execution ---
    processed_this_run, generation_tasks = await service._process_keyframe_pieces(
        mock_handle,
        mock_queue,
        task_state,
        keyframe_info,
        piece_to_keyframes,
        mock_keyframes
    )

    # 3. --- Assertions ---
    # The corrupted frame is still considered "processed" by being skipped
    assert len(processed_this_run) == num_keyframes
    # But a generation task should not have been created for it
    assert len(generation_tasks) == num_keyframes - 1
    assert service.generator.generate.call_count == num_keyframes - 1

    # Check that generate was called for the correct keyframes
    good_kf_indices = {0, 2}
    called_kf_timestamps = {call.kwargs['timestamp_str'] for call in service.generator.generate.call_args_list}

    # Recreate the expected timestamps for the good keyframes
    expected_timestamps = set()
    for i in good_kf_indices:
        kf = mock_keyframes[i]
        ts_sec = kf.pts / kf.timescale
        m, s = divmod(ts_sec, 60)
        h, m = divmod(m, 60)
        expected_timestamps.add(f"{int(h):02d}-{int(m):02d}-{int(s):02d}")

    assert called_kf_timestamps == expected_timestamps
