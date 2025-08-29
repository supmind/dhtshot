import pytest
from unittest.mock import MagicMock
from screenshot.service import ScreenshotService
from screenshot.extractor import H264KeyframeExtractor, Keyframe, SampleInfo

@pytest.fixture
def service():
    # We don't need a running loop or a real client for these unit tests
    # We can mock them if a specific test needs them.
    service = ScreenshotService(loop=None)
    service.client = MagicMock()
    service.generator = MagicMock()
    return service

class TestServiceHelpers:
    def test_get_pieces_for_range(self, service):
        assert service._get_pieces_for_range(offset_in_torrent=500, size=100, piece_length=1000) == [0]
        assert service._get_pieces_for_range(offset_in_torrent=900, size=200, piece_length=1000) == [0, 1]
        assert service._get_pieces_for_range(offset_in_torrent=1500, size=2000, piece_length=1000) == [1, 2, 3]
        assert service._get_pieces_for_range(offset_in_torrent=2000, size=500, piece_length=1000) == [2]
        assert service._get_pieces_for_range(offset_in_torrent=1500, size=500, piece_length=1000) == [1]
        assert service._get_pieces_for_range(offset_in_torrent=1500, size=0, piece_length=1000) == []

    def test_assemble_data_from_pieces(self, service):
        piece_length = 1024
        pieces_data = { 10: b'a' * 1000 + b'B' * 24, 11: b'C' * 100 + b'd' * 924 }
        result = service._assemble_data_from_pieces(pieces_data, 11240, 124, piece_length)
        assert result == b'B' * 24 + b'C' * 100

    def test_assemble_data_from_pieces_incomplete(self, service):
        piece_length = 1024
        pieces_data = { 10: b'a' * 1000 + b'B' * 24 }
        result = service._assemble_data_from_pieces(pieces_data, 11240, 124, piece_length)
        assert result == b'B' * 24 + b'\x00' * 100

@pytest.fixture
def sample_task_state():
    """Provides a sample, valid task state dictionary for testing serialization."""
    mock_extractor = H264KeyframeExtractor(moov_data=None)
    mock_extractor.extradata = b'extradata'
    mock_extractor.mode = 'avc1'
    mock_extractor.nal_length_size = 4
    mock_extractor.timescale = 90000
    mock_extractor.samples = [
        SampleInfo(offset=100, size=50, is_keyframe=True, index=1, pts=0),
        SampleInfo(offset=150, size=20, is_keyframe=False, index=2, pts=3000),
        SampleInfo(offset=170, size=60, is_keyframe=True, index=3, pts=6000),
    ]
    all_keyframes = [
        Keyframe(index=0, sample_index=1, pts=0, timescale=90000),
        Keyframe(index=1, sample_index=3, pts=6000, timescale=90000),
    ]
    mock_extractor.keyframes = all_keyframes

    return {
        "infohash": "test_hash",
        "piece_length": 16384,
        "video_file_offset": 12345,
        "video_file_size": 99999,
        "extractor": mock_extractor,
        "all_keyframes": all_keyframes,
        "selected_keyframes": [all_keyframes[1]], # select the second keyframe
        "completed_pieces": {10, 11},
        "processed_keyframes": {0}, # processed the first keyframe
    }

class TestServiceResumability:
    def test_serialize_task_state(self, service, sample_task_state):
        """Tests that the service can correctly serialize a state object to a dict."""
        serialized = service._serialize_task_state(sample_task_state)

        assert serialized['infohash'] == "test_hash"
        assert serialized['piece_length'] == 16384
        assert serialized['video_file_offset'] == 12345
        assert serialized['video_file_size'] == 99999

        # Check extractor info
        ext_info = serialized['extractor_info']
        assert ext_info['extradata'] == b'extradata'
        assert ext_info['timescale'] == 90000
        assert len(ext_info['samples']) == 3
        assert ext_info['samples'][0]['offset'] == 100

        # Check keyframes
        assert len(serialized['all_keyframes']) == 2
        assert serialized['all_keyframes'][1]['sample_index'] == 3
        assert len(serialized['selected_keyframes']) == 1
        assert serialized['selected_keyframes'][0]['sample_index'] == 3

        # Check progress
        assert sorted(serialized['completed_pieces']) == [10, 11]
        assert sorted(serialized['processed_keyframes']) == [0]

    def test_load_state_from_resume_data(self, service, sample_task_state):
        """Tests that the service can correctly deserialize a dict into a state object."""
        # First, serialize the state to get a valid resume_data dict
        resume_data = service._serialize_task_state(sample_task_state)

        # Now, load it back
        loaded_state = service._load_state_from_resume_data(resume_data)

        assert loaded_state['infohash'] == "test_hash"
        assert loaded_state['piece_length'] == 16384
        assert loaded_state['video_file_offset'] == 12345

        # Check extractor
        extractor = loaded_state['extractor']
        assert isinstance(extractor, H264KeyframeExtractor)
        assert extractor.timescale == 90000
        assert extractor.extradata == b'extradata'
        assert len(extractor.samples) == 3
        assert extractor.samples[1].pts == 3000
        assert len(extractor.keyframes) == 2 # Ensure full keyframe list is restored

        # Check keyframes
        assert len(loaded_state['all_keyframes']) == 2
        assert loaded_state['all_keyframes'][0].pts == 0
        assert len(loaded_state['selected_keyframes']) == 1
        assert loaded_state['selected_keyframes'][0].sample_index == 3

        # Check progress
        assert loaded_state['completed_pieces'] == {10, 11}
        assert loaded_state['processed_keyframes'] == {0}

@pytest.mark.asyncio
class TestServiceOrchestration:
    """
    Tests the main task handling and orchestration logic of the ScreenshotService,
    with dependencies mocked out.
    """
    async def test_handles_no_video_file_error(self, service, event_loop, caplog):
        """
        Tests that a torrent with no .mp4 file is handled gracefully,
        raising a NoVideoFileError.
        """
        service.loop = event_loop
        infohash = "no_video_file_hash"

        # Mock torrent_info to return a non-mp4 file
        mock_ti = MagicMock()
        mock_ti.files.return_value.num_files.return_value = 1
        mock_ti.files.return_value.file_path.return_value = "archive.zip"
        mock_ti.files.return_value.file_size.return_value = 1000

        mock_handle = MagicMock()
        mock_handle.is_valid.return_value = True
        mock_handle.get_torrent_info.return_value = mock_ti

        # Configure client mock
        service.client.add_torrent = asyncio.coroutine(MagicMock(return_value=mock_handle))
        service.client.remove_torrent = asyncio.coroutine(MagicMock())

        # Run the task handler
        await service._handle_screenshot_task({'infohash': infohash, 'resume_data': None})

        # Assertions
        service.client.add_torrent.assert_called_once_with(infohash)
        # Ensure we never even tried to generate a screenshot
        service.generator.generate.assert_not_called()
        # Ensure cleanup was still called
        service.client.remove_torrent.assert_called_once_with(mock_handle)

        # Check that the specific error was logged correctly
        assert "No .mp4 file found in torrent" in caplog.text
        assert f"Task failed for {infohash} with a known, non-resumable error" in caplog.text

    async def test_handles_timeout_and_creates_resume_data(self, service, event_loop, caplog):
        """
        Tests that a download timeout correctly raises a FrameDownloadTimeoutError
        and includes resume_data in the logged exception.
        """
        service.loop = event_loop
        infohash = "timeout_hash"

        # --- Mocks for a seemingly valid torrent ---
        mock_ti = MagicMock()
        mock_ti.files.return_value.num_files.return_value = 1
        mock_ti.files.return_value.file_path.return_value = "video.mp4"
        mock_ti.files.return_value.file_size.return_value = 50000
        mock_ti.piece_length.return_value = 16384

        mock_handle = MagicMock()
        mock_handle.is_valid.return_value = True
        mock_handle.get_torrent_info.return_value = mock_ti

        service.client.add_torrent = asyncio.coroutine(MagicMock(return_value=mock_handle))
        service.client.remove_torrent = asyncio.coroutine(MagicMock())

        # --- Mock finding MOOV and Extractor data ---
        service._get_moov_atom_data = asyncio.coroutine(MagicMock(return_value=b"dummy_moov_data"))

        # Mock the H264KeyframeExtractor class itself
        with pytest.MonkeyPatch.context() as m:
            mock_extractor_instance = MagicMock()
            mock_extractor_instance.keyframes = [Keyframe(index=i, sample_index=i+1, pts=i*1000, timescale=1000) for i in range(10)]
            mock_extractor_instance.samples = [SampleInfo(offset=i*100, size=50, is_keyframe=True, index=i+1, pts=i*1000) for i in range(10)]
            mock_extractor_instance.timescale = 1000
            m.setattr("screenshot.service.H264KeyframeExtractor", MagicMock(return_value=mock_extractor_instance))

            # --- Mock the piece queue to simulate a timeout ---
            service.client.finished_piece_queue.get = asyncio.coroutine(MagicMock(side_effect=asyncio.TimeoutError))

            # Run the task handler
            await service._handle_screenshot_task({'infohash': infohash, 'resume_data': None})

        # --- Assertions ---
        # It should have tried to add and then remove the torrent
        service.client.add_torrent.assert_called_once_with(infohash)
        service.client.remove_torrent.assert_called_once_with(mock_handle)

        # No frames should have been generated
        service.generator.generate.assert_not_called()

        # The key assertion: check the log for the resumable error message
        assert "Timeout waiting for pieces" in caplog.text
        assert f"Task failed for {infohash} with a resumable error" in caplog.text
        assert "Resume data is available" in caplog.text
