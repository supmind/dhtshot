import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from screenshot.service import ScreenshotService
from screenshot.errors import NoVideoFileError, FrameDownloadTimeoutError
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
    测试 ScreenshotService 的主要任务处理和编排逻辑，
    并模拟了其依赖项。
    """
    async def test_handles_no_video_file_error(self, service, caplog):
        """
        测试对于没有 .mp4 文件的 torrent，能够优雅地处理，
        并记录一个 NoVideoFileError。
        """
        service.loop = asyncio.get_running_loop()
        infohash = "no_video_file_hash"

        # 模拟 torrent_info 以返回一个非 mp4 文件
        mock_ti = MagicMock()
        mock_ti.files.return_value.num_files.return_value = 1
        mock_ti.files.return_value.file_path.return_value = "archive.zip"
        mock_ti.files.return_value.file_size.return_value = 1000

        mock_handle = MagicMock()
        mock_handle.is_valid.return_value = True
        mock_handle.get_torrent_info.return_value = mock_ti

        # 配置客户端模拟
        service.client.add_torrent = AsyncMock(side_effect=NoVideoFileError("在 torrent 中没有找到 .mp4 文件。", infohash))
        service.client.remove_torrent = AsyncMock()

        # 运行任务处理器
        await service._handle_screenshot_task({'infohash': infohash, 'resume_data': None})

        # 断言
        service.client.add_torrent.assert_called_once_with(infohash)
        # 确保我们从未尝试生成截图
        service.generator.generate.assert_not_called()
        # 确保清理逻辑仍然被调用
        service.client.remove_torrent.assert_not_called() # 句柄获取失败，不应调用移除

        # 检查特定的错误是否已正确记录
        assert "在 torrent 中没有找到 .mp4 文件。" in caplog.text
        assert f"任务 {infohash} 因永久性错误而失败" in caplog.text

    async def test_handles_timeout_and_creates_resume_data(self, service, caplog):
        """
        测试下载超时会正确地引发一个 FrameDownloadTimeoutError
        并在日志异常中包含 resume_data。
        """
        service.loop = asyncio.get_running_loop()
        infohash = "timeout_hash"

        # --- 模拟一个看起来有效的 torrent ---
        mock_ti = MagicMock()
        mock_ti.files.return_value.num_files.return_value = 1
        mock_ti.files.return_value.file_path.return_value = "video.mp4"
        mock_ti.files.return_value.file_size.return_value = 50000
        mock_ti.piece_length.return_value = 16384

        mock_handle = MagicMock()
        mock_handle.is_valid.return_value = True
        mock_handle.get_torrent_info.return_value = mock_ti

        service.client.add_torrent = AsyncMock(return_value=mock_handle)
        service.client.remove_torrent = AsyncMock()

        # --- 模拟 MOOV 和 Extractor 数据的查找 ---
        service._get_moov_atom_data = AsyncMock(return_value=b"dummy_moov_data")

        # 模拟 H264KeyframeExtractor 类本身
        with pytest.MonkeyPatch.context() as m:
            mock_extractor_instance = MagicMock()
            mock_extractor_instance.keyframes = [Keyframe(index=i, sample_index=i+1, pts=i*1000, timescale=1000) for i in range(10)]
            mock_extractor_instance.samples = [SampleInfo(offset=i*100, size=50, is_keyframe=True, index=i+1, pts=i*1000) for i in range(10)]
            mock_extractor_instance.timescale = 1000
            m.setattr("screenshot.service.H264KeyframeExtractor", MagicMock(return_value=mock_extractor_instance))

            # --- 模拟 piece 队列以模拟超时 ---
            service.client.finished_piece_queue.get = AsyncMock(side_effect=asyncio.TimeoutError)

            # 运行任务处理器
            await service._handle_screenshot_task({'infohash': infohash, 'resume_data': None})

        # --- 断言 ---
        # 它应该尝试添加然后移除 torrent
        service.client.add_torrent.assert_called_once_with(infohash)
        service.client.remove_torrent.assert_called_once_with(mock_handle)

        # 不应有任何帧被生成
        service.generator.generate.assert_not_called()

        # 关键断言：检查日志中是否有可恢复的错误消息
        assert "等待 piece 超时" in caplog.text
        assert f"任务 {infohash} 因可恢复的错误而失败" in caplog.text


@pytest.fixture
def service_with_callback():
    """提供一个带有模拟 status_callback 的 ScreenshotService 实例。"""
    service = ScreenshotService()
    service.client = MagicMock()
    service.generator = MagicMock()
    service.status_callback = AsyncMock()
    return service

@pytest.mark.asyncio
class TestStatusCallback:
    """Tests the status_callback functionality of the ScreenshotService."""

    async def test_success_callback(self, service_with_callback):
        """Tests that the callback is called on successful task completion."""
        service = service_with_callback
        infohash = "success_hash"

        # Mock the entire generation process to succeed
        service._generate_screenshots_from_torrent = AsyncMock()
        mock_handle = MagicMock()
        mock_handle.is_valid.return_value = True
        service.client.add_torrent = AsyncMock(return_value=mock_handle)
        service.client.remove_torrent = AsyncMock()

        # Run the task handler
        await service._handle_screenshot_task({'infohash': infohash})

        # Assertion
        service.status_callback.assert_called_once()
        call_args = service.status_callback.call_args[1]
        assert call_args['status'] == 'success'
        assert call_args['infohash'] == infohash

    async def test_permanent_failure_callback(self, service_with_callback):
        """Tests that the callback is called on a permanent, non-resumable error."""
        service = service_with_callback
        infohash = "permanent_fail_hash"

        # Mock add_torrent to raise a NoVideoFileError
        service.client.add_torrent = AsyncMock(side_effect=NoVideoFileError("No MP4 found", infohash))
        service.client.remove_torrent = AsyncMock()

        # Run the task handler
        await service._handle_screenshot_task({'infohash': infohash})

        # Assertion
        service.status_callback.assert_called_once()
        call_args = service.status_callback.call_args[1]
        assert call_args['status'] == 'permanent_failure'
        assert call_args['infohash'] == infohash
        assert "No MP4 found" in call_args['message']

    async def test_recoverable_failure_callback(self, service_with_callback):
        """Tests that the callback is called on a recoverable error with resume_data."""
        service = service_with_callback
        infohash = "recoverable_fail_hash"
        resume_data = {"some": "data"}

        # Mock the generation process to raise a resumable error
        error = FrameDownloadTimeoutError("Timeout", infohash, resume_data=resume_data)
        service._generate_screenshots_from_torrent = AsyncMock(side_effect=error)
        mock_handle = MagicMock()
        mock_handle.is_valid.return_value = True
        service.client.add_torrent = AsyncMock(return_value=mock_handle)
        service.client.remove_torrent = AsyncMock()

        # Run the task handler
        await service._handle_screenshot_task({'infohash': infohash})

        # Assertion
        service.status_callback.assert_called_once()
        call_args = service.status_callback.call_args[1]
        assert call_args['status'] == 'recoverable_failure'
        assert call_args['infohash'] == infohash
        assert call_args['resume_data'] == resume_data
