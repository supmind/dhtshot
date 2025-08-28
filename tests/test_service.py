# -*- coding: utf-8 -*-
import pytest
import asyncio
import base64
from unittest.mock import MagicMock, AsyncMock, patch
import pytest_asyncio

from screenshot.service import (
    ScreenshotService,
    AllSuccessResult,
    FatalErrorResult,
    PartialSuccessResult,
)
from screenshot.extractor import Keyframe, SampleInfo

# --- 现有测试保持不变 ---

@pytest.fixture
def service_helpers():
    """为辅助函数测试提供一个简单的服务实例。"""
    return ScreenshotService(loop=None)

class TestServiceHelpers:
    def test_get_pieces_for_range(self, service_helpers):
        # piece_length = 1000
        assert service_helpers._get_pieces_for_range(offset_in_torrent=500, size=100, piece_length=1000) == [0]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=900, size=200, piece_length=1000) == [0, 1]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=1500, size=2000, piece_length=1000) == [1, 2, 3]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=2000, size=500, piece_length=1000) == [2]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=1500, size=500, piece_length=1000) == [1]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=1500, size=0, piece_length=1000) == []

    def test_assemble_data_from_pieces_single_piece(self, service_helpers):
        result = service_helpers._assemble_data_from_pieces({10: b'a'*500 + b'B'*100 + b'c'*424}, 10740, 100, 1024)
        assert result == b'B' * 100

    def test_assemble_data_from_pieces_multiple_pieces(self, service_helpers):
        pieces_data = {10: b'a'*1000 + b'B'*24, 11: b'C'*100 + b'd'*924}
        result = service_helpers._assemble_data_from_pieces(pieces_data, 11240, 124, 1024)
        assert result == b'B' * 24 + b'C' * 100

    def test_assemble_data_from_pieces_incomplete_data(self, service_helpers):
        pieces_data = {10: b'a'*1000 + b'B'*24}
        result = service_helpers._assemble_data_from_pieces(pieces_data, 11240, 124, 1024)
        assert result == b'B' * 24 + b'\x00' * 100

    def test_assemble_data_from_pieces_offset_and_size(self, service_helpers):
        pieces_data = {0: b"0123456789ABCDEF", 1: b"GHIJKLMNOPQRSTUV", 2: b"WXYZabcdefghijkl"}
        result = service_helpers._assemble_data_from_pieces(pieces_data, 10, 20, 16)
        assert result == b"ABCDEF" + b"GHIJKLMNOPQRST"
        assert len(result) == 20

# --- 新的业务逻辑单元测试 ---

@pytest_asyncio.fixture
async def mock_service():
    """提供一个带有模拟依赖的 ScreenshotService 实例。"""
    with patch('screenshot.service.TorrentClient') as MockClient, \
         patch('screenshot.service.H264KeyframeExtractor') as MockExtractor, \
         patch('screenshot.service.ScreenshotGenerator') as MockGenerator:

        # 配置模拟实例
        mock_client_instance = MockClient.return_value
        mock_extractor_instance = MockExtractor.return_value
        mock_generator_instance = MockGenerator.return_value

        # --- 模拟 TorrentClient 的行为 ---
        mock_handle = MagicMock()
        mock_ti = MagicMock()
        mock_fs = MagicMock()

        # 配置 torrent info 和 file storage
        mock_fs.num_files.return_value = 2
        mock_fs.file_path.side_effect = lambda i: {0: "video.mp4", 1: "other.txt"}[i]
        mock_fs.file_size.side_effect = lambda i: {0: 100000, 1: 100}[i]
        mock_fs.file_offset.return_value = 0
        mock_ti.files.return_value = mock_fs
        mock_ti.piece_length.return_value = 16384
        mock_handle.get_torrent_info.return_value = mock_ti

        mock_client_instance.add_torrent = AsyncMock(return_value=mock_handle)
        mock_client_instance.fetch_pieces = AsyncMock(return_value={})
        mock_client_instance.finished_piece_queue = AsyncMock(spec=asyncio.Queue)

        # --- 模拟 H264KeyframeExtractor 的行为 ---
        mock_keyframes = [
            Keyframe(index=0, sample_index=1, pts=0, timescale=90000),
            Keyframe(index=1, sample_index=10, pts=1000, timescale=90000),
            Keyframe(index=2, sample_index=20, pts=2000, timescale=90000),
        ]
        # 确保样本列表足够大，并且包含有效的 PTS 值
        mock_samples = [
            SampleInfo(offset=i*100, size=100, is_keyframe=(i in [0, 9, 19]), index=i+1, pts=i*1000) for i in range(30)
        ]
        mock_extractor_instance.keyframes = mock_keyframes
        mock_extractor_instance.samples = mock_samples
        mock_extractor_instance.timescale = 90000
        mock_extractor_instance.extradata = b'mock_extradata'
        mock_extractor_instance.mode = 'avc1'
        mock_extractor_instance.nal_length_size = 4
        MockExtractor.return_value = mock_extractor_instance

        # --- 模拟 ScreenshotGenerator 的行为 ---
        mock_generator_instance.generate = AsyncMock()

        # 创建服务实例
        service = ScreenshotService(loop=asyncio.get_running_loop())
        service.client = mock_client_instance
        service.generator = mock_generator_instance

        # 模拟 _get_moov_atom_data
        service._get_moov_atom_data = AsyncMock(return_value=b'mock_moov_data')

        yield service, mock_client_instance, MockExtractor, mock_generator_instance, mock_handle


@pytest.mark.asyncio
class TestServiceOrchestration:

    async def test_fatal_error_if_moov_fails(self, mock_service):
        """测试当 _get_moov_atom_data 失败时，返回 FatalErrorResult。"""
        service, _, _, _, mock_handle = mock_service
        service._get_moov_atom_data.side_effect = Exception("模拟 MOOV 获取失败")

        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")
        assert isinstance(result, FatalErrorResult)
        assert "获取 moov atom 时出错" in result.reason

    async def test_fatal_error_if_keyframes_fail(self, mock_service):
        """测试当 H264KeyframeExtractor 未能提取关键帧时，返回 FatalErrorResult。"""
        service, _, MockExtractor, _, mock_handle = mock_service
        MockExtractor.return_value.keyframes = []

        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")
        assert isinstance(result, FatalErrorResult)
        assert "无法提取任何关键帧" in result.reason

    async def test_partial_success_on_piece_timeout(self, mock_service):
        """测试当等待 piece 超时时，返回 PartialSuccessResult。"""
        service, mock_client, _, _, mock_handle = mock_service
        mock_client.finished_piece_queue.get.side_effect = asyncio.TimeoutError

        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")

        assert isinstance(result, PartialSuccessResult)
        assert "等待 pieces 超时" in result.reason
        assert result.screenshots_count == 0
        assert "moov_data_b64" in result.resume_data
        assert result.resume_data["processed_kf_indices"] == []

    async def test_resume_and_timeout_correctly(self, mock_service):
        """测试恢复任务在一个 piece 后超时，并验证结果。"""
        service, mock_client, MockExtractor, _, mock_handle = mock_service

        piece_length = mock_handle.get_torrent_info().piece_length()
        # 确保模拟的样本列表与关键帧的 sample_index 一致
        mock_samples = [SampleInfo(offset=0, size=0, is_keyframe=False, index=i+1, pts=0) for i in range(20)]
        mock_samples[0] = SampleInfo(offset=0, size=100, is_keyframe=True, index=1, pts=0) # kf 0
        mock_samples[9] = SampleInfo(offset=piece_length, size=100, is_keyframe=True, index=10, pts=1000) # kf 1
        mock_samples[19] = SampleInfo(offset=piece_length*2, size=100, is_keyframe=True, index=20, pts=2000) # kf 2
        MockExtractor.return_value.samples = mock_samples

        # 模拟的 resume_data，表明第一个关键帧 (index 0) 已处理
        resume_data = {
            "moov_data_b64": base64.b64encode(b'mock_moov_data').decode('ascii'),
            "video_file_offset": 0, "piece_length": piece_length,
            "all_kf_indices": [0, 1, 2], "processed_kf_indices": [0],
        }

        with patch.object(service, '_process_and_generate_screenshot', new_callable=AsyncMock) as mock_process:
            mock_process.return_value = True
            # 模拟 piece 1 (用于 kf 1) 的完成，然后在等待 piece 2 时超时
            mock_client.finished_piece_queue.get.side_effect = [1, asyncio.TimeoutError]

            result = await service._generate_screenshots_from_torrent(mock_handle, "infohash", resume_data)

            mock_process.assert_called_once()
            kf_index_call = mock_process.call_args[0][0].index
            assert kf_index_call == 1
            assert isinstance(result, PartialSuccessResult)
            assert result.screenshots_count == 1
            assert sorted(result.resume_data['processed_kf_indices']) == [0, 1]

    async def test_cumulative_screenshot_count_on_resume(self, mock_service):
        """
        测试在恢复并成功完成后，AllSuccessResult 是否报告了累积的截图总数。
        """
        service, mock_client, MockExtractor, _, mock_handle = mock_service

        # --- 模拟与上一个测试类似的数据 ---
        piece_length = mock_handle.get_torrent_info().piece_length()
        MockExtractor.return_value.keyframes = [
            Keyframe(index=0, sample_index=1, pts=0, timescale=90000),
            Keyframe(index=1, sample_index=10, pts=1000, timescale=90000),
            Keyframe(index=2, sample_index=20, pts=2000, timescale=90000),
        ]
        mock_samples = [SampleInfo(offset=0, size=0, is_keyframe=False, index=i+1, pts=0) for i in range(20)]
        mock_samples[0] = SampleInfo(offset=0, size=100, is_keyframe=True, index=1, pts=0)
        mock_samples[9] = SampleInfo(offset=piece_length, size=100, is_keyframe=True, index=10, pts=1000)
        mock_samples[19] = SampleInfo(offset=piece_length*2, size=100, is_keyframe=True, index=20, pts=2000)
        MockExtractor.return_value.samples = mock_samples

        # --- 运行 1: 成功处理一个 kf 后超时 ---
        with patch.object(service, '_process_and_generate_screenshot', new_callable=AsyncMock) as mock_process_run1:
            mock_process_run1.return_value = True
            mock_client.finished_piece_queue.get.side_effect = [0, asyncio.TimeoutError] # 处理 kf 0 后超时

            result1 = await service._generate_screenshots_from_torrent(mock_handle, "infohash")
            assert isinstance(result1, PartialSuccessResult)
            assert result1.screenshots_count == 1
            assert sorted(result1.resume_data['processed_kf_indices']) == [0]

        # --- 运行 2: 使用 resume_data 恢复并完成剩余部分 ---
        with patch.object(service, '_process_and_generate_screenshot', new_callable=AsyncMock) as mock_process_run2:
            mock_process_run2.return_value = True
            # 为剩余的 kf 1 和 kf 2 提供 piece
            mock_client.finished_piece_queue.get.side_effect = [1, 2, asyncio.CancelledError] # 使用 CancelledError 结束循环

            result2 = await service._generate_screenshots_from_torrent(mock_handle, "infohash", result1.resume_data)

            # THEN 最终结果应为 AllSuccessResult
            assert isinstance(result2, AllSuccessResult)
            # AND 它应该只报告在第二次运行中生成的截图数量 (这是 bug)
            # 我们期望它能报告总数 3
            assert result2.screenshots_count == 3, "最终的截图总数应该是累积的"
