# -*- coding: utf-8 -*-
"""
ScreenshotService 的单元测试。
这些测试分为两类：
1.  针对纯辅助函数（如 piece 计算）的简单、同步的测试。
2.  针对核心业务逻辑的复杂的、异步的编排测试，其中所有外部依赖项（客户端、提取器、生成器）都被模拟。
"""
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

# --- 辅助函数的单元测试 ---

@pytest.fixture
def service_helpers():
    """为辅助函数测试提供一个简单的服务实例。"""
    return ScreenshotService(loop=None)

class TestServiceHelpers:
    """测试 ScreenshotService 中的各种辅助方法。"""
    def test_get_pieces_for_range(self, service_helpers):
        """测试 _get_pieces_for_range 方法的 piece 计算逻辑。"""
        # 假设 piece_length = 1000
        assert service_helpers._get_pieces_for_range(offset_in_torrent=500, size=100, piece_length=1000) == [0]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=900, size=200, piece_length=1000) == [0, 1]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=1500, size=2000, piece_length=1000) == [1, 2, 3]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=2000, size=500, piece_length=1000) == [2]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=1500, size=500, piece_length=1000) == [1]
        assert service_helpers._get_pieces_for_range(offset_in_torrent=1500, size=0, piece_length=1000) == []

    def test_assemble_data_from_pieces_single_piece(self, service_helpers):
        """测试从单个 piece 中组装数据。"""
        result = service_helpers._assemble_data_from_pieces({10: b'a'*500 + b'B'*100 + b'c'*424}, 10740, 100, 1024)
        assert result == b'B' * 100

    def test_assemble_data_from_pieces_multiple_pieces(self, service_helpers):
        """测试从跨越多个 piece 的数据中组装。"""
        pieces_data = {10: b'a'*1000 + b'B'*24, 11: b'C'*100 + b'd'*924}
        result = service_helpers._assemble_data_from_pieces(pieces_data, 11240, 124, 1024)
        assert result == b'B' * 24 + b'C' * 100

    def test_assemble_data_from_pieces_incomplete_data(self, service_helpers):
        """测试当 piece 数据不完整时，组装的数据是否用零字节填充。"""
        pieces_data = {10: b'a'*1000 + b'B'*24}
        result = service_helpers._assemble_data_from_pieces(pieces_data, 11240, 124, 1024)
        assert result == b'B' * 24 + b'\x00' * 100

    def test_assemble_data_from_pieces_offset_and_size(self, service_helpers):
        """测试一个更复杂的偏移和大小组合。"""
        pieces_data = {0: b"0123456789ABCDEF", 1: b"GHIJKLMNOPQRSTUV", 2: b"WXYZabcdefghijkl"}
        result = service_helpers._assemble_data_from_pieces(pieces_data, 10, 20, 16)
        assert result == b"ABCDEF" + b"GHIJKLMNOPQRST"
        assert len(result) == 20

# --- 核心业务逻辑的单元测试 ---

@pytest_asyncio.fixture
async def mock_service():
    """
    提供一个带有模拟（Mocked）依赖的 ScreenshotService 实例。
    这允许我们在隔离的环境中测试服务的业务逻辑，而无需实际的网络或文件系统操作。
    """
    with patch('screenshot.service.TorrentClient') as MockClient, \
         patch('screenshot.service.H264KeyframeExtractor') as MockExtractor, \
         patch('screenshot.service.ScreenshotGenerator') as MockGenerator:

        # --- 配置模拟实例 ---
        mock_client_instance = MockClient.return_value
        mock_extractor_instance = MockExtractor.return_value
        mock_generator_instance = MockGenerator.return_value

        # --- 模拟 TorrentClient 的行为 ---
        mock_handle = MagicMock()
        mock_ti = MagicMock()
        mock_fs = MagicMock()

        # 配置 torrent info (元数据) 和 file storage (文件列表)
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
        mock_client_instance.finished_piece_queue.get = AsyncMock()

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

        # --- 创建并配置服务实例 ---
        service = ScreenshotService(loop=asyncio.get_running_loop())
        service.client = mock_client_instance
        service.generator = mock_generator_instance

        # 模拟 _get_moov_atom_data 方法，使其直接返回模拟数据，避免实际的 piece 下载逻辑
        service._get_moov_atom_data = AsyncMock(return_value=b'mock_moov_data')

        yield service, mock_client_instance, MockExtractor, mock_generator_instance, mock_handle


@pytest.mark.asyncio
class TestServiceOrchestration:
    """测试 ScreenshotService 的核心业务流程和状态管理。"""

    async def test_fatal_error_if_moov_fails(self, mock_service):
        """
        GIVEN: 一个 ScreenshotService 实例
        WHEN: 调用 _generate_screenshots_from_torrent，但 _get_moov_atom_data 抛出异常
        THEN: 应返回一个 FatalErrorResult
        """
        service, _, _, _, mock_handle = mock_service
        service._get_moov_atom_data.side_effect = Exception("模拟 MOOV 获取失败")

        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")

        assert isinstance(result, FatalErrorResult)
        assert "获取 moov atom 时出错" in result.reason

    async def test_fatal_error_if_keyframes_fail(self, mock_service):
        """
        GIVEN: 一个 ScreenshotService 实例
        WHEN: 成功获取 moov atom，但 H264KeyframeExtractor 未能提取出任何关键帧
        THEN: 应返回一个 FatalErrorResult
        """
        service, _, MockExtractor, _, mock_handle = mock_service
        MockExtractor.return_value.keyframes = []

        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")

        assert isinstance(result, FatalErrorResult)
        assert "无法提取任何关键帧" in result.reason

    async def test_fatal_error_if_no_mp4_file_found(self, mock_service):
        """
        GIVEN: 一个 torrent，但其文件列表中不包含 .mp4 文件
        WHEN: 调用 _generate_screenshots_from_torrent
        THEN: 应立即返回一个 FatalErrorResult
        """
        service, _, _, _, mock_handle = mock_service

        # 模拟一个不含 mp4 文件的文件列表
        mock_fs = MagicMock()
        mock_fs.num_files.return_value = 1
        mock_fs.file_path.return_value = "archive.zip"
        mock_fs.file_size.return_value = 100000
        mock_handle.get_torrent_info.return_value.files.return_value = mock_fs

        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")

        assert isinstance(result, FatalErrorResult)
        assert "在 torrent 中未找到 .mp4 视频文件" in result.reason

    async def test_partial_success_on_piece_timeout(self, mock_service):
        """
        GIVEN: 一个 ScreenshotService 实例
        WHEN: 在等待 piece 下载时发生 asyncio.TimeoutError
        THEN: 应返回一个包含 resume_data 的 PartialSuccessResult
        """
        service, mock_client, _, _, mock_handle = mock_service
        # 模拟 piece 队列在 get() 时立即超时
        mock_client.finished_piece_queue.get.side_effect = asyncio.TimeoutError

        result = await service._generate_screenshots_from_torrent(mock_handle, "infohash")

        assert isinstance(result, PartialSuccessResult)
        assert "等待 pieces 超时" in result.reason
        assert result.screenshots_count == 0  # 因为没有 piece 完成，所以没有截图生成
        assert "moov_data_b64" in result.resume_data
        assert result.resume_data["processed_kf_indices"] == [] # 没有处理任何关键帧

    async def test_resume_and_timeout_correctly(self, mock_service):
        """
        GIVEN: 一个已处理了部分关键帧的恢复任务
        WHEN: 在处理下一个关键帧后，等待 piece 时发生超时
        THEN: 应返回一个 PartialSuccessResult，其截图计数为1，并且 resume_data 中包含所有已处理的帧
        """
        service, mock_client, MockExtractor, _, mock_handle = mock_service

        piece_length = mock_handle.get_torrent_info().piece_length()
        # 准备 extractor 的模拟数据，确保 sample 和 keyframe 索引对应
        mock_samples = [SampleInfo(offset=0, size=0, is_keyframe=False, index=i+1, pts=0) for i in range(20)]
        mock_samples[0] = SampleInfo(offset=0, size=100, is_keyframe=True, index=1, pts=0) # kf 0 -> piece 0
        mock_samples[9] = SampleInfo(offset=piece_length, size=100, is_keyframe=True, index=10, pts=1000) # kf 1 -> piece 1
        mock_samples[19] = SampleInfo(offset=piece_length*2, size=100, is_keyframe=True, index=20, pts=2000) # kf 2 -> piece 2
        MockExtractor.return_value.samples = mock_samples

        # 准备一个 resume_data，模拟第一个关键帧 (index 0) 已经被处理
        resume_data = {
            "moov_data_b64": base64.b64encode(b'mock_moov_data').decode('ascii'),
            "video_file_offset": 0, "piece_length": piece_length,
            "all_kf_indices": [0, 1, 2],
            "processed_kf_indices": [0],
            "screenshots_generated_so_far": 1, # 已有1张截图
        }

        with patch.object(service, '_process_and_generate_screenshot', new_callable=AsyncMock) as mock_process:
            mock_process.return_value = True
            # 模拟客户端完成 piece 1 (用于 kf 1)，然后在等待 piece 2 时超时
            mock_client.finished_piece_queue.get.side_effect = [1, asyncio.TimeoutError]

            result = await service._generate_screenshots_from_torrent(mock_handle, "infohash", resume_data)

            # 验证 `_process_and_generate_screenshot` 被调用了一次，且是为 kf 1
            mock_process.assert_called_once()
            kf_index_call = mock_process.call_args[0][0].index
            assert kf_index_call == 1

            # 验证结果是部分成功
            assert isinstance(result, PartialSuccessResult)
            # 本次运行只成功生成了 1 张截图
            assert result.screenshots_count == 1
            # resume_data 中应包含之前和本次成功处理的所有关键帧
            assert sorted(result.resume_data['processed_kf_indices']) == [0, 1]
            # resume_data 中应包含累积的截图总数
            assert result.resume_data['screenshots_generated_so_far'] == 2

    async def test_cumulative_screenshot_count_on_resume(self, mock_service):
        """
        这是一个关键测试，用于验证在恢复任务并最终成功后，
        AllSuccessResult 返回的截图总数是之前所有运行的总和。
        """
        service, mock_client, MockExtractor, _, mock_handle = mock_service

        # --- GIVEN: 准备模拟数据，与上一个测试类似 ---
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

        # --- WHEN: 运行 1, 成功处理一个 kf 后超时 ---
        with patch.object(service, '_process_and_generate_screenshot', new_callable=AsyncMock) as mock_process_run1:
            mock_process_run1.return_value = True
            # 模拟 piece 0 完成，然后 get() 抛出超时异常
            mock_client.finished_piece_queue.get.side_effect = [0, asyncio.TimeoutError]

            result1 = await service._generate_screenshots_from_torrent(mock_handle, "infohash")

            # THEN: 验证第一次运行的结果
            assert isinstance(result1, PartialSuccessResult)
            assert result1.screenshots_count == 1
            assert sorted(result1.resume_data['processed_kf_indices']) == [0]
            assert result1.resume_data['screenshots_generated_so_far'] == 1

        # --- AND WHEN: 运行 2, 使用 resume_data 恢复并完成剩余部分 ---
        with patch.object(service, '_process_and_generate_screenshot', new_callable=AsyncMock) as mock_process_run2:
            mock_process_run2.return_value = True
            # 模拟为剩余的 kf 1 和 kf 2 提供 piece，然后停止
            mock_client.finished_piece_queue.get.side_effect = [1, 2, asyncio.CancelledError]

            result2 = await service._generate_screenshots_from_torrent(mock_handle, "infohash", result1.resume_data)

            # THEN: 验证最终结果
            assert isinstance(result2, AllSuccessResult), f"期望得到 AllSuccessResult，但收到了 {type(result2).__name__}"
            # 这是要修复的 bug: 最终的截图总数应该是所有运行的累积总和。
            assert result2.screenshots_count == 3, "最终的截图总数应该是累积的 (1 from run 1 + 2 from run 2)"
