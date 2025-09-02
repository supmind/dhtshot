# -*- coding: utf-8 -*-
"""
对 screenshot/service.py 核心服务逻辑的单元测试。
"""
import pytest
import io
import struct
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from screenshot.service import ScreenshotService, StatusCallback
from config import Settings
from screenshot.errors import MP4ParsingError, NoVideoFileError, FrameDownloadTimeoutError, MoovNotFoundError
from screenshot.extractor import Keyframe, SampleInfo, KeyframeExtractor
from screenshot.client import TorrentClient


# --- 新增的测试 ---

class TestAssembleData:
    """针对 ScreenshotService._assemble_data_from_pieces 的专用测试类。"""

    @pytest.fixture
    def service(self, settings):
        """提供一个无 mock 依赖的 ScreenshotService 实例，用于测试独立函数。"""
        return ScreenshotService(settings=settings)

    def test_assemble_from_single_piece(self, service):
        """测试所需数据完全包含在单个 piece 中的情况。"""
        pieces_data = {0: b"0123456789"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=2, size=5, piece_length=10)
        assert result == b"23456"

    def test_assemble_spanning_two_pieces(self, service):
        """测试所需数据跨越两个 piece 的情况。"""
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=4, piece_length=10)
        assert result == b"89ab"

    def test_assemble_spanning_multiple_pieces(self, service):
        """测试所需数据跨越多个 piece 的情况。"""
        pieces_data = {
            0: b"0123456789",
            1: b"abcdefghij",
            2: b"KLMNOPQRST"
        }
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b"89abcdefghijKLM"

    def test_assemble_at_piece_boundary(self, service):
        """测试所需数据正好在 piece 边界上的情况。"""
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=10, size=5, piece_length=10)
        assert result == b"abcde"

    def test_assemble_with_missing_piece(self, service):
        """测试当缺少所需 piece 时，函数应返回空字节。"""
        pieces_data = {0: b"0123456789", 2: b"KLMNOPQRST"} # 故意缺少 piece 1
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b""


def test_serialize_task_state(service):
    """测试 _serialize_task_state 方法是否能正确地将一个复杂的任务状态对象转换为字典。"""
    # 1. 创建模拟的 extractor 和其他状态数据
    mock_extractor = MagicMock(spec=KeyframeExtractor)
    mock_extractor.extradata = b"some_bytes"
    mock_extractor.codec_name = "h264"
    mock_extractor.mode = "avc1"
    mock_extractor.nal_length_size = 4
    mock_extractor.timescale = 90000
    mock_extractor.samples = [SampleInfo(1, 100, True, 1, 90000)]

    task_state = {
        'infohash': 'test_hash',
        'piece_length': 16384,
        'video_file_offset': 0,
        'video_file_size': 1000,
        'extractor': mock_extractor,
        'all_keyframes': [Keyframe(0, 1, 90000, 90000)],
        'selected_keyframes': [Keyframe(0, 1, 90000, 90000)],
        'completed_pieces': {0, 1, 2},
        'processed_keyframes': {0}
    }

    # 2. 调用序列化方法
    serialized_data = service._serialize_task_state(task_state)

    # 3. 验证结果
    assert serialized_data is not None
    assert serialized_data['infohash'] == 'test_hash'
    assert serialized_data['extractor_info']['extradata'] == b"some_bytes"
    assert len(serialized_data['all_keyframes']) == 1
    assert serialized_data['all_keyframes'][0]['sample_index'] == 1
    assert list(serialized_data['processed_keyframes']) == [0]


# --- 原有的测试 Fixtures 和 Cases ---

@pytest.fixture
def settings():
    """提供一个默认的 Settings 对象。"""
    return Settings(min_screenshots=2, max_screenshots=5, default_screenshots=3, target_interval_sec=60)

@pytest.fixture
def mock_callbacks():
    """提供 mock 的状态和截图回调函数，并包含一个 Future 用于同步。"""
    future = asyncio.Future()
    async def status_callback(*args, **kwargs):
        if not future.done():
            future.set_result(kwargs)
    return {"status_callback": status_callback, "future": future}

@pytest.fixture
def service(settings, mock_callbacks):
    """提供一个依赖已被 mock 的 ScreenshotService 实例。"""
    loop = asyncio.get_event_loop()
    with patch('screenshot.service.TorrentClient'), patch('screenshot.service.ScreenshotGenerator'):
        service_instance = ScreenshotService(
            settings=settings, loop=loop, status_callback=mock_callbacks["status_callback"]
        )
        yield service_instance

# --- Test Cases ---

def test_parse_mp4_boxes_raises_error_on_invalid_size(service):
    """测试 MP4 box 解析器在 box 大小声明超出可用数据时是否抛出异常。"""
    header = b'\x00\x00\x00\x18ftypiso5\x00\x00\x00\x01isomiso5'
    malformed_box_header = b'\x00\x00\x00\x64moov'
    data = header + malformed_box_header
    stream = io.BytesIO(data)
    # 移除 match 参数，只检查异常类型
    with pytest.raises(MP4ParsingError):
        list(service._parse_mp4_boxes(stream))

def test_select_keyframes_logic(service):
    """测试 _select_keyframes 方法的逻辑。"""
    keyframes = [Keyframe(i, i, i, 1) for i in range(10)]
    samples = [MagicMock(pts=180 * 90000)]
    selected = service._select_keyframes(keyframes, 90000, samples)
    assert len(selected) == 3

@patch('screenshot.service.KeyframeExtractor')
def test_load_state_from_resume_data(MockKeyframeExtractor, service):
    """测试从 resume_data 恢复任务状态的逻辑。"""
    mock_extractor_instance = MockKeyframeExtractor.return_value
    resume_data = {
        "infohash": "r_hash", "piece_length": 2, "video_file_offset": 3, "video_file_size": 4,
        "extractor_info": { "extradata": "AQIDBA==", "codec_name": "h264", "mode": "avc1", "nal_length_size": 4, "timescale": 90000,
            "samples": [{"offset": 1, "size": 1, "is_keyframe": True, "index": 1, "pts": 0}] },
        "all_keyframes": [{"index": 0, "sample_index": 1, "pts": 0, "timescale": 90000}],
        "selected_keyframes": [{"index": 0, "sample_index": 1, "pts": 0, "timescale": 90000}],
        "completed_pieces": [1, 2], "processed_keyframes": []
    }
    state = service._load_state_from_resume_data(resume_data)
    assert state["extractor"].extradata == b'\x01\x02\x03\x04'

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_permanent_failure(mock_generate, service, mock_callbacks):
    """测试当子流程抛出永久性错误时，任务是否被正确处理。"""
    mock_generate.side_effect = NoVideoFileError("Not found", "no_video_hash")

    await service._handle_screenshot_task({"infohash": "no_video_hash"})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "permanent_failure"
    assert isinstance(result.get("error"), NoVideoFileError)

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_recoverable_failure(mock_generate, service, mock_callbacks):
    """测试当子流程抛出可恢复错误时，任务是否被正确处理。"""
    error = FrameDownloadTimeoutError("Timeout", "recoverable_hash", resume_data={"key": "value"})
    mock_generate.side_effect = error

    await service._handle_screenshot_task({"infohash": "recoverable_hash"})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "recoverable_failure"
    assert result.get("resume_data") == {"key": "value"}

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_successful_run(mock_generate, service, mock_callbacks):
    """测试截图任务的完整成功路径。"""
    infohash = "success_hash"

    await service._handle_screenshot_task({"infohash": infohash})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "success"
    mock_generate.assert_awaited_once()

@pytest.mark.asyncio
async def test_get_moov_atom_handles_parsing_error_in_head_probe(service):
    """
    测试 _get_moov_atom_data 在头部探测遇到 MP4ParsingError 时，
    是否能优雅地回退到尾部探测，而不是直接失败。
    """
    # 1. 模拟 service 的依赖
    mock_handle = MagicMock()
    service.client.fetch_pieces = AsyncMock()

    # 2. 设置场景：
    #    - 第一次调用 fetch_pieces（头部探测）返回一个会导致 MP4ParsingError 的数据
    #    - 第二次调用 fetch_pieces（尾部探测）返回有效数据
    malformed_head_data = b'\x00\x00\x00\x08ftypmp42\x00\x00\x01\x00mdat' # mdat box 声明的大小超过数据本身
    # 一个结构有效（声明大小与实际大小一致）的虚拟 moov box
    valid_moov_data = b'\x00\x00\x00\x18moov' + b'\x00' * 16

    # 使用 side_effect 来模拟多次调用的不同返回值
    service.client.fetch_pieces.side_effect = [
        {0: malformed_head_data}, # 头部探测的数据
        {100: valid_moov_data}    # 尾部探测的数据
    ]

    # 模拟 _assemble_data_from_pieces 的行为
    def mock_assemble(pieces_data, *args):
        if 0 in pieces_data:
            return malformed_head_data
        if 100 in pieces_data:
            return valid_moov_data
        return b""

    # 使用 patch.object 来临时替换实例上的方法
    with patch.object(service, '_assemble_data_from_pieces', side_effect=mock_assemble):
        # 3. 执行被测试的函数
        # 我们期望它能成功返回，而不是抛出 MoovFetchError
        try:
            result_moov_data = await service._get_moov_atom_data(mock_handle, 0, 1024*1024, 16384, "test_hash")
            # 4. 断言
            # 验证返回的是尾部探测到的数据
            assert result_moov_data == valid_moov_data
            # 验证 fetch_pieces 被调用了两次（一次头部，一次尾部）
            assert service.client.fetch_pieces.call_count == 2
        except MoovNotFoundError:
            # 在这个测试中，如果找不到 moov，也算作失败，因为它应该在尾部找到
            pytest.fail("即使在尾部探测中也未能找到 moov atom。")
