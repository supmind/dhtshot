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
from screenshot.config import Settings
from screenshot.errors import MP4ParsingError, NoVideoFileError, FrameDownloadTimeoutError, MoovNotFoundError
from screenshot.extractor import Keyframe, SampleInfo
from screenshot.client import TorrentClient

# --- Fixtures ---

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
