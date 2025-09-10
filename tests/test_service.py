# -*- coding: utf-8 -*-
"""
对 screenshot/service.py 的单元测试。

此文件将专注于测试 ScreenshotService 的核心业务逻辑和错误处理能力。
由于 ScreenshotService 是一个高度集成的协调器，这些测试将严重依赖 mock
来隔离服务并独立地验证其行为。
"""
import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path

# 待测试的模块和类
from screenshot.service import ScreenshotService
from screenshot.errors import NoVideoFileError, MoovNotFoundError, MoovParsingError, FrameDownloadTimeoutError, DecodingError, MoovFetchError
from screenshot.extractor import Keyframe, SampleInfo, MP4Extractor
from config import Settings
from tests.test_extractor import get_mp4_moov_box
from tests.test_generator import generate_video_file, TEST_VIDEO_DIR

# --- 测试固件 (Fixtures) ---

@pytest.fixture(scope="module")
def multi_keyframe_video_file() -> Path:
    """
    生成一个包含多个关键帧的测试视频文件，以确保截图选择逻辑可以被充分测试。
    我们通过设置一个较小的 GOP (Group of Pictures) 大小来强制生成更多关键帧。
    """
    file_path = TEST_VIDEO_DIR / "multi_keyframe_video.mp4"
    if not file_path.exists():
        generate_video_file(
            file_path,
            codec='libx264',
            # 设置 FPS=30, 总帧数=60 (2秒), GOP size=10, 应该能生成 6 个关键帧
            options={'g': '10'},
            total_frames=60,
            fps=30
        )
    return file_path

@pytest.fixture(scope="module")
def real_extractor_data(multi_keyframe_video_file: Path) -> MP4Extractor:
    """
    通过在真实的、包含多个关键帧的视频文件上运行真实的 MP4Extractor，
    提供一个包含真实 keyframes, samples 等数据的 extractor 实例。
    """
    moov_data = get_mp4_moov_box(multi_keyframe_video_file)
    extractor = MP4Extractor(moov_data)
    return extractor

@pytest.fixture
def mock_settings() -> Settings:
    """提供一个包含测试配置的 Settings 对象。"""
    return Settings(
        num_workers=2,
        default_screenshots=3,
        min_screenshots=2,
        max_screenshots=10,
        target_interval_sec=1, # Pydantic expects an int, changed from 0.5
        moov_probe_timeout=5.0,
        piece_fetch_timeout=5.0,
        piece_queue_timeout=5.0,
        mkv_default_fetch_size=1024*1024,
        output_dir="/tmp/screenshots"
    )

@pytest.fixture
def mock_callbacks() -> dict:
    """提供一组 mock 回调函数，用于断言它们是否被正确调用。"""
    return {
        "status_callback": AsyncMock(),
        "screenshot_callback": AsyncMock(),
        "details_callback": AsyncMock(),
    }

@pytest.fixture
def service(mock_settings, mock_callbacks) -> ScreenshotService:
    """
    提供一个 ScreenshotService 实例，其依赖项 (如 TorrentClient) 已被 mock。
    """
    mock_client = MagicMock()
    mock_cm = AsyncMock()
    mock_client.get_handle.return_value = mock_cm
    mock_handle = AsyncMock()
    mock_cm.__aenter__.return_value = mock_handle

    loop = asyncio.get_event_loop()
    service_instance = ScreenshotService(
        settings=mock_settings,
        loop=loop,
        client=mock_client,
        **mock_callbacks
    )
    service_instance.mock_handle = mock_handle
    return service_instance

# --- 测试用例 ---

@pytest.mark.asyncio
async def test_service_initialization(service: ScreenshotService, mock_settings):
    """测试服务是否能被正确初始化。"""
    assert service.settings == mock_settings
    assert service._running is False
    assert service.get_queue_size() == 0
    assert service.client is not None
    assert service.generator is not None

@pytest.mark.asyncio
@patch('screenshot.service.MP4Extractor')
@patch('screenshot.service.ScreenshotGenerator')
async def test_handle_screenshot_task_success_path(
    MockGenerator, MockExtractor, service: ScreenshotService, mock_callbacks, real_extractor_data: MP4Extractor
):
    """
    测试 _handle_screenshot_task 在理想情况下的成功路径。
    此测试现在使用一个专门生成的、包含多个关键帧的视频文件来驱动模拟。
    """
    infohash_hex = "a" * 40
    mock_handle = service.mock_handle

    mock_ti = MagicMock()
    mock_ti.piece_length.return_value = 16384
    mock_files = MagicMock()
    mock_files.num_files.return_value = 1
    mock_files.file_path.return_value = "video.mp4"
    mock_files.file_size.return_value = 100000
    mock_files.file_offset.return_value = 0
    mock_ti.files.return_value = mock_files
    mock_handle.get_torrent_info = MagicMock(return_value=mock_ti)

    mock_extractor_instance = MockExtractor.return_value
    mock_extractor_instance.keyframes = real_extractor_data.keyframes
    mock_extractor_instance.samples = real_extractor_data.samples
    mock_extractor_instance.duration_pts = real_extractor_data.duration_pts
    mock_extractor_instance.timescale = real_extractor_data.timescale
    mock_extractor_instance.mode = real_extractor_data.mode
    mock_extractor_instance.nal_length_size = real_extractor_data.nal_length_size

    mock_generator_instance = MockGenerator.return_value
    mock_generator_instance.generate = AsyncMock()
    service.generator = mock_generator_instance

    service._get_moov_atom_data = AsyncMock(return_value=b'moov_data')

    async def fake_process_pieces(*args, **kwargs):
        remaining_keyframes = args[5]
        kf_indices = {kf.index for kf in remaining_keyframes}
        mock_tasks = {
            kf.index: asyncio.create_task(
                service.generator.generate("h264", b"extradata", b"packetdata", infohash_hex, str(kf.index))
            ) for kf in remaining_keyframes
        }
        return kf_indices, mock_tasks

    service._process_keyframe_pieces = AsyncMock(side_effect=fake_process_pieces)

    task_info = {'infohash': infohash_hex}
    await service._handle_screenshot_task(task_info)

    mock_callbacks['status_callback'].assert_called_once()
    _, call_kwargs = mock_callbacks['status_callback'].call_args
    assert call_kwargs.get('status') == 'success'
    assert call_kwargs.get('infohash') == infohash_hex

    # The video is 2s long (60 frames / 30 fps).
    # target_interval_sec is 1s.
    # num_screenshots = max(2, min(int(2 / 1), 10)) = max(2, min(2, 10)) = 2
    # The generated video should have ~6 keyframes. _select_keyframes will pick 2.
    assert mock_generator_instance.generate.call_count == 2
    assert infohash_hex not in service.active_tasks

@pytest.mark.asyncio
async def test_handle_task_no_video_file_found(service: ScreenshotService, mock_callbacks):
    infohash_hex = "b" * 40
    mock_handle = service.mock_handle
    mock_ti = MagicMock()
    mock_files = MagicMock()
    mock_files.num_files.return_value = 1
    mock_files.file_path.return_value = "document.txt"
    mock_files.file_size.return_value = 100
    mock_files.file_offset.return_value = 0
    mock_ti.files.return_value = mock_files
    mock_handle.get_torrent_info = MagicMock(return_value=mock_ti)

    await service._handle_screenshot_task({'infohash': infohash_hex})

    mock_callbacks['status_callback'].assert_called_once()
    _, kwargs = mock_callbacks['status_callback'].call_args
    assert kwargs.get('status') == 'permanent_failure'
    assert kwargs.get('infohash') == infohash_hex
    assert isinstance(kwargs.get('error'), NoVideoFileError)

@pytest.mark.asyncio
async def test_handle_task_moov_not_found(service: ScreenshotService, mock_callbacks):
    infohash_hex = "c" * 40
    mock_handle = service.mock_handle
    mock_ti = MagicMock()
    mock_files = MagicMock()
    mock_files.num_files.return_value = 1
    mock_files.file_path.return_value = "video.mp4"
    mock_files.file_size.return_value = 100000
    mock_files.file_offset.return_value = 0
    mock_ti.files.return_value = mock_files
    mock_handle.get_torrent_info = MagicMock(return_value=mock_ti)

    service._get_moov_atom_data = AsyncMock(side_effect=MoovNotFoundError("test", infohash_hex))

    await service._handle_screenshot_task({'infohash': infohash_hex})

    mock_callbacks['status_callback'].assert_called_once()
    _, kwargs = mock_callbacks['status_callback'].call_args
    assert kwargs.get('status') == 'permanent_failure'
    assert kwargs.get('infohash') == infohash_hex
    assert isinstance(kwargs.get('error'), MoovNotFoundError)

@pytest.mark.asyncio
async def test_handle_task_recoverable_failure_on_fetch(service: ScreenshotService, mock_callbacks):
    infohash_hex = "d" * 40
    mock_handle = service.mock_handle
    mock_ti = MagicMock()
    mock_files = MagicMock()
    mock_files.num_files.return_value = 1
    mock_files.file_path.return_value = "video.mp4"
    mock_files.file_size.return_value = 100000
    mock_files.file_offset.return_value = 0
    mock_ti.files.return_value = mock_files
    mock_handle.get_torrent_info = MagicMock(return_value=mock_ti)

    service._get_moov_atom_data = AsyncMock(side_effect=MoovFetchError("test timeout", infohash_hex))

    await service._handle_screenshot_task({'infohash': infohash_hex})

    mock_callbacks['status_callback'].assert_called_once()
    _, kwargs = mock_callbacks['status_callback'].call_args
    assert kwargs.get('status') == 'recoverable_failure'
    assert kwargs.get('infohash') == infohash_hex
    assert isinstance(kwargs.get('error'), MoovFetchError)
    assert 'resume_data' in kwargs
