# -*- coding: utf-8 -*-
"""
对 screenshot/generator.py 的单元测试。

此文件包括一个用于程序化生成测试视频的辅助函数，
以及用于验证截图生成器行为的测试用例。
"""
import asyncio
import logging
import os
import pytest
import av
import numpy as np
from pathlib import Path
from typing import Tuple

from screenshot.generator import ScreenshotGenerator
from screenshot.errors import CodecNotFoundError, DecodingError

# --- 配置 ---
TEST_VIDEO_DIR = Path(__file__).parent / "videos"
TEST_VIDEO_DIR.mkdir(exist_ok=True)
FRAME_WIDTH = 128
FRAME_HEIGHT = 72
TOTAL_FRAMES = 5  # 创建一个包含少量帧的短视频
FPS = 24

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- 测试视频生成辅助函数 ---

def generate_video_file(
    file_path: Path,
    codec: str,
    pixel_format: str = "yuv420p",
    options: dict = None,
) -> Tuple[bytes, bytes]:
    """
    一个健壮的、可参数化的视频生成函数。

    :param file_path: 输出视频文件的路径 (例如 'videos/test_h264.mp4')。
    :param codec: 要使用的编解码器 (例如 'libx264', 'libx265', 'vp9')。
    :param pixel_format: 像素格式。'yuv420p' 是最常见的。
    :param options: 传递给编码器的额外选项字典。
    :return: 一个元组，包含 (视频的 extradata, 第一个数据包的数据)。
    """
    log.info(f"正在生成测试视频: {file_path} (编解码器: {codec})")
    options = options or {}
    first_packet_data = None
    extradata = None

    # 确保视频目录存在
    file_path.parent.mkdir(exist_ok=True)

    with av.open(str(file_path), mode="w") as container:
        stream = container.add_stream(codec, rate=FPS)
        stream.width = FRAME_WIDTH
        stream.height = FRAME_HEIGHT
        stream.pix_fmt = pixel_format
        stream.options = options

        for frame_i in range(TOTAL_FRAMES):
            img = np.zeros((FRAME_HEIGHT, FRAME_WIDTH, 3), dtype=np.uint8)
            color = (frame_i * 30 % 256, frame_i * 50 % 256, frame_i * 70 % 256)
            img[:, :] = color
            frame = av.VideoFrame.from_ndarray(img, format="rgb24")

            for packet in stream.encode(frame):
                container.mux(packet)
                if first_packet_data is None and packet.is_keyframe:
                    log.info(f"捕获到第一个关键帧数据包 (大小: {len(bytes(packet))} 字节)")
                    first_packet_data = bytes(packet)

        log.info("正在冲刷编码器...")
        for packet in stream.encode(None):
            log.info("从编码器冲刷出一个数据包。")
            container.mux(packet)
            if first_packet_data is None and packet.is_keyframe:
                log.info(f"在冲刷期间捕获到第一个关键帧数据包 (大小: {len(bytes(packet))} 字节)")
                first_packet_data = bytes(packet)

        extradata = stream.codec_context.extradata

    log.info(f"视频 '{file_path}' 已成功生成。")
    if not first_packet_data:
        raise RuntimeError(f"未能为视频 '{file_path}' 生成或捕获任何关键帧数据包。")

    return extradata, first_packet_data

# --- 测试固件 (Fixtures) ---

@pytest.fixture(scope="module")
def h264_flushing_issue_video() -> Tuple[str, bytes, bytes]:
    """返回 H.264 解码器名称和相关视频数据。"""
    encoder_codec = "libx264"
    decoder_codec = "h264"
    file_path = TEST_VIDEO_DIR / "h264_flushing_issue.mp4"
    options = {"preset": "ultrafast", "bf": "2"}
    if not file_path.exists():
        generate_video_file(file_path, encoder_codec, options=options)

    # 我们需要从文件中读取数据包，以确保我们测试的是解码过程
    with av.open(str(file_path)) as container:
        packet = next(container.demux())
        extradata = container.streams.video[0].codec_context.extradata
        return decoder_codec, extradata, bytes(packet)


@pytest.fixture(scope="module")
def hevc_video_mp4() -> Tuple[str, bytes, bytes]:
    """返回 HEVC 解码器名称和相关视频数据。"""
    encoder_codec = "libx265"
    decoder_codec = "hevc"
    file_path = TEST_VIDEO_DIR / "hevc_video.mp4"
    options = {"preset": "ultrafast", "x265-params": "log-level=error"}
    if not file_path.exists():
        generate_video_file(file_path, encoder_codec, options=options)

    with av.open(str(file_path)) as container:
        packet = next(container.demux())
        extradata = container.streams.video[0].codec_context.extradata
        return decoder_codec, extradata, bytes(packet)


@pytest.fixture(scope="module")
def h264_video_mkv() -> Tuple[str, bytes, bytes]:
    """返回 H.264 解码器名称和 MKV 视频数据。"""
    encoder_codec = "libx264"
    decoder_codec = "h264"
    file_path = TEST_VIDEO_DIR / "h264_video.mkv"
    options = {"preset": "ultrafast"}
    if not file_path.exists():
        generate_video_file(file_path, encoder_codec, options=options)

    with av.open(str(file_path)) as container:
        packet = next(container.demux())
        extradata = container.streams.video[0].codec_context.extradata
        return decoder_codec, extradata, bytes(packet)


@pytest.fixture(scope="module")
def vp9_video_mkv() -> Tuple[str, bytes, bytes]:
    """返回 VP9 解码器名称和 MKV 视频数据。"""
    encoder_codec = "libvpx-vp9"
    decoder_codec = "vp9"
    file_path = TEST_VIDEO_DIR / "vp9_video.mkv"
    options = {}
    if not file_path.exists():
        generate_video_file(file_path, encoder_codec, options=options)

    with av.open(str(file_path)) as container:
        packet = next(container.demux())
        extradata = container.streams.video[0].codec_context.extradata
        return decoder_codec, extradata, bytes(packet)


# --- 测试用例 ---

@pytest.mark.asyncio
async def test_decode_succeeds_with_robust_flush(h264_flushing_issue_video):
    """
    验证修复后的 ScreenshotGenerator 能够从需要冲刷的视频中成功解码帧。
    """
    log.info("--- 开始测试: test_decode_succeeds_with_robust_flush ---")
    codec_name, extradata, packet_data = h264_flushing_issue_video

    loop = asyncio.get_event_loop()
    screenshot_generated_event = asyncio.Event()
    generated_image = None

    async def on_success(infohash, image_bytes, timestamp):
        nonlocal generated_image
        log.info(f"成功回调被调用！收到了 {len(image_bytes)} 字节的图片。")
        generated_image = image_bytes
        screenshot_generated_event.set()

    # 使用修复后的生成器
    generator = ScreenshotGenerator(loop=loop, on_success=on_success)

    await generator.generate(
        codec_name=codec_name,
        extradata=extradata,
        packet_data=packet_data,
        infohash_hex="test_infohash",
        timestamp_str="0"
    )

    try:
        # 我们现在期望回调能够成功被调用
        await asyncio.wait_for(screenshot_generated_event.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        pytest.fail("on_success 回调在预期时间内未被调用，测试失败。")

    # 验证截图确实已生成
    assert generated_image is not None
    assert isinstance(generated_image, bytes)
    assert len(generated_image) > 100 # 确保不是一个空的或无效的图片
    log.info("测试成功：已生成有效截图。")

@pytest.mark.asyncio
@pytest.mark.parametrize("video_fixture", [
    "hevc_video_mp4",
    "h264_video_mkv",
    "vp9_video_mkv",
])
async def test_decode_succeeds_for_various_formats(video_fixture, request):
    """
    验证修复后的生成器能成功处理多种视频格式和容器。
    """
    log.info(f"--- 开始测试: test_decode_succeeds_for_various_formats (格式: {video_fixture}) ---")
    codec_name, extradata, packet_data = request.getfixturevalue(video_fixture)

    loop = asyncio.get_event_loop()
    screenshot_generated_event = asyncio.Event()
    generated_image = None

    async def on_success(infohash, image_bytes, timestamp):
        nonlocal generated_image
        generated_image = image_bytes
        screenshot_generated_event.set()

    generator = ScreenshotGenerator(loop=loop, on_success=on_success)

    await generator.generate(
        codec_name=codec_name,
        extradata=extradata,
        packet_data=packet_data,
        infohash_hex=f"test_{video_fixture}",
        timestamp_str="0"
    )

    try:
        await asyncio.wait_for(screenshot_generated_event.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        pytest.fail(f"回调在处理 {video_fixture} 时超时。")

    assert generated_image is not None
    assert len(generated_image) > 100
    log.info(f"测试成功：已为 {video_fixture} 生成有效截图。")


# --- Generator 失败场景测试 ---

# 未来测试策略说明:
# 当前的失败场景测试是端到端的集成测试，它们真实地在线程池中运行解码代码，
# 这对于验证线程交互和实际的异常传播至关重要。
#
# 为了实现更快的、更隔离的单元测试，可以采用 mock 技术。例如，使用
# `unittest.mock.patch` 来模拟 `loop.run_in_executor`。可以设置
# `run_in_executor` 的 `side_effect` 来模拟解码成功或立即抛出异常。
# 这样就可以在不启动线程或执行实际解码的情况下，测试 `generate` 方法的调用逻辑。
#
# 示例 (伪代码):
#
# @patch("asyncio.get_event_loop.run_in_executor")
# async def test_generate_logic_with_mock(mock_run_in_executor):
#     mock_run_in_executor.side_effect = CodecNotFoundError("mocked error")
#     generator = ScreenshotGenerator(...)
#     with pytest.raises(CodecNotFoundError):
#         await generator.generate(...)
#     mock_run_in_executor.assert_called_once_with(...)

@pytest.mark.asyncio
async def test_generator_raises_codec_not_found(h264_flushing_issue_video):
    """
    测试当提供一个无效的编解码器名称时，生成器会引发 CodecNotFoundError。
    """
    _, extradata, packet_data = h264_flushing_issue_video
    loop = asyncio.get_event_loop()
    generator = ScreenshotGenerator(loop=loop)

    with pytest.raises(CodecNotFoundError):
        await generator.generate(
            codec_name="invalid_codec_name_12345",
            extradata=extradata,
            packet_data=packet_data,
            infohash_hex="test_invalid_codec",
            timestamp_str="0"
        )
    log.info("测试成功：无效的编解码器名称正确地引发了 CodecNotFoundError。")


@pytest.mark.asyncio
async def test_generator_raises_decoding_error_on_corrupt_packet(h264_flushing_issue_video):
    """
    测试当提供一个损坏的数据包时，生成器会引发 DecodingError。
    """
    codec_name, extradata, _ = h264_flushing_issue_video
    corrupt_packet_data = os.urandom(100) # 使用随机字节作为损坏的数据包

    loop = asyncio.get_event_loop()
    generator = ScreenshotGenerator(loop=loop)

    with pytest.raises(DecodingError):
        await generator.generate(
            codec_name=codec_name,
            extradata=extradata,
            packet_data=corrupt_packet_data,
            infohash_hex="test_corrupt_packet",
            timestamp_str="0"
        )
    log.info("测试成功：损坏的数据包正确地引发了 DecodingError。")
