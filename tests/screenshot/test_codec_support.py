# -*- coding: utf-8 -*-
"""
本测试模块旨在对视频解码的核心逻辑进行单元测试，以验证对多种视频
容器格式和编解码器的支持。

与集成测试不同，本模块直接测试 `VideoMetadataExtractor` 和 `ScreenshotGenerator`，
完全绕过了 `TorrentClient` 和网络层，使得测试更快速、稳定，且专注于解码逻辑。
"""
import os
import asyncio
import tempfile
import pytest

from screenshot.extractor import VideoMetadataExtractor
from screenshot.generator import ScreenshotGenerator
from config import Settings

# --- 测试配置 ---
TEST_ASSETS_DIR = "tests/assets"
TEST_VIDEOS = [
    "test_h264.mp4",
    "test_hevc.mp4",
    "test_h264.mkv", # 新增 MKV 容器测试用例
    pytest.param("test_av1.mp4", marks=pytest.mark.xfail(reason="AV1 decoding fails in PyAV under test conditions")),
]

# --- Pytest Fixtures ---
@pytest.fixture
def settings():
    """提供一个临时的、用于测试的配置实例。"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Settings(output_dir=temp_dir)

# --- 测试用例 ---
@pytest.mark.parametrize("video_filename", TEST_VIDEOS)
@pytest.mark.asyncio
async def test_format_and_codec_support(settings, video_filename: str):
    """
    一个参数化的单元测试，验证对不同视频格式和编码的核心解析和解码能力。

    :param settings: 由 fixture 提供的 Settings 实例。
    :param video_filename: 由 pytest.mark.parametrize 提供的视频文件名。
    """
    video_path = os.path.join(TEST_ASSETS_DIR, video_filename)
    assert os.path.exists(video_path), f"测试视频文件不存在: {video_path}"

    with open(video_path, "rb") as f:
        # 对于单元测试，我们直接将整个文件读入内存。
        # 在实际服务中，只会读取文件头部。
        video_data = f.read()

    # 1. 使用新的提取器从视频数据中解析元数据
    # 不再需要手动查找 'moov' atom
    extractor = VideoMetadataExtractor(video_data=video_data)

    assert extractor.codec_name is not None, "未能从视频流中解析出编解码器名称"
    assert len(extractor.keyframes) > 0, "未能从视频流中解析出任何关键帧"
    print(f"[{video_filename}] VideoMetadataExtractor 成功: Codec={extractor.codec_name}, Keyframes={len(extractor.keyframes)}")

    # 2. 准备解码所需的数据
    # 我们选择第一个关键帧进行测试
    keyframe = extractor.keyframes[0]
    sample_info = next(s for s in extractor.samples if s.index == keyframe.sample_index)

    # 从完整的视频数据中，根据提取器提供的偏移量和大小，切出原始的数据包
    packet_data_bytes = video_data[sample_info.offset : sample_info.offset + sample_info.size]
    packet_data = packet_data_bytes

    # 模拟服务端的逻辑：对于 MKV，跳过 SimpleBlock 头
    if 'matroska' in extractor.container_format and extractor.codec_name == 'h264':
        packet_data_bytes = packet_data_bytes[4:]

    # 为 H.264/HEVC 的 AVCC 格式数据包，手动转换为 Annex B 格式，以模拟服务端的逻辑
    if extractor.codec_name in ('h264', 'hevc') and extractor.nal_length_size > 0:
        annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
        nal_size = extractor.nal_length_size
        while cursor < len(packet_data_bytes):
            nal_length = int.from_bytes(packet_data_bytes[cursor: cursor + nal_size], 'big')
            cursor += nal_size
            nal_data = packet_data_bytes[cursor: cursor + nal_length]
            annexb_data.extend(start_code + nal_data)
            cursor += nal_length
        packet_data = bytes(annexb_data)


    # 3. 设置回调和事件来捕获截图生成的结果
    generated_files = []
    file_generated_event = asyncio.Event()

    async def on_success_callback(infohash, image_bytes, timestamp_str):
        print(f"测试回调被触发，收到 {len(image_bytes)} 字节的图片数据。")
        generated_files.append(image_bytes)
        file_generated_event.set()

    # 4. 创建 ScreenshotGenerator 并执行解码
    generator = ScreenshotGenerator(
        loop=asyncio.get_running_loop(),
        output_dir=settings.output_dir,
        on_success=on_success_callback
    )

    # 在 generate 调用上加上 try/except 块，以便在解码失败时能提供更清晰的测试失败信息
    try:
        await generator.generate(
            codec_name=extractor.codec_name,
            extradata=extractor.extradata,
            packet_data=packet_data,
            infohash_hex="test-" + video_filename,
            timestamp_str="00-00-00"
        )
    except Exception as e:
        pytest.fail(f"为 {video_filename} 解码时发生意外错误: {e}", pytrace=True)

    # 5. 等待回调被触发并验证结果
    try:
        await asyncio.wait_for(file_generated_event.wait(), timeout=5)
    except asyncio.TimeoutError:
        pytest.fail(f"为 {video_filename} 生成截图后，成功回调未在5秒内被触发。")

    assert len(generated_files) == 1, "成功回调被触发，但未记录任何文件"
    image_data = generated_files[0]
    assert isinstance(image_data, bytes), "回调未传递 bytes 类型的数据"
    assert len(image_data) > 100, f"截图数据大小异常，仅为 {len(image_data)} 字节" # 检查非空且有一定大小
    print(f"[{video_filename}] 验证成功: 已在内存中生成截图。")
