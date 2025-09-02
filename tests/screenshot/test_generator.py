# -*- coding: utf-8 -*-
"""
对 screenshot/generator.py 中截图生成器功能的单元测试。
"""
import pytest
import os
import asyncio
from PIL import Image

from screenshot.generator import ScreenshotGenerator
from screenshot.extractor import KeyframeExtractor
from tests.screenshot.test_extractor import moov_atom_data # 复用 extractor 测试的 fixture

# --- 测试资源 ---
TEST_VIDEO_PATH = "tests/assets/test_video.mp4"

@pytest.fixture(scope="module")
def video_packet_data(moov_atom_data):
    """
    一个 Pytest fixture，从测试视频中提取解码第一帧所需的所有信息。
    包括：codec_name, extradata, 和第一个数据包 (packet) 的内容。
    """
    # 1. 使用 KeyframeExtractor 获取元数据
    extractor = KeyframeExtractor(moov_atom_data)
    assert extractor.samples, "测试视频中未找到样本"

    # 2. 读取第一个样本（即视频帧）的原始数据
    first_sample = extractor.samples[0]
    with open(TEST_VIDEO_PATH, "rb") as f:
        f.seek(first_sample.offset)
        packet_data = f.read(first_sample.size)

    return {
        "codec_name": extractor.codec_name,
        "extradata": extractor.extradata,
        "packet_data": packet_data,
        "nal_length_size": extractor.nal_length_size,
        "mode": extractor.mode
    }

# --- 测试用例 ---

@pytest.mark.asyncio
async def test_screenshot_generator_creates_valid_jpeg(tmpdir, video_packet_data):
    """
    测试 ScreenshotGenerator 是否能成功解码一个数据包并生成一个有效的 JPEG 文件。
    """
    # 1. 准备测试环境和参数
    output_dir = str(tmpdir)
    loop = asyncio.get_running_loop()
    infohash_hex = "test_generator_infohash"
    timestamp_str = "00-00-00"

    # 创建一个 Future 用于回调函数，以便我们可以等待它完成
    callback_called = asyncio.Future()

    async def on_success_callback(infohash, filepath):
        """测试用的回调函数，当被调用时，设置 Future 的结果。"""
        callback_called.set_result((infohash, filepath))

    # 2. 实例化并运行生成器
    generator = ScreenshotGenerator(loop=loop, output_dir=output_dir, on_success=on_success_callback)

    packet_data = video_packet_data["packet_data"]

    # 如果是 avc1 格式, 需要转换为 Annex B
    if video_packet_data["mode"] == 'avc1':
        annexb_data, start_code, cursor = bytearray(), b'\x00\x00\x00\x01', 0
        nal_length_size = video_packet_data["nal_length_size"]
        while cursor < len(packet_data):
            nal_length = int.from_bytes(packet_data[cursor : cursor + nal_length_size], 'big')
            cursor += nal_length_size
            nal_data = packet_data[cursor : cursor + nal_length]
            annexb_data.extend(start_code + nal_data)
            cursor += nal_length
        packet_data = bytes(annexb_data)

    await generator.generate(
        codec_name=video_packet_data["codec_name"],
        extradata=video_packet_data["extradata"],
        packet_data=packet_data,
        infohash_hex=infohash_hex,
        timestamp_str=timestamp_str
    )

    # 3. 验证结果
    # 验证回调函数是否被调用
    cb_infohash, cb_filepath = await asyncio.wait_for(callback_called, timeout=5)
    assert cb_infohash == infohash_hex

    # 验证文件是否已创建
    expected_filename = f"{infohash_hex}_{timestamp_str}.jpg"
    expected_filepath = os.path.join(output_dir, expected_filename)
    assert cb_filepath == expected_filepath
    assert os.path.exists(expected_filepath)

    # 验证生成的文件是一个有效的、非空的 JPEG 图片
    try:
        with Image.open(expected_filepath) as img:
            assert img.format == "JPEG"
            assert img.width > 0
            assert img.height > 0
    except Exception as e:
        pytest.fail(f"无法打开或验证生成的图片文件: {e}")
