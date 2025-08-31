# -*- coding: utf-8 -*-
"""
一个独立的集成测试脚本，用于验证对新视频编解码器的支持。

该脚本会：
1. 接受一个视频URL作为命令行参数。
2. 使用 PyAV (av) 直接从URL打开视频流。
3. 查找第一个视频流。
4. 提取解码器所需的 `extradata` 和 `codec_name`。
5. 寻找第一个关键帧数据包。
6. 使用 `ScreenshotGenerator` 来解码该关键帧并保存为JPEG文件。
7. 检查输出的JPEG文件是否存在，以确认测试成功。

这个方法绕过了自己编写的 `KeyframeExtractor`，因为它需要复杂的逻辑来通过HTTP
获取 `moov` atom。相反，我们依赖 PyAV 来处理流的解封装 (demuxing)，从而
可以直接测试 `ScreenshotGenerator` 在处理真实 H.265 和 AV1 码流时的表现。
"""
import asyncio
import logging
import os
import pytest
import av

from screenshot.generator import ScreenshotGenerator

# --- 配置日志 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("IntegrationTest")

# 使用 parametrize 来为同一个测试函数提供不同的URL
@pytest.mark.parametrize("video_url", [
    'https://repo.jellyfin.org/test-videos/SDR/HEVC%208bit/Test%20Jellyfin%201080p%20HEVC%208bit%203M.mp4',
    'https://repo.jellyfin.org/test-videos/SDR/AV1/Test%20Jellyfin%201080p%20AV1%2010bit%203M.mp4',
])
@pytest.mark.asyncio
async def test_real_video_screenshot_generation(video_url: str):
    """
    对给定的视频URL执行集成测试。
    """
    log.info(f"开始测试 URL: {video_url}")

    try:
        container = av.open(video_url, mode='r', timeout=30)
    except Exception as e:
        pytest.fail(f"使用 PyAV 打开 URL '{video_url}' 失败: {e}")

    video_stream = next((s for s in container.streams if s.type == 'video'), None)
    if not video_stream:
        pytest.fail("在 URL 中未找到视频流。")

    log.info(f"找到视频流。Codec: {video_stream.codec_context.name}, Profile: {video_stream.profile}")

    codec_name = video_stream.codec_context.name
    extradata = video_stream.codec_context.extradata

    first_keyframe_packet = next((p for p in container.demux(video_stream) if p.is_keyframe), None)

    container.close()

    if not first_keyframe_packet:
        pytest.fail("在视频流中未找到任何关键帧。")

    log.info(f"找到第一个关键帧。大小: {first_keyframe_packet.size} 字节, PTS: {first_keyframe_packet.pts}")

    loop = asyncio.get_running_loop()
    generator = ScreenshotGenerator(loop=loop, output_dir="integration_test_output")

    base_filename = os.path.basename(video_url).split('.')[0]
    timestamp_str = "00-00-01"
    output_filename = f"integration_test_output/{base_filename}_{timestamp_str}.jpg"

    log.info(f"准备使用编解码器 '{codec_name}' 生成截图...")

    try:
        await generator.generate(
            codec_name=codec_name,
            extradata=extradata,
            packet_data=bytes(first_keyframe_packet),
            infohash_hex=base_filename,
            timestamp_str=timestamp_str
        )
    except Exception as e:
        pytest.fail(f"调用 ScreenshotGenerator 时发生错误: {e}")

    assert os.path.exists(output_filename), f"测试失败！截图文件未生成: {output_filename}"
    log.info(f"✅ 测试成功！截图已生成: {output_filename}")
