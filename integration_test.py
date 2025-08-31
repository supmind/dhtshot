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
import sys
import av

from screenshot.generator import ScreenshotGenerator

# --- 配置日志 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("IntegrationTest")

async def test_video_url(video_url: str):
    """
    对给定的视频URL执行集成测试。
    """
    log.info(f"开始测试 URL: {video_url}")

    try:
        # 1. 使用 PyAV 直接打开 URL
        # PyAV/FFmpeg 在后台处理 HTTP 请求和数据缓冲
        container = av.open(video_url, mode='r')
    except Exception as e:
        log.error(f"使用 PyAV 打开 URL '{video_url}' 失败: {e}", exc_info=True)
        return False

    # 2. 找到第一个视频流
    video_stream = None
    for stream in container.streams:
        if stream.type == 'video':
            video_stream = stream
            break

    if not video_stream:
        log.error("在 URL 中未找到视频流。")
        container.close()
        return False

    log.info(f"找到视频流。Codec: {video_stream.codec_context.name}, Profile: {video_stream.profile}")

    # 3. 提取解码所需的信息
    codec_name = video_stream.codec_context.name
    extradata = video_stream.codec_context.extradata

    # 4. 寻找第一个关键帧数据包
    first_keyframe_packet = None
    for packet in container.demux(video_stream):
        if packet.is_keyframe:
            first_keyframe_packet = packet
            log.info(f"找到第一个关键帧。大小: {packet.size} 字节, PTS: {packet.pts}")
            break

    container.close()

    if not first_keyframe_packet:
        log.error("在视频流中未找到任何关键帧。")
        return False

    # 5. 使用 ScreenshotGenerator 生成截图
    loop = asyncio.get_running_loop()
    generator = ScreenshotGenerator(loop=loop, output_dir="integration_test_output")

    # 从URL中提取一个基本的文件名作为截图名称
    base_filename = os.path.basename(video_url).split('.')[0]
    timestamp_str = "00-00-01"
    output_filename = f"integration_test_output/{base_filename}_{timestamp_str}.jpg"

    log.info(f"准备使用编解码器 '{codec_name}' 生成截图...")

    try:
        await generator.generate(
            codec_name=codec_name,
            extradata=extradata,
            packet_data=bytes(first_keyframe_packet),
            infohash_hex=base_filename, # 使用文件名代替infohash
            timestamp_str=timestamp_str
        )
    except Exception as e:
        log.error(f"调用 ScreenshotGenerator 时发生错误: {e}", exc_info=True)
        return False

    # 6. 检查输出文件是否存在
    if os.path.exists(output_filename):
        log.info(f"✅ 测试成功！截图已生成: {output_filename}")
        return True
    else:
        log.error(f"❌ 测试失败！截图文件未生成: {output_filename}")
        return False


async def main():
    if len(sys.argv) < 2:
        print("用法: python integration_test.py <video_url>")
        sys.exit(1)

    video_url = sys.argv[1]
    success = await test_video_url(video_url)

    if not success:
        sys.exit(1) # 以错误码退出

if __name__ == "__main__":
    asyncio.run(main())
