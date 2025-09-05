# -*- coding: utf-8 -*-
"""
此脚本用于生成一个用于单元测试的、最小化的、结构有效的 MP4 视频文件。

该脚本可以通过命令行参数指定视频的编解码器和输出文件名，
支持生成多种不同编码格式的视频，以用于全面的编解码器兼容性测试。

用法:
    python tests/utils/create_test_video.py --codec libx264 --output test_h264.mp4
    python tests/utils/create_test_video.py --codec libx265 --output test_hevc.mp4
    python tests/utils/create_test_video.py --codec libaom-av1 --output test_av1.mp4
"""
import av
import numpy as np
import os
import argparse

# --- 默认配置 ---
OUTPUT_DIR = "tests/assets"
WIDTH = 16
HEIGHT = 16
FPS = 1
DURATION_SECONDS = 1
DEFAULT_PIXEL_FORMAT = "yuv420p"  # H.264/H.265 编码器常用像素格式

def generate_minimal_mp4(codec: str, output_filename: str):
    """
    使用 PyAV 生成一个最小化的 MP4 文件。

    :param codec: 使用的 libav 编解码器名称 (例如, 'libx264', 'libx265', 'libaom-av1')。
    :param output_filename: 输出的视频文件名。
    """
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    print(f"正在生成测试视频文件: {output_path} (编解码器: {codec})")

    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 为不同的编解码器设置特定的像素格式和选项
    if codec == "libaom-av1":
        # AV1 对像素格式有更严格的要求
        pix_fmt = "yuv420p"
        options = {"crf": "50", "cpu-used": "8"}
    else: # 默认为 H.264/H.265 的设置
        pix_fmt = DEFAULT_PIXEL_FORMAT
        options = {"tune": "zerolatency", "preset": "ultrafast", "crf": "28"}

    # 使用 'mp4' 格式创建输出容器
    with av.open(output_path, mode="w") as container:
        stream = container.add_stream(codec, rate=FPS)
        stream.width = WIDTH
        stream.height = HEIGHT
        stream.pix_fmt = pix_fmt
        stream.options = options

        # 创建一个黑色的视频帧
        black_frame_data = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(black_frame_data, format="rgb24")

        # 编码帧
        for packet in stream.encode(frame):
            container.mux(packet)

        # 冲刷编码器缓冲区
        print("正在冲刷编码器...")
        for packet in stream.encode(None):
            container.mux(packet)

    print(f"文件已成功生成: {output_path}")
    file_size = os.path.getsize(output_path)
    print(f"文件大小: {file_size} 字节")
    if file_size == 0:
        raise RuntimeError("生成的文件大小为 0，可能存在错误。")

def main():
    """
    主函数，用于解析命令行参数并调用生成函数。
    """
    parser = argparse.ArgumentParser(
        description="生成用于测试的最小化视频文件。",
        formatter_class=argparse.RawTextHelpFormatter  # 保持描述格式
    )
    parser.add_argument(
        "--codec",
        type=str,
        required=True,
        help="要使用的 libav 编解码器。\n例如: 'libx264', 'libx265', 'libaom-av1'"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="输出的视频文件名。\n例如: 'test_h264.mp4'"
    )
    args = parser.parse_args()

    try:
        generate_minimal_mp4(args.codec, args.output)
    except Exception as e:
        print(f"生成测试视频时发生错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()
