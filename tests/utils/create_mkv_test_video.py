# -*- coding: utf-8 -*-
"""
此脚本用于生成一个用于单元测试的、最小化的、结构有效的 MKV 视频文件。
这个 MKV 文件将包含一个简单的视频轨道和 Cues 索引，用于测试关键帧提取。

用法:
    python tests/utils/create_mkv_test_video.py --codec libx264 --output test_video.mkv
    python tests/utils/create_mkv_test_video.py --codec libx265 --output test_hevc.mkv
    python tests/utils/create_mkv_test_video.py --codec libaom-av1 --output test_av1.mkv
"""
import av
import numpy as np
import os
import argparse

# --- 默认配置 ---
OUTPUT_DIR = "tests/assets"
TEST_MKV_PATH = os.path.join(OUTPUT_DIR, "test_video.mkv")
WIDTH = 32
HEIGHT = 32
FPS = 5 # Use a lower FPS to keep file size down
DURATION_SECONDS = 2

def generate_minimal_mkv(codec: str, output_filename: str):
    """
    使用 PyAV 生成一个最小化的 MKV 文件。

    :param codec: 使用的 libav 编解码器名称 (例如, 'libx264', 'libx265', 'libaom-av1')。
    :param output_filename: 输出的视频文件名。
    """
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    print(f"正在生成 MKV 测试视频文件: {output_path} (编解码器: {codec})")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 为不同的编解码器设置特定的像素格式和选项
    options = {}
    pix_fmt = "yuv420p" # Common pixel format
    if codec == "libx264" or codec == "libx265":
        options = {"tune": "zerolatency", "preset": "ultrafast", "crf": "28"}
    elif codec == "libaom-av1":
        options = {"crf": "50", "cpu-used": "8"}

    with av.open(output_path, mode="w", format="matroska") as container:
        stream = container.add_stream(codec, rate=FPS)
        stream.width = WIDTH
        stream.height = HEIGHT
        stream.pix_fmt = pix_fmt
        if options:
            stream.options = options

        for frame_i in range(DURATION_SECONDS * FPS):
            img = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
            img[:, :, (frame_i % 3)] = 255 * (frame_i % FPS) / FPS
            frame = av.VideoFrame.from_ndarray(img, format="rgb24")

            for packet in stream.encode(frame):
                container.mux(packet)

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
        description="生成用于测试的最小化 MKV 视频文件。",
        formatter_class=argparse.RawTextHelpFormatter
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
        help="输出的视频文件名。\n例如: 'test_video.mkv'"
    )
    args = parser.parse_args()

    try:
        generate_minimal_mkv(args.codec, args.output)
    except Exception as e:
        print(f"生成测试视频时发生错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()
