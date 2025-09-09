# -*- coding: utf-8 -*-
"""
此脚本用于生成一个用于单元测试的、最小化的、结构有效的 MKV 视频文件。
这个 MKV 文件将包含一个简单的视频轨道和 Cues 索引，用于测试关键帧提取。
"""
import av
import numpy as np
import os
import argparse

# --- 默认配置 ---
OUTPUT_DIR = "tests/assets"
WIDTH = 32
HEIGHT = 32
FPS = 24
DURATION_SECONDS = 5 # 生成一个包含多帧的视频以确保 Cues 生成

def generate_minimal_mkv(codec: str, output_filename: str):
    """
    使用 PyAV 生成一个最小化的 MKV 文件。

    :param codec: 使用的 libav 编解码器名称 (例如, 'libx264')。
    :param output_filename: 输出的视频文件名。
    """
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    print(f"正在生成 MKV 测试视频文件: {output_path} (编解码器: {codec})")

    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 使用 'matroska' 格式创建输出容器
    # PyAV 会自动为 MKV 创建 Cues 索引
    with av.open(output_path, mode="w", format="matroska") as container:
        stream = container.add_stream(codec, rate=FPS)
        stream.width = WIDTH
        stream.height = HEIGHT
        stream.pix_fmt = "yuv420p"
        stream.options = {"tune": "zerolatency", "preset": "ultrafast", "crf": "28"}

        # 生成几秒钟的视频帧
        for frame_i in range(DURATION_SECONDS * FPS):
            # 创建一个简单的、颜色会变化的帧
            img = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
            img[:, :, (frame_i // FPS) % 3] = 255 * (frame_i % FPS) / FPS
            frame = av.VideoFrame.from_ndarray(img, format="rgb24")

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
        description="生成用于测试的最小化 MKV 视频文件。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--codec",
        type=str,
        default="libx264",
        help="要使用的 libav 编解码器 (默认为 'libx264')。"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="test_video.mkv",
        help="输出的视频文件名 (默认为 'test_video.mkv')。"
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
