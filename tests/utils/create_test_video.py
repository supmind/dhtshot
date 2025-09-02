# -*- coding: utf-8 -*-
"""
此脚本用于生成一个用于单元测试的、最小化的、结构有效的 MP4 视频文件。

该视频文件包含：
- 一个视频流。
- 使用 H.264 (libx264) 编解码器。
- 分辨率为 16x16 像素。
- 帧率为 1 fps。
- 仅包含一帧黑色的画面。
- 时长为 1 秒。

生成的文件将被保存在 `tests/assets/test_video.mp4`，可被其他测试用例重复使用。
"""
import av
import numpy as np
import os

# --- 配置 ---
OUTPUT_DIR = "tests/assets"
OUTPUT_FILENAME = "test_video.mp4"
WIDTH = 16
HEIGHT = 16
FPS = 1
DURATION_SECONDS = 1
CODEC = "libx264"
PIXEL_FORMAT = "yuv420p"  # H.264 编码器常用像素格式

def generate_minimal_mp4():
    """
    使用 PyAV 生成一个最小化的 MP4 文件。
    """
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
    print(f"正在生成测试视频文件: {output_path}")

    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 使用 'mp4' 格式创建输出容器
    with av.open(output_path, mode="w") as container:
        # 添加一个视频流，并设置编解码器
        # 使用 'h264_mp4toannexb' bitstream filter 来确保与 MP4 格式的兼容性
        stream = container.add_stream(CODEC, rate=FPS)
        stream.width = WIDTH
        stream.height = HEIGHT
        stream.pix_fmt = PIXEL_FORMAT

        # H.264 编码器的一些特定选项，以确保最小化和兼容性
        stream.options = {
            "tune": "zerolatency",  # 优化以减少延迟
            "preset": "ultrafast",  # 快速编码
            "crf": "28",            # 压缩质量 (Constant Rate Factor)
        }

        # 创建一个黑色的视频帧
        # 创建一个 (height, width, channels) 的 numpy 数组
        black_frame_data = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(black_frame_data, format="rgb24")

        # 编码这一帧
        # stream.encode() 会返回一个 AVPacket 列表
        for packet in stream.encode(frame):
            # 将编码后的数据包混流到容器中
            container.mux(packet)

        # 编码器可能有延迟，需要发送一个 None 来冲刷（flush）编码器缓冲区
        print("正在冲刷编码器...")
        for packet in stream.encode(None):
            container.mux(packet)

    print(f"文件已成功生成: {output_path}")
    # 验证文件大小
    file_size = os.path.getsize(output_path)
    print(f"文件大小: {file_size} 字节")
    if file_size == 0:
        raise RuntimeError("生成的文件大小为 0，可能存在错误。")

if __name__ == "__main__":
    try:
        generate_minimal_mp4()
    except Exception as e:
        print(f"生成测试视频时发生错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
