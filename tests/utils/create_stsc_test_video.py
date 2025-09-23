# -*- coding: utf-8 -*-
"""
使用 PyAV 创建一个特殊的 H.264 视频文件，用于测试 'stsc' box 的解析逻辑。

这个脚本的目标是生成一个 MP4 文件，其 'stsc' (Sample-to-Chunk) box
包含多个条目。这将强制测试 KeyframeExtractor 是否能正确处理
每个 chunk 中样本数（即帧数）可变的情况。

生成的视频特性：
- 编解码器: H.264 (libx264)
- 分辨率: 64x64
- 帧率: 10
- 总帧数: 6
- 文件名: tests/assets/test_variable_stsc.mp4
- 图像组 (GOP) 结构，旨在创建不同的块大小:
  - 块 1: 1 帧 (关键帧)
  - 块 2: 2 帧 (1 关键帧, 1 P帧)
  - 块 3: 3 帧 (1 关键帧, 2 P帧)
  这会产生一个 `stsc` 表，其中样本/块的比率是变化的，从而暴露bug。
"""
import av
import os
import numpy as np

# --- 配置 ---
OUTPUT_DIR = "tests/assets"
OUTPUT_FILENAME = "test_variable_stsc.mp4"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

WIDTH, HEIGHT = 64, 64
FPS = 10
TOTAL_FRAMES = 6

def create_video_file():
    """
    使用纯 PyAV 生成具有可变样本/块比率的测试视频。
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    try:
        with av.open(OUTPUT_PATH, mode='w') as container:
            stream = container.add_stream('libx264', rate=FPS)
            stream.width = WIDTH
            stream.height = HEIGHT
            stream.pix_fmt = 'yuv420p'
            # 关键帧间隔设得很大，以便我们能手动控制
            stream.options = {'g': '9999'}

            # 我们将创建 6 帧，并手动指定哪些是关键帧 (I-Frame)
            # 这会创建 3 个 GOP，大小分别为 1, 2, 3
            keyframe_indices = [0, 1, 3]

            for i in range(TOTAL_FRAMES):
                # 创建一个简单的图像
                img = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
                img[:, :, i % 3] = 50 + i * 30 # 给每帧一个独特的颜色
                frame = av.VideoFrame.from_ndarray(img, format='rgb24')

                # 强制指定帧类型
                if i in keyframe_indices:
                    frame.pict_type = av.video.frame.PictureType.I
                else:
                    frame.pict_type = av.video.frame.PictureType.P

                for packet in stream.encode(frame):
                    container.mux(packet)

            # 刷出编码器中剩余的帧
            for packet in stream.encode(None):
                container.mux(packet)

        print(f"成功创建具有可变 STSC 的测试视频: {OUTPUT_PATH}")

    except Exception as e:
        print(f"使用 PyAV 创建视频时发生错误: {e}")
        if os.path.exists(OUTPUT_PATH):
            os.remove(OUTPUT_PATH)
        exit(1)


if __name__ == "__main__":
    create_video_file()
