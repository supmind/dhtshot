# -*- coding: utf-8 -*-
"""
本模块负责实际的截图生成过程。

核心组件是 `ScreenshotGenerator` 类，它利用 PyAV 库来解码 H.264 视频流。
由于 PyAV 的解码操作是 CPU 密集型的同步阻塞操作，因此我们将这些操作
放在一个单独的线程池中执行，以避免阻塞 asyncio 事件循环。
"""
import logging
import os
from typing import Optional
import av  # PyAV 库，用于音视频处理

# --- 配置日志 ---
log = logging.getLogger(__name__)


class ScreenshotGenerator:
    """
    通过向 PyAV 解码器提供 H.264 数据包来处理截图的创建。

    此类封装了与 PyAV 的所有交互。它接收解码所需的数据（extradata 和数据包），
    并在一个独立的线程中执行解码和文件保存，以防止阻塞主事件循环。
    """
    def __init__(self, loop, output_dir='./screenshots_output'):
        """
        初始化生成器。
        :param loop: asyncio 事件循环，用于调度线程池任务。
        :param output_dir: 保存生成截图的目录。
        """
        self.loop = loop
        self.output_dir = output_dir

    def _save_frame_to_jpeg(self, frame: av.VideoFrame, infohash_hex: str, timestamp_str: str):
        """
        将一个已解码的 PyAV 帧同步保存为 JPG 文件。
        这是一个阻塞 I/O 操作，因此应在工作线程中运行。
        """
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"

        try:
            # PyAV 的 to_image() 方法依赖于 Pillow 库来创建图像对象
            frame.to_image().save(output_filename, "JPEG")
            log.info(f"成功：截图已保存至 {output_filename}")
        except Exception:
            log.exception(f"保存帧到文件 {output_filename} 时发生错误。")
            raise

    def _decode_and_save(self, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        同步解码单个 H.264 数据包并将其保存为 JPG 图像。
        此方法被设计为在线程池执行器 (`run_in_executor`) 中运行，以避免阻塞主线程。

        :param extradata: 从 'avcC' box 提取的解码器配置信息（SPS/PPS）。如果为 None，则为带内模式。
        :param packet_data: 包含一个或多个 NALU 的原始数据包。
        :param infohash_hex: 用于命名文件的 infohash。
        :param timestamp_str: 用于命名文件的时间戳。
        """
        try:
            # 1. 创建 H.264 解码器上下文
            codec = av.CodecContext.create('h264', 'r')  # 'r' 表示读取（解码）模式

            # 2. 配置解码器
            if extradata:
                # '带外' (Out-of-band) 模式：解码器配置（如SPS/PPS）在码流之外提供。
                # 这通常来自 MP4 的 'avcC' box。
                log.debug(f"正在使用 {len(extradata)} 字节的 extradata (带外) 配置解码器...")
                codec.extradata = extradata
            else:
                # '带内' (In-band) 模式：配置信息内嵌在 H.264 码流中。
                # 解码器需要自行从数据包中解析。
                log.debug("未提供 extradata，解码器将尝试从码流 (带内) 中寻找配置。")

            # 3. 将数据包装成 PyAV 的 Packet 对象
            # 这是解码器唯一接受的输入格式。
            packet = av.Packet(packet_data)
            log.debug(f"将一个大小为 {packet.size} 字节的数据包送入解码器...")

            # 4. 解码数据包
            # decode() 方法可能会返回一个帧列表，也可能不返回（如果它需要更多数据来形成一个完整的帧）。
            frames = codec.decode(packet)

            # 5. 处理解码器延迟（获取缓存的帧）
            if not frames:
                # 某些解码器存在延迟，需要发送一个空的 "刷新" 包来取出内部缓冲的帧。
                log.warning("第一次解码未返回帧，尝试发送一个空的刷新包...")
                try:
                    frames = codec.decode(None)  # 发送刷新包
                except av.EOFError:
                    # 刷新时遇到 EOF 是正常的，可以忽略。
                    pass

            if not frames:
                log.error(f"解码失败：解码器未能从时间戳 {timestamp_str} 的数据包中解码出任何帧。")
                return

            # 通常一个关键帧数据包只解码出一个图像帧。我们只取第一个。
            self._save_frame_to_jpeg(frames[0], infohash_hex, timestamp_str)

        except av.error.InvalidDataError as e:
            # 当码流数据损坏或格式不正确时，PyAV 会抛出此异常。
            log.error(f"解码器报告无效数据 (时间戳: {timestamp_str}): {e}")
        except Exception:
            # 捕获所有其他潜在异常，以便记录详细的错误信息。
            log.exception(f"为帧 {timestamp_str} 进行同步解码/保存时发生未知错误")
            # 重新抛出异常，以便 run_in_executor 可以捕获它。
            raise

    async def generate(self, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        异步地启动截图生成过程。

        此方法是外部调用的主要入口点。它通过 `run_in_executor` 将
        CPU 密集型和阻塞的 `_decode_and_save` 方法调度到线程池中执行，
        从而使主 asyncio 事件循环保持非阻塞状态。
        """
        log.debug(f"正在为时间戳 {timestamp_str} 安排截图生成任务")

        await self.loop.run_in_executor(
            None,  # 使用默认的线程池执行器
            self._decode_and_save,  # 要在线程中运行的函数
            extradata,          # 以下是传递给该函数的参数
            packet_data,
            infohash_hex,
            timestamp_str
        )
