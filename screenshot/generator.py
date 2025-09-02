# -*- coding: utf-8 -*-
"""
本模块负责实际的截图生成过程。

核心组件是 `ScreenshotGenerator` 类，它利用 PyAV 库来解码单个视频数据包。
由于 PyAV 的解码操作是 CPU 密集型的同步阻塞操作，因此我们将这些操作
放在一个单独的线程池中执行，以避免阻塞主程序的 asyncio 事件循环。
"""
import logging
import os
import asyncio
from typing import Optional, Callable, Awaitable
import av  # PyAV 库，用于音视频处理

# --- 配置日志 ---
log = logging.getLogger(__name__)


class ScreenshotGenerator:
    """
    通过向 PyAV 解码器提供数据包来处理截图的创建。
    此类封装了与 PyAV 的所有交互，并通过线程池实现了异步接口。
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, output_dir: str = './screenshots_output', on_success: Optional[Callable[[str, str], Awaitable[None]]] = None):
        """
        初始化截图生成器。

        :param loop: asyncio 事件循环，用于调度线程池任务。
        :param output_dir: 保存生成截图的目录。
        :param on_success: 一个可选的异步回调函数。成功保存截图后，
                           将从工作线程中安全地调用此回调，并传入 (infohash, filepath)。
        """
        self.loop = loop
        self.output_dir = output_dir
        self.on_success = on_success

    def _save_frame_to_jpeg(self, frame: av.VideoFrame, infohash_hex: str, timestamp_str: str):
        """
        将一个已解码的 PyAV 帧同步保存为 JPG 文件。
        这是一个阻塞 I/O 操作，因此必须在工作线程中运行。
        """
        os.makedirs(self.output_dir, exist_ok=True)
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"

        try:
            # PyAV 的 to_image() 方法依赖 Pillow 库来创建图像对象并保存。
            frame.to_image().save(output_filename, "JPEG")
            log.info("成功：截图已保存至 %s", output_filename)

            if self.on_success:
                # 从当前工作线程中，安全地在主事件循环上调度 on_success 回调。
                # 这是从同步世界（线程）与异步世界（事件循环）通信的标准方式。
                coro = self.on_success(infohash_hex, output_filename)
                asyncio.run_coroutine_threadsafe(coro, self.loop)
        except Exception:
            log.exception("保存帧到文件 %s 时发生错误。", output_filename)
            raise

    def _decode_and_save(self, codec_name: str, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        同步解码单个视频数据包并将其保存为 JPG 图像。
        此方法被设计为在线程池执行器 (`run_in_executor`) 中运行，以避免阻塞主事件循环。

        :param codec_name: 要使用的编解码器名称 (例如 'h264', 'hevc', 'av1')。
        :param extradata: 从容器元数据中提取的解码器配置信息 (带外模式)。如果为 None，则为带内模式。
        :param packet_data: 包含一个或多个帧的原始数据包。
        :param infohash_hex: 用于命名文件的 infohash。
        :param timestamp_str: 用于命名文件的时间戳。
        """
        if not codec_name:
            log.error("解码失败：未提供编解码器名称。")
            return
        try:
            # 1. 根据名称动态创建解码器上下文
            codec = av.CodecContext.create(codec_name, 'r')  # 'r' 表示读取（解码）模式

            # 2. 如果提供了 extradata (带外模式)，则配置解码器
            if extradata:
                codec.extradata = extradata

            # 3. 将原始数据包装成 PyAV 的 Packet 对象并送入解码器
            packet = av.Packet(packet_data)
            frames = codec.decode(packet)

            # 4. 处理解码器延迟：某些解码器可能不会立即返回帧，需要发送一个空包来“冲刷”内部缓冲区。
            if not frames:
                log.warning("第一次解码未返回帧，尝试发送一个空的刷新包...")
                try:
                    frames = codec.decode(None)  # 发送刷新包
                except av.EOFError:
                    # 解码器在冲刷时可能会正常地抛出 EOFError
                    pass

            if not frames:
                log.error("解码失败：解码器未能从时间戳 %s 的数据包中解码出任何帧。", timestamp_str)
                return

            # 5. 保存解码后的第一帧
            self._save_frame_to_jpeg(frames[0], infohash_hex, timestamp_str)

        except av.error.InvalidDataError as e:
            log.error("解码器报告无效数据 (时间戳: %s, 编解码器: %s): %s", timestamp_str, codec_name, e)
            raise
        except Exception:
            log.exception("为帧 %s (编解码器: %s) 进行同步解码/保存时发生未知错误", timestamp_str, codec_name)
            raise

    async def generate(self, codec_name: str, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        异步地启动截图生成过程。

        此方法是外部调用的主要入口点。它通过 `run_in_executor` 将
        CPU 密集型和阻塞的 `_decode_and_save` 方法调度到默认的线程池中执行，
        从而使主 asyncio 事件循环保持非阻塞状态。
        """
        log.debug("正在为时间戳 %s (编解码器: %s) 安排截图生成任务", timestamp_str, codec_name)

        await self.loop.run_in_executor(
            None,  # 使用默认的线程池执行器
            self._decode_and_save,
            codec_name,
            extradata,
            packet_data,
            infohash_hex,
            timestamp_str
        )
