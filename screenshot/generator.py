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
import io
from typing import Optional, Callable, Awaitable
import av  # PyAV 库，用于音视频处理
from .errors import CodecNotFoundError, DecodingError, GeneratorError

# --- 配置日志 ---
log = logging.getLogger(__name__)


class ScreenshotGenerator:
    """
    通过向 PyAV 解码器提供数据包来处理截图的创建。
    此类封装了与 PyAV 的所有交互，并通过线程池实现了异步接口。
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, output_dir: str = './screenshots_output', on_success: Optional[Callable[[str, bytes, str], Awaitable[None]]] = None):
        """
        初始化截图生成器。

        :param loop: asyncio 事件循环，用于调度线程池任务。
        :param output_dir: 保存生成截图的目录 (在此版本中已废弃，但保留以兼容旧接口)。
        :param on_success: 一个可选的异步回调函数。成功生成截图后，
                           将从工作线程中安全地调用此回调，并传入 (infohash, image_bytes, timestamp_str)。
        """
        self.loop = loop
        self.on_success = on_success
        # output_dir is no longer used for saving but might be kept for other purposes if needed.
        self.output_dir = output_dir


    def _generate_jpeg_from_frame(self, frame: av.VideoFrame, infohash_hex: str, timestamp_str: str):
        """
        将一个已解码的 PyAV 帧同步地编码为 JPG 格式的字节串。
        这是一个阻塞 I/O 操作（编码到内存缓冲区），因此必须在工作线程中运行。
        """
        try:
            # PyAV 的 to_image() 方法依赖 Pillow 库来创建图像对象。
            # 我们将其保存到一个内存中的字节缓冲区，而不是磁盘文件。
            buffer = io.BytesIO()
            frame.to_image().save(buffer, format="JPEG")
            image_bytes = buffer.getvalue()
            log.info("成功：为 infohash %s 在时间戳 %s 处生成了 %d 字节的截图。", infohash_hex, timestamp_str, len(image_bytes))

            if self.on_success:
                # 从当前工作线程中，安全地在主事件循环上调度 on_success 回调。
                # 这是从同步世界（线程）与异步世界（事件循环）通信的标准方式。
                coro = self.on_success(infohash_hex, image_bytes, timestamp_str)
                asyncio.run_coroutine_threadsafe(coro, self.loop)
        except Exception:
            log.exception("为 infohash %s 生成帧 %s 的 JPG 时发生错误。", infohash_hex, timestamp_str)
            raise

    def _decode_and_generate(self, codec_name: str, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        同步解码单个视频数据包并将其编码为 JPG 图像字节。
        此方法被设计为在线程池执行器 (`run_in_executor`) 中运行，以避免阻塞主事件循环。

        :param codec_name: 要使用的编解码器名称 (例如 'h264', 'hevc', 'av1')。
        :param extradata: 从容器元数据中提取的解码器配置信息 (带外模式)。如果为 None，则为带内模式。
        :param packet_data: 包含一个或多个帧的原始数据包。
        :param infohash_hex: 用于日志和回调的 infohash。
        :param timestamp_str: 用于日志和回调的时间戳。
        """
        if not codec_name:
            log.error("解码失败：未提供编解码器名称。")
            raise ValueError("Codec name must be provided.")

        try:
            codec = av.CodecContext.create(codec_name, 'r')
        except ValueError as e:
            # PyAV 在找不到编解码器时会引发 ValueError 或其子类 (如 UnknownCodecError)
            log.error("无法创建编解码器 '%s'。它可能不受支持或名称无效。", codec_name)
            raise CodecNotFoundError(f"Codec '{codec_name}' not found or supported.") from e

        if extradata:
            codec.extradata = extradata

        try:
            all_frames = []
            # 1. 解码主数据包
            try:
                frames_from_packet = codec.decode(av.Packet(packet_data))
                if frames_from_packet:
                    all_frames.extend(frames_from_packet)
            except av.error.InvalidDataError as e:
                log.warning("解码数据包时遇到无效数据 (时间戳: %s, 编解码器: %s): %s。将尝试冲刷。", timestamp_str, codec_name, e)

            # 2. 冲刷解码器以获取所有缓冲的帧
            try:
                while True:
                    flushed_frames = codec.decode(None)
                    if not flushed_frames:
                        break
                    all_frames.extend(flushed_frames)
            except (av.EOFError, av.error.InvalidDataError):
                # EOF 是冲刷完成时的正常信号, InvalidDataError 也可能在冲刷时发生
                pass

            if not all_frames:
                log.error("解码失败：在解码和冲刷后，未能从时间戳 %s 的数据包中获得任何帧。", timestamp_str)
                raise DecodingError(f"No frames could be decoded for timestamp {timestamp_str} with codec {codec_name}.")

            # 3. 从解码后的第一帧生成 JPG
            log.info("成功从数据包中解码出 %d 帧，将使用第一帧生成截图。", len(all_frames))
            self._generate_jpeg_from_frame(all_frames[0], infohash_hex, timestamp_str)

        except av.error.InvalidDataError as e:
            log.error("解码器报告无效数据 (时间戳: %s, 编解码器: %s): %s", timestamp_str, codec_name, e)
            raise DecodingError(f"Invalid data for codec {codec_name} at timestamp {timestamp_str}") from e
        except Exception as e:
            # 如果是我们自己定义的生成器错误，直接重新抛出，不做重复包装。
            if isinstance(e, GeneratorError):
                raise
            # 对于其他未知异常，记录日志并包装为 GeneratorError。
            log.exception("为帧 %s (编解码器: %s) 进行同步解码/生成时发生未知错误", timestamp_str, codec_name)
            raise GeneratorError(f"An unexpected error occurred during generation for frame {timestamp_str}") from e

    async def generate(self, codec_name: str, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        异步地启动截图生成过程。

        此方法是外部调用的主要入口点。它通过 `run_in_executor` 将
        CPU 密集型和阻塞的 `_decode_and_generate` 方法调度到默认的线程池中执行，
        从而使主 asyncio 事件循环保持非阻塞状态。
        """
        log.debug("正在为时间戳 %s (编解码器: %s) 安排截图生成任务", timestamp_str, codec_name)

        await self.loop.run_in_executor(
            None,  # 使用默认的线程池执行器
            self._decode_and_generate,
            codec_name,
            extradata,
            packet_data,
            infohash_hex,
            timestamp_str
        )
