# -*- coding: utf-8 -*-
"""
该模块包含 ScreenshotGenerator 类，它使用 PyAV 通过发送 H.264 数据包的方式来解码和生成截图。
"""
import logging
import os
from typing import Optional
import av


class ScreenshotGenerator:
    """通过向 PyAV 解码器发送 H.264 数据包来处理截图创建。"""
    def __init__(self, loop, output_dir='./screenshots_output'):
        self.loop = loop
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotGenerator")

    def _save_frame_to_jpeg(self, frame, infohash_hex, timestamp_str):
        """将解码后的 PyAV 帧保存为 JPG 文件。"""
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
        os.makedirs(self.output_dir, exist_ok=True)
        frame.to_image().save(output_filename)
        self.log.info(f"成功：截图已保存至 {output_filename}")

    def _decode_and_save(self, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        同步解码 H.264 数据包并保存为 JPG 文件。
        此方法设计为在线程池执行器中运行。
        """
        try:
            codec = av.CodecContext.create('h264', 'r')
            if extradata:
                self.log.debug(f"正在使用 {len(extradata)} 字节的 extradata (带外) 配置解码器...")
                codec.extradata = extradata
            else:
                self.log.debug("未提供 extradata，解码器将尝试从码流 (带内) 中寻找配置。")

            packet = av.Packet(packet_data)
            self.log.debug(f"将一个大小为 {packet.size} 字节的数据包送入解码器...")

            frames = codec.decode(packet)
            if not frames:
                self.log.warning("第一次解码未返回帧，尝试发送一个空的刷新包...")
                try:
                    frames = codec.decode(None) # Flushing packet
                except av.EOFError:
                    pass # Expected when flushing

            if not frames:
                self.log.error(f"解码失败：解码器未能从时间戳 {timestamp_str} 的数据包中解码出任何帧。")
                return

            self._save_frame_to_jpeg(frames[0], infohash_hex, timestamp_str)

        except av.error.InvalidDataError as e:
            self.log.error(f"解码器报告无效数据 (时间戳: {timestamp_str}): {e}")
        except Exception as e:
            self.log.exception(f"为帧 {timestamp_str} 进行同步解码/保存时出错")
            raise e

    async def generate(self, extradata: Optional[bytes], packet_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        通过将 H.264 数据包发送到解码器来生成截图。
        解码过程在线程池中异步执行。
        """
        self.log.debug(f"正在为时间戳 {timestamp_str} 生成截图")

        await self.loop.run_in_executor(
            None, self._decode_and_save, extradata, packet_data, infohash_hex, timestamp_str
        )
