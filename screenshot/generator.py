# -*- coding: utf-8 -*-
"""
该模块包含 ScreenshotGenerator 类，它使用 PyAV 通过拼接 H.264 裸流的方式来解码和生成截图。
"""
import logging
import os
import av

class ScreenshotGenerator:
    """通过拼接 H.264 裸流的方式处理截图创建。"""
    def __init__(self, loop, output_dir='./screenshots_output'):
        self.loop = loop
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotGenerator")
        self.start_code = b'\x00\x00\x00\x01'

    def _save_frame_to_jpeg(self, frame, infohash_hex, timestamp_str):
        """将解码后的 PyAV 帧保存为 JPG 文件。"""
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
        os.makedirs(self.output_dir, exist_ok=True)
        frame.to_image().save(output_filename)
        self.log.info(f"成功：截图已保存至 {output_filename}")

    async def generate(self, sps: bytes, pps: bytes, keyframe_data: bytes, infohash_hex: str, timestamp_str: str):
        """
        通过将 SPS, PPS 和一个 I-Frame 拼接成一个可解码的 H.264 裸流来生成截图。
        """
        self.log.debug(f"正在为时间戳 {timestamp_str} 生成截图 (H.264 裸流方法)")

        def decode_and_save_sync():
            """在线程池中运行的同步解码和保存操作。"""
            try:
                codec = av.CodecContext.create('h264', 'r')

                # 拼接码流
                stream_data = self.start_code + sps + self.start_code + pps + self.start_code + keyframe_data

                packets = codec.parse(stream_data)
                if not packets:
                    self.log.warning(f"无法从时间戳 {timestamp_str} 的码流中解析出数据包。")
                    return

                frames = codec.decode(packets[0])
                if not frames:
                    self.log.warning(f"无法从时间戳 {timestamp_str} 的数据包中解码出帧。")
                    return

                self._save_frame_to_jpeg(frames[0], infohash_hex, timestamp_str)

            except Exception as e:
                self.log.exception(f"为帧 {timestamp_str} 进行同步解码/保存时出错: {e}")

        await self.loop.run_in_executor(None, decode_and_save_sync)
