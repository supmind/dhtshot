# -*- coding: utf-8 -*-
"""
该模块包含 ScreenshotGenerator 类，负责接收关键帧数据并使用 PyAV 生成 JPG 格式的截图。
"""
import io
import logging
import struct
import os
import av
import asyncio

class ScreenshotGenerator:
    """处理从 moov 和关键帧数据创建截图的过程。"""
    def __init__(self, loop, output_dir='./screenshots_output'):
        self.loop = loop
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotGenerator")

    def _create_minimal_mp4(self, moov_data, keyframe_data):
        """
        在内存中创建一个最小化的、有效的 MP4 文件。
        该文件包含解码单个关键帧所需的上下文（moov）和数据（mdat）。
        """
        ftyp_box = b'\x00\x00\x00\x18ftypisom\x00\x00\x02\x00iso2avc1mp41'
        mdat_size = len(keyframe_data) + 8
        mdat_header = struct.pack('>I', mdat_size) + b'mdat'
        mdat_box = mdat_header + keyframe_data
        return ftyp_box + moov_data + mdat_box

    def _save_frame_to_jpeg(self, frame, infohash_hex, timestamp_str):
        """将解码后的 PyAV 帧保存为 JPG 文件。"""
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
        os.makedirs(self.output_dir, exist_ok=True)
        frame.to_image().save(output_filename)
        self.log.info(f"成功：截图已保存至 {output_filename}")

    async def generate(self, moov_data, keyframe_data, keyframe_info, infohash_hex, timestamp_str):
        """
        通过在内存中创建并解码一个最小化的 MP4，
        从 moov 和关键帧数据生成并保存截图。
        """
        self.log.debug(f"正在为时间戳 {timestamp_str} (PTS: {keyframe_info.pts}) 创建并解码最小化 MP4")

        def decode_and_save_sync():
            """同步执行的解码和保存操作，以在线程池中运行。"""
            try:
                minimal_mp4_bytes = self._create_minimal_mp4(moov_data, keyframe_data)
                with av.open(io.BytesIO(minimal_mp4_bytes), 'r') as container:
                    frame = next(container.decode(video=0))
                    self._save_frame_to_jpeg(frame, infohash_hex, timestamp_str)
            except Exception as e:
                self.log.exception(f"为帧 {timestamp_str} 进行同步解码/保存时出错: {e}")

        # 在线程池中运行同步的解码函数，避免阻塞事件循环
        await self.loop.run_in_executor(None, decode_and_save_sync)
