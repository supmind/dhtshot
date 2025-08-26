# -*- coding: utf-8 -*-
"""
This module contains the ScreenshotGenerator class, responsible for
taking keyframe data and generating a JPG screenshot using pyav.
"""
import io
import logging
import struct
import os
import av
import asyncio

class ScreenshotGenerator:
    """Handles the creation of a screenshot from moov and keyframe data."""
    def __init__(self, loop, output_dir='./screenshots_output'):
        self.loop = loop
        self.output_dir = output_dir
        self.log = logging.getLogger("ScreenshotGenerator")

    def _create_minimal_mp4(self, moov_data, keyframe_data):
        """
        Creates a minimal, valid MP4 file in memory containing the necessary
        context (moov) and data (mdat) to decode a single keyframe.
        """
        ftyp_box = b'\x00\x00\x00\x18ftypisom\x00\x00\x02\x00iso2avc1mp41'
        mdat_size = len(keyframe_data) + 8
        mdat_header = struct.pack('>I', mdat_size) + b'mdat'
        mdat_box = mdat_header + keyframe_data
        return ftyp_box + moov_data + mdat_box

    def _save_frame_to_jpeg(self, frame, infohash_hex, timestamp_str):
        """Saves a decoded PyAV frame to a JPG file."""
        output_filename = f"{self.output_dir}/{infohash_hex}_{timestamp_str.replace(':', '-')}.jpg"
        os.makedirs(self.output_dir, exist_ok=True)
        frame.to_image().save(output_filename)
        self.log.info(f"Success: Screenshot saved to {output_filename}")

    async def generate(self, moov_data, keyframe_data, keyframe_info, infohash_hex, timestamp_str):
        """
        Generates and saves a screenshot from moov and keyframe data by
        creating a minimal MP4 in memory and decoding it.
        """
        self.log.debug(f"Creating and decoding minimal MP4 for timestamp {timestamp_str} (PTS: {keyframe_info.pts})")

        def decode_and_save_sync():
            try:
                minimal_mp4_bytes = self._create_minimal_mp4(moov_data, keyframe_data)
                with av.open(io.BytesIO(minimal_mp4_bytes), 'r') as container:
                    frame = next(container.decode(video=0))
                    self._save_frame_to_jpeg(frame, infohash_hex, timestamp_str)
            except Exception as e:
                self.log.exception(f"Error in synchronous decode/save for frame {timestamp_str}: {e}")

        await self.loop.run_in_executor(None, decode_and_save_sync)
