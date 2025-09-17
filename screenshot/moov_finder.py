# -*- coding: utf-8 -*-
"""
This module defines the MoovFinder, which is responsible for intelligently
finding and extracting the 'moov' atom from a video file in a torrent.
"""
import io
import logging
import struct
from typing import Generator, Tuple

from .errors import MoovFetchError, MoovNotFoundError, MP4ParsingError
from .piece_manager import PieceManager
from config import Settings


class MoovFinder:
    """
    Finds and extracts the 'moov' atom from a video file, using an
    intelligent probing strategy to minimize downloads.
    """
    def __init__(self, piece_manager: PieceManager, video_file_offset: int, video_file_size: int, settings: Settings, infohash_hex: str):
        self.piece_manager = piece_manager
        self.video_file_offset = video_file_offset
        self.video_file_size = video_file_size
        self.settings = settings
        self.infohash_hex = infohash_hex
        self.log = logging.getLogger("MoovFinder")

    def _parse_mp4_boxes(self, stream: io.BytesIO) -> Generator[Tuple[str, bytes, int, int], None, None]:
        """A robust MP4 box parser."""
        stream_buffer = stream.getbuffer()
        buffer_size = len(stream_buffer)
        current_offset = stream.tell()
        while current_offset <= buffer_size - 8:
            stream.seek(current_offset)
            try:
                header_data = stream.read(8)
                if len(header_data) < 8:
                    break
                declared_size, box_type_bytes = struct.unpack('>I4s', header_data)
                box_type = box_type_bytes.decode('ascii', 'ignore')
            except struct.error:
                self.log.warning("[%s] struct.error while parsing MP4 box header at offset %d.", self.infohash_hex, current_offset)
                break

            box_header_size = 8
            if declared_size == 1:
                if current_offset + 16 > buffer_size:
                    break
                declared_size = struct.unpack('>Q', stream.read(8))[0]
                box_header_size = 16
            elif declared_size == 0:
                declared_size = buffer_size - current_offset

            if declared_size < box_header_size:
                self.log.warning("[%s] Invalid box size %d at offset %d.", self.infohash_hex, declared_size, current_offset)
                break

            effective_box_size = declared_size
            if current_offset + declared_size > buffer_size:
                effective_box_size = buffer_size - current_offset

            box_content = stream_buffer[current_offset: current_offset + effective_box_size]
            yield box_type, bytes(box_content), current_offset, declared_size

            current_offset += declared_size

    async def find_moov_atom(self) -> bytes:
        """
        Intelligently finds and returns the full 'moov' atom data.
        It first probes the head of the file, and if not found, probes the tail.
        """
        mdat_info = None

        # 1. Probe the head of the file
        try:
            head_size = min(self.settings.moov_head_probe_size, self.video_file_size)
            if head_size > 0:
                head_data = await self.piece_manager.fetch_and_assemble_range(
                    self.video_file_offset, head_size, self.settings.moov_probe_timeout
                )
                stream = io.BytesIO(head_data)
                for box_type, partial_box_data, box_offset, declared_size in self._parse_mp4_boxes(stream):
                    if box_type == 'moov':
                        # If we got the full box, return it
                        if len(partial_box_data) >= declared_size:
                            return partial_box_data

                        # We found a partial moov box, fetch the full data
                        self.log.info("[%s] Found partial 'moov' at head, fetching full box.", self.infohash_hex)
                        full_moov_offset_in_torrent = self.video_file_offset + box_offset
                        return await self.piece_manager.fetch_and_assemble_range(
                            full_moov_offset_in_torrent, declared_size, self.settings.moov_probe_timeout
                        )

                    if box_type == 'mdat':
                        mdat_info = {'offset': box_offset, 'size': declared_size}
        except Exception as e:
            raise MoovFetchError(f"Failed to fetch pieces during moov head probe: {e}", self.infohash_hex) from e

        # 2. If moov not in head, try the tail (if mdat was found)
        if mdat_info:
            try:
                mdat_end_offset_in_file = mdat_info['offset'] + mdat_info['size']
                if mdat_end_offset_in_file >= self.video_file_size:
                    raise MoovNotFoundError(f"mdat box seems to extend to or past the end of the file.", self.infohash_hex)

                tail_torrent_offset = self.video_file_offset + mdat_end_offset_in_file
                tail_size = self.video_file_size - mdat_end_offset_in_file

                if tail_size > 0:
                    self.log.info("[%s] 'moov' not in head, probing tail.", self.infohash_hex)
                    tail_data = await self.piece_manager.fetch_and_assemble_range(
                        tail_torrent_offset, tail_size, self.settings.moov_probe_timeout
                    )
                    stream = io.BytesIO(tail_data)
                    for box_type, box_data, _, _ in self._parse_mp4_boxes(stream):
                        if box_type == 'moov':
                            return box_data
            except Exception as e:
                raise MoovFetchError(f"Failed during intelligent moov tail probe: {e}", self.infohash_hex) from e

        raise MoovNotFoundError("Could not locate 'moov' atom in the file's head or tail.", self.infohash_hex)
