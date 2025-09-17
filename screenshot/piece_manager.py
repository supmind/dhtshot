# -*- coding: utf-8 -*-
"""
This module defines the PieceManager, which is responsible for fetching and
assembling data pieces from a torrent.
"""
import asyncio
import logging
from typing import Dict

from .client import TorrentClient, TorrentClientError


class PieceManager:
    """
    Handles fetching and assembling data pieces for a specific torrent.
    """
    def __init__(self, client: TorrentClient, handle, piece_length: int, infohash_hex: str):
        self.client = client
        self.handle = handle
        self.piece_length = piece_length
        self.infohash_hex = infohash_hex
        self.log = logging.getLogger("PieceManager")

    def _get_pieces_for_range(self, offset_in_torrent: int, size: int) -> list[int]:
        """Calculates all piece indices covering a given byte range in the torrent."""
        if size <= 0:
            return []
        start_piece = offset_in_torrent // self.piece_length
        end_piece = (offset_in_torrent + size - 1) // self.piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data: Dict[int, bytes], offset_in_torrent: int, size: int) -> bytes:
        """Assembles the required data segment from a dictionary of piece data."""
        start_piece = offset_in_torrent // self.piece_length
        end_piece = (offset_in_torrent + size - 1) // self.piece_length

        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data:
                self.log.warning("[%s] Missing piece #%d when assembling data.", self.infohash_hex, piece_index)
                return b""

        buffer = bytearray(size)
        buffer_offset = 0
        for piece_index in range(start_piece, end_piece + 1):
            piece_data = pieces_data[piece_index]
            copy_from_start = offset_in_torrent % self.piece_length if piece_index == start_piece else 0
            copy_to_end = (offset_in_torrent + size - 1) % self.piece_length + 1 if piece_index == end_piece else self.piece_length
            chunk = piece_data[copy_from_start:copy_to_end]
            bytes_to_copy = min(len(chunk), size - buffer_offset)
            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy
        return bytes(buffer)

    async def fetch_and_assemble_range(self, offset: int, size: int, timeout: int) -> bytes:
        """
        Fetches all necessary pieces for a given range and assembles them.
        """
        if size <= 0:
            return b""

        needed_pieces = self._get_pieces_for_range(offset, size)
        if not needed_pieces:
            return b""

        try:
            pieces_data = await self.client.fetch_pieces(self.handle, needed_pieces, timeout=timeout)
        except TorrentClientError as e:
            self.log.warning("[%s] Failed to fetch pieces for range (%d, %d): %s", self.infohash_hex, offset, size, e)
            # Re-raise or handle as appropriate for the caller
            raise

        return self._assemble_data_from_pieces(pieces_data, offset, size)
