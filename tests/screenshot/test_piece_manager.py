# -*- coding: utf-8 -*-
"""
对 screenshot/piece_manager.py 的单元测试。
"""
import pytest
from unittest.mock import AsyncMock

from screenshot.piece_manager import PieceManager
from screenshot.client import TorrentClient


@pytest.fixture
def piece_manager():
    mock_client = AsyncMock(spec=TorrentClient)
    # Mock handle can be a simple object
    mock_handle = object()
    manager = PieceManager(client=mock_client, handle=mock_handle, piece_length=10, infohash_hex="test_hash")
    return manager


class TestAssembleData:
    def test_assemble_from_single_piece(self, piece_manager):
        pieces_data = {0: b"0123456789"}
        result = piece_manager._assemble_data_from_pieces(pieces_data, offset_in_torrent=2, size=5)
        assert result == b"23456"

    def test_assemble_spanning_two_pieces(self, piece_manager):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = piece_manager._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=4)
        assert result == b"89ab"

    def test_assemble_spanning_multiple_pieces(self, piece_manager):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij", 2: b"KLMNOPQRST"}
        result = piece_manager._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15)
        assert result == b"89abcdefghijKLM"

    def test_assemble_at_piece_boundary(self, piece_manager):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = piece_manager._assemble_data_from_pieces(pieces_data, offset_in_torrent=10, size=5)
        assert result == b"abcde"

    def test_assemble_with_missing_piece(self, piece_manager):
        pieces_data = {0: b"0123456789", 2: b"KLMNOPQRST"}
        result = piece_manager._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15)
        assert result == b""

    def test_get_pieces_for_range(self, piece_manager):
        # piece_length is 10
        assert piece_manager._get_pieces_for_range(offset_in_torrent=5, size=3) == [0]
        assert piece_manager._get_pieces_for_range(offset_in_torrent=8, size=4) == [0, 1]
        assert piece_manager._get_pieces_for_range(offset_in_torrent=10, size=10) == [1]
        assert piece_manager._get_pieces_for_range(offset_in_torrent=10, size=11) == [1, 2]
        assert piece_manager._get_pieces_for_range(offset_in_torrent=0, size=0) == []

@pytest.mark.asyncio
async def test_fetch_and_assemble_range(piece_manager):
    piece_manager.client.fetch_pieces.return_value = {0: b"0123456789", 1: b"abcdefghij"}

    result = await piece_manager.fetch_and_assemble_range(offset=8, size=4, timeout=5)

    assert result == b"89ab"
    piece_manager.client.fetch_pieces.assert_awaited_once_with(piece_manager.handle, [0, 1], timeout=5)
