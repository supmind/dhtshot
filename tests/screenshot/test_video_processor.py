# -*- coding: utf-8 -*-
"""
对 screenshot/video_processor.py 视频处理逻辑的单元测试。
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from screenshot.video_processor import VideoProcessor
from config import Settings
from screenshot.extractor import Keyframe
from screenshot.client import TorrentClient


@pytest.fixture
def settings():
    return Settings(min_screenshots=2, max_screenshots=5, default_screenshots=3, target_interval_sec=60, moov_probe_timeout=5, piece_fetch_timeout=5, piece_queue_timeout=5)

@pytest.fixture
def processor(settings):
    loop = asyncio.get_event_loop()
    processor_instance = VideoProcessor(
        settings=settings,
        client=AsyncMock(spec=TorrentClient),
        generator=AsyncMock(),
        loop=loop,
    )
    # Attach a mock client for easy access in tests
    processor_instance.mock_client = processor_instance.client
    return processor_instance


class TestAssembleData:
    def test_assemble_from_single_piece(self, processor):
        pieces_data = {0: b"0123456789"}
        result = processor._assemble_data_from_pieces(pieces_data, offset_in_torrent=2, size=5, piece_length=10)
        assert result == b"23456"

    def test_assemble_spanning_two_pieces(self, processor):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = processor._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=4, piece_length=10)
        assert result == b"89ab"

    def test_assemble_spanning_multiple_pieces(self, processor):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij", 2: b"KLMNOPQRST"}
        result = processor._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b"89abcdefghijKLM"

    def test_assemble_at_piece_boundary(self, processor):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = processor._assemble_data_from_pieces(pieces_data, offset_in_torrent=10, size=5, piece_length=10)
        assert result == b"abcde"

    def test_assemble_with_missing_piece(self, processor):
        pieces_data = {0: b"0123456789", 2: b"KLMNOPQRST"}
        result = processor._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b""


def test_select_keyframes_logic(processor):
    all_keyframes = [
        Keyframe(0, 0, 0, 90000), Keyframe(1, 1, 10 * 90000, 90000),
        Keyframe(2, 2, 20 * 90000, 90000), Keyframe(3, 3, 88 * 90000, 90000),
        Keyframe(4, 4, 95 * 90000, 90000), Keyframe(5, 5, 170 * 90000, 90000)
    ]
    duration_pts = 180 * 90000
    selected = processor._select_keyframes(all_keyframes, 90000, duration_pts, None)
    assert len(selected) == 3
    selected_pts = {kf.pts for kf in selected}
    expected_pts = {0, 88 * 90000, 95 * 90000}
    assert selected_pts == expected_pts


@pytest.mark.asyncio
async def test_get_moov_atom_fetches_full_box_on_partial_find(processor):
    mock_handle = MagicMock()
    partial_moov_data = b'\x00\x00\x03\xe8moov' + b'\x01' * 42
    full_moov_data = b'\x00\x00\x03\xe8moov' + b'\xff' * (1000 - 8)

    processor.mock_client.fetch_pieces.side_effect = [
        {0: partial_moov_data},
        {0: full_moov_data[:512], 1: full_moov_data[512:]}
    ]
    with patch.object(processor, '_assemble_data_from_pieces', new_callable=MagicMock) as mock_assemble:
        mock_assemble.side_effect = [partial_moov_data, full_moov_data]
        result_moov_data = await processor._get_moov_atom_data(mock_handle, 0, 2000, 512, "partial_moov_hash")

    assert result_moov_data == full_moov_data
    assert processor.mock_client.fetch_pieces.call_count == 2
