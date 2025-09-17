# -*- coding: utf-8 -*-
"""
对 screenshot/moov_finder.py 的单元测试。
"""
import pytest
from unittest.mock import AsyncMock, patch

from screenshot.moov_finder import MoovFinder
from screenshot.piece_manager import PieceManager
from config import Settings
from screenshot.errors import MoovNotFoundError


@pytest.fixture
def settings():
    return Settings(moov_head_probe_size=1024, moov_probe_timeout=5)

@pytest.fixture
def moov_finder(settings):
    mock_piece_manager = AsyncMock(spec=PieceManager)
    finder = MoovFinder(
        piece_manager=mock_piece_manager,
        video_file_offset=0,
        video_file_size=5168, # A realistic size
        settings=settings,
        infohash_hex="test_hash"
    )
    return finder

@pytest.mark.asyncio
async def test_find_moov_in_head(moov_finder):
    moov_box_data = b'\x00\x00\x00\x90moov' + b'\x01' * 136

    # Set the return value for the piece manager call
    moov_finder.piece_manager.fetch_and_assemble_range.return_value = b'some_head_data'

    with patch.object(moov_finder, '_parse_mp4_boxes') as mock_parse:
        # Simulate the parser finding the moov box on the first try
        mock_parse.return_value = [('moov', moov_box_data, 0, 144)]

        result = await moov_finder.find_moov_atom()

        assert result == moov_box_data
        moov_finder.piece_manager.fetch_and_assemble_range.assert_awaited_once()
        mock_parse.assert_called_once()

@pytest.mark.asyncio
async def test_find_moov_in_tail(moov_finder):
    header_data = b'\x00\x00\x00\x18ftypiso5' + b'\x00\x00\x13\x88mdat'
    moov_box_data = b'\x00\x00\x00\x90moov' + b'\x02' * 136

    moov_finder.piece_manager.fetch_and_assemble_range.side_effect = [header_data, moov_box_data]

    with patch.object(moov_finder, '_parse_mp4_boxes') as mock_parse:
        def parse_side_effect(stream):
            if stream.getvalue() == header_data:
                yield ('ftyp', b'', 0, 24)
                yield ('mdat', b'', 24, 5000)
            elif stream.getvalue() == moov_box_data:
                yield ('moov', moov_box_data, 0, 144)
            else:
                yield ('unknown', b'', 0, 0)

        mock_parse.side_effect = parse_side_effect

        result = await moov_finder.find_moov_atom()

        assert result == moov_box_data
        assert moov_finder.piece_manager.fetch_and_assemble_range.call_count == 2
        assert mock_parse.call_count == 2

@pytest.mark.asyncio
async def test_moov_not_found(moov_finder):
    moov_finder.piece_manager.fetch_and_assemble_range.return_value = b'no_moov_here_just_garbage'

    with patch.object(moov_finder, '_parse_mp4_boxes') as mock_parse:
        mock_parse.return_value = [('unkn', b'', 0, 0)]

        with pytest.raises(MoovNotFoundError):
            await moov_finder.find_moov_atom()

        moov_finder.piece_manager.fetch_and_assemble_range.assert_awaited_once()
        mock_parse.assert_called_once()
