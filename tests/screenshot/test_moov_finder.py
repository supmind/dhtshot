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

import os

@pytest.mark.asyncio
async def test_find_moov_in_tail_with_real_video_file(moov_finder, settings):
    """
    Tests the tail-probe logic using a real video file where the moov atom
    is at the end.
    """
    video_path = os.path.join("tests", "assets", "test_moov_at_end.mp4")
    with open(video_path, "rb") as f:
        video_data = f.read()

    file_size = len(video_data)
    moov_finder.video_file_size = file_size

    # Simulate the two-phase fetch
    head_probe_size = settings.moov_head_probe_size
    head_data = video_data[:head_probe_size]

    # The real file's mdat box will be found, and its size will be used
    # to calculate the tail offset. We don't need to know the exact tail offset
    # in the test, just that the second fetch will be for the rest of the file.
    # We find the 'mdat' atom to calculate where the tail begins
    import io, struct
    stream = io.BytesIO(head_data)
    mdat_offset = -1
    mdat_size = -1
    cursor = 0
    while cursor <= len(head_data) - 8:
        size_bytes = head_data[cursor:cursor+4]
        type_bytes = head_data[cursor+4:cursor+8]
        size = struct.unpack('>I', size_bytes)[0]
        if type_bytes == b'mdat':
            mdat_offset = cursor
            mdat_size = size
            break
        cursor += size

    assert mdat_offset != -1, "Test setup error: mdat box not found in head probe of real video file"

    tail_offset = mdat_offset + mdat_size
    tail_data = video_data[tail_offset:]

    moov_finder.piece_manager.fetch_and_assemble_range.side_effect = [head_data, tail_data]

    # Now, run the actual finder logic
    result_moov = await moov_finder.find_moov_atom()

    # The result should not be empty and should contain the 'moov' atom type identifier
    assert result_moov is not None
    assert len(result_moov) > 8
    assert result_moov[4:8] == b'moov'

    # Verify that the two-phase probe happened
    assert moov_finder.piece_manager.fetch_and_assemble_range.call_count == 2

@pytest.mark.asyncio
async def test_find_moov_after_garbage(moov_finder):
    """Tests that the parser can find a box even if it's preceded by garbage."""
    moov_box_data = b'\x00\x00\x00\x90moov' + b'\x01' * 136
    # Simulate a data chunk with invalid bytes at the beginning
    data_with_garbage = b'\xff\xff\xff\xff' + moov_box_data

    moov_finder.piece_manager.fetch_and_assemble_range.return_value = data_with_garbage

    # We call the real parser, not a mock
    result = await moov_finder.find_moov_atom()

    assert result == moov_box_data

@pytest.mark.asyncio
async def test_moov_not_found(moov_finder):
    moov_finder.piece_manager.fetch_and_assemble_range.return_value = b'no_moov_here_just_garbage'

    with patch.object(moov_finder, '_parse_mp4_boxes') as mock_parse:
        mock_parse.return_value = [('unkn', b'', 0, 0)]

        with pytest.raises(MoovNotFoundError):
            await moov_finder.find_moov_atom()

        moov_finder.piece_manager.fetch_and_assemble_range.assert_awaited_once()
        mock_parse.assert_called_once()
