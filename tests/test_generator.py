# -*- coding: utf-8 -*-
import asyncio
import pytest
from unittest.mock import MagicMock, patch

from screenshot.generator import ScreenshotGenerator

@pytest.fixture(autouse=True)
def mock_av():
    """Mock the av library at the module level to avoid real decoding."""
    with patch('screenshot.generator.av') as mock_av_library:
        # Mock the AVError to be a subclass of Exception for safe catching
        mock_av_library.AVError = type('AVError', (Exception,), {})
        yield mock_av_library

@pytest.mark.asyncio
async def test_generate_success(mock_av):
    """
    Tests the success path of the H.264 stream generation method.
    """
    loop = asyncio.get_running_loop()
    generator = ScreenshotGenerator(loop=loop)

    # Configure mocks
    mock_codec_context = mock_av.CodecContext.create.return_value
    mock_packets = [MagicMock()]
    mock_frames = [MagicMock()]
    mock_codec_context.parse.return_value = mock_packets
    mock_codec_context.decode.return_value = mock_frames

    with patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        await generator.generate(
            sps=b'sps_data',
            pps=b'pps_data',
            keyframe_data=b'keyframe',
            infohash_hex='infohash_success',
            timestamp_str='ts_success'
        )

        # Assertions
        mock_av.CodecContext.create.assert_called_once_with('h264', 'r')
        mock_codec_context.parse.assert_called_once()
        mock_codec_context.decode.assert_called_once_with(mock_packets[0])
        mock_save.assert_called_once_with(mock_frames[0], 'infohash_success', 'ts_success')

@pytest.mark.asyncio
async def test_generate_parsing_fails(mock_av):
    """
    Tests that a warning is logged if parsing the stream yields no packets.
    """
    loop = asyncio.get_running_loop()
    generator = ScreenshotGenerator(loop=loop)
    mock_codec_context = mock_av.CodecContext.create.return_value
    mock_codec_context.parse.return_value = [] # No packets

    with patch.object(generator.log, 'warning') as mock_log_warning:
        await generator.generate(b'', b'', b'', 'infohash', 'ts')
        mock_log_warning.assert_called_once()

@pytest.mark.asyncio
async def test_generate_decoding_fails(mock_av):
    """
    Tests that a warning is logged if decoding a packet yields no frames.
    """
    loop = asyncio.get_running_loop()
    generator = ScreenshotGenerator(loop=loop)
    mock_codec_context = mock_av.CodecContext.create.return_value
    mock_codec_context.parse.return_value = [MagicMock()]
    mock_codec_context.decode.return_value = [] # No frames

    with patch.object(generator.log, 'warning') as mock_log_warning:
        await generator.generate(b'', b'', b'', 'infohash', 'ts')
        mock_log_warning.assert_called_once()
