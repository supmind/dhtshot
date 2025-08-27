# -*- coding: utf-8 -*-
import pytest
import av as real_av
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.generator import ScreenshotGenerator

@pytest.fixture
def generator():
    """Provides a ScreenshotGenerator instance with a mock loop."""
    return ScreenshotGenerator(loop=MagicMock())

@patch('os.makedirs')
@patch('screenshot.generator.av')
def test_save_frame_to_jpeg(mock_av, mock_makedirs, generator):
    """
    Tests that the JPEG saving function calls the correct methods.
    """
    mock_frame = MagicMock()
    mock_image = MagicMock()
    mock_frame.to_image.return_value = mock_image

    with patch.object(mock_image, 'save') as mock_save:
        generator._save_frame_to_jpeg(mock_frame, "infohash", "timestamp")

        mock_makedirs.assert_called_once_with(generator.output_dir, exist_ok=True)
        mock_save.assert_called_once_with(f"{generator.output_dir}/infohash_timestamp.jpg")


@pytest.mark.asyncio
@patch('screenshot.generator.av')
async def test_generate_calls_executor(mock_av, generator):
    """
    Tests that the main `generate` method calls the executor with the correct data.
    """
    generator.loop.run_in_executor = AsyncMock()

    await generator.generate(
        extradata=b'extra', packet_data=b'packet',
        infohash_hex='infohash', timestamp_str='ts'
    )

    generator.loop.run_in_executor.assert_awaited_once_with(
        None, generator._decode_and_save, b'extra', b'packet', 'infohash', 'ts'
    )

@patch('screenshot.generator.av')
def test_decode_and_save_success_with_extradata(mock_av, generator):
    """
    Tests the success path of `_decode_and_save` when extradata is provided.
    """
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_frame = MagicMock()
    mock_codec_ctx.decode.return_value = [mock_frame]
    mock_packet_instance = mock_av.Packet.return_value

    with patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(b'extradata', b'packetdata', 'infohash', 'ts')

        mock_av.CodecContext.create.assert_called_once_with('h264', 'r')
        assert mock_codec_ctx.extradata == b'extradata'
        mock_av.Packet.assert_called_once_with(b'packetdata')
        mock_codec_ctx.decode.assert_called_once_with(mock_packet_instance)
        mock_save.assert_called_once_with(mock_frame, 'infohash', 'ts')

@patch('screenshot.generator.av')
def test_decode_and_save_success_no_extradata(mock_av, generator):
    """
    Tests the success path of `_decode_and_save` when no extradata is provided.
    """
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_frame = MagicMock()
    mock_codec_ctx.decode.return_value = [mock_frame]

    with patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(None, b'packetdata', 'infohash', 'ts')
        # The 'extradata' attribute is not set on the mock codec context
        mock_codec_ctx.decode.assert_called_once()
        mock_save.assert_called_once_with(mock_frame, 'infohash', 'ts')

@patch('screenshot.generator.av')
def test_decode_and_save_decoding_fails_with_flush(mock_av, generator):
    """
    Tests that a warning is logged and flush is attempted if decoding fails.
    """
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_packet = mock_av.Packet.return_value
    # First decode returns nothing, second (flush) returns a frame
    mock_codec_ctx.decode.side_effect = [[], [MagicMock()]]

    with patch.object(generator.log, 'warning') as mock_log_warning, \
         patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(None, b'packet', 'infohash', 'ts')

        mock_log_warning.assert_called_once_with("第一次解码未返回帧，尝试发送一个空的刷新包...")
        assert mock_codec_ctx.decode.call_count == 2
        mock_codec_ctx.decode.assert_any_call(mock_packet)
        mock_codec_ctx.decode.assert_any_call(None)
        mock_save.assert_called_once()

@patch('screenshot.generator.av')
def test_decode_and_save_decoding_fails_completely(mock_av, generator):
    """
    Tests that an error is logged if both decoding and flushing fail.
    """
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_codec_ctx.decode.side_effect = [[], []] # Both calls return nothing

    with patch.object(generator.log, 'error') as mock_log_error, \
         patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(None, b'packet', 'infohash', 'ts')
        mock_log_error.assert_called_once_with("解码失败：解码器未能从时间戳 ts 的数据包中解码出任何帧。")
        mock_save.assert_not_called()

@patch('screenshot.generator.av')
def test_decode_and_save_invalid_data_error(mock_av, generator):
    """
    Tests that a specific pyav error is caught and logged.
    """
    # Make the mock use the real av.error module so that the exception
    # type is caught correctly.
    mock_av.error = real_av.error
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_codec_ctx.decode.side_effect = real_av.error.InvalidDataError(1, "Invalid data")

    with patch.object(generator.log, 'error') as mock_log_error:
        generator._decode_and_save(None, b'packet', 'infohash', 'ts')
        mock_log_error.assert_called_once()
        assert "解码器报告无效数据" in mock_log_error.call_args[0][0]
