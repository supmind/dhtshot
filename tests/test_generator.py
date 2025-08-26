# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

# Mock pyav before it's imported by other modules
av_mock = MagicMock()
pil_mock = MagicMock()
with patch.dict('sys.modules', {'av': av_mock, 'PIL': pil_mock}):
    from screenshot.generator import ScreenshotGenerator

@pytest.fixture
def reset_mocks():
    """Resets module-level mocks before each test to ensure isolation."""
    av_mock.reset_mock()
    pil_mock.reset_mock()

@pytest.fixture
def generator():
    """Provides a ScreenshotGenerator instance with a mock loop."""
    return ScreenshotGenerator(loop=MagicMock())

@patch('os.makedirs')
def test_save_frame_to_jpeg(mock_makedirs, generator, reset_mocks):
    """
    Tests that the JPEG saving function calls the correct methods.
    """
    mock_frame = MagicMock()
    mock_image = MagicMock()
    mock_frame.to_image.return_value = mock_image

    with patch.object(mock_image, 'save'):
        generator._save_frame_to_jpeg(mock_frame, "infohash", "timestamp")

        mock_makedirs.assert_called_once_with(generator.output_dir, exist_ok=True)
        mock_image.save.assert_called_once_with(f"{generator.output_dir}/infohash_timestamp.jpg")


@pytest.mark.asyncio
async def test_generate_calls_executor(generator, reset_mocks):
    """
    Tests that the main `generate` method correctly builds the stream
    and calls the executor with the `_decode_and_save` method.
    """
    generator.loop.run_in_executor = AsyncMock()

    await generator.generate(
        sps=b'sps', pps=b'pps', keyframe_data=b'keyframe',
        infohash_hex='infohash', timestamp_str='ts'
    )

    expected_stream = b'\x00\x00\x00\x01sps\x00\x00\x00\x01pps\x00\x00\x00\x01keyframe'

    generator.loop.run_in_executor.assert_awaited_once_with(
        None, generator._decode_and_save, expected_stream, 'infohash', 'ts'
    )

def test_decode_and_save_success(generator, reset_mocks):
    """
    Tests the success path of the synchronous `_decode_and_save` method.
    """
    mock_codec_ctx = av_mock.CodecContext.create.return_value
    mock_packet = MagicMock()
    mock_codec_ctx.parse.return_value = [mock_packet]
    mock_frame = MagicMock()
    mock_codec_ctx.decode.return_value = [mock_frame]

    with patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(b'stream', 'infohash', 'ts')

        av_mock.CodecContext.create.assert_called_once_with('h264', 'r')
        mock_codec_ctx.parse.assert_called_once_with(b'stream')
        mock_codec_ctx.decode.assert_called_once_with(mock_packet)
        mock_save.assert_called_once_with(mock_frame, 'infohash', 'ts')

def test_decode_and_save_parsing_fails(generator, reset_mocks):
    """
    Tests that a warning is logged if parsing the stream fails.
    """
    mock_codec_ctx = av_mock.CodecContext.create.return_value
    mock_codec_ctx.parse.return_value = [] # Simulate no packets found

    with patch.object(generator.log, 'warning') as mock_log_warning:
        generator._decode_and_save(b'stream', 'infohash', 'ts')
        mock_log_warning.assert_called_once_with("无法从时间戳 ts 的码流中解析出数据包。")

def test_decode_and_save_decoding_fails(generator, reset_mocks):
    """
    Tests that a warning is logged if decoding the packet fails.
    """
    mock_codec_ctx = av_mock.CodecContext.create.return_value
    mock_packet = MagicMock()
    mock_codec_ctx.parse.return_value = [mock_packet]
    mock_codec_ctx.decode.return_value = [] # Simulate no frames decoded

    with patch.object(generator.log, 'warning') as mock_log_warning:
        generator._decode_and_save(b'stream', 'infohash', 'ts')
        mock_log_warning.assert_called_once_with("无法从时间戳 ts 的数据包中解码出帧。")

def test_decode_and_save_generic_exception(generator, reset_mocks):
    """
    Tests that a generic exception during the sync operation is caught, logged and re-raised.
    """
    av_mock.CodecContext.create.side_effect = ValueError("Test AV Error")

    with pytest.raises(ValueError, match="Test AV Error"), \
         patch.object(generator.log, 'exception') as mock_log_exception:
        generator._decode_and_save(b'stream', 'infohash', 'ts')
        mock_log_exception.assert_called_once_with("为帧 ts 进行同步解码/保存时出错")
