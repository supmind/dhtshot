import asyncio
import pytest
from unittest.mock import MagicMock
import av

# Import the module that we will be patching
import screenshot.generator

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio

async def test_generate_calls_decoder_and_saver(monkeypatch):
    """
    Tests the generator by replacing the entire 'av' module in the generator's
    namespace with a mock object. This avoids TypeErrors from immutable C types.
    """
    # --- Mocks ---
    mock_frame = MagicMock()
    mock_codec = MagicMock()
    mock_codec.decode.return_value = [mock_frame]

    # Create a mock that mimics the 'av' module's structure
    mock_av = MagicMock()
    mock_av.Packet.return_value = MagicMock()  # The code calls av.Packet(data)
    mock_av.CodecContext.create.return_value = mock_codec

    # Replace the 'av' module in the generator's namespace with our mock
    monkeypatch.setattr(screenshot.generator, 'av', mock_av)

    # --- Setup ---
    loop = asyncio.get_running_loop()
    generator_instance = screenshot.generator.ScreenshotGenerator(loop=loop, output_dir="/tmp/test_output")
    mock_save = MagicMock()
    monkeypatch.setattr(generator_instance, '_save_frame_to_jpeg', mock_save)

    # --- Test Data ---
    extradata = b'some_extradata'
    packet_data = b'some_packet_data'
    infohash = 'deadbeefcafebabe'
    timestamp = '00-01-02'

    # --- Action ---
    await generator_instance.generate(extradata, packet_data, infohash, timestamp)

    # --- Assertions ---
    mock_av.CodecContext.create.assert_called_once_with('h264', 'r')
    assert mock_codec.extradata == extradata
    mock_av.Packet.assert_called_once_with(packet_data)
    mock_codec.decode.assert_called()
    mock_save.assert_called_once_with(mock_frame, infohash, timestamp)


async def test_decode_returns_no_frames_initially(monkeypatch, caplog):
    """
    Tests the flushing mechanism. If the first decode call returns no frames,
    a second call with None should be made to flush the decoder.
    """
    # --- Mocks ---
    mock_frame = MagicMock()
    mock_codec = MagicMock()
    # First call returns empty list, second (flush) call returns the frame
    mock_codec.decode.side_effect = [[], [mock_frame]]

    mock_av = MagicMock()
    mock_av.Packet.return_value = MagicMock()
    mock_av.CodecContext.create.return_value = mock_codec

    monkeypatch.setattr(screenshot.generator, 'av', mock_av)

    # --- Setup ---
    loop = asyncio.get_running_loop()
    generator_instance = screenshot.generator.ScreenshotGenerator(loop=loop)
    mock_save = MagicMock()
    monkeypatch.setattr(generator_instance, '_save_frame_to_jpeg', mock_save)

    # --- Action ---
    await generator_instance.generate(None, b'some_data', 'hash', 'time')

    # --- Assertions ---
    assert mock_codec.decode.call_count == 2
    mock_codec.decode.assert_any_call(None) # Check that the flush call was made
    mock_save.assert_called_once_with(mock_frame, 'hash', 'time')
    assert "第一次解码未返回帧，尝试发送一个空的刷新包..." in caplog.text


async def test_decode_fails_with_invalid_data_error(monkeypatch, caplog):
    """
    Tests that if PyAV raises InvalidDataError, it is caught, logged,
    and does not propagate. The save function should not be called.
    """
    # --- Mocks ---
    mock_av = MagicMock()
    # Mock the specific error class within the mocked 'av' module
    mock_av.error.InvalidDataError = av.error.InvalidDataError
    mock_av.EOFError = av.EOFError  # Also patch EOFError for the other try/except block
    mock_av.Packet.return_value = MagicMock()
    mock_codec = MagicMock()
    # Instantiate the real exception with a code and message
    mock_codec.decode.side_effect = av.error.InvalidDataError(-1, "test error")
    mock_av.CodecContext.create.return_value = mock_codec

    monkeypatch.setattr(screenshot.generator, 'av', mock_av)

    # --- Setup ---
    loop = asyncio.get_running_loop()
    generator_instance = screenshot.generator.ScreenshotGenerator(loop=loop)
    mock_save = MagicMock()
    monkeypatch.setattr(generator_instance, '_save_frame_to_jpeg', mock_save)

    # --- Action ---
    # We expect this to complete without raising an exception
    await generator_instance.generate(None, b'bad_data', 'hash', 'time')

    # --- Assertions ---
    mock_save.assert_not_called()
    assert "解码器报告无效数据" in caplog.text
    assert "test error" in caplog.text


async def test_decode_fails_with_generic_exception(monkeypatch):
    """
    Tests that a generic (unexpected) exception during decoding is re-raised.
    """
    # --- Mocks ---
    mock_av = MagicMock()
    # Patch the mock to use the REAL exception classes so the 'except' blocks work
    mock_av.error.InvalidDataError = av.error.InvalidDataError
    mock_av.EOFError = av.EOFError
    mock_av.Packet.return_value = MagicMock()
    mock_codec = MagicMock()
    mock_codec.decode.side_effect = RuntimeError("Unexpected failure")
    mock_av.CodecContext.create.return_value = mock_codec

    monkeypatch.setattr(screenshot.generator, 'av', mock_av)

    # --- Setup ---
    loop = asyncio.get_running_loop()
    generator_instance = screenshot.generator.ScreenshotGenerator(loop=loop)
    mock_save = MagicMock()
    monkeypatch.setattr(generator_instance, '_save_frame_to_jpeg', mock_save)

    # --- Action & Assertion ---
    with pytest.raises(RuntimeError, match="Unexpected failure"):
        await generator_instance.generate(None, b'data', 'hash', 'time')

    mock_save.assert_not_called()


async def test_generate_handles_no_extradata(monkeypatch):
    """
    Tests the generator with no extradata using the full module mock.
    """
    # --- Mocks ---
    mock_frame = MagicMock()
    mock_codec = MagicMock()
    # Configure the mock to have a default `None` value for the extradata attribute.
    # The code under test doesn't set this attribute when extradata is None,
    # so the mock needs this default state to pass the assertion.
    mock_codec.extradata = None
    mock_codec.decode.return_value = [mock_frame]

    mock_av = MagicMock()
    mock_av.Packet.return_value = MagicMock()
    mock_av.CodecContext.create.return_value = mock_codec

    monkeypatch.setattr(screenshot.generator, 'av', mock_av)

    # --- Setup ---
    loop = asyncio.get_running_loop()
    generator_instance = screenshot.generator.ScreenshotGenerator(loop=loop, output_dir="/tmp/test_output")
    mock_save = MagicMock()
    monkeypatch.setattr(generator_instance, '_save_frame_to_jpeg', mock_save)

    # --- Test Data ---
    packet_data = b'in_band_packet_data'
    infohash = 'facefeed'
    timestamp = '00-03-04'

    # --- Action ---
    await generator_instance.generate(None, packet_data, infohash, timestamp)

    # --- Assertions ---
    assert mock_codec.extradata is None
    mock_av.Packet.assert_called_once_with(packet_data)
    mock_codec.decode.assert_called()
    mock_save.assert_called_once_with(mock_frame, infohash, timestamp)
