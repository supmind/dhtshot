import asyncio
import pytest
from unittest.mock import MagicMock

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
