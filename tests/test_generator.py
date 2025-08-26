# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch

# Mock pyav before it's imported by other modules
av_mock = MagicMock()
pil_mock = MagicMock()
with patch.dict('sys.modules', {'av': av_mock, 'PIL': pil_mock}):
    from screenshot.generator import ScreenshotGenerator
    from screenshot.service import KeyframeInfo

# This fixture will run automatically before each test
@pytest.fixture(autouse=True)
def reset_mocks():
    """Resets module-level mocks before each test to ensure isolation."""
    av_mock.reset_mock()
    pil_mock.reset_mock()

def test_create_minimal_mp4_structure():
    """
    Tests the structure of the file created by _create_minimal_mp4.
    """
    generator = ScreenshotGenerator(loop=None)
    fake_moov = b'\x00\x00\x00\x08moov'
    fake_keyframe = b'\x01\x02\x03\x04'
    result = generator._create_minimal_mp4(fake_moov, fake_keyframe)
    # ftyp (24) + moov (8) + mdat (header 8 + data 4 = 12) = 44 bytes
    assert len(result) == 44

@patch('os.makedirs')
def test_save_frame_to_jpeg(mock_makedirs):
    """
    Tests that the JPEG saving function calls the correct methods.
    """
    generator = ScreenshotGenerator(loop=None, output_dir="/tmp/ss_output")
    mock_frame = MagicMock()
    mock_image = mock_frame.to_image.return_value
    generator._save_frame_to_jpeg(mock_frame, "infohash", "timestamp")
    mock_makedirs.assert_called_once_with("/tmp/ss_output", exist_ok=True)
    mock_image.save.assert_called_once_with("/tmp/ss_output/infohash_timestamp.jpg")

@pytest.mark.asyncio
@patch('logging.getLogger')
async def test_generate_success(mock_get_logger):
    """
    Tests the generate method's success path.
    """
    mock_log = mock_get_logger.return_value
    loop = asyncio.get_running_loop()
    generator = ScreenshotGenerator(loop=loop)

    mock_frame = MagicMock()
    mock_image = mock_frame.to_image.return_value
    mock_container = av_mock.open.return_value.__enter__.return_value
    mock_container.decode.return_value = iter([mock_frame])

    await generator.generate(b'moov', b'keyframe', MagicMock(), 'infohash', 'ts_success')

    mock_image.save.assert_called_once()
    assert mock_log.exception.call_count == 0
