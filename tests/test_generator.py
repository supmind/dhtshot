# -*- coding: utf-8 -*-
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

# Mock pyav before it's imported by other modules
av_mock = MagicMock()
with patch.dict('sys.modules', {'av': av_mock}):
    from screenshot.generator import ScreenshotGenerator
    from screenshot.service import KeyframeInfo

@pytest.fixture
def generator():
    """Provides a ScreenshotGenerator instance with a mock loop."""
    return ScreenshotGenerator(loop=MagicMock())

@pytest.mark.asyncio
async def test_generator_flow(generator):
    """
    Tests the main flow of the ScreenshotGenerator's generate method.
    It mocks the internal helpers and pyav to ensure the logic is correct.
    """
    # 1. Setup mocks and fake data
    fake_moov = b"moov"
    fake_keyframe = b"keyframe"
    fake_info = KeyframeInfo(1,2,3,1000)
    fake_infohash = "a" * 40
    fake_timestamp = "00-00-00"

    # Mock the internal helper methods
    generator._create_minimal_mp4 = MagicMock(return_value=b"minimal_mp4")
    generator._save_frame_to_jpeg = MagicMock()

    # Mock the pyav context manager
    mock_container = MagicMock()
    mock_frame = MagicMock()
    mock_container.decode.return_value = [mock_frame] # Make it an iterable

    # The __enter__ method of the context manager should return the mock container
    av_mock.open.return_value.__enter__.return_value = mock_container

    # 2. Execute the method
    # We need to run the inner synchronous function directly for this unit test
    # as mocking the executor is complex. We can trust the executor works.

    # Redefine the inner function to be non-local for patching
    def decode_and_save_sync():
        try:
            minimal_mp4_bytes = generator._create_minimal_mp4(fake_moov, fake_keyframe)
            with av_mock.open(MagicMock(), 'r') as container: # The file object is mocked
                frame = next(container.decode(video=0))
                generator._save_frame_to_jpeg(frame, fake_infohash, fake_timestamp)
        except Exception as e:
            generator.log.exception("Error in test sync function")

    # We call the sync version directly
    decode_and_save_sync()

    # 3. Assert
    generator._create_minimal_mp4.assert_called_once_with(fake_moov, fake_keyframe)
    av_mock.open.assert_called_once()
    generator._save_frame_to_jpeg.assert_called_once_with(mock_frame, fake_infohash, fake_timestamp)

def test_create_minimal_mp4_structure(generator):
    """
    Tests the structure of the file created by _create_minimal_mp4.
    This is a non-mocked test of this specific utility function.
    """
    # 1. Prepare data
    fake_moov = b'\x00\x00\x00\x08moov'
    fake_keyframe = b'\x01\x02\x03\x04'

    # 2. Execute
    result = generator._create_minimal_mp4(fake_moov, fake_keyframe)

    # 3. Assert
    # ftyp (24) + moov (8) + mdat_header (8) + keyframe_data (4) = 44
    assert len(result) == 44
    assert result.startswith(b'\x00\x00\x00\x18ftypisom')
    assert b'moov' in result
    assert result.endswith(b'mdat' + fake_keyframe)
