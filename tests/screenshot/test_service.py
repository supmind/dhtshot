import pytest
import io
import struct

from screenshot.service import ScreenshotService
from screenshot.config import Settings
from screenshot.errors import MP4ParsingError

@pytest.fixture
def service():
    """Provides a default ScreenshotService instance for tests."""
    return ScreenshotService(settings=Settings(), status_callback=None, screenshot_callback=None)

def test_parse_mp4_boxes_raises_error_on_invalid_size(service):
    """
    Tests that the MP4 box parser raises an MP4ParsingError if a box declares
    a size that is larger than the available data buffer.
    """
    header = b'\x00\x00\x00\x18ftypiso5\x00\x00\x00\x01isomiso5'
    malformed_box_header = b'\x00\x00\x00\x64moov' # Size 100 (0x64)
    data = header + malformed_box_header
    stream = io.BytesIO(data)

    # The corrected behavior should be to raise an exception.
    # The low-level parser should raise the context-free MP4ParsingError.
    with pytest.raises(MP4ParsingError) as excinfo:
        # We need to consume the generator to trigger the error
        list(service._parse_mp4_boxes(stream))

    # Check if the exception message is informative (and in Chinese).
    assert "超出了可用数据的范围" in str(excinfo.value)
