# -*- coding: utf-8 -*-
"""
Pytest fixtures for screenshot tests.
"""
import pytest
import struct
from io import BytesIO

# --- Test Resources ---
TEST_VIDEO_PATH = "tests/assets/test_video.mp4"

@pytest.fixture(scope="module")
def moov_atom_data():
    """
    A Pytest fixture that reads the 'moov' atom data from the test video file.
    This fixture has a 'module' scope, so the file is read only once per module.
    """
    try:
        with open(TEST_VIDEO_PATH, "rb") as f:
            data = f.read()

        stream = BytesIO(data)
        while True:
            header_data = stream.read(8)
            if not header_data:
                break
            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii')

            if box_type == 'moov':
                # Go back to the start of the box and read the whole thing
                stream.seek(stream.tell() - 8)
                return stream.read(size)

            # Jump to the next box
            if size == 1: # 64-bit size
                size = struct.unpack('>Q', stream.read(8))[0]
                stream.seek(size - 16, 1)
            else:
                stream.seek(size - 8, 1)

    except FileNotFoundError:
        pytest.fail(f"Test video file not found: {TEST_VIDEO_PATH}. Please run 'tests/utils/create_test_video.py' to generate it.")

    pytest.fail("Could not find 'moov' atom in the test video file.")
