import pytest
import io
import struct

from screenshot.service import ScreenshotService

# Helper to create a box
def create_box(box_type: bytes, payload: bytes) -> bytes:
    """Creates a simple MP4 box with a 32-bit size."""
    size = 8 + len(payload)
    return struct.pack('>I4s', size, box_type) + payload

@pytest.fixture
def service():
    """Provides a ScreenshotService instance for testing its pure methods."""
    # We don't need a running service with a loop, just an instance.
    return ScreenshotService(loop=None)

def test_parse_mp4_boxes_simple(service):
    """Tests that the box parser can correctly parse a stream of multiple, valid boxes."""
    box1_payload = b'data1'
    box1 = create_box(b'box1', box1_payload)
    box2_payload = b'data2!'
    box2 = create_box(b'box2', box2_payload)

    data = io.BytesIO(box1 + box2)

    boxes = list(service._parse_mp4_boxes(data))

    assert len(boxes) == 2
    assert boxes[0][0] == 'box1'
    assert boxes[0][1] == box1
    assert boxes[1][0] == 'box2'
    assert boxes[1][1] == box2

def test_parse_mp4_boxes_stops_on_partial_data(service):
    """
    Tests the fix in the parser.
    The buffer contains one full box and one partial box. The parser should
    only yield the first, complete box and then stop gracefully.
    """
    box1_payload = b'fulldata'
    box1 = create_box(b'box1', box1_payload)

    box2_payload = b'partialdata'
    box2 = create_box(b'box2', box2_payload)

    # Create a buffer with all of box1 and only a slice of box2
    data = io.BytesIO(box1 + box2[:10])

    boxes = list(service._parse_mp4_boxes(data))

    # The generator should have only yielded the one complete box
    assert len(boxes) == 1
    assert boxes[0][0] == 'box1'

def test_assemble_data_from_pieces(service):
    """
    Tests the refactored data assembly logic with various scenarios.
    """
    piece_length = 16384

    # Create some dummy piece data
    piece0 = b'A' * piece_length
    piece1 = b'B' * piece_length
    piece2 = b'C' * piece_length

    pieces_data = {
        0: piece0,
        1: piece1,
        2: piece2
    }

    # Test case 1: Data is fully within a single piece
    offset = 100
    size = 1000
    result = service._assemble_data_from_pieces(pieces_data, offset, size, piece_length)
    expected = b'A' * 1000
    assert result == expected
    assert len(result) == size

    # Test case 2: Data spans two pieces
    offset = piece_length - 500  # last 500 bytes of piece 0
    size = 1000  # 500 from piece 0, 500 from piece 1
    result = service._assemble_data_from_pieces(pieces_data, offset, size, piece_length)
    expected = (b'A' * 500) + (b'B' * 500)
    assert result == expected
    assert len(result) == size

    # Test case 3: Data spans three pieces
    offset = piece_length - 500  # last 500 of piece 0
    size = piece_length + 1000  # 500 from p0, all of p1, 500 from p2
    result = service._assemble_data_from_pieces(pieces_data, offset, size, piece_length)
    expected = (b'A' * 500) + piece1 + (b'C' * 500)
    assert result == expected
    assert len(result) == size

    # Test case 4: A piece is missing in the middle
    offset = piece_length - 500
    size = piece_length + 1000
    pieces_data_missing = {0: piece0, 2: piece2}  # Missing piece 1
    result = service._assemble_data_from_pieces(pieces_data_missing, offset, size, piece_length)
    # The new logic stops when it hits a missing piece and returns what it has,
    # which is then truncated to the requested size.
    expected = (b'A' * 500)
    # The final result is truncated to `size`, so even if more data was assembled,
    # it would be cut. In this case, the assembled data is shorter than `size`.
    assert result == expected

    # Test case 5: Request starts at the beginning of a piece
    offset = piece_length
    size = 500
    result = service._assemble_data_from_pieces(pieces_data, offset, size, piece_length)
    expected = b'B' * 500
    assert result == expected
    assert len(result) == size
