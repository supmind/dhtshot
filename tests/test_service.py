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

def test_parse_mp4_boxes_handles_partial_data(service):
    """
    Tests that the parser correctly handles a stream with partial data.
    The original, correct behavior is to yield the partial box along with its
    full declared size, allowing the caller to detect the partial data.
    """
    box1_payload = b'fulldata'
    box1 = create_box(b'box1', box1_payload)

    box2_payload = b'partialdata'
    box2 = create_box(b'box2', box2_payload)  # Declared size will be len('partialdata') + 8

    # Create a buffer with all of box1 and only a slice of box2
    partial_box2_data = box2[:10]
    data = io.BytesIO(box1 + partial_box2_data)

    boxes = list(service._parse_mp4_boxes(data))

    # It should now yield BOTH boxes.
    assert len(boxes) == 2

    # The first box should be complete.
    box1_type, box1_data, _, box1_size = boxes[0]
    assert box1_type == 'box1'
    assert box1_data == box1
    assert len(box1_data) == box1_size  # data length == declared size

    # The second box should be partial.
    box2_type, box2_data, _, box2_size = boxes[1]
    assert box2_type == 'box2'
    # The declared size should be the full size of box2
    assert box2_size == len(box2)
    # The actual data should be shorter than the declared size
    assert len(box2_data) < box2_size
    # The actual data should be what we put in the buffer
    assert box2_data == partial_box2_data

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

    # Test case 4: A piece is missing in the middle.
    # The original code's logic is to pre-allocate a buffer and then copy
    # available piece data into it sequentially. If a piece is missing,
    # the buffer_offset is not advanced, so the next available piece's data
    # is written immediately after the previous one.
    offset = piece_length - 500
    size = piece_length + 1000
    pieces_data_missing = {0: piece0, 2: piece2}  # Missing piece 1
    result = service._assemble_data_from_pieces(pieces_data_missing, offset, size, piece_length)

    # The expected result is 500 bytes of 'A', followed by 500 bytes of 'C'
    # The new, correct behavior is to return empty bytes.
    assert result == b""

    # Test case 5: Request starts at the beginning of a piece
    offset = piece_length
    size = 500
    result = service._assemble_data_from_pieces(pieces_data, offset, size, piece_length)
    expected = b'B' * 500
    assert result == expected
    assert len(result) == size


def test_select_keyframes_uses_custom_config():
    """测试 _select_keyframes 方法能正确使用在服务中配置的自定义参数。"""
    # 重新配置服务实例以使用非默认值
    service = ScreenshotService(
        loop=None,
        min_screenshots=10,
        max_screenshots=10,
    )

    # 模拟一个很长的视频，确保我们的 max_screenshots 会生效
    # (10000s duration / 180s interval) > 10
    duration_sec = 10000
    timescale = 1
    # 创建足够的关键帧和样本
    # The actual keyframe content doesn't matter for this test
    all_keyframes = [f"kf_{i}" for i in range(100)]
    # We only need the last sample to determine duration
    samples = [type('Sample', (), {'pts': duration_sec * timescale})()]

    selected = service._select_keyframes(all_keyframes, timescale, samples)

    # 因为 max_screenshots 被设为 10，所以结果应该只有 10 个
    assert len(selected) == 10


def test_select_keyframes_default_for_zero_duration():
    """测试当视频时长为0时，使用 default_screenshots 参数。"""
    service = ScreenshotService(loop=None, default_screenshots=15)

    duration_sec = 0
    timescale = 1
    all_keyframes = [f"kf_{i}" for i in range(100)]
    # All samples have the same pts, so duration is 0
    samples = [type('Sample', (), {'pts': 0})()] * 2

    selected = service._select_keyframes(all_keyframes, timescale, samples)
    assert len(selected) == 15
