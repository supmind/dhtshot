import pytest
from screenshot.service import ScreenshotService

@pytest.fixture
def service():
    # We don't need a running loop or a real client for these unit tests
    return ScreenshotService(loop=None)

class TestServiceHelpers:
    def test_get_pieces_for_range(self, service):
        # piece_length = 1000
        # Test case 1: Contained in a single piece
        assert service._get_pieces_for_range(offset_in_torrent=500, size=100, piece_length=1000) == [0]

        # Test case 2: Spanning two pieces exactly at boundary
        assert service._get_pieces_for_range(offset_in_torrent=900, size=200, piece_length=1000) == [0, 1]

        # Test case 3: Spanning multiple pieces
        assert service._get_pieces_for_range(offset_in_torrent=1500, size=2000, piece_length=1000) == [1, 2, 3]

        # Test case 4: Starts exactly on a piece boundary
        assert service._get_pieces_for_range(offset_in_torrent=2000, size=500, piece_length=1000) == [2]

        # Test case 5: Ends exactly on a piece boundary
        assert service._get_pieces_for_range(offset_in_torrent=1500, size=500, piece_length=1000) == [1]

        # Test case 6: Zero size
        assert service._get_pieces_for_range(offset_in_torrent=1500, size=0, piece_length=1000) == []

    def test_assemble_data_from_pieces_single_piece(self, service):
        piece_length = 1024
        pieces_data = {
            10: b'a' * 500 + b'B' * 100 + b'c' * 424
        }
        # offset = 10 * 1024 + 500 = 10740
        # size = 100
        result = service._assemble_data_from_pieces(pieces_data, 10740, 100, piece_length)
        assert result == b'B' * 100

    def test_assemble_data_from_pieces_multiple_pieces(self, service):
        piece_length = 1024
        pieces_data = {
            10: b'a' * 1000 + b'B' * 24,
            11: b'C' * 100 + b'd' * 924
        }
        # offset = 10 * 1024 + 1000 = 11240
        # size = 24 + 100 = 124
        result = service._assemble_data_from_pieces(pieces_data, 11240, 124, piece_length)
        assert result == b'B' * 24 + b'C' * 100

    def test_assemble_data_from_pieces_incomplete_data(self, service):
        # Test when a required piece is missing
        piece_length = 1024
        pieces_data = {
            10: b'a' * 1000 + b'B' * 24,
            # piece 11 is missing
        }
        offset = 11240
        size = 124
        # It should assemble what it can and fill the rest with null bytes
        result = service._assemble_data_from_pieces(pieces_data, offset, size, piece_length)
        assert result == b'B' * 24 + b'\x00' * 100

    def test_assemble_data_from_pieces_offset_and_size(self, service):
        piece_length = 16
        pieces_data = {
            0: b"0123456789ABCDEF",
            1: b"GHIJKLMNOPQRSTUV",
            2: b"WXYZabcdefghijkl",
        }
        # Read 20 bytes starting from offset 10
        # This means bytes 10-15 from piece 0 (6 bytes)
        # and bytes 0-13 from piece 1 (14 bytes)
        offset = 10
        size = 20
        expected = b"ABCDEF" + b"GHIJKLMNOPQRST"
        result = service._assemble_data_from_pieces(pieces_data, offset, size, piece_length)
        assert result == expected
        assert len(result) == size
