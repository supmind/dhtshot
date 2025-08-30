import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock

@pytest.fixture
def mock_basetorrent_info():
    """
    Provides a mock libtorrent.torrent_info object, with a nested mock
    for the file_storage object.
    """
    info = MagicMock(name="torrent_info_mock")
    info.piece_length.return_value = 16384

    # This will be the mock for the file_storage object returned by ti.files()
    fs_mock = MagicMock(name="file_storage_mock")
    fs_mock.num_files.return_value = 1
    fs_mock.file_path.return_value = "video.mp4"
    fs_mock.file_size.return_value = 16384 * 10
    fs_mock.file_offset.return_value = 0

    info.files.return_value = fs_mock
    return info

@pytest.fixture
def mock_torrent_client(mock_basetorrent_info):
    """Provides a mock TorrentClient."""
    client = MagicMock()
    # Make all async methods that are actually called by the service awaitable
    client.start = AsyncMock()
    client.stop = AsyncMock()

    # _handle_screenshot_task needs add_torrent to return a mock handle
    mock_handle = MagicMock()
    mock_handle.is_valid.return_value = True
    mock_handle.get_torrent_info.return_value = mock_basetorrent_info
    client.add_torrent = AsyncMock(return_value=mock_handle)

    # These methods are called inside the screenshot generation logic
    client.get_metadata = AsyncMock(return_value="metadata_bytes")
    client.fetch_pieces = AsyncMock(return_value={0: b"piece_data"})
    client.download_pieces_for_frame = AsyncMock(return_value={0: b"piece_data"})

    # This is called in the finally block for cleanup
    client.remove_torrent = AsyncMock()

    return client

@pytest.fixture
def mock_keyframe_extractor():
    """Provides a mock H264KeyframeExtractor."""
    extractor = MagicMock()
    # Simulate two keyframes found
    keyframe1 = MagicMock()
    keyframe1.pts = 1000
    keyframe1.timescale = 1000
    keyframe2 = MagicMock()
    keyframe2.pts = 5000
    keyframe2.timescale = 1000
    extractor.keyframes = [keyframe1, keyframe2]
    extractor.extradata = b"extradata"
    # Mock the __enter__ and __exit__ for context management
    instance = MagicMock(return_value=extractor)
    instance.__enter__.return_value = extractor
    instance.__exit__.return_value = None
    return instance

@pytest.fixture
def mock_screenshot_generator():
    """Provides a mock ScreenshotGenerator."""
    generator = MagicMock()
    generator.generate = AsyncMock()
    return generator

@pytest.fixture
def mock_service_dependencies(monkeypatch, mock_torrent_client, mock_keyframe_extractor, mock_screenshot_generator):
    """A fixture to patch all dependencies of ScreenshotService."""
    # This lambda must accept all arguments that the real constructor takes.
    monkeypatch.setattr('screenshot.service.TorrentClient', lambda loop, save_path: mock_torrent_client)
    monkeypatch.setattr('screenshot.service.H264KeyframeExtractor', mock_keyframe_extractor)
    monkeypatch.setattr('screenshot.service.ScreenshotGenerator', lambda loop, output_dir: mock_screenshot_generator)

    return {
        "client": mock_torrent_client,
        "extractor": mock_keyframe_extractor,
        "generator": mock_screenshot_generator
    }

@pytest.fixture
def status_callback():
    """Provides a mock status_callback function."""
    return AsyncMock()
