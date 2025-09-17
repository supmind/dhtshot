# -*- coding: utf-8 -*-
"""
对 screenshot/service.py 核心服务逻辑的单元测试。
"""
import pytest
import io
import struct
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from screenshot.service import ScreenshotService, StatusCallback
from config import Settings
from screenshot.errors import MP4ParsingError, NoVideoFileError, FrameDownloadTimeoutError, MoovNotFoundError
from screenshot.extractor import Keyframe, SampleInfo, KeyframeExtractor
from screenshot.client import TorrentClient


class TestAssembleData:
    @pytest.fixture
    def service(self, settings):
        return ScreenshotService(settings=settings, loop=None)

    def test_assemble_from_single_piece(self, service):
        pieces_data = {0: b"0123456789"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=2, size=5, piece_length=10)
        assert result == b"23456"

    def test_assemble_spanning_two_pieces(self, service):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=4, piece_length=10)
        assert result == b"89ab"

    def test_assemble_spanning_multiple_pieces(self, service):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij", 2: b"KLMNOPQRST"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b"89abcdefghijKLM"

    def test_assemble_at_piece_boundary(self, service):
        pieces_data = {0: b"0123456789", 1: b"abcdefghij"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=10, size=5, piece_length=10)
        assert result == b"abcde"

    def test_assemble_with_missing_piece(self, service):
        pieces_data = {0: b"0123456789", 2: b"KLMNOPQRST"}
        result = service._assemble_data_from_pieces(pieces_data, offset_in_torrent=8, size=15, piece_length=10)
        assert result == b""


@pytest.fixture
def settings():
    return Settings(min_screenshots=2, max_screenshots=5, default_screenshots=3, target_interval_sec=60, moov_probe_timeout=5, piece_fetch_timeout=5, piece_queue_timeout=5)

@pytest.fixture
def mock_callbacks():
    future = asyncio.Future()
    async def status_callback(*args, **kwargs):
        if not future.done():
            future.set_result(kwargs)
    return {"status_callback": status_callback, "future": future}

@pytest.fixture
def service(settings, mock_callbacks):
    loop = asyncio.get_event_loop()
    service_instance = ScreenshotService(
        settings=settings, loop=loop, status_callback=mock_callbacks["status_callback"]
    )
    mock_client = AsyncMock(spec=TorrentClient)
    mock_client.add_torrent = AsyncMock()
    mock_client.remove_torrent = AsyncMock()
    mock_client.fetch_pieces = AsyncMock()
    mock_client.subscribe_pieces = MagicMock()
    mock_client.unsubscribe_pieces = MagicMock()
    mock_client.request_pieces = MagicMock()
    service_instance.client = mock_client
    service_instance.generator = AsyncMock()
    service_instance.mock_client = mock_client
    yield service_instance

def test_select_keyframes_logic(service):
    all_keyframes = [
        Keyframe(0, 0, 0, 90000), Keyframe(1, 1, 10 * 90000, 90000),
        Keyframe(2, 2, 20 * 90000, 90000), Keyframe(3, 3, 88 * 90000, 90000),
        Keyframe(4, 4, 95 * 90000, 90000), Keyframe(5, 5, 170 * 90000, 90000)
    ]
    duration_pts = 180 * 90000
    selected = service._select_keyframes(all_keyframes, 90000, duration_pts, None)
    assert len(selected) == 3
    selected_pts = {kf.pts for kf in selected}
    expected_pts = {0, 88 * 90000, 95 * 90000}
    assert selected_pts == expected_pts

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_permanent_failure(mock_generate, service, mock_callbacks):
    mock_generate.side_effect = NoVideoFileError("Not found", "no_video_hash")
    service.mock_client.add_torrent.return_value = MagicMock()

    await service._handle_screenshot_task({"infohash": "no_video_hash", "metadata": b"somemetadata"})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "permanent_failure"
    assert isinstance(result.get("error"), NoVideoFileError)
    service.mock_client.remove_torrent.assert_awaited_once_with(service.mock_client.add_torrent.return_value, delete_files=True)

@pytest.mark.asyncio
@patch.object(ScreenshotService, '_generate_screenshots_from_torrent', new_callable=AsyncMock)
async def test_handle_task_successful_run(mock_generate, service, mock_callbacks):
    infohash = "success_hash"
    service.mock_client.add_torrent.return_value = MagicMock()

    await service._handle_screenshot_task({"infohash": infohash, "metadata": b"somemetadata"})

    result = await asyncio.wait_for(mock_callbacks["future"], timeout=1)
    assert result.get("status") == "success"
    mock_generate.assert_awaited_once()
    service.mock_client.remove_torrent.assert_awaited_once()

@pytest.mark.asyncio
async def test_get_moov_atom_fetches_full_box_on_partial_find(service):
    mock_handle = MagicMock()
    partial_moov_data = b'\x00\x00\x03\xe8moov' + b'\x01' * 42
    full_moov_data = b'\x00\x00\x03\xe8moov' + b'\xff' * (1000 - 8)

    service.mock_client.fetch_pieces.side_effect = [
        {0: partial_moov_data},
        {0: full_moov_data[:512], 1: full_moov_data[512:]}
    ]
    with patch.object(service, '_assemble_data_from_pieces', new_callable=MagicMock) as mock_assemble:
        mock_assemble.side_effect = [partial_moov_data, full_moov_data]
        result_moov_data = await service._get_moov_atom_data(mock_handle, 0, 2000, 512, "partial_moov_hash")

    assert result_moov_data == full_moov_data
    assert service.mock_client.fetch_pieces.call_count == 2

@pytest.mark.skip(reason="Needs update for new intelligent probing mock logic")
@pytest.mark.asyncio
async def test_get_moov_atom_from_tail_intelligently(service):
    from tests.screenshot.test_advanced_video_features import get_moov_atom, ASSETS_DIR
    moov_at_end_file = ASSETS_DIR / "test_moov_at_end.mp4"
    real_moov_atom = get_moov_atom(moov_at_end_file)

    header_data = b'\x00\x00\x00\x18ftypiso5' + b'\x00\x00\x13\x88mdat' + b'\x01' * (5000 - 8)
    tail_data = b'\x02' * 100 + real_moov_atom
    video_file_size = len(header_data) + len(tail_data)

    mock_handle = MagicMock()
    service.mock_client.fetch_pieces.side_effect = [{0: header_data}, {0: tail_data}]
    with patch.object(service, '_assemble_data_from_pieces', new_callable=MagicMock) as mock_assemble:
        mock_assemble.side_effect = [header_data, tail_data]
        result_moov_data = await service._get_moov_atom_data(
            mock_handle, 0, video_file_size, piece_length=16384, infohash_hex="tail_moov_hash"
        )

    assert result_moov_data == real_moov_atom
    assert service.mock_client.fetch_pieces.call_count == 2

@pytest.mark.asyncio
async def test_get_queue_size(service):
    assert service.get_queue_size() == 0
    await service.task_queue.put("task1")
    assert service.get_queue_size() == 1
    await service.task_queue.get()
    assert service.get_queue_size() == 0

@pytest.mark.skip(reason="Needs update for mock return value of _process_keyframe_pieces")
@pytest.mark.asyncio
async def test_generate_screenshots_filters_existing(service):
    infohash = "filter_test_hash"
    service.screenshot_check_callback = AsyncMock(return_value=[f"{infohash}_90.jpg"])

    mock_handle = MagicMock()
    mock_ti = MagicMock()
    mock_handle.get_torrent_info.return_value = mock_ti
    mock_ti.piece_length.return_value = 16384

    # The keyframes that will be "found" by the extractor
    kfs_to_process = [Keyframe(0, 0, 30 * 90000, 90000), Keyframe(1, 1, 60 * 90000, 90000)]
    kfs_to_filter = [Keyframe(2, 2, 90 * 90000, 90000)]
    all_kfs = kfs_to_process + kfs_to_filter

    # This mock should return the set of keyframes that it "processed"
    mock_process_return = ({kf.index for kf in kfs_to_process}, {})

    with patch.object(service, '_find_video_file', return_value=(0, 1000, 0, 'video.mp4')), \
         patch.object(service, '_get_moov_atom_data', new_callable=AsyncMock, return_value=b'moovdata'), \
         patch.object(service, '_process_keyframe_pieces', new_callable=AsyncMock, return_value=mock_process_return) as mock_process, \
         patch('screenshot.service.KeyframeExtractor') as MockExtractor:

        mock_extractor_instance = MockExtractor.return_value
        mock_extractor_instance.keyframes = all_kfs
        mock_extractor_instance.timescale = 90000
        mock_extractor_instance.duration_pts = 100 * 90000
        mock_extractor_instance.samples = [MagicMock(size=100, offset=i*100) for i in range(len(all_kfs))]

        with patch.object(service, '_select_keyframes', return_value=all_kfs):
            await service._generate_screenshots_from_torrent(mock_handle, infohash)

            # Assert that _process_keyframe_pieces was called with the correct filtered list
            call_args = mock_process.call_args[0]
            remaining_keyframes_arg = call_args[5]
            assert len(remaining_keyframes_arg) == 2
            remaining_pts = {kf.pts for kf in remaining_keyframes_arg}
            assert 90 * 90000 not in remaining_pts
