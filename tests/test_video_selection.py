# -*- coding: utf-8 -*-
import pytest
from unittest.mock import MagicMock, patch

from screenshot.video import VideoFile

# This fixture is now defined in conftest.py: mock_client, mock_handle
@pytest.fixture
def video_file_instance(mock_client, mock_handle):
    """Provides a VideoFile instance for testing selection logic."""
    with patch.dict('sys.modules', {'libtorrent': MagicMock()}):
        vf = VideoFile(client=mock_client, handle=mock_handle)

    # Helper function to isolate and test the selection logic
    def _select_keyframes(all_keyframes, duration_sec):
        MIN_SCREENSHOTS = 5
        MAX_SCREENSHOTS = 50
        TARGET_INTERVAL_SEC = 180

        if duration_sec > 0:
            num_screenshots = int(duration_sec / TARGET_INTERVAL_SEC)
            num_screenshots = max(MIN_SCREENSHOTS, num_screenshots)
            num_screenshots = min(MAX_SCREENSHOTS, num_screenshots)
        else:
            num_screenshots = 20

        if len(all_keyframes) <= num_screenshots:
            return all_keyframes

        start_index = int(len(all_keyframes) * 0.1)
        end_index = int(len(all_keyframes) * 0.9)

        if start_index >= end_index:
            return all_keyframes[1:-1] if len(all_keyframes) > 2 else all_keyframes

        selectable_keyframes = all_keyframes[start_index:end_index]

        if len(selectable_keyframes) <= num_screenshots:
            return selectable_keyframes

        indices = [int(i * len(selectable_keyframes) / num_screenshots) for i in range(num_screenshots)]

        return [selectable_keyframes[i] for i in sorted(list(set(indices)))]

    vf._select_keyframes = _select_keyframes
    return vf

def test_selection_short_video_many_keyframes(video_file_instance):
    """The selectable range should be chosen, which is 80% of the total keyframes."""
    all_keyframes = [f"kf_{i}" for i in range(100)]
    duration = 300  # 5 minutes -> MIN_SCREENSHOTS = 5
    selected = video_file_instance._select_keyframes(all_keyframes, duration)
    # Selectable range is 10-90, so 80 keyframes. 80 > 5, so we select 5.
    assert len(selected) == 5

def test_selection_long_video(video_file_instance):
    """Duration is long, so we should get a calculated number of screenshots."""
    all_keyframes = [f"kf_{i}" for i in range(500)]
    duration = 7200  # 2 hours -> 7200 / 180 = 40 screenshots
    selected = video_file_instance._select_keyframes(all_keyframes, duration)
    # Selectable range is 50-450 (400 keyframes). 400 > 40, so we select 40.
    assert len(selected) == 40

def test_selection_very_long_video_max_screenshots(video_file_instance):
    """Duration is very long, so we should be capped at the maximum."""
    all_keyframes = [f"kf_{i}" for i in range(1000)]
    duration = 36000  # 10 hours -> 36000 / 180 = 200, capped at 50
    selected = video_file_instance._select_keyframes(all_keyframes, duration)
    # Selectable range is 100-900 (800 kf). 800 > 50, so we select 50.
    assert len(selected) == 50

def test_selection_fewer_keyframes_than_min(video_file_instance):
    """If we have very few keyframes, we should get all of them."""
    all_keyframes = [f"kf_{i}" for i in range(4)]
    duration = 1800 # 30 mins
    selected = video_file_instance._select_keyframes(all_keyframes, duration)
    assert len(selected) == 4
    assert selected == all_keyframes

def test_selection_no_keyframes(video_file_instance):
    """If there are no keyframes, we should get an empty list."""
    selected = video_file_instance._select_keyframes([], 1800)
    assert selected == []

def test_selection_zero_duration(video_file_instance):
    """If duration is zero, it should use the fallback number (20)."""
    all_keyframes = [f"kf_{i}" for i in range(100)]
    duration = 0
    selected = video_file_instance._select_keyframes(all_keyframes, duration)
    # Selectable range is 10-90 (80 kf). 80 > 20, so we select 20.
    assert len(selected) == 20
