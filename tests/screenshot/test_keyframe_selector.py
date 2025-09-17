# -*- coding: utf-8 -*-
"""
对 screenshot/keyframe_selector.py 的单元测试。
"""
import pytest
from screenshot.keyframe_selector import select_keyframes
from screenshot.extractor import Keyframe
from config import Settings


@pytest.fixture
def settings():
    # Provide a baseline settings object
    return Settings()

def test_select_keyframes_logic(settings):
    all_keyframes = [
        Keyframe(index=0, sample_index=0, pts=0, timescale=90000),
        Keyframe(index=1, sample_index=1, pts=10 * 90000, timescale=90000),
        Keyframe(index=2, sample_index=2, pts=20 * 90000, timescale=90000),
        Keyframe(index=3, sample_index=3, pts=88 * 90000, timescale=90000),
        Keyframe(index=4, sample_index=4, pts=95 * 90000, timescale=90000),
        Keyframe(index=5, sample_index=5, pts=170 * 90000, timescale=90000)
    ]
    # Override settings for this specific test case
    settings.default_screenshots = 3
    settings.min_screenshots = 3
    settings.max_screenshots = 3
    settings.keyframe_trim_percentage = 0.0
    settings.target_interval_sec = 60 # duration_sec (180) / 60 = 3 screenshots

    duration_pts = 180 * 90000
    selected = select_keyframes(all_keyframes, 90000, duration_pts, settings, None)

    assert len(selected) == 3
    selected_pts = {kf.pts for kf in selected}
    expected_pts = {0, 88 * 90000, 95 * 90000}
    assert selected_pts == expected_pts

def test_keyframe_trimming(settings):
    all_keyframes = [Keyframe(i, i, i * 1000, 1000) for i in range(20)]
    settings.keyframe_trim_percentage = 0.1 # trim 10%, so 2 from start, 2 from end
    settings.max_screenshots = 20 # Ensure we don't cap the number of screenshots
    settings.min_screenshots = 1
    settings.target_interval_sec = 0.5 # 20s duration / 0.5 = 40, but capped at max_screenshots

    selected = select_keyframes(all_keyframes, 1000, 20000, settings, None)

    # After trimming, 16 keyframes remain. Since max_screenshots is 20, it should select all 16.
    assert len(selected) == 16
    assert selected[0].pts == 2000
    assert selected[-1].pts == 17000

def test_not_enough_keyframes_to_trim(settings):
    all_keyframes = [Keyframe(i, i, i * 1000, 1000) for i in range(5)]
    settings.keyframe_trim_percentage = 0.1 # Should not trim as 10% of 5 is < 1 on each side
    settings.max_screenshots = 10
    settings.min_screenshots = 1
    settings.target_interval_sec = 1

    selected = select_keyframes(all_keyframes, 1000, 5000, settings, None)

    assert len(selected) == 5

def test_selects_all_if_less_than_target(settings):
    all_keyframes = [Keyframe(i, i, i * 1000, 1000) for i in range(4)]
    settings.default_screenshots = 5
    settings.keyframe_trim_percentage = 0.0
    settings.max_screenshots = 10
    settings.min_screenshots = 1
    settings.target_interval_sec = 1

    selected = select_keyframes(all_keyframes, 1000, 4000, settings, None)

    # The number of screenshots to take is 4 (from duration/interval), but since we only have 4, it should just return all of them.
    assert len(selected) == 4
    assert selected == all_keyframes
