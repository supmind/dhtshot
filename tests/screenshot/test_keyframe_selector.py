# -*- coding: utf-8 -*-
"""
对 screenshot/keyframe_selector.py 的单元测试。
"""
import pytest
from screenshot.keyframe_selector import select_keyframes
from screenshot.extractor import Keyframe
from config import Settings


def test_select_keyframes_logic():
    settings = Settings(
        default_screenshots=3,
        min_screenshots=3,
        max_screenshots=3,
        keyframe_trim_percentage=0.0,
        target_interval_sec=60
    )
    all_keyframes = [
        Keyframe(index=0, sample_index=0, pts=0, timescale=90000),
        Keyframe(index=1, sample_index=1, pts=10 * 90000, timescale=90000),
        Keyframe(index=2, sample_index=2, pts=20 * 90000, timescale=90000),
        Keyframe(index=3, sample_index=3, pts=88 * 90000, timescale=90000),
        Keyframe(index=4, sample_index=4, pts=95 * 90000, timescale=90000),
        Keyframe(index=5, sample_index=5, pts=170 * 90000, timescale=90000)
    ]
    duration_pts = 180 * 90000
    selected = select_keyframes(all_keyframes, 90000, duration_pts, settings, None)

    assert len(selected) == 3
    selected_pts = {kf.pts for kf in selected}
    expected_pts = {0, 88 * 90000, 95 * 90000}
    assert selected_pts == expected_pts

def test_keyframe_trimming():
    settings = Settings(
        keyframe_trim_percentage=0.1,
        max_screenshots=20,
        min_screenshots=1,
        target_interval_sec=1  # Use integer value
    )
    all_keyframes = [Keyframe(i, i, i * 1000, 1000) for i in range(20)]
    selected = select_keyframes(all_keyframes, 1000, 20000, settings, None)

    assert len(selected) == 16
    assert selected[0].pts == 2000
    assert selected[-1].pts == 17000

def test_not_enough_keyframes_to_trim():
    settings = Settings(
        keyframe_trim_percentage=0.1,
        max_screenshots=10,
        min_screenshots=1,
        target_interval_sec=1
    )
    all_keyframes = [Keyframe(i, i, i * 1000, 1000) for i in range(5)]
    selected = select_keyframes(all_keyframes, 1000, 5000, settings, None)

    assert len(selected) == 5

def test_selects_all_if_less_than_target():
    settings = Settings(
        default_screenshots=5,
        keyframe_trim_percentage=0.0,
        max_screenshots=10,
        min_screenshots=1,
        target_interval_sec=1
    )
    all_keyframes = [Keyframe(i, i, i * 1000, 1000) for i in range(4)]
    selected = select_keyframes(all_keyframes, 1000, 4000, settings, None)

    assert len(selected) == 4
    assert selected == all_keyframes
