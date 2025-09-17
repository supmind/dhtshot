# -*- coding: utf-8 -*-
"""
This module provides the logic for selecting a representative subset of keyframes
from a video file based on a defined strategy.
"""
from typing import List
from .extractor import Keyframe
from config import Settings


def select_keyframes(
    all_keyframes: List[Keyframe],
    timescale: int,
    duration_pts: int,
    settings: Settings,
    samples: list = None
) -> List[Keyframe]:
    """
    Selects a subset of keyframes based on the configured strategy.

    - Trims a percentage of keyframes from the beginning and end.
    - Determines the target number of screenshots based on video duration.
    - Selects keyframes that are closest to evenly spaced timestamps.
    """
    if not all_keyframes:
        return []

    # 1. Trim a percentage from the start and end
    trim_percentage = settings.keyframe_trim_percentage
    if 0 < trim_percentage < 0.5:
        total_keyframes = len(all_keyframes)
        trim_count = int(total_keyframes * trim_percentage)
        if trim_count > 0 and total_keyframes > trim_count * 2:
            all_keyframes = all_keyframes[trim_count:-trim_count]

    if not all_keyframes:
        return []

    # 2. Determine the number of screenshots
    if duration_pts == 0 and samples:
        duration_pts = samples[-1].pts

    duration_sec = duration_pts / timescale if timescale > 0 else 0
    num_screenshots = settings.default_screenshots
    if duration_sec > 0:
        num_screenshots = max(
            settings.min_screenshots,
            min(int(duration_sec / settings.target_interval_sec), settings.max_screenshots)
        )

    if len(all_keyframes) <= num_screenshots:
        return all_keyframes

    # 3. Select keyframes closest to target timestamps
    target_timestamps_pts = [int(i * duration_pts / num_screenshots) for i in range(num_screenshots)]

    selected_keyframes = []
    for target_pts in target_timestamps_pts:
        # Find the keyframe with the minimum absolute difference in presentation timestamp
        closest_keyframe = min(all_keyframes, key=lambda kf: abs(kf.pts - target_pts))
        if closest_keyframe not in selected_keyframes:
            selected_keyframes.append(closest_keyframe)

    selected_keyframes.sort(key=lambda kf: kf.pts)
    return selected_keyframes
