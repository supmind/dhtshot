# -*- coding: utf-8 -*-
"""
本模块提供了根据预定策略从视频文件中选择代表性关键帧子集的逻辑。
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
    根据配置的策略选择关键帧的子集。

    主要步骤如下：
    1.  从视频的开头和结尾裁剪掉一定百分比的关键帧，以避免片头和片尾。
    2.  根据视频的总时长和配置，动态确定要生成的截图数量。
    3.  选择与均匀分布的目标时间戳最接近的关键帧。
    """
    if not all_keyframes:
        return []

    # 步骤 1: 从开头和结尾裁剪一定百分比的关键帧
    trim_percentage = settings.keyframe_trim_percentage
    if 0 < trim_percentage < 0.5:
        total_keyframes = len(all_keyframes)
        trim_count = int(total_keyframes * trim_percentage)
        # 确保裁剪后仍有关键帧剩余
        if trim_count > 0 and total_keyframes > trim_count * 2:
            all_keyframes = all_keyframes[trim_count:-trim_count]

    if not all_keyframes:
        return []

    # 步骤 2: 根据视频时长和配置确定截图数量
    # 如果 `duration_pts` 未提供，则尝试从 `samples` 中获取
    if duration_pts == 0 and samples:
        duration_pts = samples[-1].pts

    duration_sec = duration_pts / timescale if timescale > 0 else 0
    num_screenshots = settings.default_screenshots
    if duration_sec > 0:
        # 根据目标间隔计算截图数量，并确保其在配置的最小和最大值之间
        num_screenshots = max(
            settings.min_screenshots,
            min(int(duration_sec / settings.target_interval_sec), settings.max_screenshots)
        )

    # 如果剩余的关键帧数量少于或等于目标数量，则直接返回所有剩余关键帧
    if len(all_keyframes) <= num_screenshots:
        return all_keyframes

    # 步骤 3: 选择与目标时间戳最接近的关键帧
    # 首先，计算出 N 个均匀分布在视频时长内的目标时间戳 (以 pts 为单位)
    target_timestamps_pts = [int(i * duration_pts / num_screenshots) for i in range(num_screenshots)]

    selected_keyframes = []
    # 对于每个目标时间戳，找到时间上最接近它的关键帧
    for target_pts in target_timestamps_pts:
        # 使用 min 函数和 lambda 表达式高效地找到差异最小的项
        closest_keyframe = min(all_keyframes, key=lambda kf: abs(kf.pts - target_pts))
        # 避免重复添加同一个关键帧
        if closest_keyframe not in selected_keyframes:
            selected_keyframes.append(closest_keyframe)

    # 最后，按时间戳对选出的关键帧进行排序并返回
    selected_keyframes.sort(key=lambda kf: kf.pts)
    return selected_keyframes
