# -*- coding: utf-8 -*-
"""
对 screenshot/extractor.py 中 MKVExtractor 功能的单元测试。
该测试使用一个真实生成的 MKV 文件。
"""
import pytest
from screenshot.extractor import MKVExtractor

@pytest.mark.parametrize(
    "video_path, expected_codec",
    [
        ("tests/assets/test_video.mkv", "h264"),
        ("tests/assets/test_hevc.mkv", "hevc"),
        ("tests/assets/test_av1.mkv", "av1"),
    ]
)
def test_mkv_extractor_parses_correctly(video_path, expected_codec):
    """
    测试 MKVExtractor 是否能从一个真实的、使用不同编解码器的 MKV 文件中
    正确解析出所有元数据。
    """
    try:
        with open(video_path, "rb") as f:
            mkv_file_data = f.read()
    except FileNotFoundError:
        pytest.fail(f"测试视频文件未找到: {video_path}。请先运行 'tests/utils/create_mkv_test_video.py' 生成该文件。")

    # 在这个测试中，我们给提取器完整的文件数据，因为它需要访问
    # 文件头部的 Tracks 和文件尾部的 Cues。
    extractor = MKVExtractor(head_data=mkv_file_data, cues_data=mkv_file_data)
    extractor.parse()

    # 1. 验证编解码器和配置信息
    assert extractor.codec_name == expected_codec
    assert extractor.extradata is not None
    assert extractor.timescale is not None

    # 2. 验证关键帧信息
    assert len(extractor.keyframes) > 0
    assert len(extractor.samples) > 0
    assert len(extractor.keyframes) == len(extractor.samples)

    # 验证第一个关键帧的数据
    keyframe = extractor.keyframes[0]
    sample = extractor.samples[0]

    assert sample.is_keyframe is True
    assert sample.offset > 0
    assert sample.pts >= 0
    assert keyframe.pts == sample.pts
    assert keyframe.sample_index == sample.index
