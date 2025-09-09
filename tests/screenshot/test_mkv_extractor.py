# -*- coding: utf-8 -*-
"""
对 screenshot/extractor.py 中 MKVExtractor 功能的单元测试。
该测试使用一个真实生成的 MKV 文件。
"""
import pytest
from screenshot.extractor import MKVExtractor

# --- 测试资源 ---
TEST_MKV_PATH = "tests/assets/test_video.mkv"

@pytest.fixture(scope="module")
def mkv_file_data():
    """
    一个 Pytest fixture，从测试 MKV 文件中读取所有数据。
    """
    try:
        with open(TEST_MKV_PATH, "rb") as f:
            return f.read()
    except FileNotFoundError:
        pytest.fail(f"测试视频文件未找到: {TEST_MKV_PATH}。请先运行 'tests/utils/create_mkv_test_video.py' 生成该文件。")

def test_mkv_extractor_parses_correctly(mkv_file_data):
    """
    测试 MKVExtractor 是否能从一个真实的 MKV 文件中正确解析出所有元数据。
    """
    # 在这个测试中，我们给提取器完整的文件数据，因为它需要访问
    # 文件头部的 Tracks 和文件尾部的 Cues。
    extractor = MKVExtractor(head_data=mkv_file_data, cues_data=mkv_file_data)
    extractor.parse()

    # 1. 验证编解码器和配置信息
    assert extractor.codec_name == "h264"
    assert extractor.extradata is not None
    # The timescale in the generated file is 1000.
    assert extractor.timescale == 1000

    # 2. 验证关键帧信息
    # 我们生成了5秒，24fps的视频，应该有几个关键帧。
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
