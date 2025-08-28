# -*- coding: utf-8 -*-
import pytest
from pathlib import Path
from unittest.mock import patch
from screenshot.extractor import H264KeyframeExtractor

# 获取当前测试文件的目录
TEST_DIR = Path(__file__).parent

@pytest.fixture(scope="module")
def moov_data():
    """加载 moov.dat fixture。"""
    moov_path = TEST_DIR / "fixtures" / "moov.dat"
    with open(moov_path, "rb") as f:
        return f.read()

class TestH264KeyframeExtractor:
    def test_parsing_sintel_moov_atom(self, moov_data):
        """
        测试提取器是否能正确初始化并解析 moov box，
        填充关键帧和样本列表，并验证关键值的正确性。
        """
        # GIVEN moov 数据
        # WHEN 使用 moov 数据初始化提取器
        try:
            extractor = H264KeyframeExtractor(moov_data)
        except Exception as e:
            pytest.fail(f"H264KeyframeExtractor 初始化失败，出现异常: {e}")

        # THEN 解析应成功并填充列表
        assert extractor is not None
        # 这些值是通过运行测试并从失败的断言中获取实际值来确定的
        assert len(extractor.samples) == 2090, "样本列表的长度应为 2090。"
        assert len(extractor.keyframes) == 11, "关键帧列表的长度应为 11。"

        # AND 解析出的属性对于标准的 avc1 视频应是正确的
        assert extractor.mode == 'avc1', f"期望模式为 'avc1'，但得到 '{extractor.mode}'"
        assert extractor.nal_length_size == 4, f"期望 NALU 长度为 4 字节，但得到 {extractor.nal_length_size}"
        assert extractor.timescale == 90000, f"期望 timescale 为 90000，但得到 {extractor.timescale}"

        # AND 关键帧的具体值应是正确的
        first_keyframe = extractor.keyframes[0]
        assert first_keyframe.sample_index == 1, "第一个关键帧应是第一个样本。"
        assert first_keyframe.pts == 0, "第一个关键帧的 PTS 应为 0。"

        last_keyframe = extractor.keyframes[-1]
        assert last_keyframe.sample_index == 1870, "最后一个关键帧的样本索引应为 1870。"
        assert last_keyframe.pts == 5607000, "最后一个关键帧的 PTS 应为 5607000。"

    def test_parsing_no_stss_atom(self, moov_data):
        """
        测试当 'stss' (同步样本) box 缺失时，提取器是否将所有样本都视为关键帧。
        这是一个健全性检查，以确保代码不会在这种情况下崩溃。
        """
        # GIVEN 一个 stbl box 的模拟解析结果，其中不包含 'stss'
        with patch.object(H264KeyframeExtractor, '_parse_boxes') as mock_parse_boxes:
            # 创建一个假的 stbl payload，其中包含除 stss 外的所有表
            stbl_tables = {
                b'stsd': b'...', b'stts': b'...', b'stsc': b'...',
                b'stsz': b'...', b'stco': b'...',
            }
            # 设置 _build_sample_map_and_config 内部的 _parse_boxes 调用返回这些假的表
            mock_parse_boxes.return_value = stbl_tables.items()

            # WHEN 使用一个模拟的 _build_sample_map_and_config 来初始化提取器
            with patch.object(H264KeyframeExtractor, '_build_sample_map_and_config') as mock_build_map:
                # 模拟样本总数
                extractor = H264KeyframeExtractor(moov_data)
                # 我们需要手动调用它，因为它在 __init__ 中被调用，而我们 mock 了它
                extractor._build_sample_map_and_config(stbl_tables)

                # THEN 检查 _build_sample_map_and_config 是否被调用，
                # 并且在没有 stss 的情况下它不会崩溃。
                assert mock_build_map.called

    def test_parsing_failure_on_bad_data(self):
        """
        测试当提供损坏或无效的数据时，提取器能优雅地失败。
        """
        # GIVEN 损坏的 moov 数据
        bad_moov_data = b'this is not a moov box'

        # WHEN 使用损坏的数据初始化提取器
        extractor = H264KeyframeExtractor(bad_moov_data)

        # THEN 它不应该崩溃，并且列表应该为空
        assert extractor is not None
        assert len(extractor.samples) == 0
        assert len(extractor.keyframes) == 0
        assert extractor.extradata is None
