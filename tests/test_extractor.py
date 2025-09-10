# -*- coding: utf-8 -*-
"""
对 screenshot/extractor.py 的单元测试。

此文件将验证 MP4Extractor 和 MKVExtractor 是否能从
真实的视频文件数据中正确解析出元数据和关键帧信息。
"""
import pytest
import io
from pathlib import Path

from screenshot.extractor import MP4Extractor, MKVExtractor, Keyframe, SampleInfo
from screenshot.errors import MP4BoxNotFoundError, MP4ParsingError

# --- 配置 ---
TEST_VIDEO_DIR = Path(__file__).parent / "videos"

# --- 测试固件 (Fixtures) ---

@pytest.fixture(scope="module")
def h264_mp4_file() -> Path:
    """返回 H.264 MP4 视频文件的路径。"""
    return TEST_VIDEO_DIR / "h264_flushing_issue.mp4"

@pytest.fixture(scope="module")
def hevc_mp4_file() -> Path:
    """返回 HEVC MP4 视频文件的路径。"""
    return TEST_VIDEO_DIR / "hevc_video.mp4"

@pytest.fixture(scope="module")
def h264_mkv_file() -> Path:
    """返回 H.264 MKV 视频文件的路径。"""
    return TEST_VIDEO_DIR / "h264_video.mkv"

@pytest.fixture(scope="module")
def vp9_mkv_file() -> Path:
    """返回 VP9 MKV 视频文件的路径。"""
    return TEST_VIDEO_DIR / "vp9_video.mkv"

# --- 辅助函数 ---

def get_mp4_moov_box(file_path: Path) -> bytes:
    """从一个 MP4 文件中读取并返回 'moov' box 的内容。"""
    with open(file_path, "rb") as f:
        data = f.read()
    moov_offset = data.find(b'moov')
    if moov_offset == -1:
        raise ValueError(f"在文件 {file_path} 中未找到 'moov' box")
    box_size = int.from_bytes(data[moov_offset-4:moov_offset], 'big')
    return data[moov_offset-4:moov_offset-4+box_size]

def get_mkv_metadata_parts(file_path: Path) -> (bytes, bytes):
    """
    为测试 MKVExtractor，返回整个文件内容作为 head_data 和 cues_data。
    这更接近于实际情况，因为提取器应该能在完整的数据段中找到所需元素。
    """
    with open(file_path, "rb") as f:
        data = f.read()
    # 传递整个文件内容，让提取器自己解析
    return data, data

# --- MP4Extractor 测试用例 ---

def test_mp4_extractor_h264(h264_mp4_file):
    """测试 MP4Extractor 能否正确处理 H.264 (AVC) 视频。"""
    moov_data = get_mp4_moov_box(h264_mp4_file)
    extractor = MP4Extractor(moov_data)
    assert extractor.codec_name == 'h264'
    assert extractor.mode == 'avc1'
    assert isinstance(extractor.extradata, bytes) and len(extractor.extradata) > 0
    assert isinstance(extractor.timescale, int) and extractor.timescale > 0
    assert len(extractor.samples) > 0
    assert all(isinstance(s, SampleInfo) for s in extractor.samples)
    assert len(extractor.keyframes) > 0
    assert all(isinstance(k, Keyframe) for k in extractor.keyframes)
    keyframe_sample_indices = {k.sample_index for k in extractor.keyframes}
    assert keyframe_sample_indices.issubset(set(range(1, len(extractor.samples) + 2)))
    first_keyframe = extractor.keyframes[0]
    first_sample = extractor.samples[first_keyframe.sample_index - 1]
    assert first_keyframe.pts == first_sample.pts

def test_mp4_extractor_hevc(hevc_mp4_file):
    """测试 MP4Extractor 能否正确处理 HEVC (H.265) 视频。"""
    moov_data = get_mp4_moov_box(hevc_mp4_file)
    extractor = MP4Extractor(moov_data)
    assert extractor.codec_name == 'hevc'
    assert extractor.mode == 'hvc1'
    assert isinstance(extractor.extradata, bytes) and len(extractor.extradata) > 0
    assert isinstance(extractor.timescale, int) and extractor.timescale > 0
    assert len(extractor.samples) > 0
    assert all(isinstance(s, SampleInfo) for s in extractor.samples)
    assert len(extractor.keyframes) > 0
    assert all(isinstance(k, Keyframe) for k in extractor.keyframes)


# --- MP4Extractor 失败场景测试 ---

@pytest.fixture
def valid_moov_data(h264_mp4_file: Path) -> bytes:
    """提供一个有效的 'moov' box 数据作为基础，用于后续的篡改。"""
    return get_mp4_moov_box(h264_mp4_file)


def replace_box_name(data: bytes, old_name: bytes, new_name: bytes) -> bytes:
    """一个简单的辅助函数，用于在二进制数据中替换 box 的名称，用于制造解析错误。"""
    return data.replace(old_name, new_name)


def test_mp4_extractor_no_trak_box(valid_moov_data):
    """测试当 'moov' box 中没有 'trak' box 时，会引发 MP4BoxNotFoundError。"""
    # 将 'trak' box 的名称篡改为 'xxxx'
    corrupted_data = replace_box_name(valid_moov_data, b'trak', b'xxxx')
    with pytest.raises(MP4BoxNotFoundError) as excinfo:
        MP4Extractor(corrupted_data)
    assert excinfo.value.box_name == "trak"
    assert excinfo.value.parent_box_name == "moov"


def test_mp4_extractor_no_stbl_box(valid_moov_data):
    """测试当视频轨道中没有 'stbl' box 时，会引发 MP4BoxNotFoundError。"""
    corrupted_data = replace_box_name(valid_moov_data, b'stbl', b'xxxx')
    with pytest.raises(MP4BoxNotFoundError) as excinfo:
        MP4Extractor(corrupted_data)
    assert excinfo.value.box_name == "stbl"


def test_mp4_extractor_no_stsd_box(valid_moov_data):
    """测试当 'stbl' box 中没有 'stsd' box 时，会引发 MP4BoxNotFoundError。"""
    corrupted_data = replace_box_name(valid_moov_data, b'stsd', b'xxxx')
    with pytest.raises(MP4BoxNotFoundError) as excinfo:
        MP4Extractor(corrupted_data)
    assert excinfo.value.box_name == "stsd"


def test_mp4_extractor_no_stco_box(valid_moov_data):
    """测试当 'stbl' box 中没有 'stco' 或 'co64' box 时，会引发 MP4BoxNotFoundError。"""
    # 同时移除 stco 和 co64 (如果存在)
    corrupted_data = replace_box_name(valid_moov_data, b'stco', b'xxxx')
    corrupted_data = replace_box_name(corrupted_data, b'co64', b'yyyy')
    with pytest.raises(MP4BoxNotFoundError) as excinfo:
        MP4Extractor(corrupted_data)
    assert "'stco' or 'co64'" in str(excinfo.value)


def test_mp4_extractor_no_supported_codec(valid_moov_data):
    """测试当 'stsd' box 中没有受支持的编解码器条目时，会引发 MP4ParsingError。"""
    corrupted_data = replace_box_name(valid_moov_data, b'avc1', b'xxxx')
    with pytest.raises(MP4ParsingError) as excinfo:
        MP4Extractor(corrupted_data)
    assert "未找到任何受支持的视频采样条目" in str(excinfo.value)


# --- MKVExtractor 测试用例 ---

def test_mkv_extractor_h264(h264_mkv_file):
    """测试 MKVExtractor 能否正确处理 H.264 (AVC) 视频。"""
    head_data, cues_data = get_mkv_metadata_parts(h264_mkv_file)
    extractor = MKVExtractor(head_data, cues_data)
    extractor.parse()

    assert extractor.codec_name == 'h264'
    assert isinstance(extractor.extradata, bytes) and len(extractor.extradata) > 0
    assert isinstance(extractor.timescale, int) and extractor.timescale > 0
    assert len(extractor.samples) > 0
    assert all(isinstance(s, SampleInfo) for s in extractor.samples)
    assert len(extractor.keyframes) > 0
    assert all(isinstance(k, Keyframe) for k in extractor.keyframes)
    assert len(extractor.keyframes) == len(extractor.samples)

def test_mkv_extractor_vp9(vp9_mkv_file):
    """测试 MKVExtractor 能否正确处理 VP9 视频。"""
    head_data, cues_data = get_mkv_metadata_parts(vp9_mkv_file)

    extractor = MKVExtractor(head_data, cues_data)
    extractor.parse()

    assert extractor.codec_name == 'vp9' # 映射自 V_VP9
    # VP9 通常没有 extradata，所以我们断言它为 None 或空
    assert extractor.extradata is None or len(extractor.extradata) == 0
    assert isinstance(extractor.timescale, int) and extractor.timescale > 0
    assert len(extractor.samples) > 0
    assert len(extractor.keyframes) > 0

def test_mkv_extractor_no_cues(vp9_mkv_file):
    """测试当 MKV 文件中缺少 'Cues' 元素时，提取器能够优雅地处理。"""
    head_data, cues_data = get_mkv_metadata_parts(vp9_mkv_file)

    # 通过从 cues_data 中移除 Cues 元素来模拟一个损坏的文件
    # Cues element ID in EBML is 0x1C53BB6B
    cues_id = b'\x1C\x53\xBB\x6B'
    cues_pos = cues_data.find(cues_id)
    assert cues_pos != -1, "测试文件中断言应包含 Cues 元素"
    # 创建一个没有 Cues 的版本
    corrupted_cues_data = cues_data[:cues_pos]

    # 使用损坏的 cues 数据进行初始化
    extractor = MKVExtractor(head_data, corrupted_cues_data)
    extractor.parse() # 应该不会抛出异常

    # 断言 keyframes 和 samples 列表为空
    assert extractor.codec_name == 'vp9' # head_data 仍然是有效的
    assert len(extractor.samples) == 0
    assert len(extractor.keyframes) == 0
