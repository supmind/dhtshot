# -*- coding: utf-8 -*-
"""
对 screenshot/extractor.py 中 MP4Extractor 功能的单元测试。
"""
import pytest
import struct
from io import BytesIO

from screenshot.extractor import MP4Extractor
from screenshot.errors import MP4ParsingError

def get_moov_atom_data(video_path: str) -> bytes:
    """
    一个辅助函数，从给定的视频文件中读取 'moov' atom 的数据。
    """
    try:
        with open(video_path, "rb") as f:
            data = f.read()

        stream = BytesIO(data)
        while True:
            header_data = stream.read(8)
            if not header_data:
                break
            size, box_type_bytes = struct.unpack('>I4s', header_data)
            box_type = box_type_bytes.decode('ascii')

            if box_type == 'moov':
                stream.seek(stream.tell() - 8)
                return stream.read(size)

            if size == 1:
                size = struct.unpack('>Q', stream.read(8))[0]
                stream.seek(size - 16, 1)
            else:
                stream.seek(size - 8, 1)

    except FileNotFoundError:
        pytest.fail(f"测试视频文件未找到: {video_path}。")

    pytest.fail(f"在 {video_path} 中未找到 'moov' atom。")


@pytest.mark.parametrize(
    "video_path, expected_codec",
    [
        ("tests/assets/test_video.mp4", "h264"),
        ("tests/assets/test_hevc.mp4", "hevc"),
        ("tests/assets/test_av1.mp4", "av1"),
    ]
)
def test_mp4_extractor_parses_correctly(video_path, expected_codec):
    """
    测试 MP4Extractor 是否能从使用不同编解码器的 MP4 文件中正确解析元数据。
    """
    moov_data = get_moov_atom_data(video_path)
    extractor = MP4Extractor(moov_data)

    # 1. 验证编解码器和配置信息
    assert extractor.codec_name == expected_codec
    assert isinstance(extractor.extradata, bytes)
    assert len(extractor.extradata) > 0
    assert extractor.timescale > 0

    # 2. 验证样本和关键帧信息
    assert len(extractor.samples) > 0
    assert len(extractor.keyframes) > 0
    assert extractor.samples[0].is_keyframe is True


def test_extractor_with_invalid_data():
    """
    测试当提供无效或损坏的 'moov' 数据时，MP4Extractor 是否会引发异常。
    """
    invalid_data = b'this is not a valid moov box'
    with pytest.raises(ValueError, match="在 'moov' Box 中未找到有效的视频轨道"):
        MP4Extractor(invalid_data)
