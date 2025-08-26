# -*- coding: utf-8 -*-
import pytest
import os
import struct

# 定义测试视频文件的路径
TEST_VIDEO_PATH = os.path.join(os.path.dirname(__file__), 'Big_Buck_Bunny_720_10s_10MB.mp4')

def find_box_data(file_path, box_type_str):
    """
    一个辅助函数，用于在 MP4 文件中查找指定类型的 box 并返回其内容。
    这对于隔离 'moov' box 进行单元测试非常有用。
    """
    with open(file_path, 'rb') as f:
        while True:
            header_bytes = f.read(8)
            if not header_bytes:
                break

            size, box_type_bytes = struct.unpack('>I4s', header_bytes)
            box_type = box_type_bytes.decode('ascii', errors='ignore')

            if size == 1: # 64-bit size
                size_bytes = f.read(8)
                if not size_bytes: break
                size = struct.unpack('>Q', size_bytes)[0]
                content_size = size - 16
            else:
                content_size = size - 8

            if box_type == box_type_str:
                # 找到了！返回整个 box 的数据 (header + content)
                f.seek(f.tell() - 8 - (8 if size == 1 else 0)) # 回到 box 的起始位置
                return f.read(size)

            # 跳转到下一个 box
            f.seek(content_size, os.SEEK_CUR)
    return None

@pytest.fixture(scope="session")
def moov_data():
    """
    一个 pytest fixture，它在模块测试开始前运行一次，
    负责找到并加载 'moov' box 的数据，供后续测试用例使用。
    """
    # 确认测试视频文件存在
    assert os.path.exists(TEST_VIDEO_PATH), f"测试视频文件不存在于: {TEST_VIDEO_PATH}"
    data = find_box_data(TEST_VIDEO_PATH, 'moov')
    assert data is not None, f"测试失败：未能在 {TEST_VIDEO_PATH} 文件中找到 'moov' box。"
    return data
