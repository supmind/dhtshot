# -*- coding: utf-8 -*-
import pytest
import os
import struct

# 定义固件文件的路径
FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')
MOOV_DAT_PATH = os.path.join(FIXTURE_DIR, 'moov.dat')

@pytest.fixture(scope="session")
def moov_data():
    """
    一个 pytest fixture，它在测试会话开始前只运行一次，
    负责从预先提取的 'moov.dat' 文件中加载 'moov' box 的二进制数据。
    这避免了在每次测试运行时都去解析一个完整的视频文件，从而提高了效率和稳定性。
    """
    # 确认 'moov.dat' 文件存在
    assert os.path.exists(MOOV_DAT_PATH), f"测试固件文件 'moov.dat' 不存在于: {MOOV_DAT_PATH}"

    with open(MOOV_DAT_PATH, 'rb') as f:
        data = f.read()

    assert data is not None and len(data) > 0, f"测试失败：未能从 {MOOV_DAT_PATH} 文件中读取到有效数据。"
    return data
