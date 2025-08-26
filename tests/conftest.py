# -*- coding: utf-8 -*-
import pytest
import os
import struct

# 定义固件文件的路径
FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')
MOOV_DAT_PATH = os.path.join(FIXTURE_DIR, 'moov.dat')

from unittest.mock import MagicMock
from screenshot.client import TorrentClient

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

@pytest.fixture
def mock_client():
    """Provides a mock TorrentClient."""
    return MagicMock(spec=TorrentClient)

@pytest.fixture
def mock_handle():
    """
    Provides a mock libtorrent handle with an explicitly mocked file list.
    """
    handle = MagicMock()
    files_mock = MagicMock()

    # Create individual mock file entries
    file1 = MagicMock(path="video1.mkv", size=100)
    file2 = MagicMock(path="video2.mp4", size=200)
    file3 = MagicMock(path="other.txt", size=50)

    files_mock.num_files.return_value = 3
    def file_path_side_effect(index):
        return [file1.path, file2.path, file3.path][index]
    def file_size_side_effect(index):
        return [file1.size, file2.size, file3.size][index]

    files_mock.file_path.side_effect = file_path_side_effect
    files_mock.file_size.side_effect = file_size_side_effect

    handle.torrent_file.return_value.files.return_value = files_mock
    return handle
