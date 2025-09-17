# -*- coding: utf-8 -*-
"""
本模块包含 ScreenshotClient 的集成测试。

这些测试会连接到真实的 BitTorrent 网络，因此被标记为 'integration'，
以便可以与常规的单元测试分开运行。
"""
import pytest
import asyncio

from screenshot.client import TorrentClient
from screenshot.errors import MetadataTimeoutError
from config import Settings

# Since the functionality to fetch metadata from the network has been removed,
# there are currently no integration tests for the TorrentClient.
# This file is kept as a placeholder for future integration tests that
# might test other functionalities, like piece fetching with provided metadata.
