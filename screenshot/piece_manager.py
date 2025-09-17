# -*- coding: utf-8 -*-
"""
本模块定义了 PieceManager 类，它是一个重要的抽象层，负责从 torrent 中
获取和组装指定范围的数据块 (pieces)。
"""
import asyncio
import logging
from typing import Dict

from .client import TorrentClient, TorrentClientError


class PieceManager:
    """
    为一个特定的 torrent 任务处理数据块的获取和组装。

    它的核心作用是将对任意字节范围的高级请求（例如“获取从偏移量 X 开始的 Y 字节数据”）
    转换为对底层 TorrentClient 的、针对特定数据块索引的低级请求。
    """
    def __init__(self, client: TorrentClient, handle, piece_length: int, infohash_hex: str):
        self.client = client
        self.handle = handle
        self.piece_length = piece_length
        self.infohash_hex = infohash_hex
        self.log = logging.getLogger("PieceManager")

    def _get_pieces_for_range(self, offset_in_torrent: int, size: int) -> list[int]:
        """根据在 torrent 内的字节范围，计算出所有需要下载的数据块索引。"""
        if size <= 0:
            return []
        # 计算起始数据块和结束数据块的索引
        start_piece = offset_in_torrent // self.piece_length
        end_piece = (offset_in_torrent + size - 1) // self.piece_length
        return list(range(start_piece, end_piece + 1))

    def _assemble_data_from_pieces(self, pieces_data: Dict[int, bytes], offset_in_torrent: int, size: int) -> bytes:
        """从一个包含原始数据块数据的字典中，精确地组装出请求的数据段。"""
        start_piece = offset_in_torrent // self.piece_length
        end_piece = (offset_in_torrent + size - 1) // self.piece_length

        # 验证所有需要的数据块都已成功下载
        for piece_index in range(start_piece, end_piece + 1):
            if piece_index not in pieces_data:
                self.log.warning("[%s] 组装数据时缺少数据块 #%d。", self.infohash_hex, piece_index)
                return b""

        # 创建一个目标大小的缓冲区
        buffer = bytearray(size)
        buffer_offset = 0
        # 遍历所需的数据块，并将相关部分复制到缓冲区中
        for piece_index in range(start_piece, end_piece + 1):
            piece_data = pieces_data[piece_index]

            # 计算在此数据块中要复制的起始和结束位置
            copy_from_start = offset_in_torrent % self.piece_length if piece_index == start_piece else 0
            copy_to_end = (offset_in_torrent + size - 1) % self.piece_length + 1 if piece_index == end_piece else self.piece_length

            chunk = piece_data[copy_from_start:copy_to_end]

            # 计算实际要复制的字节数，防止超出缓冲区范围
            bytes_to_copy = min(len(chunk), size - buffer_offset)
            if bytes_to_copy > 0:
                buffer[buffer_offset : buffer_offset + bytes_to_copy] = chunk[:bytes_to_copy]
                buffer_offset += bytes_to_copy
        return bytes(buffer)

    async def fetch_and_assemble_range(self, offset: int, size: int, timeout: int) -> bytes:
        """
        获取指定范围所需的所有数据块，并将它们组装成一个连续的字节串。
        这是该类的主要公共接口。
        """
        if size <= 0:
            return b""

        # 1. 确定需要哪些数据块
        needed_pieces = self._get_pieces_for_range(offset, size)
        if not needed_pieces:
            return b""

        # 2. 调用底层客户端去获取这些数据块
        try:
            pieces_data = await self.client.fetch_pieces(self.handle, needed_pieces, timeout=timeout)
        except TorrentClientError as e:
            self.log.warning("[%s] 获取范围 (%d, %d) 的数据块失败: %s", self.infohash_hex, offset, size, e)
            # 将异常重新抛出，由调用者处理
            raise

        # 3. 从获取到的数据块中组装出最终需要的数据
        return self._assemble_data_from_pieces(pieces_data, offset, size)
