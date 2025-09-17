# -*- coding: utf-8 -*-
"""
本模块负责初始化和管理与 Redis 服务器的异步连接。
"""
from redis import asyncio as aioredis
from typing import Optional
import logging

from config import Settings

log = logging.getLogger(__name__)

# 全局变量，用于持有 Redis 客户端实例
redis_client: Optional[aioredis.Redis] = None

async def init_redis_pool():
    """
    创建并初始化一个全局的 Redis 连接池。
    此函数应在应用启动时被调用。
    """
    global redis_client
    settings = Settings()
    try:
        redis_client = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True # 自动将 Redis 的响应从 bytes 解码为 utf-8 字符串
        )
        # 测试连接
        await redis_client.ping()
        log.info("成功连接到 Redis 并初始化了连接池。")
    except Exception as e:
        log.error(f"无法连接到 Redis 或初始化连接池: {e}", exc_info=True)
        redis_client = None

async def close_redis_pool():
    """
    关闭全局的 Redis 连接池。
    此函数应在应用关闭时被调用。
    """
    global redis_client
    if redis_client:
        await redis_client.close()
        log.info("Redis 连接池已成功关闭。")

def get_redis_client() -> aioredis.Redis:
    """
    一个依赖项函数，用于在需要时获取 Redis 客户端实例。

    :raises ConnectionError: 如果 Redis 客户端尚未初始化。
    :return: 一个活动的 aioredis.Redis 实例。
    """
    if not redis_client:
        raise ConnectionError("Redis 客户端尚未初始化。请在应用启动时调用 init_redis_pool。")
    return redis_client
