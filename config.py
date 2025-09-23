# -*- coding: utf-8 -*-
"""
本模块定义了应用的所有配置参数。
通过使用 Pydantic 的 BaseSettings，配置可以从环境变量或 .env 文件中加载，
从而实现了配置与代码的分离。
"""
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """
    应用配置模型，集中管理所有可调参数。
    """
    # --- 数据库配置 ---
    db_url: str = "sqlite:///./scheduler.db"

    # --- 调度器 API 配置 ---
    scheduler_host: str = "http://localhost:8000"
    scheduler_api_key: str = "a_secure_api_key_please_change"

    # --- 工作节点 (Worker) 配置 ---
    worker_id_prefix: str = "worker"
    worker_poll_interval: int = 10  # 秒，Worker 空闲时轮询新任务的间隔
    worker_heartbeat_interval: int = 30  # 秒，Worker 发送心跳的间隔
    worker_http_timeout: int = 20  # 秒，Worker 调用调度器 API 的超时时间
    worker_max_queue_size: int = 10 # Worker 本地任务队列的最大长度

    # --- 调度器后台任务配置 ---
    scheduler_stuck_worker_timeout: int = 180  # 秒 (3 分钟)，判断 Worker 是否失联的阈值
    scheduler_max_task_duration: int = 1800 # 秒 (30 分钟)，一个任务最长的执行时间，超过则认为卡死
    scheduler_check_interval: int = 60  # 秒 (1 分钟)，调度器检查卡死任务的周期
    scheduler_retry_interval: int = 300 # 秒 (5 分钟)，调度器重试失败任务的周期

    # --- Torrent 客户端配置 ---
    torrent_save_path: str = "/tmp/torrent_files"
    listen_port: int = 6881
    dht_routers: list[tuple[str, int]] = [
        ("router.utorrent.com", 6881),
        ("dht.transmissionbt.com", 6881),
    ]
    lt_listen_interfaces: str = "0.0.0.0:6881"
    lt_active_limit: int = 100
    lt_active_downloads: int = 10
    lt_connections_limit: int = 200
    lt_half_open_limit: int = 50
    lt_upload_rate_limit: int = 0
    lt_download_rate_limit: int = 0
    lt_peer_connect_timeout: int = 15
    lt_cache_size: int = 2048

    # --- Moov Finder 配置 ---
    moov_head_probe_size: int = 256 * 1024 # 256 KB
    moov_probe_timeout: int = 120 # 2 分钟

    # --- Piece Manager & Downloader 配置 ---
    piece_download_timeout: int = 300  # 秒 (5 分钟)，下载单个数据块的超时时间

    # --- Keyframe Selector (截图选择算法) 配置 ---
    max_screenshots: int = 5
    min_screenshots: int = 1
    default_screenshots: int = 5
    keyframe_trim_percentage: float = 0.05 # 从视频的开头和结尾各裁剪掉的關鍵幀百分比
    min_interval_seconds: int = 60 # 两个关键帧之间的最小时间间隔（秒）
    target_interval_sec: int = 60 # 这个参数似乎与 min_interval_seconds 重复，但被测试使用

    # --- R2 对象存储配置 (可选) ---
    r2_endpoint_url: str = ""
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""
    r2_bucket_name: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
