# -*- coding: utf-8 -*-
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    存储所有应用配置的类。
    配置从 .env 文件加载。
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --- 调度器设置 ---
    scheduler_url: str = "http://127.0.0.1:8000"
    db_url: str = "sqlite:///./scheduler.db"

    # --- 工作节点设置 ---
    num_workers: int = 10  # 单个 ScreenshotService 实例中用于处理任务的内部工作协程数量
    worker_idle_time: int = 30  # seconds
    worker_max_queue_size: int = 20  # 工作节点在暂停从调度器获取新任务前，其内部任务队列的最大长度

    # --- 截图服务设置 ---
    output_dir: str = "./screenshots_output"  # 存储最终截图的目录
    torrent_save_path: str = "./torrent_files"  # 存储 .torrent 文件的目录
    min_screenshots: int = 2
    max_screenshots: int = 10
    default_screenshots: int = 20
    target_interval_sec: int = 10

    # Timeouts for various operations
    metadata_timeout: int = 180  # 3 minutes
    moov_probe_timeout: int = 120  # 2 minutes
    piece_fetch_timeout: int = 60  # 1 minute
    piece_queue_timeout: int = 300  # 5 minutes
