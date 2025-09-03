# -*- coding: utf-8 -*-
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Configuration class for storing all application settings.
    Settings are loaded from a .env file.
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Scheduler settings
    scheduler_url: str = "http://127.0.0.1:8000"
    db_url: str = "sqlite:///./scheduler.db"

    # Worker settings
    num_workers: int = 10
    worker_idle_time: int = 30  # seconds
    worker_concurrency: int = 4  # The maximum number of tasks a worker can process concurrently

    # Screenshot service settings
    output_dir: str = "./screenshots_output"
    torrent_save_path: str = "./torrent_files"
    min_screenshots: int = 2
    max_screenshots: int = 10
    default_screenshots: int = 20
    target_interval_sec: int = 10

    # Timeouts for various operations
    metadata_timeout: int = 180  # 3 minutes
    moov_probe_timeout: int = 120  # 2 minutes
    piece_fetch_timeout: int = 60  # 1 minute
    piece_queue_timeout: int = 300  # 5 minutes
