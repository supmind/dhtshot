# -*- coding: utf-8 -*-
"""
该模块为截图服务定义了自定义的、结构化的异常。
"""

class ScreenshotError(Exception):
    """本应用中所有异常的基类。"""
    pass


class TaskError(ScreenshotError):
    """
    与特定截图任务相关的错误的基类。
    它包含 infohash 以提供上下文。
    """
    def __init__(self, message: str, infohash: str):
        super().__init__(message)
        self.infohash = infohash

    def __str__(self):
        return f"[{self.infohash}] {super().__str__()}"


class MetadataTimeoutError(TaskError):
    """在获取 torrent 元数据（.torrent 文件）超时时引发。"""
    pass


class NoVideoFileError(TaskError):
    """在 torrent 中找不到合适的视频文件（例如 .mp4）时引发。"""
    pass


class MoovNotFoundError(TaskError):
    """在视频文件中无法定位 'moov' atom 时引发。"""
    pass


class MoovFetchError(TaskError):
    """在尝试获取 moov atom 数据时发生客户端错误时引发。"""
    pass


class MoovParsingError(TaskError):
    """在定位到 'moov' atom 后解析时发生错误时引发。"""
    pass


class FrameDownloadTimeoutError(TaskError):
    """
    在下载关键帧所需的 piece 超时时引发。
    这可能是一个可恢复的错误。
    """
    def __init__(self, message: str, infohash: str, resume_data=None):
        super().__init__(message, infohash)
        self.resume_data = resume_data


class FrameDecodeError(TaskError):
    """当视频帧无法被底层库（PyAV）解码时引发。"""
    pass


class TorrentClientError(ScreenshotError):
    """
    为 TorrentClient 内部的或未分类的错误引发，
    包装来自底层 libtorrent 库的错误。
    """
    pass
