# -*- coding: utf-8 -*-
"""
This module defines custom, structured exceptions for the screenshot service.
"""

class ScreenshotError(Exception):
    """Base class for all exceptions in this application."""
    pass


class TaskError(ScreenshotError):
    """
    Base class for errors related to a specific screenshot task.
    It includes the infohash for context.
    """
    def __init__(self, message: str, infohash: str):
        super().__init__(message)
        self.infohash = infohash

    def __str__(self):
        return f"[{self.infohash}] {super().__str__()}"


class MetadataTimeoutError(TaskError):
    """Raised when fetching torrent metadata (the .torrent file) times out."""
    pass


class NoVideoFileError(TaskError):
    """Raised when no suitable video file (e.g., .mp4) is found in the torrent."""
    pass


class MoovNotFoundError(TaskError):
    """Raised when the 'moov' atom cannot be located in the video file."""
    pass


class MoovParsingError(TaskError):
    """Raised when there is an error parsing the 'moov' atom after it has been located."""
    pass


class FrameDownloadTimeoutError(TaskError):
    """
    Raised when downloading the necessary pieces for keyframes times out.
    This is potentially a resumable error.
    """
    def __init__(self, message: str, infohash: str, resume_data=None):
        super().__init__(message, infohash)
        self.resume_data = resume_data


class FrameDecodeError(TaskError):
    """Raised when a video frame cannot be decoded by the underlying library (PyAV)."""
    pass


class TorrentClientError(ScreenshotError):
    """
    Raised for internal or unclassified errors within the TorrentClient,
    wrapping errors from the underlying libtorrent library.
    """
    pass
