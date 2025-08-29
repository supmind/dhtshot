# -*- coding: utf-8 -*-
import pytest
from screenshot.errors import (
    ScreenshotError,
    TaskError,
    MetadataTimeoutError,
    NoVideoFileError,
    MoovNotFoundError,
    MoovParsingError,
    FrameDownloadTimeoutError,
    FrameDecodeError,
    TorrentClientError,
)

def test_task_error_str():
    """Tests that the __str__ method of TaskError includes the infohash."""
    infohash = "test_infohash"
    message = "This is a test error"
    err = TaskError(message, infohash)
    assert infohash in str(err)
    assert message in str(err)
    assert str(err) == f"[{infohash}] {message}"

def test_error_hierarchy():
    """Tests that the custom errors inherit from the correct base classes."""
    assert issubclass(TaskError, ScreenshotError)
    assert issubclass(MetadataTimeoutError, TaskError)
    assert issubclass(NoVideoFileError, TaskError)
    assert issubclass(MoovNotFoundError, TaskError)
    assert issubclass(MoovParsingError, TaskError)
    assert issubclass(FrameDownloadTimeoutError, TaskError)
    assert issubclass(FrameDecodeError, TaskError)
    assert issubclass(TorrentClientError, ScreenshotError)
    assert not issubclass(TorrentClientError, TaskError)

def test_frame_download_timeout_error_resume_data():
    """Tests that FrameDownloadTimeoutError can store resume_data."""
    infohash = "test_infohash"
    message = "Timeout"
    resume_data = {"some_key": "some_value"}
    err = FrameDownloadTimeoutError(message, infohash, resume_data=resume_data)
    assert err.resume_data == resume_data
    assert err.infohash == infohash
    assert message in str(err)

def test_all_errors_can_be_raised():
    """A simple test to ensure all defined custom errors can be raised."""
    with pytest.raises(ScreenshotError):
        raise ScreenshotError("test")
    with pytest.raises(TaskError):
        raise TaskError("test", "testhash")
    with pytest.raises(MetadataTimeoutError):
        raise MetadataTimeoutError("test", "testhash")
    with pytest.raises(NoVideoFileError):
        raise NoVideoFileError("test", "testhash")
    with pytest.raises(MoovNotFoundError):
        raise MoovNotFoundError("test", "testhash")
    with pytest.raises(MoovParsingError):
        raise MoovParsingError("test", "testhash")
    with pytest.raises(FrameDownloadTimeoutError):
        raise FrameDownloadTimeoutError("test", "testhash")
    with pytest.raises(FrameDecodeError):
        raise FrameDecodeError("test", "testhash")
    with pytest.raises(TorrentClientError):
        raise TorrentClientError("test")
