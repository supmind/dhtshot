# Screenshot Core Library

This directory contains the core library responsible for the underlying logic of fetching and processing video data from torrents. It is designed to be a self-contained, high-performance module that can be used by other applications (like the `worker.py` in the parent directory).

## Core Components

-   **`service.py` (`ScreenshotService`)**
    This is the main orchestrator for a single screenshot task. It coordinates the other components to perform the end-to-end workflow, from fetching metadata to generating the final JPG files. It also handles error classification (e.g., recoverable vs. permanent failures) and generates `resume_data` for fault tolerance.

-   **`client.py` (`TorrentClient`)**
    This is a sophisticated, `asyncio`-friendly wrapper around the `libtorrent` library. It handles all low-level BitTorrent network operations, such as managing the session, adding torrents via magnet links, fetching metadata, and downloading specific pieces on demand. It uses a separate thread for `libtorrent`'s blocking calls to ensure the main `asyncio` event loop is never blocked.

-   **`extractor.py` (`KeyframeExtractor`)**
    A robust parser for the MP4 container format. Its primary job is to parse the `moov` atom (the video's "table of contents") to build a complete map of all video frames (samples) without needing to download the entire file. It identifies keyframes and their precise byte-level offsets and sizes, which is crucial for the minimal download strategy. It supports H.264, H.265/HEVC, and AV1 video codecs.

-   **`generator.py` (`ScreenshotGenerator`)**
    This component is responsible for the final stage: decoding raw video frame data and saving it as a JPEG image. It uses the `PyAV` library (which wraps FFmpeg) and intelligently offloads the CPU-intensive decoding work to a separate thread pool to keep the application responsive. It has been modified to support a real-time, per-screenshot success callback.

-   **`config.py` (`Settings`)**
    Defines all configurable parameters for the library (e.g., timeouts, number of screenshots) using Pydantic-Settings, allowing for configuration via environment variables or a `.env` file.

-   **`errors.py`**
    Defines a set of custom, structured exception classes (e.g., `MetadataTimeoutError`, `MoovNotFoundError`) that represent specific failure modes within the screenshotting process. This allows for clear and precise error handling by the calling application.
