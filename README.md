# Torrent-based Video Screenshot Service

## 1. Overview

This project provides a high-performance, asynchronous service for generating screenshots from video files contained within torrents. The key feature of this service is its **minimal download strategy**. Instead of downloading an entire torrent, it only fetches the necessary metadata and the specific data chunks required to extract keyframes, thus saving significant bandwidth and time.

The service is built with Python using `asyncio` for concurrency and leverages the powerful `libtorrent` library for all BitTorrent-related operations.

## 2. Core Features

- **Minimal Download:** Only downloads the torrent metadata and the specific pieces needed for keyframes.
- **High-Performance Concurrency:** Utilizes an `asyncio`-based worker pool to process multiple torrents (infohashes) simultaneously.
- **Robust `moov` Atom Parsing:** Implements a sophisticated two-stage search (header and tail probing) to reliably locate the `moov` atom (video metadata) in various MP4 file structures.
- **Resilience:** Correctly handles common torrenting issues like timeouts and differentiates between permanent and recoverable errors.
- **Configurable:** Settings such as worker count, timeouts, and screenshotting strategy are managed via a Pydantic settings class and can be overridden by environment variables.

## 3. Architecture and Design

The service is composed of several key components that work together in an asynchronous pipeline.

### 3.1. Components

- **`ScreenshotService` (`service.py`):**
  - The central orchestrator of the application.
  - Manages a task queue (`asyncio.Queue`) for incoming infohash jobs.
  - Spawns and manages a pool of worker coroutines to process tasks concurrently.
  - Coordinates interactions between the `TorrentClient`, `H264KeyframeExtractor`, and `ScreenshotGenerator`.

- **`TorrentClient` (`client.py`):**
  - A sophisticated wrapper around the `libtorrent` C++ library.
  - Manages a single `libtorrent` session to handle all torrents efficiently.
  - Provides an `asyncio`-friendly interface for torrent operations like adding torrents, fetching metadata, and requesting specific pieces.
  - Implements a thread-safe alert loop to process notifications from `libtorrent` without blocking the main event loop.

- **`H264KeyframeExtractor` (`extractor.py`):**
  - A robust parser for MP4 file structures.
  - Takes the raw bytes of a `moov` atom and parses it to build a complete map of all samples (video frames).
  - Identifies which samples are keyframes, providing the necessary information (offsets, sizes) to fetch them.

- **`ScreenshotGenerator` (`generator.py`):**
  - Responsible for the final step of generating a JPG screenshot from raw video frame data.
  - Uses the `PyAV` library (an FFMpeg wrapper) to decode the H.264 frame data and save it as an image.

### 3.2. Workflow

The process for generating screenshots from a single infohash follows these steps:

1.  **Task Submission:** An `infohash` is submitted to the `ScreenshotService`, which places it in a queue.
2.  **Worker Assignment:** An available worker picks up the task.
3.  **Metadata Fetching:** The worker, via the `TorrentClient`, adds the torrent using a magnet link and waits for the metadata to be downloaded from the DHT network.
4.  **`moov` Atom Probing:**
    - **Head Probe:** The service first downloads a small chunk (the first 256KB) from the beginning of the video file to search for the `moov` atom. If found, it proceeds.
    - **Tail Probe:** If the `moov` atom is not found in the header, the service assumes it's at the end. It calculates the size of the tail to probe (either a fixed 10MB, or a dynamic size if a large `mdat` box was found in the header) and downloads that chunk. It then uses a robust "find and validate" search to locate the `moov` atom within this tail data.
5.  **Keyframe Extraction:** The downloaded `moov` data is passed to the `H264KeyframeExtractor`, which parses it and returns a list of all keyframes in the video.
6.  **Keyframe Selection:** The service applies a selection strategy (e.g., selecting 5 keyframes evenly distributed throughout the video) to choose a representative set of frames for the screenshots.
7.  **Targeted Piece Downloading:** The service calculates exactly which torrent pieces are needed to reconstruct the data for the selected keyframes. It then instructs the `TorrentClient` to download only these specific pieces.
8.  **Screenshot Generation:** As each required piece finishes downloading, the service assembles the keyframe data and passes it to the `ScreenshotGenerator`, which decodes the frame and saves it as a `.jpg` file.
9.  **Task Completion:** Once all selected keyframes are processed, the task is marked as complete.

## 4. Setup and Usage

### 4.1. Installation

The project uses Python 3.10+. All dependencies are listed in `requirements.txt`.

```bash
# Install all required packages
pip install -r requirements.txt
```

### 4.2. Running the Example

The `example.py` script demonstrates how to use the service. It submits a list of pre-defined infohashes and prints status updates as they are processed.

```bash
# Run the example script
python3 example.py
```
Screenshots will be saved in the `./screenshots_output` directory by default.

## 5. Code Improvement Suggestions

The current architecture is solid and performant, but the following areas could be enhanced for a production environment:

1.  **Enhanced Resume/Recovery Logic:** The service currently has the building blocks for resuming tasks (passing `resume_data`), but there is no application-level logic to persist this data and automatically re-submit tasks that fail with recoverable errors (e.g., timeouts). A persistent queue (like Redis or a database-backed queue) could be implemented to manage retries.
2.  **Expanded Format Support:** The `H264KeyframeExtractor` is specific to H.264 streams in an MP4 container. This could be abstracted into a more general `Extractor` class with different implementations for other common codecs and containers (e.g., HEVC, VP9, MKV).
3.  **Improved Test Coverage:** The project has a good testing structure, but coverage should be expanded, particularly for the complex asynchronous interactions in the `TorrentClient` and the various error-handling paths in the `ScreenshotService`.
4.  **Resource Management Configuration:** The existing Pydantic-based settings could be expanded to include `libtorrent` session-level settings (e.g., `active_torrents_limit`, `max_peerlist_size`, connection limits). This would provide finer-grained control over resource usage in a large-scale deployment.
5.  **Graceful Shutdown Enhancement:** The current shutdown logic cancels worker tasks. For long-running keyframe processing, it might be beneficial to allow in-progress keyframes to finish processing before shutting down, while preventing new tasks from starting.
