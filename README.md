# 异步 Torrent 截图服务 (Async Torrent Screenshot Service)

这是一个功能强大且健壮的 Python 服务，能够通过 Torrent 协议下载视频文件，并为其自动生成一系列截图。本项目基于 `asyncio` 构建，具有高并发、高可用和可恢复的特性。

## 主要功能

- **基于 Torrent**: 直接使用 `infohash` 或 `magnet` 链接作为输入，无需预先下载视频。
- **智能关键帧提取**: 自动解析 MP4 文件结构 (`moov` atom)，智能定位 H.264 关键帧，只下载必要的数据块。
- **异步高并发**: 使用 `asyncio` 和多工作进程（worker）模型，可以同时处理多个截图任务。
- **健壮的错误处理**: 能够处理网络超时、种子失效等问题，并支持从失败点恢复任务。
- **灵活配置**: 支持自定义截图策略（如截图数量、间隔等）。
- **跨平台**: 可在 Linux, macOS, 和 Windows 上运行。

## 依赖要求

本项目依赖于以下 Python 库：
- `libtorrent`: 用于处理所有 Torrent 相关的功能。
- `bencode2`: Torrent 元数据编解码。
- `av`: (PyAV) 用于视频解码，是 `ffmpeg` 的 Python 封装。
- `Pillow`: 用于图像处理和保存。
- `bitstring` & `six`: 辅助库。

测试框架使用 `pytest` 和 `pytest-asyncio`。

## 安装

1. 克隆本代码库。
2. 强烈建议在虚拟环境（virtual environment）中进行安装。
3. 安装所有依赖：
   ```bash
   pip install -r requirements.txt
   ```

## 如何使用

项目提供了一个功能完整的示例脚本 `example.py`，用于演示如何使用 `ScreenshotService`。

直接运行即可：
```bash
python example.py
```

该脚本会：
1. 启动服务。
2. 提交多个开源电影的 `infohash` 作为截图任务。
3. 在后台处理这些任务，并将生成的截图保存在 `screenshots_output/` 目录下。
4. 按下 `Ctrl+C` 可以优雅地停止服务。

## 工作原理

本服务主要由以下几个核心组件构成：

- **`ScreenshotService`**: 核心的服务调度器。负责管理任务队列、工作进程（worker）和任务的生命周期（包括从失败中恢复）。
- **`TorrentClient`**: `libtorrent` 库的异步封装。负责处理所有底层 P2P 下载、元数据获取和数据块请求。
- **`H264KeyframeExtractor`**: MP4 文件解析器。它能智能地解析视频的 `moov` box，找到所有关键帧的位置和元数据。
- **`ScreenshotGenerator`**: 截图生成器。它使用 `PyAV` 库将原始的 H.264 关键帧数据包解码为 `.jpg` 图像。

## 近期改进 (全面代码审查)

本代码库近期经历了一次全面的代码审查和重构，显著提升了其稳定性、健壮性和可维护性。主要改进包括：
- **修复了 6 个关键 Bug**: 涵盖了任务恢复、并发安全、资源管理、数据完整性和跨平台兼容性等多个方面。
- **代码重构**: 优化了核心方法的代码结构，使其更清晰、更易于维护。
- **增强了测试覆盖**: 新增了关键的集成测试和单元测试，确保了核心业务流程（特别是任务恢复）的正确性。

详细的改进内容请参见 `FINAL_REPORT.md`。
