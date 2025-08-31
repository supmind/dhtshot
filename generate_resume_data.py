# -*- coding: utf-8 -*-
"""
本脚本用于以可控、快速、稳定的方式生成 'resume_data'。

工作原理:
通过 `unittest.mock.patch` 技术，我们完全接管了 ScreenshotService 的执行流程，
移除了所有真实的网络依赖，使其成为一个完全确定性的程序。

1.  Patch `TorrentClient.add_torrent`: 绕过真实的网络请求，立即返回一个
    包含虚拟文件信息的模拟 "handle"。
2.  Patch `ScreenshotService._get_moov_atom_data`: 绕过对 torrent 数据的下载，
    立即返回一个虚拟的 'moov' 数据。
3.  Patch `H264KeyframeExtractor`: 确保提取器能处理我们的虚拟 'moov' 数据，
    并返回一个结构正确的虚拟关键帧列表。
4.  Patch `ScreenshotService._process_keyframe_pieces`: 这是最后一步。当执行流
    到达这里时，任务状态已经完全建立。我们在此处立即抛出一个可恢复的异常，
    从而捕获包含所有已建立状态的 `resume_data`。
"""
import asyncio
import json
import logging
import base64
from unittest.mock import patch, MagicMock, AsyncMock

from screenshot.service import ScreenshotService, FrameDownloadTimeoutError, Keyframe, SampleInfo
from screenshot.client import TorrentClient

# --- 配置 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
INFO_HASH = "08ada5a7a6183aae1e09d831df6748d566095a10"
OUTPUT_FILE = "resume_data.json"

async def main():
    """主程序入口"""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    captured_resume_data = None

    async def status_callback(status: str, infohash: str, **kwargs):
        nonlocal captured_resume_data
        logging.info(f"CALLBACK: status={status.upper()}, infohash={infohash}")
        if status == 'recoverable_failure':
            resume_data = kwargs.get('resume_data')
            if resume_data:
                logging.info("✅ 成功捕获到可恢复的失败状态和 resume_data！")
                captured_resume_data = resume_data
                stop_event.set()

    # --- 定义所有需要的模拟对象和 Patch 函数 ---

    # 1. 模拟 TorrentClient.add_torrent
    mock_handle = MagicMock()
    mock_handle.is_valid.return_value = True
    mock_torrent_info = MagicMock()
    mock_torrent_info.piece_length.return_value = 16384
    mock_files = MagicMock()
    mock_files.num_files.return_value = 1
    mock_files.file_path.return_value = "video.mp4"
    mock_files.file_size.return_value = 16384 * 10
    mock_files.file_offset.return_value = 0
    mock_torrent_info.files.return_value = mock_files
    mock_handle.get_torrent_info.return_value = mock_torrent_info

    # 2. 模拟 H264KeyframeExtractor
    mock_extractor_class = MagicMock()
    extractor_instance = mock_extractor_class.return_value
    extractor_instance.keyframes = [Keyframe(index=i, sample_index=i+1, pts=i*1000, timescale=1000) for i in range(10)]
    extractor_instance.samples = [SampleInfo(offset=i*16384, size=16384, is_keyframe=True, index=i+1, pts=i*1000) for i in range(10)]
    extractor_instance.timescale = 1000
    extractor_instance.extradata = b'mock_extradata'
    extractor_instance.mode = 'avc1'
    extractor_instance.nal_length_size = 4

    # 3. 模拟 _get_moov_atom_data
    async def patched_get_moov_atom_data(*args, **kwargs):
        logging.info("🔧 Patch: 绕过 moov atom 搜索，立即返回模拟数据。")
        return b'mock_moov_data'

    # 4. 模拟 _process_keyframe_pieces 以触发失败
    async def patched_process_keyframe_pieces(self, handle, local_queue, task_state, *args, **kwargs):
        logging.info("🔧 Patch: 任务状态已构建，现在模拟可恢复的失败...")
        raise FrameDownloadTimeoutError(
            "人为触发的失败，用于生成 resume_data",
            task_state['infohash'],
            resume_data=self._serialize_task_state(task_state)
        )

    service = ScreenshotService(loop=loop, status_callback=status_callback)

    # 使用多个 patch 作为上下文管理器，完全控制执行流程
    with patch.object(TorrentClient, 'add_torrent', AsyncMock(return_value=mock_handle)), \
         patch.object(ScreenshotService, '_get_moov_atom_data', new=patched_get_moov_atom_data), \
         patch('screenshot.service.H264KeyframeExtractor', mock_extractor_class), \
         patch.object(ScreenshotService, '_process_keyframe_pieces', new=patched_process_keyframe_pieces):

        await service.run()
        logging.info(f"提交任务: {INFO_HASH}")
        await service.submit_task(INFO_HASH)

        logging.info("⏳ 任务已提交，等待可恢复失败事件... (最多等待10秒)")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            logging.error("❌ 10秒内未收到可恢复失败事件，脚本的 patch 逻辑可能存在问题。")
        finally:
            service.stop()

    if captured_resume_data:
        extractor_info = captured_resume_data.get('extractor_info')
        if extractor_info and extractor_info.get('extradata'):
            extradata_bytes = extractor_info.get('extradata')
            if isinstance(extradata_bytes, bytes):
                extractor_info['extradata'] = base64.b64encode(extradata_bytes).decode('ascii')

        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(captured_resume_data, f, indent=4)
            logging.info(f"✅ resume_data 已成功保存到 {OUTPUT_FILE}")
        except Exception as e:
            logging.error(f"❌ 保存 resume_data 到文件时出错: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n程序被用户中断。")
