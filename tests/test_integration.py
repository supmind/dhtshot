# -*- coding: utf-8 -*-
import pytest
import asyncio
import os
import glob
import shutil

from screenshot.service import ScreenshotService

# 使用公开的、有源的 Sintel 种子进行测试
SINTEL_INFOHASH = "08ada5a7a6183aae1e09d831df6748d566095a10"
TEST_OUTPUT_DIR = "./screenshots_output_test"
# 增加超时时间，因为网络测试依赖于寻源和下载
TEST_TIMEOUT = 180

@pytest.mark.asyncio
async def test_full_screenshot_process_network():
    """
    一个完整的端到端集成测试，使用一个公开的 infohash。
    这个测试会连接到真实的 BitTorrent 网络来下载元数据和 piece。
    因此，它依赖于网络连接和种子的健康度。
    """
    # --- 1. 设置 ---
    # 清理旧的测试输出
    if os.path.exists(TEST_OUTPUT_DIR):
        shutil.rmtree(TEST_OUTPUT_DIR)
    os.makedirs(TEST_OUTPUT_DIR)

    # 清理可能的旧下载数据
    torrent_data_path = f"/dev/shm/{SINTEL_INFOHASH}"
    if os.path.exists(torrent_data_path):
        shutil.rmtree(torrent_data_path)

    service = None
    try:
        # --- 2. 运行服务并提交任务 ---
        loop = asyncio.get_running_loop()
        service = ScreenshotService(loop=loop, output_dir=TEST_OUTPUT_DIR)

        # 在后台运行服务
        service_task = loop.create_task(service.run())

        # 提交截图任务
        await service.submit_task(infohash=SINTEL_INFOHASH)

        # --- 3. 等待任务完成 ---
        # 这是一个简化的等待逻辑。我们等待固定时间，期望服务能在这段时间内
        # 完成元数据下载、部分 piece 下载和截图。
        # 一个更健壮的测试可能会轮询输出目录或检查服务内部状态。
        print(f"\n等待 {TEST_TIMEOUT} 秒，让服务处理任务...")
        await asyncio.sleep(TEST_TIMEOUT)

        # 检查任务队列是否已空
        # 注意：由于我们只有一个任务，并且服务可能仍在运行其他后台活动，
        # 所以 task_queue 可能不是判断完成的完美指标，但可以作为参考。
        assert service.task_queue.empty(), "任务队列在超时后仍未清空"

        # --- 4. 断言结果 ---
        output_files = glob.glob(os.path.join(TEST_OUTPUT_DIR, f"{SINTEL_INFOHASH}_*.jpg"))
        print(f"在 {TEST_OUTPUT_DIR} 中找到的截图文件: {output_files}")

        assert len(output_files) > 0, "服务在指定时间内未能生成任何截图文件"

        # 检查第一个文件是否有效
        first_file = output_files[0]
        assert os.path.getsize(first_file) > 1000, f"截图文件 {first_file} 的大小异常"
        with open(first_file, 'rb') as f:
            assert f.read(2) == b'\xFF\xD8', f"文件 {first_file} 不是有效的 JPG"

    finally:
        # --- 5. 清理 ---
        if service:
            service.stop()
            # 等待服务完全停止
            await asyncio.sleep(1)

        # 再次清理，确保所有资源被释放
        if os.path.exists(TEST_OUTPUT_DIR):
            shutil.rmtree(TEST_OUTPUT_DIR)
        if os.path.exists(torrent_data_path):
            shutil.rmtree(torrent_data_path)
