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
# 定义一个更合理的超时时间
TEST_TIMEOUT = 300 # 秒

async def wait_for_screenshots(output_dir, infohash, timeout):
    """
    一个辅助函数，用于轮询输出目录，等待截图文件的出现。
    这比固定的 sleep 更健壮、更高效。
    """
    start_time = asyncio.get_event_loop().time()
    while True:
        output_files = glob.glob(os.path.join(output_dir, f"{infohash}_*.jpg"))
        if output_files:
            print(f"\n在 {asyncio.get_event_loop().time() - start_time:.2f} 秒后找到截图。")
            return output_files

        if asyncio.get_event_loop().time() - start_time > timeout:
            raise asyncio.TimeoutError(f"在 {timeout} 秒内未找到截图文件。")

        await asyncio.sleep(1) # 每秒轮询一次

@pytest.mark.skip(reason="此测试依赖于真实的 P2P 网络连接，在受限的沙盒网络环境中无法稳定运行。")
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
        # 使用新的轮询函数代替固定的 sleep，以提高测试的稳定性和效率
        try:
            print(f"\n在最多 {TEST_TIMEOUT} 秒内等待截图生成...")
            output_files = await wait_for_screenshots(TEST_OUTPUT_DIR, SINTEL_INFOHASH, TEST_TIMEOUT)
        except asyncio.TimeoutError:
            pytest.fail(f"测试超时：在 {TEST_TIMEOUT} 秒内未能生成任何截图文件。")

        # --- 4. 断言结果 ---
        print(f"在 {TEST_OUTPUT_DIR} 中找到的截图文件: {output_files}")
        assert len(output_files) > 0, "即使 wait_for_screenshots 成功，仍然没有找到文件，这不应该发生"

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
