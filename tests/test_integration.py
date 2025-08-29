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
TEST_DATA_DIR = "./torrent_data_test"
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

@pytest.mark.asyncio
async def test_full_screenshot_process_network():
    """
    一个完整的端到端集成测试，使用一个公开的 infohash。
    这个测试会连接到真实的 BitTorrent 网络来下载元数据和 piece。
    因此，它依赖于网络连接和种子的健康度。
    """
    # --- 1. 设置 ---
    # 清理旧的测试输出和数据
    for dir_path in [TEST_OUTPUT_DIR, TEST_DATA_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path)

    service = None
    try:
        # --- 2. 运行服务并提交任务 ---
        loop = asyncio.get_running_loop()
        service = ScreenshotService(
            loop=loop,
            output_dir=TEST_OUTPUT_DIR,
            torrent_save_path=TEST_DATA_DIR
        )

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
        for dir_path in [TEST_OUTPUT_DIR, TEST_DATA_DIR]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)


from screenshot.client import TorrentClient
from screenshot.errors import FrameDownloadTimeoutError

class FaultyTorrentClient(TorrentClient):
    """A torrent client that fails after fetching a certain number of pieces."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetch_calls = 0
        # Fail on the 5th call to fetch_pieces, which is enough to get metadata but not all frames.
        self.fail_after_calls = 5

    async def fetch_pieces(self, handle, piece_indices: list[int], timeout=300.0) -> dict[int, bytes]:
        self.fetch_calls += 1
        if self.fetch_calls >= self.fail_after_calls:
            self.log.warning(f"FaultyTorrentClient: Intentionally failing on call {self.fetch_calls}")
            raise asyncio.TimeoutError("Simulated fetch_pieces timeout")

        return await super().fetch_pieces(handle, piece_indices, timeout)

@pytest.mark.asyncio
async def test_full_resumability_process_network(caplog):
    """
    An end-to-end integration test for the resumability feature.
    It runs a task, makes it fail, captures resume data, then restarts
    the task with that data and verifies completion.
    """
    # --- 1. Cleanup ---
    for dir_path in [TEST_OUTPUT_DIR, TEST_DATA_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path)

    loop = asyncio.get_running_loop()
    resume_data = None

    # --- 2. First Run (the one that fails) ---
    faulty_client = FaultyTorrentClient(loop=loop, save_path=TEST_DATA_DIR)
    faulty_service = ScreenshotService(
        loop=loop, output_dir=TEST_OUTPUT_DIR,
        torrent_save_path=TEST_DATA_DIR, client=faulty_client
    )

    task_info = {'infohash': SINTEL_INFOHASH, 'resume_data': None}

    print("\n--- Starting faulty run, expecting a timeout ---")
    await faulty_client.start()
    await faulty_service._handle_screenshot_task(task_info)
    faulty_client.stop()
    await asyncio.sleep(1) # Allow for cleanup

    # --- 3. Assertions for the First Run ---
    resumable_error_record = next((
        r for r in caplog.records if "resumable error" in r.message and hasattr(r, 'exc_info') and r.exc_info
    ), None)

    assert resumable_error_record is not None, "A resumable error should have been logged"
    error_instance = resumable_error_record.exc_info[1]
    assert isinstance(error_instance, FrameDownloadTimeoutError)
    resume_data = error_instance.resume_data

    assert resume_data is not None, "Resume data should have been attached to the logged exception"
    assert resume_data['infohash'] == SINTEL_INFOHASH
    assert len(resume_data['completed_pieces']) > 0, "Some pieces should have been completed before timeout"
    print(f"\n--- Captured resume data with {len(resume_data['completed_pieces'])} completed pieces ---")

    # --- 4. Second Run (the one that resumes and succeeds) ---
    print("\n--- Starting recovery run with resume data ---")
    caplog.clear() # Clear logs for the second run

    recovery_service = ScreenshotService(
        loop=loop, output_dir=TEST_OUTPUT_DIR, torrent_save_path=TEST_DATA_DIR
    )

    try:
        service_task = loop.create_task(recovery_service.run())
        await recovery_service.submit_task(infohash=SINTEL_INFOHASH, resume_data=resume_data)

        try:
            print(f"\nWaiting up to {TEST_TIMEOUT}s for screenshots in recovery run...")
            output_files = await wait_for_screenshots(TEST_OUTPUT_DIR, SINTEL_INFOHASH, TEST_TIMEOUT)
        except asyncio.TimeoutError:
            pytest.fail(f"Recovery run timed out: Failed to generate screenshots in {TEST_TIMEOUT}s.")

        # --- 5. Assertions for the Second Run ---
        print(f"\n--- Recovery run successful. Found files: {output_files} ---")
        assert len(output_files) > 0
        assert "Resuming task from provided data" in caplog.text

    finally:
        # --- 6. Cleanup ---
        if recovery_service:
            recovery_service.stop()
            await asyncio.sleep(1)

        for dir_path in [TEST_OUTPUT_DIR, TEST_DATA_DIR]:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
