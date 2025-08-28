# -*- coding: utf-8 -*-
import pytest
import asyncio
import os
import shutil
import glob
import pytest_asyncio

from screenshot.service import (
    ScreenshotService,
    AllSuccessResult,
    FatalErrorResult,
    PartialSuccessResult,
)

# A known torrent with a video file
SINTEL_INFOHASH = "08ada5a7a6183aae1e09d831df6748d566095a10"
# A known torrent with no video file (Project Gutenberg poetry collection)
NO_VIDEO_INFOHASH = "d5d2c854c3563914a1f6a4574996969542b8813b"

TEST_OUTPUT_DIR = "./screenshots_output_test"
TEST_DATA_DIR = "./torrent_data_test"
# Generous timeout for tests that rely on the network, must be > internal client timeouts
NETWORK_TEST_TIMEOUT = 200

@pytest_asyncio.fixture
async def service_setup():
    """Fixture to set up and tear down the service and directories for each test."""
    # Setup: clean and create directories
    for dir_path in [TEST_OUTPUT_DIR, TEST_DATA_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path)

    service = ScreenshotService(
        output_dir=TEST_OUTPUT_DIR,
        torrent_save_path=TEST_DATA_DIR
    )
    await service.run()

    yield service

    # Teardown: stop service and clean up directories
    service.stop()
    await asyncio.sleep(0.1)  # allow time for shutdown
    for dir_path in [TEST_OUTPUT_DIR, TEST_DATA_DIR]:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

@pytest.mark.asyncio
class TestIntegration:
    """A suite of integration tests for the ScreenshotService."""

    async def test_full_success_scenario(self, service_setup):
        """
        Tests the end-to-end success case with a valid torrent.
        """
        service = service_setup
        result_event = asyncio.Event()
        result_storage = {}

        def on_complete(result):
            result_storage['result'] = result
            result_event.set()

        await service.submit_task(infohash=SINTEL_INFOHASH, on_complete=on_complete)

        await asyncio.wait_for(result_event.wait(), timeout=NETWORK_TEST_TIMEOUT)

        result = result_storage['result']
        assert isinstance(result, AllSuccessResult)
        assert result.infohash == SINTEL_INFOHASH
        assert result.screenshots_count > 0

        # Verify that files were actually created
        output_files = glob.glob(os.path.join(TEST_OUTPUT_DIR, f"{SINTEL_INFOHASH}_*.jpg"))
        assert len(output_files) == result.screenshots_count

        # Check one file to ensure it's a valid JPG
        assert os.path.getsize(output_files[0]) > 1000
        with open(output_files[0], 'rb') as f:
            assert f.read(2) == b'\xFF\xD8'

    async def test_fatal_error_on_metadata_timeout(self, service_setup):
        """
        Tests the fatal error case when a torrent's metadata cannot be fetched,
        which is a common real-world failure mode.
        """
        service = service_setup
        result_event = asyncio.Event()
        result_storage = {}

        def on_complete(result):
            result_storage['result'] = result
            result_event.set()

        await service.submit_task(infohash=NO_VIDEO_INFOHASH, on_complete=on_complete)

        await asyncio.wait_for(result_event.wait(), timeout=NETWORK_TEST_TIMEOUT)

        result = result_storage['result']
        assert isinstance(result, FatalErrorResult)
        assert result.infohash == NO_VIDEO_INFOHASH
        # This torrent reliably fails to fetch metadata, which is a valid fatal error.
        assert "获取元数据超时" in result.reason  # Check for "metadata timeout" in the reason

        # Verify no screenshots were created
        output_files = glob.glob(os.path.join(TEST_OUTPUT_DIR, f"{NO_VIDEO_INFOHASH}_*.jpg"))
        assert len(output_files) == 0

    async def test_resume_from_partial_success(self, service_setup):
        """
        Tests the recovery logic by forcing a partial result and then resuming.
        """
        service = service_setup

        # --- Step 1: Force a partial failure by setting a very short timeout ---
        service.TIMEOUT_FOR_TESTING = 0.1

        result_event_1 = asyncio.Event()
        result_storage_1 = {}
        def on_complete_1(result):
            result_storage_1['result'] = result
            result_event_1.set()

        await service.submit_task(infohash=SINTEL_INFOHASH, on_complete=on_complete_1)
        await asyncio.wait_for(result_event_1.wait(), timeout=NETWORK_TEST_TIMEOUT)

        # --- Step 2: Assert that we got a partial result ---
        partial_result = result_storage_1['result']
        assert isinstance(partial_result, PartialSuccessResult)
        assert partial_result.infohash == SINTEL_INFOHASH
        assert "Timeout" in partial_result.reason
        assert 'unprocessed_kf_indices' in partial_result.resume_data

        # Reset the timeout for the next run
        del service.TIMEOUT_FOR_TESTING

        # --- Step 3: Resubmit the task with the resume_data ---
        result_event_2 = asyncio.Event()
        result_storage_2 = {}
        def on_complete_2(result):
            result_storage_2['result'] = result
            result_event_2.set()

        await service.submit_task(
            infohash=SINTEL_INFOHASH,
            resume_data=partial_result.resume_data,
            on_complete=on_complete_2
        )
        await asyncio.wait_for(result_event_2.wait(), timeout=NETWORK_TEST_TIMEOUT)

        # --- Step 4: Assert that the resumed task succeeded ---
        final_result = result_storage_2['result']
        assert isinstance(final_result, AllSuccessResult)
        assert final_result.infohash == SINTEL_INFOHASH
        assert final_result.screenshots_count > 0
        assert final_result.screenshots_count == len(partial_result.resume_data['unprocessed_kf_indices'])

        # --- Step 5: Verify total files on disk ---
        total_screenshots_generated = partial_result.screenshots_count + final_result.screenshots_count
        output_files = glob.glob(os.path.join(TEST_OUTPUT_DIR, f"{SINTEL_INFOHASH}_*.jpg"))
        assert len(output_files) == total_screenshots_generated
