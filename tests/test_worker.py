# -*- coding: utf-8 -*-
import asyncio
from contextlib import suppress
from unittest.mock import patch, AsyncMock, MagicMock, PropertyMock

import pytest

import worker
from config import Settings
from screenshot.service import ScreenshotService


@pytest.fixture
def mock_settings(tmp_path):
    """Override settings for tests and set a default concurrency."""
    return Settings(
        scheduler_url="http://test-scheduler",
        output_dir=str(tmp_path),
        worker_concurrency=1  # Default to 1 for simplicity in tests
    )


@pytest.mark.asyncio
@patch('asyncio.sleep', new_callable=AsyncMock)
@patch('worker.ScreenshotService', spec=True)
@patch('worker.SchedulerAPIClient')
async def test_worker_main_loop_fetches_task(MockAPIClient, MockScreenshotService, mock_sleep, mock_settings):
    """
    Test that the worker's main loop successfully registers, fetches a task,
    and submits it to the ScreenshotService.
    """
    # --- Setup Mocks ---
    mock_client = MockAPIClient.return_value
    mock_client.register = AsyncMock(return_value=True)
    mock_client.send_heartbeat = AsyncMock()
    mock_client.get_next_task = AsyncMock(side_effect=[
        {"infohash": "test_infohash", "metadata": None},
        None
    ])

    mock_service = MockScreenshotService.return_value
    mock_service.submit_task = AsyncMock()

    # Use PropertyMock to simulate the `active_tasks` property.
    type(mock_service).active_tasks = PropertyMock(return_value=set())
    # Mock the new get_queue_size method.
    mock_service.get_queue_size.return_value = 0

    # --- Run Main Loop ---
    main_task = asyncio.create_task(worker.main(session=MagicMock()))
    await asyncio.sleep(0.01)
    main_task.cancel()
    with suppress(asyncio.CancelledError):
        await main_task

    # --- Assertions ---
    mock_client.register.assert_awaited_once()
    assert mock_client.get_next_task.await_count == 2
    mock_service.submit_task.assert_awaited_once_with(infohash="test_infohash", metadata=None)
    mock_sleep.assert_called_once_with(worker.POLL_INTERVAL)


@pytest.mark.asyncio
@patch('asyncio.sleep', new_callable=AsyncMock)
@patch('worker.ScreenshotService', spec=True)
@patch('worker.SchedulerAPIClient')
async def test_worker_respects_concurrency_limit(MockAPIClient, MockScreenshotService, mock_sleep, mock_settings):
    """
    Test that the worker does not fetch new tasks when it has reached its
    concurrency limit.
    """
    # --- Setup Mocks ---
    # Correctly access the lowercase attribute.
    mock_settings.worker_concurrency = 1

    mock_client = MockAPIClient.return_value
    mock_client.register = AsyncMock(return_value=True)
    mock_client.send_heartbeat = AsyncMock()
    mock_client.get_next_task = AsyncMock()

    mock_service = MockScreenshotService.return_value

    active_tasks = {"some_active_task"}
    type(mock_service).active_tasks = PropertyMock(return_value=active_tasks)
    mock_service.get_queue_size.return_value = 0

    # --- Run Main Loop ---
    main_task = asyncio.create_task(worker.main(session=MagicMock()))
    await asyncio.sleep(0.01)
    main_task.cancel()
    with suppress(asyncio.CancelledError):
        await main_task

    # --- Assertions ---
    mock_client.register.assert_awaited_once()
    mock_client.get_next_task.assert_not_awaited()
    mock_sleep.assert_called_once_with(worker.POLL_INTERVAL)


@pytest.mark.asyncio
@patch('worker.SchedulerAPIClient')
async def test_on_task_finished_callback(MockAPIClient):
    """
    Tests that the on_task_finished callback correctly calls the client's
    update_task_status method.
    """
    mock_client_instance = MockAPIClient.return_value
    mock_client_instance.update_task_status = AsyncMock()

    infohash = "test_hash"
    status = "success"
    message = "All good"
    resume_data = {"key": "value"}

    await worker.on_task_finished(mock_client_instance, status, infohash, message, resume_data=resume_data)

    mock_client_instance.update_task_status.assert_awaited_once_with(
        infohash, status, message, resume_data
    )
