import asyncio
import aiohttp
import uuid
import logging
import time
import base64
import json
import os
import signal
from functools import partial

from screenshot.service import ScreenshotService
from screenshot.config import Settings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("Worker")

SCHEDULER_URL = "http://127.0.0.1:8000"
WORKER_ID = f"worker-{uuid.uuid4()}"
HEARTBEAT_INTERVAL = 30  # seconds

async def register_worker(session):
    """Registers this worker with the scheduler."""
    url = f"{SCHEDULER_URL}/workers/register"
    payload = {"worker_id": WORKER_ID, "status": "idle"}
    try:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                log.info(f"Worker {WORKER_ID} registered successfully.")
                return True
            else:
                log.error(f"Failed to register worker. Status: {response.status}, Response: {await response.text()}")
                return False
    except aiohttp.ClientError as e:
        log.error(f"Error registering worker: {e}")
        return False

# --- Callback Functions ---

async def on_screenshot_saved(session, infohash, filepath):
    """Callback to upload a screenshot and record its filename."""
    log.info(f"[{infohash}] Screenshot saved to {filepath}. Uploading...")

    filename = os.path.basename(filepath)

    # 1. Upload the file
    upload_url = f"{SCHEDULER_URL}/screenshots/{infohash}"
    try:
        with open(filepath, "rb") as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename=filename, content_type='image/jpeg')
            async with session.post(upload_url, data=data) as response:
                if response.status != 200:
                    log.error(f"[{infohash}] Failed to upload screenshot {filename}. Status: {response.status}")
                    return
    except aiohttp.ClientError as e:
        log.error(f"[{infohash}] Error uploading screenshot {filename}: {e}")
        return
    except FileNotFoundError:
        log.error(f"[{infohash}] File not found for upload: {filepath}")
        return

    # 2. Record the filename
    record_url = f"{SCHEDULER_URL}/tasks/{infohash}/screenshots"
    payload = {"filename": filename}
    try:
        async with session.post(record_url, json=payload) as response:
            if response.status == 200:
                log.info(f"[{infohash}] Successfully recorded screenshot {filename}.")
            else:
                log.error(f"[{infohash}] Failed to record screenshot {filename}. Status: {response.status}, Response: {await response.text()}")
    except aiohttp.ClientError as e:
        log.error(f"[{infohash}] Error recording screenshot {filename}: {e}")

async def on_task_finished(session, status, infohash, message, **kwargs):
    """Callback to report the final status of a task."""
    log.info(f"[{infohash}] Task finished with status: {status.upper()}. Message: {message}")

    url = f"{SCHEDULER_URL}/tasks/{infohash}/status"
    payload = {
        "status": status,
        "message": str(message), # Ensure message is a string
        "resume_data": kwargs.get("resume_data")
    }

    # Base64-encode extradata if it exists, as it's bytes
    if payload.get("resume_data") and payload["resume_data"].get("extractor_info", {}).get("extradata"):
        extradata_bytes = payload["resume_data"]["extractor_info"]["extradata"]
        if isinstance(extradata_bytes, bytes):
            payload["resume_data"]["extractor_info"]["extradata"] = base64.b64encode(extradata_bytes).decode('ascii')

    try:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                log.info(f"[{infohash}] Final status reported successfully.")
            else:
                log.error(f"[{infohash}] Failed to report final status. Status: {response.status}, Response: {await response.text()}")
    except aiohttp.ClientError as e:
        log.error(f"[{infohash}] Error reporting final status: {e}")

async def send_heartbeat(session, service):
    """Sends a heartbeat to the scheduler."""
    url = f"{SCHEDULER_URL}/workers/heartbeat"
    while True:
        status = "busy" if service.active_tasks else "idle"
        payload = {"worker_id": WORKER_ID, "status": status}
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    log.info(f"Heartbeat sent (status: {status}).")
                else:
                    log.warning(f"Failed to send heartbeat. Status: {response.status}")
        except aiohttp.ClientError as e:
            log.warning(f"Error sending heartbeat: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def main():
    """Main function for the worker, refactored for long-lived services."""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    async with aiohttp.ClientSession() as session:
        if not await register_worker(session):
            log.error("Could not register with scheduler. Exiting.")
            return

        settings = Settings()
        status_cb = partial(on_task_finished, session)
        screenshot_cb = partial(on_screenshot_saved, session)

        service = ScreenshotService(
            settings=settings,
            loop=loop,
            status_callback=status_cb,
            screenshot_callback=screenshot_cb
        )
        await service.run()
        log.info("ScreenshotService is running in the background.")

        heartbeat_task = asyncio.create_task(send_heartbeat(session, service))
        log.info("Heartbeat task started.")

        log.info("Starting main task polling loop...")
        while not stop_event.is_set():
            try:
                # Only poll if the service is not already busy
                if not service.active_tasks:
                    log.info("Polling for next available task...")
                    url = f"{SCHEDULER_URL}/tasks/next?worker_id={WORKER_ID}"
                    async with session.get(url, timeout=15) as response:
                        if response.status == 200:
                            task_data = await response.json()
                            if task_data:
                                log.info(f"Received task: {task_data['infohash']}. Submitting to local service.")
                                if task_data.get('metadata'):
                                    task_data['metadata'] = base64.b64decode(task_data['metadata'])
                                await service.submit_task(
                                    infohash=task_data['infohash'],
                                    metadata=task_data.get('metadata'),
                                    resume_data=task_data.get('resume_data')
                                )
                        elif response.status == 204:
                            log.info("No tasks available. Waiting...")

                await asyncio.sleep(10)

            except aiohttp.ClientError as e:
                log.error(f"Error connecting to scheduler: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break

        log.info("Shutdown signal received. Stopping services...")
        heartbeat_task.cancel()
        await service.stop()
        log.info("Worker has shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Worker shutting down.")
