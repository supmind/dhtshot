import asyncio
import aiohttp
import uuid
import logging
import time
import base64
import json
import os

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

# --- Main Task Processing ---

async def process_task(session, task_data):
    """
    Processes a single screenshot task using the screenshot module.
    """
    infohash = task_data['infohash']
    log.info(f"[{infohash}] Starting task processing.")

    # This is a temporary, direct way to use the service logic for a single task.
    # A proper implementation might involve refactoring ScreenshotService.

    # 1. Prepare callbacks
    status_callback = lambda **cb_kwargs: on_task_finished(session, **cb_kwargs)
    screenshot_callback = lambda filepath: on_screenshot_saved(session, infohash, filepath)

    # 2. Prepare service and task data
    settings = Settings()
    loop = asyncio.get_running_loop()
    service = ScreenshotService(
        settings=settings,
        loop=loop,
        status_callback=status_callback,
        screenshot_callback=screenshot_callback
    )

    # Decode metadata from Base64 if present
    if task_data.get('metadata'):
        task_data['metadata'] = base64.b64decode(task_data['metadata'])

    # 3. Run the task
    try:
        # We directly call the internal handler for a single task.
        await service._handle_screenshot_task(task_data)
    except Exception as e:
        log.exception(f"[{infohash}] An unexpected error occurred during _handle_screenshot_task.")
        # Report a final, fatal error to the scheduler
        await on_task_finished(session, status='permanent_failure', infohash=infohash, message=f"Worker-level error: {e}")
    finally:
        # Ensure the torrent client session inside the service is cleaned up
        await service.stop()
        log.info(f"[{infohash}] Task processing finished.")


async def send_heartbeat(session):
    """Sends a heartbeat to the scheduler."""
    url = f"{SCHEDULER_URL}/workers/heartbeat"
    # In a real worker, this would reflect the actual status
    payload = {"worker_id": WORKER_ID, "status": "idle"}
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    log.info("Heartbeat sent successfully.")
                else:
                    log.warning(f"Failed to send heartbeat. Status: {response.status}")
        except aiohttp.ClientError as e:
            log.warning(f"Error sending heartbeat: {e}")

async def main():
    """Main function for the worker."""
    async with aiohttp.ClientSession() as session:
        # Register first
        if not await register_worker(session):
            log.error("Could not register with scheduler. Exiting.")
            return

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(send_heartbeat(session))
        log.info("Heartbeat task started.")

        # Main task processing loop
        log.info("Starting main task loop...")
        while True:
            try:
                log.info("Polling for next available task...")
                url = f"{SCHEDULER_URL}/tasks/next?worker_id={WORKER_ID}"
                async with session.get(url, timeout=45) as response:
                    if response.status == 200:
                        task_data = await response.json()
                        if task_data:
                            log.info(f"Received task: {task_data['infohash']}")
                            await process_task(session, task_data)
                        else:
                            log.info("No tasks available (empty 200 OK). Waiting...")
                    elif response.status == 204:
                        log.info("No tasks available (204 No Content). Waiting...")
                    else:
                        log.error(f"Error fetching task. Status: {response.status}, Response: {await response.text()}")

                await asyncio.sleep(10) # Wait 10 seconds before polling again

            except aiohttp.ClientError as e:
                log.error(f"Error connecting to scheduler: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL) # Wait longer if scheduler is down
            except asyncio.CancelledError:
                log.info("Task loop cancelled.")
                break

        # This part is not reachable in the current loop, but good for cleanup
        heartbeat_task.cancel()
        await heartbeat_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Worker shutting down.")
