import asyncio
import signal
import logging
from functools import partial

# Import the service and the new result classes
from screenshot.service import (
    ScreenshotService,
    AllSuccessResult,
    FatalErrorResult,
    PartialSuccessResult,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Changed to INFO for a cleaner example output
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("ScreenshotService").setLevel(logging.DEBUG)


# 1. Define the callback handler
def my_result_handler(result, task_completion_event: asyncio.Event):
    """This function will be called when a task is complete."""
    print("\n--- Task Result Received ---")
    if isinstance(result, AllSuccessResult):
        print(f"✅  SUCCESS: Task for {result.infohash} completed successfully.")
        print(f"   Screenshots generated: {result.screenshots_count}")
    elif isinstance(result, PartialSuccessResult):
        print(f"⚠️  PARTIAL: Task for {result.infohash} failed but is recoverable.")
        print(f"   Reason: {result.reason}")
        print(f"   Screenshots generated so far: {result.screenshots_count}")
        print(f"   Resume data: {result.resume_data}")
        # In a real application, you would save this resume_data and might
        # re-submit the task later.
    elif isinstance(result, FatalErrorResult):
        print(f"❌  FATAL: Task for {result.infohash} failed with an unrecoverable error.")
        print(f"   Reason: {result.reason}")
    elif result is None:
        print("❓  UNKNOWN: Task finished, but no result object was returned.")
        print("   This might indicate an issue in the service logic before the result is created.")
    else:
        print(f"🧐 UNEXPECTED RESULT TYPE: {type(result)}")

    # Signal that the task has finished so the main script can exit.
    task_completion_event.set()


async def main():
    loop = asyncio.get_running_loop()

    # Create an event that the callback will use to signal completion.
    task_finished_event = asyncio.Event()

    # Create and run the screenshot service
    service = ScreenshotService(loop=loop)
    await service.run()

    # Sintel - an open-source movie torrent.
    # Using the same one as the integration test for consistency.
    infohash_to_submit = "08ada5a7a6183aae1e09d831df6748d566095a10"

    print(f"\nSubmitting task for infohash: {infohash_to_submit}")
    print("The result will be delivered to the 'my_result_handler' callback.")

    # Use functools.partial to pass the event to the handler
    on_complete_handler = partial(my_result_handler, task_completion_event=task_finished_event)

    # Submit the task with the callback
    await service.submit_task(
        infohash=infohash_to_submit,
        on_complete=on_complete_handler
    )

    print("\nScreenshot service is running in the background.")
    print("Waiting for the task to complete... (or press Ctrl+C to exit)")

    # Also listen for Ctrl+C
    stop_event = asyncio.Event()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)

    # Wait for either the task to finish or for Ctrl+C
    done, pending = await asyncio.wait(
        [
            loop.create_task(task_finished_event.wait()),
            loop.create_task(stop_event.wait())
        ],
        return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()

    if stop_event.is_set():
        print("\nCtrl+C pressed.")

    # Clean up
    print("\nStopping service...")
    service.stop()
    print("Service stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
