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
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("ScreenshotService").setLevel(logging.DEBUG)


# --- Callback Handler ---
def my_result_handler(result, state: dict):
    """
    This function is called when a task is complete.
    It uses a shared state dictionary to manage results and events.
    """
    print("\n--- Task Result Received ---")
    if isinstance(result, AllSuccessResult):
        print(f"✅  SUCCESS: Task for {result.infohash} completed.")
        print(f"   Screenshots generated in this run: {result.screenshots_count}")
        state['status'] = 'done'
    elif isinstance(result, PartialSuccessResult):
        print(f"⚠️  PARTIAL: Task for {result.infohash} failed but is recoverable.")
        print(f"   Reason: {result.reason}")
        print(f"   Screenshots generated in this run: {result.screenshots_count}")
        # Store the resume_data for a potential retry
        state['resume_data'] = result.resume_data
        state['status'] = 'partial'
    elif isinstance(result, FatalErrorResult):
        print(f"❌  FATAL: Task for {result.infohash} failed with an unrecoverable error.")
        print(f"   Reason: {result.reason}")
        state['status'] = 'fatal'
    else:
        print(f"🧐 UNEXPECTED RESULT TYPE: {type(result)}")
        state['status'] = 'fatal'

    # Signal that this step of the task has finished
    state['event'].set()


async def main():
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop)
    await service.run()

    # Sintel - a known good torrent for screenshots
    infohash_to_submit = "08ada5a7a6183aae1e09d831df6748d566095a10"

    # --- Run 1: Normal execution ---
    print(f"\n--- Submitting task for the first time ---")
    state1 = {'event': asyncio.Event()}
    handler1 = partial(my_result_handler, state=state1)
    await service.submit_task(infohash=infohash_to_submit, on_complete=handler1)
    await state1['event'].wait()

    # --- Run 2: Demonstrate resume if Run 1 was partial ---
    if state1.get('status') == 'partial':
        print(f"\n--- Task failed partially. Resubmitting with resume_data ---")
        print(f"   Resume data being used: {state1.get('resume_data')}")
        state2 = {'event': asyncio.Event()}
        handler2 = partial(my_result_handler, state=state2)
        await service.submit_task(
            infohash=infohash_to_submit,
            on_complete=handler2,
            resume_data=state1.get('resume_data')
        )
        await state2['event'].wait()

    print("\n--- Example finished ---")

    # Clean up
    print("Stopping service...")
    service.stop()
    print("Service stopped.")


if __name__ == "__main__":
    try:
        # To test the timeout-based resume, you can inject a timeout
        # into the service for testing, like so:
        # service.TIMEOUT_FOR_TESTING = 5
        # This will trigger the PartialSuccessResult path.
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")
