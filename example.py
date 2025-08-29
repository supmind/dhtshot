import asyncio
import signal
import logging

from screenshot.service import ScreenshotService

# Configure logging to see the output from the service
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Set the service log level to INFO to reduce verbosity
# logging.getLogger("ScreenshotService").setLevel(logging.INFO)


async def main():
    loop = asyncio.get_running_loop()

    # --- Status Callback Definition ---
    # This is where you can process the results of screenshot tasks.
    # You could save results to a database, send a notification, or
    # re-submit a task that had a recoverable failure.
    async def task_status_callback(status_data: dict):
        print("\n--- TASK STATUS UPDATE ---")
        print(f"  Infohash: {status_data.get('infohash')}")
        print(f"  Status: {status_data.get('status')}")
        print(f"  Message: {status_data.get('message')}")
        if status_data.get('status') == 'recoverable_failure':
            print("  This task can be resumed later with the provided resume_data.")
            # In a real application, you might save this resume_data to a database
            # and have a mechanism to retry the task.
            # For this example, we'll just print a confirmation.
            print(f"  Resume data available: {'resume_data' in status_data}")
        print("--------------------------\n")


    # Create and run the screenshot service
    # We pass our callback function to the service.
    service = ScreenshotService(loop=loop, status_callback=task_status_callback)
    await service.run()

    # --- Example Tasks ---
    # Submit a few screenshot tasks to the service.
    # The user should replace these with real infohashes and desired timestamps.
    # Sintel - an open-source movie torrent
    infohash_to_submit = "ea8988740fa7a125e1593ce26ee5f24c29478f12"

    await service.submit_task(infohash_to_submit)

    print("\nScreenshot service is running.")
    print("Submitted 1 example task. The service will now process it.")
    print(f"Screenshots will be saved in the '{service.output_dir}' directory.")
    print("Press Ctrl+C to stop the service.")

    # Wait for graceful shutdown or timeout
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

    timeout = 300  # seconds
    print(f"Running for a maximum of {timeout} seconds. Will stop automatically.")

    try:
        await asyncio.wait_for(stop, timeout=timeout)
    except asyncio.TimeoutError:
        print(f"\nTimeout reached after {timeout} seconds.")

    # Clean up
    print("\nStopping service...")
    service.stop()
    print("Service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
