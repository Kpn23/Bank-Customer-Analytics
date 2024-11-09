import asyncio
import atexit
import signal
from filelock import FileLock
from dotenv import load_dotenv
import os
import logging
import sys
from api.fastapi_app import *
from gui.main_window import MainSystem


load_dotenv()
folder_directory = os.getenv("folder_path")

sys.path.append(folder_directory)


logger = logging.getLogger(__name__)

AIRFLOW_LOCK = f"{folder_directory}/tmp/airflow.lock"
FASTAPI_LOCK = f"{folder_directory}/tmp/fastapi.lock"


async def start_airflow():
    logger.info("Attempting to start Airflow")

    if is_process_running("airflow webserver") or is_process_running(
        "airflow scheduler"
    ):
        logger.info("Airflow processes are already running")
        return None, None

    with FileLock(AIRFLOW_LOCK):
        if os.path.exists(AIRFLOW_LOCK):
            if not is_lock_file_valid(AIRFLOW_LOCK):
                remove_lock_file(AIRFLOW_LOCK)
            else:
                logger.info(
                    f"Valid lock file {AIRFLOW_LOCK} exists, Airflow might be running"
                )
                return None, None

        logger.info("Starting Airflow webserver")
        webserver = await asyncio.create_subprocess_exec(
            "airflow", "webserver", "--port", "8080"
        )
        logger.info(f"Airflow webserver started with PID: {webserver.pid}")

        logger.info("Starting Airflow scheduler")
        scheduler = await asyncio.create_subprocess_exec("airflow", "scheduler")
        logger.info(f"Airflow scheduler started with PID: {scheduler.pid}")

        pids = f"{webserver.pid},{scheduler.pid}"
        logger.info(f"Writing PIDs to lock file: {pids}")
        with open(AIRFLOW_LOCK, "w") as f:
            f.write(pids)

        logger.info(f"Airflow started successfully. PIDs: {pids}")
        return webserver, scheduler


def cleanup():
    logger.info("Starting cleanup process")
    for lock_file in [AIRFLOW_LOCK, FASTAPI_LOCK]:
        if os.path.exists(lock_file):
            with open(lock_file, "r") as f:
                content = f.read().strip()
                logger.info(f"Lock file {lock_file} content: '{content}'")
                pids = content.split(",")
                for pid in pids:
                    if pid and pid.isdigit():
                        try:
                            os.kill(int(pid), signal.SIGTERM)
                            logger.info(f"Terminated process with PID: {pid}")
                        except ProcessLookupError:
                            logger.info(f"Process with PID {pid} not found")
            remove_lock_file(lock_file)
    logger.info("Cleanup process completed")


async def main():
    [airflow_webserver, airflow_scheduler], fastapi_process = await asyncio.gather(
        start_airflow(), start_fastapi()
    )

    def check_lock_files():
        logger.info("Checking lock files")
        for lock_file in [AIRFLOW_LOCK, FASTAPI_LOCK]:
            if os.path.exists(lock_file):
                with open(lock_file, "r") as f:
                    content = f.read().strip()
                    logger.info(f"{os.path.basename(lock_file)} content: {content}")
            else:
                logger.info(f"{os.path.basename(lock_file)} does not exist")

    # Check lock files
    check_lock_files()

    # Keep the script running
    try:
        while True:
            await asyncio.sleep(3)
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    # Set AIRFLOW_HOME environment variable
    os.environ["AIRFLOW_HOME"] = (
        "/Users/superdayuanjingzhi/Documents/JDE-python/p_AnalyticsPlatform/etl/airflow"
    )

    atexit.register(cleanup)

    asyncio.run(main())

