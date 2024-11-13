import asyncio
import atexit
import signal
import os
import logging
import sys
from filelock import FileLock
from dotenv import load_dotenv
from api.fastapi_app import * 
from gui.main_window import MainSystem 

# Load environment variables
load_dotenv()
FOLDER_DIRECTORY = os.getenv("folder_path")
AIRFLOW_HOME = "/Users/superdayuanjingzhi/Documents/JDE-python/p_AnalyticsPlatform/etl/airflow"
AIRFLOW_LOCK = os.path.join(FOLDER_DIRECTORY, "tmp", "airflow.lock")
FASTAPI_LOCK = os.path.join(FOLDER_DIRECTORY, "tmp", "fastapi.lock")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.append(FOLDER_DIRECTORY)

async def start_airflow():
    logger.info("Attempting to start Airflow")

    if is_process_running("airflow webserver") or is_process_running("airflow scheduler"):
        logger.info("Airflow processes are already running")
        return None, None

    with FileLock(AIRFLOW_LOCK):
        if os.path.exists(AIRFLOW_LOCK) and not is_lock_file_valid(AIRFLOW_LOCK):
            remove_lock_file(AIRFLOW_LOCK)

        if os.path.exists(AIRFLOW_LOCK):
            logger.info(f"Valid lock file {AIRFLOW_LOCK} exists; Airflow might be running")
            return None, None

        logger.info("Starting Airflow webserver")
        webserver = await asyncio.create_subprocess_exec("airflow", "webserver", "--port", "8080")
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
                pids = content.split(",")
                for pid in pids:
                    if pid.isdigit() and is_process_running(pid):
                        try:
                            os.kill(int(pid), signal.SIGTERM)
                            logger.info(f"Terminated process with PID: {pid}")
                        except ProcessLookupError:
                            logger.warning(f"Process with PID {pid} not found; it may have already exited.")
            remove_lock_file(lock_file)
    logger.info("Cleanup process completed")

async def main():
    airflow_webserver, airflow_scheduler = await asyncio.gather(
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

    check_lock_files()

    try:
        while True:
            await asyncio.sleep(30)
    except KeyboardInterrupt:
        logger.info("Shutting down...")

if __name__ == "__main__":
    os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
    atexit.register(cleanup)
    asyncio.run(main())