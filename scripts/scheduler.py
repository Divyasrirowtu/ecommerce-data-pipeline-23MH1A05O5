import schedule
import time
import subprocess
from datetime import datetime
import logging
from pathlib import Path

# Setup logging
log_file = Path("logs/scheduler_activity.log")
log_file.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

PIPELINE_CMD = ["python", ".\\scripts\\pipeline_orchestrator.py"]

def run_pipeline():
    logging.info("Scheduler triggered pipeline run.")
    try:
        result = subprocess.run(PIPELINE_CMD, capture_output=True, text=True, check=True)
        logging.info(f"Pipeline Output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Pipeline failed: {e.stderr}")

# Schedule daily execution at 02:00 AM
schedule.every().day.at("02:00").do(run_pipeline)

logging.info("Scheduler started. Waiting for next run...")
while True:
    schedule.run_pending()
    time.sleep(10)

