import os
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Logging setup
log_file = Path("logs/cleanup.log")
log_file.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Directories to clean
directories = ["data/raw", "data/staging", "logs"]
retention_days = 7

def cleanup():
    cutoff = datetime.now() - timedelta(days=retention_days)
    for dir_path in directories:
        p = Path(dir_path)
        if not p.exists():
            continue
        for file in p.iterdir():
            if file.is_file() and file.stat().st_mtime < cutoff.timestamp():
                if "summary" in file.name or "report" in file.name:
                    continue
                try:
                    file.unlink()
                    logging.info(f"Deleted old file: {file}")
                except Exception as e:
                    logging.error(f"Failed to delete {file}: {e}")

if __name__ == "__main__":
    cleanup()
