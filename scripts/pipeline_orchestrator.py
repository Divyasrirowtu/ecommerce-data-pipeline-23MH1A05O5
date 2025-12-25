# scripts/pipeline_orchestrator.py
import subprocess
import time
import json
import os
from datetime import datetime

# Define pipeline steps in order
PIPELINE_STEPS = [
    {"name": "Data Generation", "script": "scripts/data_generation/generate_data.py"},
    {"name": "Data Ingestion", "script": "scripts/ingestion/load_to_staging.py"},
    {"name": "Data Quality Checks", "script": "scripts/quality_checks/validate_data.py"},
    {"name": "Staging to Production", "script": "scripts/transformation/staging_to_production.py"},
    {"name": "Warehouse Load", "script": "scripts/transformation/load_to_warehouse.py"},
    {"name": "Analytics Generation", "script": "scripts/transformation/generate_analytics.py"},
]

# Log folder
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Pipeline report path
REPORT_DIR = "data/processed"
os.makedirs(REPORT_DIR, exist_ok=True)
REPORT_FILE = os.path.join(REPORT_DIR, "pipeline_execution_report.json")

MAX_RETRIES = 3
BACKOFF_TIMES = [1, 2, 4]  # exponential backoff in seconds

def run_step(step_name, script_path):
    """Run a pipeline step with retry and backoff."""
    step_result = {
        "status": "failed",
        "duration_seconds": 0,
        "records_processed": 0,
        "error_message": None,
        "retry_attempts": 0
    }
    for attempt in range(1, MAX_RETRIES + 1):
        start_time = time.time()
        try:
            print(f"[INFO] Starting step: {step_name}, Attempt: {attempt}")
            subprocess.run(["python", script_path], check=True, shell=True)
            duration = time.time() - start_time
            step_result["status"] = "success"
            step_result["duration_seconds"] = duration
            step_result["retry_attempts"] = attempt
            print(f"[INFO] Completed step: {step_name} in {duration:.2f}s")
            return step_result
        except subprocess.CalledProcessError as e:
            duration = time.time() - start_time
            step_result["error_message"] = str(e)
            step_result["retry_attempts"] = attempt
            step_result["duration_seconds"] = duration
            print(f"[ERROR] Step failed: {step_name}, Attempt: {attempt}, Error: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_TIMES[attempt - 1])
            else:
                print(f"[ERROR] Max retries exceeded for step: {step_name}")
                return step_result

def orchestrate_pipeline():
    """Run the full ETL pipeline in sequence and generate report."""
    pipeline_report = {
        "pipeline_execution_id": f"PIPE_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "start_time": datetime.now().isoformat(),
        "end_time": None,
        "total_duration_seconds": 0,
        "status": "success",
        "steps_executed": {},
        "data_quality_summary": {},
        "errors": [],
        "warnings": []
    }

    pipeline_start = time.time()

    for step in PIPELINE_STEPS:
        result = run_step(step["name"], step["script"])
        pipeline_report["steps_executed"][step["name"]] = result
        if result["status"] != "success":
            pipeline_report["status"] = "failed"
            pipeline_report["errors"].append(f"{step['name']} failed: {result['error_message']}")
            break  # Stop pipeline if a step fails

    pipeline_report["end_time"] = datetime.now().isoformat()
    pipeline_report["total_duration_seconds"] = time.time() - pipeline_start

    # Save pipeline report
    with open(REPORT_FILE, "w") as f:
        json.dump(pipeline_report, f, indent=4)

    print(f"[INFO] Pipeline finished with status: {pipeline_report['status']}")
    print(f"[INFO] Report saved to: {REPORT_FILE}")

if __name__ == "__main__":
    orchestrate_pipeline()
