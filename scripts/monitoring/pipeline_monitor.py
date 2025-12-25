import psycopg2
import json
from datetime import datetime, timedelta
from pathlib import Path

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="ecommerce",
    user="postgres",
    password="postgres123"
)

def check_pipeline_health():
    report = {
        "monitoring_timestamp": datetime.now().isoformat(),
        "pipeline_health": "healthy",
        "checks": {},
        "alerts": [],
        "overall_health_score": 100
    }

    # Example: Check last pipeline execution
    try:
        with open("data/processed/pipeline_execution_report.json") as f:
            execution_report = json.load(f)
        last_run = datetime.fromisoformat(execution_report["end_time"])
        hours_since_last_run = (datetime.now() - last_run).total_seconds() / 3600
        status = "ok" if hours_since_last_run <= 25 else "critical"
        report["checks"]["last_execution"] = {
            "status": status,
            "last_run": last_run.isoformat(),
            "hours_since_last_run": hours_since_last_run,
            "threshold_hours": 25
        }
        if status == "critical":
            report["pipeline_health"] = "critical"
            report["alerts"].append({
                "severity": "critical",
                "check": "last_execution",
                "message": "Pipeline has not run in the last 25 hours",
                "timestamp": datetime.now().isoformat()
            })
    except Exception as e:
        report["pipeline_health"] = "critical"
        report["alerts"].append({
            "severity": "critical",
            "check": "last_execution",
            "message": f"Failed to check last execution: {e}",
            "timestamp": datetime.now().isoformat()
        })

    # Add more checks: data freshness, volume anomalies, quality, database connectivity
    # Example: Data freshness for staging table
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(loaded_at) FROM staging_customers;")
        latest_staging = cur.fetchone()[0]
        hours_lag = (datetime.now() - latest_staging).total_seconds() / 3600 if latest_staging else None
        status = "ok" if hours_lag and hours_lag < 24 else "warning"
        report["checks"]["data_freshness"] = {
            "status": status,
            "staging_latest_record": str(latest_staging),
            "max_lag_hours": 24
        }

    Path("data/processed").mkdir(parents=True, exist_ok=True)
    with open("data/processed/monitoring_report.json", "w") as f:
        json.dump(report, f, indent=4)
    print("Monitoring report generated!")

if __name__ == "__main__":
    check_pipeline_health()
