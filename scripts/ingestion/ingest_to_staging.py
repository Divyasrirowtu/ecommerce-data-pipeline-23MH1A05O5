import psycopg2
import json
import time
import yaml
import os

# Load DB config
with open("config.yaml") as f:
    db = yaml.safe_load(f)["database"]

conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["dbname"],
    user=db["user"],
    password=db["password"]
)
cur = conn.cursor()

start_time = time.time()
summary = {}

tables = [
    "customers",
    "products",
    "transactions",
    "transaction_items"
]

try:
    cur.execute("BEGIN;")

    for table in tables:
        cur.execute(f"TRUNCATE staging.{table};")

        csv_path = f"data/raw/{table}.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found")

        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY staging.{table} FROM STDIN WITH CSV HEADER",
                f
            )

        cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
        rows = cur.fetchone()[0]

        summary[f"staging.{table}"] = {
            "rows_loaded": rows,
            "status": "success"
        }

    conn.commit()

except Exception as e:
    conn.rollback()
    summary["error"] = str(e)

finally:
    cur.close()
    conn.close()

summary["ingestion_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
summary["total_execution_time_seconds"] = round(
    time.time() - start_time, 2
)

os.makedirs("data/staging", exist_ok=True)
with open("data/staging/ingestion_summary.json", "w") as f:
    json.dump(summary, f, indent=4)

print("✅ Data ingestion completed. Check ingestion_summary.json")
