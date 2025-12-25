import psycopg2
import json
from datetime import datetime

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="ecommerce",
    user="postgres", password="postgres123"
)

def null_check(table, column):
    query = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL OR {column}='';"
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]

def generate_report():
    tables_columns = {
        "staging_customers": ["customer_id", "email"],
        "staging_transactions": ["transaction_id", "customer_id"]
    }

    report = {
        "check_timestamp": datetime.now().isoformat(),
        "checks_performed": {"null_checks": {"status":"passed","tables_checked":list(tables_columns.keys()),"null_violations":0,"details":{}}}
    }

    for table, columns in tables_columns.items():
        for col in columns:
            count = null_check(table, col)
            report["checks_performed"]["null_checks"]["details"][f"{table}.{col}"] = count
            if count > 0:
                report["checks_performed"]["null_checks"]["status"] = "failed"
                report["checks_performed"]["null_checks"]["null_violations"] += count

    # Save report
    with open("data/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

if __name__ == "__main__":
    generate_report()
    print("Data Quality report generated!")
