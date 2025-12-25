import psycopg2
import pandas as pd
import json
from datetime import datetime
import time

# Connect to warehouse
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="ecommerce",
    user="postgres",
    password="postgres123"
)

def execute_query(connection, query_name, sql):
    start = time.time()
    df = pd.read_sql(sql, connection)
    exec_time = (time.time() - start) * 1000
    return df, exec_time

def export_to_csv(df, filename):
    df.to_csv(f"data/processed/analytics/{filename}", index=False)

def generate_summary(results):
    summary = {
        "generation_timestamp": datetime.now().isoformat(),
        "queries_executed": len(results),
        "query_results": {},
        "total_execution_time_seconds": sum([r["execution_time_ms"] for r in results])/1000
    }
    for r in results:
        summary["query_results"][r["query_name"]] = {
            "rows": r["data"].shape[0],
            "columns": r["data"].shape[1],
            "execution_time_ms": r["execution_time_ms"]
        }
    with open("data/processed/analytics/analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

def main():
    results = []
    queries = {
        "query1_top_products": "SELECT * FROM sql/queries/analytical_queries.sql WHERE id=1;",
        # Add paths or load queries for query2..query10
    }
    for name, sql in queries.items():
        df, exec_time = execute_query(conn, name, sql)
        export_to_csv(df, f"{name}.csv")
        results.append({"query_name": name, "data": df, "execution_time_ms": exec_time})
    
    generate_summary(results)
    print("Analytics generation completed!")

if __name__ == "__main__":
    main()
