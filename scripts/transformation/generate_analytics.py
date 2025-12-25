import psycopg2
import pandas as pd
import json
from datetime import datetime
import time

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="ecommerce",
    user="postgres", password="postgres123"
)

# Load queries from SQL file
with open("sql/queries/analytical_queries.sql", "r") as f:
    sql_text = f.read()

queries = sql_text.split(";")[:-1]  # Split 10 queries

query_names = [
    "query1_top_products","query2_monthly_trend","query3_customer_segmentation",
    "query4_category_performance","query5_payment_distribution",
    "query6_geographic_analysis","query7_customer_lifetime_value",
    "query8_product_profitability","query9_day_of_week_pattern",
    "query10_discount_impact"
]

results = []

def execute_query(connection, sql):
    start = time.time()
    df = pd.read_sql(sql, connection)
    exec_time = (time.time() - start)*1000
    return df, exec_time

def export_to_csv(df, filename):
    df.to_csv(f"data/processed/analytics/{filename}", index=False)

for i, sql in enumerate(queries):
    df, exec_time = execute_query(conn, sql)
    export_to_csv(df, f"{query_names[i]}.csv")
    results.append({
        "query_name": query_names[i],
        "data": df,
        "execution_time_ms": exec_time
    })

# Generate summary
summary = {
    "generation_timestamp": datetime.now().isoformat(),
    "queries_executed": len(results),
    "query_results": {
        r["query_name"]: {
            "rows": r["data"].shape[0],
            "columns": r["data"].shape[1],
            "execution_time_ms": r["execution_time_ms"]
        } for r in results
    },
    "total_execution_time_seconds": sum([r["execution_time_ms"] for r in results])/1000
}

with open("data/processed/analytics/analytics_summary.json","w") as f:
    json.dump(summary, f, indent=4)

print("Analytics queries executed and exported!")
