import psycopg2
import pandas as pd
from datetime import datetime

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="ecommerce",
    user="postgres", password="postgres123"
)

# Load production tables
prod_customers = pd.read_sql("SELECT * FROM production.customers;", conn)
prod_transactions = pd.read_sql("SELECT * FROM production.transactions;", conn)

# Example transformation: compute line_total
prod_transactions['line_total'] = prod_transactions['quantity'] * prod_transactions['unit_price'] * (1 - prod_transactions['discount']/100)

# Load to warehouse fact_sales
with conn.cursor() as cur:
    # Example: truncate fact_sales before load
    cur.execute("TRUNCATE TABLE warehouse.fact_sales;")
    
    for _, row in prod_transactions.iterrows():
        cur.execute(
            """INSERT INTO warehouse.fact_sales(transaction_id, customer_id, product_id, quantity, unit_price, discount, line_total, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (row.transaction_id, row.customer_id, row.product_id, row.quantity, row.unit_price, row.discount, row.line_total, datetime.now())
        )

conn.commit()
print("Warehouse load completed!")
