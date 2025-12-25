import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="ecommerce",
    user="postgres", password="postgres123"
)

# Load staging tables
customers = pd.read_sql("SELECT * FROM staging_customers;", conn)
transactions = pd.read_sql("SELECT * FROM staging_transactions;", conn)

# Data Cleansing
customers['email'] = customers['email'].str.lower()
customers['first_name'] = customers['first_name'].str.title()
customers['last_name'] = customers['last_name'].str.title()

# Filter invalid transactions
transactions = transactions[transactions['quantity'] > 0]
transactions = transactions[transactions['unit_price'] > 0]

# Load to production tables
with conn.cursor() as cur:
    # Truncate dimension tables
    cur.execute("TRUNCATE TABLE production.customers;")
    cur.execute("TRUNCATE TABLE production.transactions;")

    # Load customers
    for _, row in customers.iterrows():
        cur.execute(
            """INSERT INTO production.customers(customer_id, first_name, last_name, email, city, state, country, registration_date)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (row.customer_id, row.first_name, row.last_name, row.email, row.city, row.state, row.country, row.registration_date)
        )

    # Load transactions
    for _, row in transactions.iterrows():
        cur.execute(
            """INSERT INTO production.transactions(transaction_id, customer_id, product_id, quantity, unit_price, discount, transaction_date)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (row.transaction_id, row.customer_id, row.product_id, row.quantity, row.unit_price, row.discount, row.transaction_date)
        )

conn.commit()
print("Staging → Production ETL completed successfully!")
