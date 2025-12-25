import psycopg2
import pandas as pd

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="ecommerce",
    user="postgres",
    password="postgres123"
)
cursor = conn.cursor()

# -------------------------
# 1. Transform & load Customers
# -------------------------
staging_customers = pd.read_sql("SELECT * FROM staging_customers;", conn)

# Remove duplicates
final_customers = staging_customers.drop_duplicates(subset=['customer_id'])

# Insert into final table
for _, row in final_customers.iterrows():
    cursor.execute("""
        INSERT INTO customers (customer_id, first_name, last_name, email, phone)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """, (row['customer_id'], row['first_name'], row['last_name'], row['email'], row['phone']))

# -------------------------
# 2. Transform & load Orders
# -------------------------
staging_orders = pd.read_sql("SELECT * FROM staging_orders;", conn)

for _, row in staging_orders.iterrows():
    cursor.execute("""
        INSERT INTO orders (order_id, customer_id, order_date, total_amount)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING;
    """, (row['order_id'], row['customer_id'], row['order_date'], row['total_amount']))

# -------------------------
# 3. Transform & load Payments
# -------------------------
staging_payments = pd.read_sql("SELECT * FROM staging_payments;", conn)

for _, row in staging_payments.iterrows():
    cursor.execute("""
        INSERT INTO payments (payment_id, order_id, payment_date, amount, payment_method)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (payment_id) DO NOTHING;
    """, (row['payment_id'], row['order_id'], row['payment_date'], row['amount'], row['payment_method']))

# -------------------------
# Commit and close connection
# -------------------------
conn.commit()
cursor.close()
conn.close()

print("Phase 2 Step 2.3: Transformation & load to warehouse completed successfully!")
