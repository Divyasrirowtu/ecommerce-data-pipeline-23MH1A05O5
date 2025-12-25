import pandas as pd
import random
import json
from faker import Faker
from datetime import datetime, timezone
import yaml
from pathlib import Path

# ---------------- INITIAL SETUP ---------------- #
fake = Faker()
random.seed(42)

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- LOAD CONFIG ---------------- #
with open(BASE_DIR / "config.yaml", "r") as f:
    cfg = yaml.safe_load(f)["data_generation"]

# ---------------- HELPER FUNCTION ---------------- #
def padded_id(prefix, num, width):
    return f"{prefix}{str(num).zfill(width)}"

# ---------------- CUSTOMERS ---------------- #
def generate_customers(n):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "customer_id": padded_id("CUST", i, 4),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "registration_date": fake.date_between("-3y", "today").isoformat(),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(["18-25", "26-35", "36-50", "50+"])
        })
    return pd.DataFrame(customers)

# ---------------- PRODUCTS ---------------- #
def generate_products(n):
    categories = {
        "Electronics": ["Mobiles", "Laptops", "Accessories"],
        "Clothing": ["Men", "Women", "Kids"],
        "Home & Kitchen": ["Furniture", "Appliances"],
        "Books": ["Fiction", "Education"],
        "Sports": ["Outdoor", "Fitness"],
        "Beauty": ["Skincare", "Makeup"]
    }

    products = []
    for i in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        price = round(random.uniform(10, 500), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)

        products.append({
            "product_id": padded_id("PROD", i, 4),
            "product_name": fake.word().title(),
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 1000),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })
    return pd.DataFrame(products)

# ---------------- TRANSACTIONS ---------------- #
def generate_transactions(n, customer_ids):
    transactions = []
    for i in range(1, n + 1):
        tx_date = fake.date_between(cfg["start_date"], cfg["end_date"])
        transactions.append({
            "transaction_id": padded_id("TXN", i, 5),
            "customer_id": random.choice(customer_ids),
            "transaction_date": tx_date.isoformat(),
            "transaction_time": fake.time(),
            "payment_method": random.choice(
                ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
            ),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.00
        })
    return pd.DataFrame(transactions)

# ---------------- TRANSACTION ITEMS ---------------- #
def generate_transaction_items(transactions, products):
    items = []
    item_counter = 1

    for _, txn in transactions.iterrows():
        item_count = random.randint(cfg["min_items_per_txn"], cfg["max_items_per_txn"])
        sampled_products = products.sample(item_count)

        for _, prod in sampled_products.iterrows():
            quantity = random.randint(1, 5)
            discount = random.choice([0, 5, 10, 15])
            line_total = round(
                quantity * prod["price"] * (1 - discount / 100), 2
            )

            items.append({
                "item_id": padded_id("ITEM", item_counter, 5),
                "transaction_id": txn["transaction_id"],
                "product_id": prod["product_id"],
                "quantity": quantity,
                "unit_price": round(prod["price"], 2),
                "discount_percentage": discount,
                "line_total": line_total
            })
            item_counter += 1

    return pd.DataFrame(items)

# ---------------- VALIDATION ---------------- #
def validate_referential_integrity(customers, products, transactions, items):
    orphan_customers = transactions[
        ~transactions.customer_id.isin(customers.customer_id)
    ]
    orphan_products = items[
        ~items.product_id.isin(products.product_id)
    ]
    orphan_transactions = items[
        ~items.transaction_id.isin(transactions.transaction_id)
    ]

    total_violations = (
        len(orphan_customers)
        + len(orphan_products)
        + len(orphan_transactions)
    )

    score = 100 if total_violations == 0 else max(0, 100 - total_violations)

    return {
        "orphan_records": total_violations,
        "constraint_violations": total_violations,
        "data_quality_score": score
    }

# ---------------- MAIN ---------------- #
def main():
    print("Starting data generation...")

    customers = generate_customers(cfg["customers"])
    products = generate_products(cfg["products"])
    transactions = generate_transactions(
        cfg["transactions"], customers.customer_id.tolist()
    )
    items = generate_transaction_items(transactions, products)

    # Calculate total amount per transaction
    totals = items.groupby("transaction_id")["line_total"].sum().round(2)
    transactions["total_amount"] = transactions["transaction_id"].map(totals)

    # Save CSVs
    customers.to_csv(RAW_DIR / "customers.csv", index=False)
    products.to_csv(RAW_DIR / "products.csv", index=False)
    transactions.to_csv(RAW_DIR / "transactions.csv", index=False)
    items.to_csv(RAW_DIR / "transaction_items.csv", index=False)

    metadata = {
        "generation_timestamp": datetime.now(timezone.utc).isoformat(),
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(items)
        },
        "transaction_date_range": {
            "start": str(cfg["start_date"]),
            "end": str(cfg["end_date"])
        },
        "data_quality": validate_referential_integrity(
            customers, products, transactions, items
        )
    }

    with open(RAW_DIR / "generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("Data generation completed successfully.")

if __name__ == "__main__":
    main()
