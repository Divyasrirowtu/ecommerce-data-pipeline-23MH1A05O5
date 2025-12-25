import pandas as pd
import os
import re

DATA_DIR = "data/raw"

def test_csv_files_exist():
    files = [
        "customers.csv",
        "products.csv",
        "transactions.csv",
        "transaction_items.csv"
    ]
    for f in files:
        assert os.path.exists(os.path.join(DATA_DIR, f)), f"{f} not generated"

def test_required_columns():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    assert {"customer_id", "email"}.issubset(customers.columns)

def test_no_null_customer_id():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    assert customers["customer_id"].isnull().sum() == 0

def test_email_format():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    pattern = r"[^@]+@[^@]+\.[^@]+"
    assert customers["email"].str.match(pattern).all()

def test_referential_integrity():
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    transactions = pd.read_csv(f"{DATA_DIR}/transactions.csv")
    assert transactions["customer_id"].isin(customers["customer_id"]).all()

def test_line_total_calculation():
    items = pd.read_csv(f"{DATA_DIR}/transaction_items.csv")
    assert (items["quantity"] * items["unit_price"] == items["line_total"]).all()
