# API Documentation - E-commerce Data Pipeline (Phase 3)

## Overview
This document describes the scripts and functions exposed for data validation, ETL transformations, and warehouse loading in the Phase 3 pipeline.

---

## 1. Data Quality API

### `validate_data.py`

**Functions:**
- `check_nulls(connection)`
  - Description: Checks for NULL values in mandatory fields.
  - Returns: Dictionary with table.column and null counts.

- `check_duplicates(connection)`
  - Description: Detects duplicate primary/business keys and transactions.
  - Returns: Dictionary with duplicate counts per table.

- `check_referential_integrity(connection)`
  - Description: Validates foreign key relationships and identifies orphan records.
  - Returns: Dictionary with relationship and orphan counts.

- `check_data_consistency(connection)`
  - Description: Validates calculated fields (e.g., line_total = quantity × unit_price × (1 - discount/100)).
  - Returns: Dictionary with mismatched records.

- `run_all_checks(connection)`
  - Description: Executes all quality checks and computes overall quality score.
  - Returns: JSON object with detailed report and grade.

---

## 2. ETL API

### `staging_to_production.py`

**Functions:**
- `cleanse_data(df)`
  - Trims whitespace, formats emails/phone, standardizes dates and decimals.
  
- `enrich_data(df)`
  - Adds derived columns such as `profit_margin`, `price_category`, etc.

- `validate_business_rules(df)`
  - Applies rules like removing invalid transactions/items and recalculating totals.

- `load_dimensions(df, connection, table_name)`
  - Truncate and reload dimension tables (idempotent).

- `load_fact_table(df, connection, table_name)`
  - Incremental append for fact tables, preserving referential integrity.

---

### 3. Warehouse Loading API

### `load_warehouse.py`

**Functions:**
- `build_dim_date(start_date, end_date, connection)`
  - Generates `dim_date` for time-series analysis.

- `load_dim_customers(connection, df_customers)`
  - Implements SCD Type 2 for customers.

- `load_dim_products(connection, df_products)`
  - Implements SCD Type 2 for products.

- `load_fact_sales(connection, df_transactions)`
  - Loads transaction line items using surrogate keys.

- `populate_aggregates(connection)`
  - Populates `agg_daily_sales`, `agg_product_performance`, `agg_customer_metrics`.

---

## Notes
- All functions require a valid DB connection object.
- Ensure ETL functions are executed in order: dimensions → fact → aggregates.
- API is idempotent: repeated runs produce consistent results.
