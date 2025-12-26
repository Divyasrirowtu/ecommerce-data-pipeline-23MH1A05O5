# Architecture - E-commerce Data Pipeline (Phase 3)

## Overview
The Phase 3 pipeline transforms, cleanses, validates, and loads e-commerce transactional data into a dimensional data warehouse. The architecture ensures data quality, referential integrity, and analytical readiness.

---

## 1. Layers

### **1. Staging Layer**
- Temporary storage of raw data.
- Tables mirror source structure: `staging_customers`, `staging_products`, `staging_transactions`, `staging_transaction_items`.
- Data validation and cleansing performed here before production load.

### **2. Production Layer**
- Cleansed and enriched data.
- Dimension tables (full reload, Type 1 SCD):
  - `customers`, `products`
- Fact tables (incremental append):
  - `transactions`, `transaction_items`
- Business rules applied here.

### **3. Data Warehouse**
- Star schema design for analytics.
- Dimensions:
  - `dim_customers` (SCD Type 2)
  - `dim_products` (SCD Type 2)
  - `dim_date`
  - `dim_payment_method`
- Fact:
  - `fact_sales` (transaction_items grain)
- Aggregates:
  - `agg_daily_sales`
  - `agg_product_performance`
  - `agg_customer_metrics`

---

## 2. Data Flow

1. Raw data ingested into **staging tables**.
2. **Data Quality Checks**:
   - Completeness, uniqueness, validity, consistency, referential integrity, accuracy.
3. **ETL Transformation**:
   - Cleansing → Enrichment → Business Rule Validation → Load to Production.
4. **Warehouse Loading**:
   - Load dimensions → Load fact_sales → Populate aggregates.
5. Data ready for **analytics and dashboarding**.

---

## 3. Key Features
- **Idempotency**: ETL can run multiple times without affecting results.
- **SCD Type 2**: Tracks historical changes for customers/products.
- **Referential Integrity**: Enforced at each stage.
- **Aggregate Tables**: Improve query performance for analytics.
