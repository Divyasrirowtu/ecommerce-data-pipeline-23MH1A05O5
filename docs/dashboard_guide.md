# Dashboard Guide - E-commerce Analytics

## Overview
This guide explains how to use the data warehouse to build dashboards for sales, product performance, and customer metrics.

---

## 1. Key Tables

### Fact Table
- `fact_sales`
  - Grain: One row per transaction line item
  - Key columns: `transaction_id`, `customer_key`, `product_key`, `date_key`, `payment_method_key`
  - Measures: `quantity`, `unit_price`, `discount_amount`, `line_total`, `profit`

### Dimensions
- `dim_customers`: Customer info, segments, age groups, SCD Type 2
- `dim_products`: Product info, category, brand, SCD Type 2
- `dim_date`: Full date hierarchy (year, quarter, month, week, day)
- `dim_payment_method`: Payment type and category

### Aggregate Tables
- `agg_daily_sales`: Total transactions, revenue, profit, unique customers
- `agg_product_performance`: Total quantity, revenue, profit, avg discount per product
- `agg_customer_metrics`: Total transactions, total spent, avg order value, last purchase date

---

## 2. Sample Dashboard Metrics

1. **Sales Overview**
   - Total Revenue vs Total Profit
   - Daily/Monthly Trends using `agg_daily_sales`
   - Number of Unique Customers

2. **Product Performance**
   - Top Selling Products by Quantity/Revenue
   - Profitability per Product
   - Price category analysis

3. **Customer Insights**
   - Customer Segmentation (New, Regular, VIP)
   - Average Order Value
   - Purchase Frequency

4. **Payment Method Analysis**
   - Revenue by Payment Type
   - Online vs Offline Distribution

---

## 3. Dashboard Tips
- Join fact table with dimensions for descriptive context:
```sql
SELECT f.line_total, c.customer_segment, p.category, d.month_name
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
JOIN dim_products p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key;
