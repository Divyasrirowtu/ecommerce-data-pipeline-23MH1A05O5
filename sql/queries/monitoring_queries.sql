-- 1. Last pipeline execution
SELECT end_time, status FROM pipeline_execution_report
ORDER BY end_time DESC
LIMIT 1;

-- 2. Daily transaction counts (last 30 days)
SELECT created_at::date AS day, COUNT(*) AS total_transactions
FROM fact_sales
WHERE created_at >= current_date - interval '30 days'
GROUP BY day
ORDER BY day;

-- 3. Null violations in staging
SELECT 'staging_customers' AS table_name, COUNT(*) AS null_count
FROM staging_customers
WHERE customer_id IS NULL OR email IS NULL;

-- 4. Latest loaded timestamps for production and warehouse
SELECT MAX(created_at) AS latest_production FROM production_customers;
SELECT MAX(created_at) AS latest_fact FROM fact_sales;
