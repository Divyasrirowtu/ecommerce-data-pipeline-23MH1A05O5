-- Query 1: Top 10 Products by Revenue
WITH product_sales AS (
    SELECT 
        p.product_name,
        p.category,
        SUM(f.line_total) AS total_revenue,
        SUM(f.quantity) AS units_sold,
        AVG(f.unit_price) AS avg_price
    FROM fact_sales f
    JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_name, p.category
)
SELECT *
FROM product_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Monthly Sales Trend
WITH monthly_sales AS (
    SELECT 
        CONCAT(d.year,'-',LPAD(d.month::text,2,'0')) AS year_month,
        SUM(f.line_total) AS total_revenue,
        COUNT(DISTINCT f.transaction_id) AS total_transactions,
        AVG(f.line_total) AS avg_order_value,
        COUNT(DISTINCT f.customer_id) AS unique_customers
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY year_month
)
SELECT *
FROM monthly_sales
ORDER BY year_month;

-- Query 3: Customer Segmentation Analysis
WITH customer_totals AS (
    SELECT 
        c.customer_id,
        CONCAT(c.first_name,' ',c.last_name) AS full_name,
        SUM(f.line_total) AS total_spent,
        AVG(f.line_total) AS avg_transaction_value
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id
)
SELECT 
    CASE 
        WHEN total_spent <= 1000 THEN '$0-$1,000'
        WHEN total_spent <= 5000 THEN '$1,000-$5,000'
        WHEN total_spent <= 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    AVG(avg_transaction_value) AS avg_transaction_value
FROM customer_totals
GROUP BY spending_segment;

-- Query 4: Category Performance
SELECT 
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.line_total - f.cost) AS total_profit,
    ROUND(SUM(f.line_total - f.cost)/SUM(f.line_total)*100,2) AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Query 5: Payment Method Distribution
WITH total_transactions AS (
    SELECT COUNT(*) AS total_txn, SUM(line_total) AS total_rev FROM fact_sales
)
SELECT 
    f.payment_method,
    COUNT(*) AS transaction_count,
    SUM(f.line_total) AS total_revenue,
    ROUND(COUNT(*)*100.0/(SELECT total_txn FROM total_transactions),2) AS pct_of_transactions,
    ROUND(SUM(f.line_total)*100.0/(SELECT total_rev FROM total_transactions),2) AS pct_of_revenue
FROM fact_sales f
GROUP BY f.payment_method;

-- Query 6: Geographic Analysis
SELECT 
    c.state,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    ROUND(SUM(f.line_total)/COUNT(DISTINCT c.customer_id),2) AS avg_revenue_per_customer
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.state
ORDER BY total_revenue DESC;

-- Query 7: Customer Lifetime Value (CLV)
SELECT 
    c.customer_id,
    CONCAT(c.first_name,' ',c.last_name) AS full_name,
    SUM(f.line_total) AS total_spent,
    COUNT(f.transaction_id) AS transaction_count,
    EXTRACT(DAY FROM CURRENT_DATE - c.registration_date) AS days_since_registration,
    AVG(f.line_total) AS avg_order_value
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_id;

-- Query 8: Product Profitability Analysis
SELECT 
    p.product_name,
    p.category,
    SUM(f.line_total - f.cost) AS total_profit,
    ROUND(SUM(f.line_total - f.cost)/SUM(f.line_total)*100,2) AS profit_margin,
    SUM(f.line_total) AS revenue,
    SUM(f.quantity) AS units_sold
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY total_profit DESC
LIMIT 10;

-- Query 9: Day of Week Sales Pattern
SELECT 
    d.day_name,
    AVG(f.line_total) AS avg_daily_revenue,
    AVG(f.quantity) AS avg_daily_transactions,
    SUM(f.line_total) AS total_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.day_name
ORDER BY total_revenue DESC;

-- Query 10: Discount Impact Analysis
SELECT 
    CASE 
        WHEN f.discount = 0 THEN '0%'
        WHEN f.discount <= 0.10 THEN '1-10%'
        WHEN f.discount <= 0.25 THEN '11-25%'
        WHEN f.discount <= 0.50 THEN '26-50%'
        ELSE '50%+'
    END AS discount_range,
    AVG(f.discount) AS avg_discount_pct,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.line_total) AS total_revenue,
    AVG(f.line_total) AS avg_line_total
FROM fact_sales f
GROUP BY discount_range
ORDER BY total_revenue DESC;
