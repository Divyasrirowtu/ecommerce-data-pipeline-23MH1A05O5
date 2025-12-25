-- Query 1: Top 10 Products by Revenue
-- Objective: Identify best-selling products
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
