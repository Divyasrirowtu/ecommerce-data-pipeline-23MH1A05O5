-- Drop schema if exists
DROP SCHEMA IF EXISTS warehouse CASCADE;
CREATE SCHEMA warehouse;

-- Dim Customers
CREATE TABLE warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    full_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    age_group VARCHAR(20),
    customer_segment VARCHAR(20),
    registration_date DATE,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- Dim Products
CREATE TABLE warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    brand VARCHAR(50),
    price_range VARCHAR(20),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- Dim Date
CREATE TABLE warehouse.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend BOOLEAN
);

-- Dim Payment Method
CREATE TABLE warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(50),
    payment_type VARCHAR(20)
);

-- Fact Sales
CREATE TABLE warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    payment_method_key INT NOT NULL,
    transaction_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL,
    discount_amount DECIMAL,
    line_total DECIMAL,
    profit DECIMAL,
    created_at TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES warehouse.dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES warehouse.dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES warehouse.dim_products(product_key),
    FOREIGN KEY (payment_method_key) REFERENCES warehouse.dim_payment_method(payment_method_key)
);
