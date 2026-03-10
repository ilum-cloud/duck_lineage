-- =============================================================================
-- Duck Lineage Quickstart Demo: E-Commerce ETL Pipeline
-- =============================================================================
-- This script creates a realistic ETL pipeline that generates a rich lineage
-- graph visible in the Marquez Web UI at http://localhost:3000
--
-- Pipeline layers:
--   Layer 1 (Sources):    customers, products, orders, order_items
--   Layer 2 (Staging):    stg_order_details, stg_customer_orders
--   Layer 3 (Analytics):  analytics_revenue_by_product,
--                         analytics_customer_lifetime_value,
--                         analytics_daily_sales
--   Layer 4 (Summary):    executive_summary
-- =============================================================================

-- Install and load the Duck Lineage extension
INSTALL duck_lineage FROM community;
LOAD duck_lineage;

-- Configure lineage tracking
SET duck_lineage_url = 'http://localhost:5000/api/v1/lineage';
SET duck_lineage_namespace = 'demo';
SET duck_lineage_debug = true;

-- =============================================================================
-- Layer 1: Source Tables
-- =============================================================================

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    segment VARCHAR,
    region VARCHAR,
    created_at DATE
);

INSERT INTO customers VALUES
    (1, 'Acme Corp', 'acme@example.com', 'Enterprise', 'North America', '2024-01-15'),
    (2, 'TechStart Inc', 'tech@example.com', 'Startup', 'Europe', '2024-02-20'),
    (3, 'Global Retail', 'global@example.com', 'Enterprise', 'Asia Pacific', '2024-03-10'),
    (4, 'Local Shop', 'local@example.com', 'SMB', 'North America', '2024-04-05'),
    (5, 'Data Dynamics', 'data@example.com', 'Enterprise', 'Europe', '2024-05-12'),
    (6, 'Cloud Nine Ltd', 'cloud@example.com', 'Startup', 'North America', '2024-06-01'),
    (7, 'Mega Mart', 'mega@example.com', 'Enterprise', 'Asia Pacific', '2024-07-18'),
    (8, 'Quick Services', 'quick@example.com', 'SMB', 'Europe', '2024-08-22');

CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR,
    category VARCHAR,
    unit_price DECIMAL(10, 2)
);

INSERT INTO products VALUES
    (101, 'DuckDB Enterprise License', 'Software', 2999.99),
    (102, 'Data Pipeline Toolkit', 'Software', 499.99),
    (103, 'Analytics Dashboard Pro', 'Software', 899.99),
    (104, 'Cloud Storage (1TB)', 'Infrastructure', 29.99),
    (105, 'Premium Support Plan', 'Services', 1499.99),
    (106, 'Training Workshop', 'Services', 3999.99);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    status VARCHAR
);

INSERT INTO orders VALUES
    (1001, 1, '2024-06-15', 'completed'),
    (1002, 2, '2024-06-20', 'completed'),
    (1003, 3, '2024-07-01', 'completed'),
    (1004, 1, '2024-07-10', 'completed'),
    (1005, 4, '2024-07-15', 'completed'),
    (1006, 5, '2024-08-01', 'completed'),
    (1007, 6, '2024-08-10', 'completed'),
    (1008, 3, '2024-08-15', 'completed'),
    (1009, 7, '2024-09-01', 'completed'),
    (1010, 2, '2024-09-05', 'completed'),
    (1011, 8, '2024-09-10', 'completed'),
    (1012, 5, '2024-09-20', 'completed');

CREATE TABLE order_items (
    item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10, 2)
);

INSERT INTO order_items VALUES
    (1, 1001, 101, 1, 2999.99),
    (2, 1001, 105, 1, 1499.99),
    (3, 1002, 102, 3, 499.99),
    (4, 1002, 104, 10, 29.99),
    (5, 1003, 101, 2, 2999.99),
    (6, 1003, 103, 5, 899.99),
    (7, 1004, 106, 2, 3999.99),
    (8, 1005, 102, 1, 499.99),
    (9, 1005, 104, 5, 29.99),
    (10, 1006, 101, 1, 2999.99),
    (11, 1006, 103, 2, 899.99),
    (12, 1007, 102, 2, 499.99),
    (13, 1008, 105, 1, 1499.99),
    (14, 1008, 106, 1, 3999.99),
    (15, 1009, 101, 3, 2999.99),
    (16, 1010, 103, 1, 899.99),
    (17, 1011, 104, 20, 29.99),
    (18, 1011, 102, 1, 499.99),
    (19, 1012, 101, 1, 2999.99),
    (20, 1012, 105, 1, 1499.99);

-- =============================================================================
-- Layer 2: Staging Tables (JOINs)
-- =============================================================================

CREATE TABLE stg_order_details AS
SELECT
    o.order_id,
    o.order_date,
    o.status,
    oi.item_id,
    oi.quantity,
    oi.unit_price AS item_price,
    p.product_id,
    p.name AS product_name,
    p.category AS product_category,
    (oi.quantity * oi.unit_price) AS line_total
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id;

CREATE TABLE stg_customer_orders AS
SELECT
    c.customer_id,
    c.name AS customer_name,
    c.segment,
    c.region,
    o.order_id,
    o.order_date,
    o.status
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;

-- =============================================================================
-- Layer 3: Analytics Tables (Aggregations)
-- =============================================================================

CREATE TABLE analytics_revenue_by_product AS
SELECT
    product_name,
    product_category,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    SUM(line_total) AS total_revenue,
    AVG(line_total) AS avg_order_value
FROM stg_order_details
GROUP BY product_name, product_category
ORDER BY total_revenue DESC;

CREATE TABLE analytics_customer_lifetime_value AS
SELECT
    co.customer_name,
    co.segment,
    co.region,
    COUNT(DISTINCT co.order_id) AS total_orders,
    SUM(oi.quantity * oi.unit_price) AS lifetime_value,
    MIN(co.order_date) AS first_order_date,
    MAX(co.order_date) AS last_order_date
FROM stg_customer_orders co
JOIN order_items oi ON co.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY co.customer_name, co.segment, co.region
ORDER BY lifetime_value DESC;

CREATE TABLE analytics_daily_sales AS
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS num_orders,
    SUM(quantity) AS total_items,
    SUM(line_total) AS daily_revenue,
    AVG(line_total) AS avg_item_value
FROM stg_order_details
GROUP BY order_date
ORDER BY order_date;

-- =============================================================================
-- Layer 4: Executive Summary (Fan-in from all analytics)
-- =============================================================================

CREATE TABLE executive_summary AS
SELECT
    (SELECT SUM(total_revenue) FROM analytics_revenue_by_product) AS total_revenue,
    (SELECT COUNT(*) FROM analytics_revenue_by_product) AS unique_products_sold,
    (SELECT SUM(total_orders) FROM analytics_customer_lifetime_value) AS total_orders,
    (SELECT COUNT(*) FROM analytics_customer_lifetime_value) AS total_customers,
    (SELECT AVG(lifetime_value) FROM analytics_customer_lifetime_value) AS avg_customer_ltv,
    (SELECT SUM(daily_revenue) / COUNT(*) FROM analytics_daily_sales) AS avg_daily_revenue,
    (SELECT MAX(order_date) FROM analytics_daily_sales) AS latest_order_date;

-- =============================================================================
-- Done!
-- =============================================================================

SELECT '✓ ETL pipeline complete! Open http://localhost:3000 to explore the lineage graph.' AS status;
