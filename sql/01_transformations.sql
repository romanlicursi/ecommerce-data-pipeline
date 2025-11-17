-- =======================================================
-- TRANSFORMATION LAYER
-- Convert cleaned tables into analytics-ready structures
-- =======================================================

CREATE SCHEMA IF NOT EXISTS transformed;

-- ================================
-- 1. FACT TABLE: fct_orders
-- ================================

CREATE OR REPLACE TABLE transformed.fct_orders AS
WITH joined AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.product_id,
        o.order_amount,
        o.customer_email,
        o.shipping_state,
        o.marketing_source,
        o.order_status,
        p.name AS product_name,
        p.category AS product_category,
        p.price AS product_price
    FROM cleaned.orders o
    LEFT JOIN raw.product_catalog p
        ON o.product_id = p.product_id
),
enriched AS (
    SELECT
        *,
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        EXTRACT(QUARTER FROM order_date) AS order_quarter,
        CASE 
            WHEN order_amount >= 500 THEN 'High Value'
            WHEN order_amount >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment
    FROM joined
)
SELECT * FROM enriched;

-- ================================
-- 2. DIM CUSTOMERS
-- ================================

CREATE OR REPLACE TABLE transformed.dim_customers AS
WITH base AS (
    SELECT
        customer_id,
        customer_email,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(*) AS total_orders,
        SUM(order_amount) AS total_revenue,
        AVG(order_amount) AS avg_order_value
    FROM transformed.fct_orders
    WHERE order_status = 'completed'
    GROUP BY customer_id, customer_email
),
classified AS (
    SELECT
        *,
        CASE
            WHEN total_orders = 1 THEN 'One-Time'
            WHEN total_orders <= 3 THEN 'Occasional'
            ELSE 'Repeat'
        END AS customer_type,
        CASE
            WHEN total_revenue >= 1000 THEN 'VIP'
            WHEN total_revenue >= 500 THEN 'Premium'
            WHEN total_revenue >= 100 THEN 'Standard'
            ELSE 'Basic'
        END AS customer_tier
    FROM base
)
SELECT * FROM classified;

-- ================================
-- 3. PRODUCT PERFORMANCE
-- ================================

CREATE OR REPLACE TABLE transformed.product_performance AS
SELECT
    product_id,
    product_name,
    product_category,
    COUNT(*) AS total_orders,
    SUM(order_amount) AS total_revenue,
    AVG(order_amount) AS avg_order_value,
    COUNT(DISTINCT shipping_state) AS states_sold_to
FROM transformed.fct_orders
WHERE order_status = 'completed'
GROUP BY product_id, product_name, product_category;

-- ================================
-- 4. MARKETING ATTRIBUTION
-- ================================

CREATE OR REPLACE TABLE transformed.marketing_attribution AS
WITH base AS (
    SELECT
        marketing_source,
        order_year,
        order_quarter,
        COUNT(*) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(order_amount) AS total_revenue,
        AVG(order_amount) AS avg_order_value
    FROM transformed.fct_orders
    WHERE order_status = 'completed'
    GROUP BY marketing_source, order_year, order_quarter
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY order_year, order_quarter
            ORDER BY total_revenue DESC
        ) AS revenue_rank
    FROM base
)
SELECT * FROM ranked;

-- ================================
-- 5. DAILY METRICS
-- ================================

CREATE OR REPLACE TABLE transformed.daily_metrics AS
SELECT
    order_date,
    COUNT(*) AS daily_orders,
    SUM(order_amount) AS daily_revenue,
    AVG(order_amount) AS avg_order_value
FROM transformed.fct_orders
WHERE order_status = 'completed'
GROUP BY order_date
ORDER BY order_date;

