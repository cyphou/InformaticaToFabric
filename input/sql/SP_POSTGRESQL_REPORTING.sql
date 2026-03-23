-- ============================================================
-- Stored Procedure: SP_POSTGRESQL_REPORTING
-- Source DB: PostgreSQL 15+
-- Purpose: Reporting queries with PostgreSQL-specific SQL
-- Tests: ::casts, ILIKE, SERIAL/BIGSERIAL, TEXT, BOOLEAN,
--        RETURNING, ARRAY_AGG, STRING_TO_ARRAY, GENERATE_SERIES,
--        ON CONFLICT, BYTEA, INTERVAL, COALESCE, JSONB
-- ============================================================

-- Step 1: Create reporting table with PostgreSQL types
CREATE TABLE IF NOT EXISTS reporting.monthly_kpi (
    id BIGSERIAL PRIMARY KEY,
    report_month DATE NOT NULL,
    region TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value NUMERIC(15,4) DEFAULT 0,
    is_final BOOLEAN DEFAULT FALSE,
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    raw_payload BYTEA,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (report_month, region, metric_name)
);

-- Step 2: Upsert KPI values with ON CONFLICT and RETURNING
INSERT INTO reporting.monthly_kpi
    (report_month, region, metric_name, metric_value, is_final, tags, metadata)
SELECT
    date_trunc('month', o.order_date)::DATE AS report_month,
    COALESCE(c.region, 'UNKNOWN') AS region,
    'TOTAL_REVENUE' AS metric_name,
    SUM(o.total_amount)::NUMERIC(15,4) AS metric_value,
    CASE WHEN date_trunc('month', o.order_date) < date_trunc('month', CURRENT_DATE)
         THEN TRUE ELSE FALSE END AS is_final,
    ARRAY['revenue', 'orders']::TEXT[] AS tags,
    jsonb_build_object(
        'order_count', COUNT(*),
        'avg_order', ROUND(AVG(o.total_amount)::NUMERIC, 2),
        'max_order', MAX(o.total_amount)
    ) AS metadata
FROM sales.orders o
JOIN sales.customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= (CURRENT_DATE - INTERVAL '12 months')
  AND o.status NOT ILIKE '%cancel%'
GROUP BY date_trunc('month', o.order_date)::DATE, COALESCE(c.region, 'UNKNOWN')
ON CONFLICT (report_month, region, metric_name) DO UPDATE
SET metric_value = EXCLUDED.metric_value,
    is_final = EXCLUDED.is_final,
    metadata = EXCLUDED.metadata
RETURNING id, report_month, region, metric_value;

-- Step 3: Array and string operations
SELECT
    c.customer_id,
    c.full_name,
    ARRAY_AGG(DISTINCT p.category ORDER BY p.category) AS purchased_categories,
    ARRAY_AGG(o.order_id ORDER BY o.order_date DESC) FILTER (WHERE o.total_amount > 100) AS big_orders,
    STRING_TO_ARRAY(c.address_line, ', ') AS address_parts,
    ARRAY_LENGTH(ARRAY_AGG(DISTINCT p.category), 1) AS category_count,
    c.total_spent::TEXT || ' ' || c.currency AS spend_display
FROM sales.customers c
JOIN sales.orders o ON c.customer_id = o.customer_id
JOIN sales.order_items oi ON o.order_id = oi.order_id
JOIN catalog.products p ON oi.product_id = p.product_id
WHERE c.full_name ILIKE '%smith%'
   OR c.email ILIKE '%@example.%'
GROUP BY c.customer_id, c.full_name, c.address_line, c.total_spent, c.currency;

-- Step 4: Generate series for gap-fill reporting
SELECT
    gs.report_date,
    COALESCE(d.order_count, 0) AS order_count,
    COALESCE(d.revenue, 0.00::NUMERIC(15,2)) AS revenue,
    COALESCE(d.unique_customers, 0) AS unique_customers
FROM GENERATE_SERIES(
    (CURRENT_DATE - INTERVAL '90 days')::DATE,
    CURRENT_DATE::DATE,
    '1 day'::INTERVAL
) AS gs(report_date)
LEFT JOIN (
    SELECT
        order_date::DATE AS order_day,
        COUNT(*)::INTEGER AS order_count,
        SUM(total_amount)::NUMERIC(15,2) AS revenue,
        COUNT(DISTINCT customer_id)::INTEGER AS unique_customers
    FROM sales.orders
    WHERE order_date >= (CURRENT_DATE - INTERVAL '90 days')
    GROUP BY order_date::DATE
) d ON gs.report_date = d.order_day
ORDER BY gs.report_date;

-- Step 5: Window functions with PostgreSQL-style casting
SELECT
    o.order_id,
    o.customer_id,
    o.total_amount::NUMERIC(12,2) AS amount,
    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date DESC) AS order_rank,
    LAG(o.total_amount::NUMERIC(12,2)) OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS prev_amount,
    o.total_amount::NUMERIC(12,2) - LAG(o.total_amount::NUMERIC(12,2)) OVER (
        PARTITION BY o.customer_id ORDER BY o.order_date
    ) AS amount_change,
    FIRST_VALUE(o.order_date) OVER (
        PARTITION BY o.customer_id ORDER BY o.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_order_date,
    pg_catalog.pg_size_pretty(o.data_size::BIGINT) AS data_size_display
FROM sales.orders o
WHERE o.order_date >= (CURRENT_DATE - INTERVAL '6 months')::DATE;
