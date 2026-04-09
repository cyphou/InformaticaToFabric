-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_POSTGRESQL_REPORTING.sql
-- DB Type: POSTGRESQL
-- Conversion: PostgreSQL → Spark SQL
-- Date: 2026-04-08
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_POSTGRESQL_REPORTING
-- Source DB: PostgreSQL 15+
-- Purpose: Reporting queries with PostgreSQL-specific SQL
-- Tests: ::casts, LIKE  -- TODO: ILIKE → case-insensitive; use LOWER() on both sides, INT  -- TODO: auto-increment via monotonically_increasing_id()/BIGINT  -- TODO: auto-increment via monotonically_increasing_id(), STRING, BOOLEAN,
--        -- TODO: RETURNING clause not supported in Spark SQL;

-- Step 2: Upsert KPI values with -- TODO: ON CONFLICT (upsert) → convert to MERGE INTO; use LOWER() on both sides '%cancel%'
GROUP BY date_trunc('month', o.order_date)::DATE, COALESCE(c.region, 'UNKNOWN')
-- TODO: ON CONFLICT (upsert) → convert to MERGE INTO;

-- Step 3: Array and string operations
SELECT
    c.customer_id,
    c.full_name,
    COLLECT_LIST(DISTINCT p.category ORDER BY p.category) AS purchased_categories,
    COLLECT_LIST(o.order_id ORDER BY o.order_date DESC) FILTER (WHERE o.total_amount > 100) AS big_orders,
    SPLIT(c.address_line, ', ') AS address_parts,
    ARRAY_LENGTH(COLLECT_LIST(DISTINCT p.category), 1) AS category_count,
    c.CAST(total_spent AS STRING) || ' ' || c.currency AS spend_display
FROM sales.customers c
JOIN sales.orders o ON c.customer_id = o.customer_id
JOIN sales.order_items oi ON o.order_id = oi.order_id
JOIN catalog.products p ON oi.product_id = p.product_id
WHERE c.full_name LIKE  -- TODO: ILIKE → case-insensitive; use LOWER() on both sides '%smith%'
   OR c.email LIKE  -- TODO: ILIKE → case-insensitive; use LOWER() on both sides '%@example.%'
GROUP BY c.customer_id, c.full_name, c.address_line, c.total_spent, c.currency;

-- Step 4: Generate series for gap-fill reporting
SELECT
    gs.report_date,
    COALESCE(d.order_count, 0) AS order_count,
    COALESCE(d.revenue, 0.CAST(00 AS NUMERIC)(15,2)) AS revenue,
    COALESCE(d.unique_customers, 0) AS unique_customers
FROM SEQUENCE(
    (CURRENT_DATE - INTERVAL 90 days)  -- TODO: Replace with EXPLODE(SEQUENCE(...))::DATE,
    CAST(CURRENT_DATE AS DATE),
    '1 day'::INTERVAL
) AS gs(report_date)
LEFT JOIN (
    SELECT
        CAST(order_date AS DATE) AS order_day,
        COUNT(*)::INTEGER AS order_count,
        SUM(total_amount)::NUMERIC(15,2) AS revenue,
        COUNT(DISTINCT customer_id)::INTEGER AS unique_customers
    FROM sales.orders
    WHERE order_date >= (CURRENT_DATE - INTERVAL 90 days)
    GROUP BY CAST(order_date AS DATE)
) d ON gs.report_date = d.order_day
ORDER BY gs.report_date;

-- Step 5: Window functions with PostgreSQL-style casting
SELECT
    o.order_id,
    o.customer_id,
    o.CAST(total_amount AS NUMERIC)(12,2) AS amount,
    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date DESC) AS order_rank,
    LAG(o.CAST(total_amount AS NUMERIC)(12,2)) OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS prev_amount,
    o.CAST(total_amount AS NUMERIC)(12,2) - LAG(o.CAST(total_amount AS NUMERIC)(12,2)) OVER (
        PARTITION BY o.customer_id ORDER BY o.order_date
    ) AS amount_change,
    FIRST_VALUE(o.order_date) OVER (
        PARTITION BY o.customer_id ORDER BY o.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_order_date,
    -- TODO: Replace pg_catalog reference(o.CAST(data_size AS BIGINT)) AS data_size_display
FROM sales.orders o
WHERE o.order_date >= (CURRENT_DATE - INTERVAL 6 months)::DATE;
