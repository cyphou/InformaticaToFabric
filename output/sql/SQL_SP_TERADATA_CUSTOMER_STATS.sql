-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_TERADATA_CUSTOMER_STATS.sql
-- DB Type: TERADATA
-- Conversion: Teradata → Spark SQL
-- Date: 2026-03-24
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_TERADATA_CUSTOMER_STATS
-- Source DB: Teradata
-- Purpose: Compute customer statistics with Teradata-specific SQL
-- Tests: -- TODO: QUALIFY clause → use subquery with ROW_NUMBER() and filter, SAMPLE, VOLATILE TABLE, SEL, -- TODO: Replace COLLECT STATISTICS with ANALYZE TABLE;

-- Step 2: -- TODO: Replace COLLECT STATISTICS with ANALYZE TABLE;
-- TODO: Replace COLLECT STATISTICS with ANALYZE TABLE;

-- Step 3: Create analytics summary in a SET TABLE
CREATE TABLE ANALYTICS_DB.customer_region_summary AS (
    SELECT region_code,
        COUNT(*) AS customer_count,
        SUM(total_spend) AS region_total_spend,
        AVG(total_spend) AS avg_spend,
        CAST(CAST(SUM(total_spend) AS -- FORMAT removed (use date_format in Spark)) AS VARCHAR(20)) AS formatted_total,
        CASE
            WHEN AVG(total_spend) > 10000 THEN 'PLATINUM'
            WHEN AVG(total_spend) > 5000 THEN 'GOLD'
            WHEN AVG(total_spend) > 1000 THEN 'SILVER'
            ELSE 'BRONZE'
        END AS tier
    FROM vt_top_customers
    GROUP BY region_code
) WITH DATA;

-- Step 4: Get a random sample for QA validation
SELECT *
FROM ANALYTICS_DB.customer_region_summary
TABLESAMPLE (10 ROWS);

-- Step 5: Customer detail with ranking
SELECT customer_id,
    customer_name,
    total_spend,
    HASHROW(customer_id, customer_name) AS hash_key,
    RANK() OVER (ORDER BY total_spend DESC) AS spend_rank,
    total_spend - LAG(total_spend, 1, 0) OVER (ORDER BY total_spend DESC) AS spend_gap
FROM vt_top_customers
WHERE order_count > 0
-- TODO: QUALIFY clause → use subquery with ROW_NUMBER() and filter DENSE_RANK() OVER (ORDER BY total_spend DESC) <= 50
ORDER BY spend_rank;

-- Step 6: Cleanup
DROP TABLE vt_top_customers;
