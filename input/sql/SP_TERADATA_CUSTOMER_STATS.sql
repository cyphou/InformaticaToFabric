-- ============================================================
-- Stored Procedure: SP_TERADATA_CUSTOMER_STATS
-- Source DB: Teradata
-- Purpose: Compute customer statistics with Teradata-specific SQL
-- Tests: QUALIFY, SAMPLE, VOLATILE TABLE, SEL, COLLECT STATISTICS,
--        FORMAT, CASESPECIFIC, ZEROIFNULL, NULLIFZERO, TITLE,
--        MULTISET TABLE, BYTEINT, HASHROW
-- ============================================================

-- Step 1: Create volatile staging table for top customers
CREATE VOLATILE TABLE vt_top_customers AS (
    SEL
        c.customer_id,
        c.customer_name (CASESPECIFIC),
        c.region_code (TITLE 'Region'),
        ZEROIFNULL(SUM(o.order_total)) AS total_spend,
        NULLIFZERO(COUNT(o.order_id)) AS order_count,
        o.order_date (FORMAT 'YYYY-MM-DD') AS last_order_date
    FROM CUSTOMER_DB.customers c
    LEFT JOIN ORDER_DB.orders o
        ON c.customer_id = o.customer_id
        AND o.order_date >= DATE '2025-01-01'
    WHERE c.status = 'ACTIVE'
    GROUP BY 1, 2, 3, 6
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.region_code
        ORDER BY SUM(o.order_total) DESC
    ) <= 100
) WITH DATA
PRIMARY INDEX (customer_id)
ON COMMIT PRESERVE ROWS;

-- Step 2: Collect statistics on the volatile table
COLLECT STATISTICS ON vt_top_customers COLUMN (customer_id);
COLLECT STATISTICS ON vt_top_customers COLUMN (region_code);

-- Step 3: Create analytics summary in a SET TABLE
CREATE MULTISET TABLE ANALYTICS_DB.customer_region_summary AS (
    SEL
        region_code,
        COUNT(*) AS customer_count,
        SUM(total_spend) AS region_total_spend,
        AVG(total_spend) AS avg_spend,
        CAST(CAST(SUM(total_spend) AS FORMAT '999,999,999.99') AS VARCHAR(20)) AS formatted_total,
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
SEL *
FROM ANALYTICS_DB.customer_region_summary
SAMPLE 10;

-- Step 5: Customer detail with ranking
SEL
    customer_id,
    customer_name,
    total_spend,
    HASHROW(customer_id, customer_name) AS hash_key,
    RANK() OVER (ORDER BY total_spend DESC) AS spend_rank,
    total_spend - LAG(total_spend, 1, 0) OVER (ORDER BY total_spend DESC) AS spend_gap
FROM vt_top_customers
WHERE order_count > 0
QUALIFY DENSE_RANK() OVER (ORDER BY total_spend DESC) <= 50
ORDER BY spend_rank;

-- Step 6: Cleanup
DROP TABLE vt_top_customers;
