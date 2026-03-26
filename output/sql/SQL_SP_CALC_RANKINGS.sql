-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_CALC_RANKINGS.sql
-- DB Type: ORACLE
-- Conversion: Oracle → Spark SQL
-- Date: 2026-03-26
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_CALC_RANKINGS
-- Source DB: Oracle
-- Purpose: Calculate customer and product rankings using analytics
-- Tests: Oracle analytic functions (LEAD, LAG, DENSE_RANK, etc.)
-- ============================================================

CREATE OR REPLACE PROCEDURE SALES.SP_CALC_RANKINGS
IS
    v_run_date DATE := current_timestamp();
BEGIN
    -- Step 1: Customer rankings with analytic functions
    MERGE INTO SALES.CUSTOMER_RANKINGS tgt
    USING (
        SELECT
            CUSTOMER_ID,
            TOTAL_REVENUE,
            DENSE_RANK() OVER (ORDER BY TOTAL_REVENUE DESC) AS REVENUE_RANK,
            NTILE(4) OVER (ORDER BY TOTAL_REVENUE DESC) AS QUARTILE,
            ROW_NUMBER() OVER (ORDER BY TOTAL_REVENUE DESC) AS ROW_NUM,
            FIRST_VALUE(CUSTOMER_ID) OVER (
                ORDER BY TOTAL_REVENUE DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS TOP_CUSTOMER,
            LAST_VALUE(CUSTOMER_ID) OVER (
                ORDER BY TOTAL_REVENUE DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS BOTTOM_CUSTOMER,
            LAG(TOTAL_REVENUE, 1, 0) OVER (ORDER BY TOTAL_REVENUE DESC) AS PREV_REVENUE,
            LEAD(TOTAL_REVENUE, 1, 0) OVER (ORDER BY TOTAL_REVENUE DESC) AS NEXT_REVENUE,
            TOTAL_REVENUE - LAG(TOTAL_REVENUE, 1, 0) OVER (ORDER BY TOTAL_REVENUE DESC) AS REVENUE_GAP
        FROM (
            SELECT
                CUSTOMER_ID,
                COALESCE(SUM(QUANTITY * UNIT_PRICE), 0) AS TOTAL_REVENUE
            FROM SALES.ORDERS
            WHERE ORDER_STATUS != 'CANCELLED'
              AND ORDER_DATE >= ADD_MONTHS(v_run_date, -12)
            GROUP BY CUSTOMER_ID
        )
    ) src
    ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID)
    WHEN MATCHED THEN UPDATE SET
        tgt.REVENUE_RANK   = src.REVENUE_RANK,
        tgt.QUARTILE        = src.QUARTILE,
        tgt.TOTAL_REVENUE   = src.TOTAL_REVENUE,
        tgt.PREV_REVENUE    = src.PREV_REVENUE,
        tgt.NEXT_REVENUE    = src.NEXT_REVENUE,
        tgt.REVENUE_GAP     = src.REVENUE_GAP,
        tgt.LAST_CALCULATED = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        CUSTOMER_ID, REVENUE_RANK, QUARTILE, TOTAL_REVENUE,
        PREV_REVENUE, NEXT_REVENUE, REVENUE_GAP, LAST_CALCULATED
    ) VALUES (
        src.CUSTOMER_ID, src.REVENUE_RANK, src.QUARTILE, src.TOTAL_REVENUE,
        src.PREV_REVENUE, src.NEXT_REVENUE, src.REVENUE_GAP, current_timestamp()
    );

    -- Step 2: Product trending using LEAD/LAG for period-over-period
    INSERT INTO SALES.PRODUCT_TRENDS (
        PRODUCT_ID, PERIOD, PERIOD_REVENUE, PREV_PERIOD_REVENUE,
        NEXT_PERIOD_REVENUE, TREND_DIRECTION
    )
    SELECT
        PRODUCT_ID,
        PERIOD,
        PERIOD_REVENUE,
        LAG(PERIOD_REVENUE, 1) OVER (PARTITION BY PRODUCT_ID ORDER BY PERIOD) AS PREV_PERIOD_REVENUE,
        LEAD(PERIOD_REVENUE, 1) OVER (PARTITION BY PRODUCT_ID ORDER BY PERIOD) AS NEXT_PERIOD_REVENUE,
        CASE WHEN SIGN(PERIOD_REVENUE - LAG(PERIOD_REVENUE, 1, 0) OVER (PARTITION BY PRODUCT_ID ORDER BY PERIOD)) = 1 THEN 'UP' WHEN SIGN(PERIOD_REVENUE - LAG(PERIOD_REVENUE, 1, 0) OVER (PARTITION BY PRODUCT_ID ORDER BY PERIOD)) = -1 THEN 'DOWN' ELSE 'FLAT' END AS TREND_DIRECTION
    FROM (
        SELECT
            PRODUCT_ID,
            TO_CHAR(ORDER_DATE, 'YYYY-MM') AS PERIOD,
            SUM(QUANTITY * UNIT_PRICE) AS PERIOD_REVENUE
        FROM SALES.ORDERS
        WHERE ORDER_DATE >= ADD_MONTHS(current_timestamp(), -24)
        GROUP BY PRODUCT_ID, TO_CHAR(ORDER_DATE, 'YYYY-MM')
    );

    -- TODO: print('Rankings calculated at ' || TO_CHAR(current_timestamp() in PySpark notebook, 'yyyy-MM-dd HH:mm:ss'));

    COMMIT;
except Exception as e:  # PL/SQL EXCEPTION WHEN OTHERS
        ROLLBACK;
        -- TODO: print('Error: ' || SQLERRM) in PySpark notebook;
        raise  # Re-raise current exception
END SP_CALC_RANKINGS;
/
