-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_UPDATE_ORDER_STATS.sql
-- DB Type: ORACLE
-- Conversion: Oracle → Spark SQL
-- Date: 2026-04-07
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_UPDATE_ORDER_STATS
-- Called by: WF_DAILY_SALES_LOAD → S_M_LOAD_ORDERS (Post SQL)
-- Purpose: Recalculate order statistics after daily load
-- Database: Oracle (SALES schema)
-- ============================================================

CREATE OR REPLACE PROCEDURE SALES.SP_UPDATE_ORDER_STATS
IS
    v_load_date    DATE := current_timestamp();
    v_row_count    DECIMAL := 0;
    v_total_rev    DECIMAL := 0;
BEGIN
    -- Step 1: Refresh materialized order stats
    MERGE INTO SALES.ORDER_STATS tgt
    USING (
        SELECT
            CUSTOMER_ID,
            COUNT(DISTINCT ORDER_ID)           AS order_count,
            SUM(QUANTITY * UNIT_PRICE)          AS total_revenue,
            AVG(QUANTITY * UNIT_PRICE)          AS avg_order_value,
            MIN(ORDER_DATE)                     AS first_order_date,
            MAX(ORDER_DATE)                     AS last_order_date,
            CASE WHEN COUNT(DISTINCT ORDER_ID) = 0 THEN 'Inactive' WHEN COUNT(DISTINCT ORDER_ID) = 1 THEN 'New' ELSE 'Repeat' END                                   AS customer_segment
        FROM SALES.ORDERS
        WHERE ORDER_STATUS != 'CANCELLED'
        GROUP BY CUSTOMER_ID
    ) src
    ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID)
    WHEN MATCHED THEN UPDATE SET
        tgt.ORDER_COUNT      = src.order_count,
        tgt.TOTAL_REVENUE    = src.total_revenue,
        tgt.AVG_ORDER_VALUE  = src.avg_order_value,
        tgt.FIRST_ORDER_DATE = src.first_order_date,
        tgt.LAST_ORDER_DATE  = src.last_order_date,
        tgt.CUSTOMER_SEGMENT = src.customer_segment,
        tgt.LAST_UPDATED     = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        CUSTOMER_ID, ORDER_COUNT, TOTAL_REVENUE, AVG_ORDER_VALUE,
        FIRST_ORDER_DATE, LAST_ORDER_DATE, CUSTOMER_SEGMENT, LAST_UPDATED
    ) VALUES (
        src.CUSTOMER_ID, src.order_count, src.total_revenue, src.avg_order_value,
        src.first_order_date, src.last_order_date, src.customer_segment, current_timestamp()
    );

    v_row_count := SQL%ROWCOUNT;

    -- Step 2: Calculate total revenue for the day
    SELECT COALESCE(SUM(QUANTITY * UNIT_PRICE), 0)
    INTO v_total_rev
    FROM SALES.ORDERS
    WHERE date_trunc('day', ORDER_DATE) = date_trunc('day', v_load_date)
      AND ORDER_STATUS != 'CANCELLED';

    -- Step 3: Log the stats update
    INSERT INTO SALES.ETL_LOG (
        PROCEDURE_NAME, EXECUTION_DATE, ROWS_AFFECTED, DAILY_REVENUE, STATUS
    ) VALUES (
        'SP_UPDATE_ORDER_STATS', v_load_date, v_row_count, v_total_rev, 'SUCCESS'
    );

    COMMIT;

    -- TODO: print('Order stats updated: ' || v_row_count || ' rows, Revenue: ' || TO_CHAR(v_total_rev, 'FM$999,999,999.00') in PySpark notebook);

except Exception as e:  # PL/SQL EXCEPTION WHEN OTHERS
        ROLLBACK;
        INSERT INTO SALES.ETL_LOG (
            PROCEDURE_NAME, EXECUTION_DATE, ROWS_AFFECTED, DAILY_REVENUE, STATUS, ERROR_MESSAGE
        ) VALUES (
            'SP_UPDATE_ORDER_STATS', current_timestamp(), 0, 0, 'FAILED', SUBSTRING(SQLERRM, 1, 500)
        );
        COMMIT;
        raise  # Re-raise current exception
END SP_UPDATE_ORDER_STATS;
/
