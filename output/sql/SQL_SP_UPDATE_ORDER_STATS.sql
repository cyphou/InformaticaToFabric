-- ============================================================================
-- SQL_SP_UPDATE_ORDER_STATS.sql
-- Converted from: input/sql/SP_UPDATE_ORDER_STATS.sql
-- Source: Oracle PL/SQL Stored Procedure -> Fabric Spark SQL / PySpark
-- Agent: sql-migration
-- Date: 2026-03-23
-- ============================================================================
-- CONVERSION NOTES:
--   Oracle MERGE INTO        -> Delta Lake MERGE INTO
--   DECODE()                 -> CASE WHEN ... END
--   NVL()                    -> COALESCE()
--   SYSDATE                  -> current_timestamp()
--   TRUNC(SYSDATE)           -> current_date()
--   TO_CHAR(date, fmt)       -> date_format(date, fmt)
--   DBMS_OUTPUT.PUT_LINE     -> print() in PySpark notebook wrapper
--   EXCEPTION WHEN           -> try/except in PySpark notebook wrapper
--   SQL%ROWCOUNT             -> Captured via Delta MERGE output metrics
-- ============================================================================

-- ============================================================================
-- PART 1: Spark SQL — Delta MERGE INTO
-- Original: MERGE INTO SALES.ORDER_STATS USING (subquery) ON ...
-- ============================================================================

MERGE INTO gold.order_stats AS tgt
USING (
    SELECT
        CUSTOMER_ID,
        COUNT(DISTINCT ORDER_ID)                     AS order_count,
        SUM(QUANTITY * UNIT_PRICE)                    AS total_revenue,
        AVG(QUANTITY * UNIT_PRICE)                    AS avg_order_value,
        MIN(ORDER_DATE)                               AS first_order_date,
        MAX(ORDER_DATE)                               AS last_order_date,
        -- Oracle: DECODE(COUNT(DISTINCT ORDER_ID), 0, 'Inactive', 1, 'New', 'Repeat')
        -- Spark:  CASE WHEN ... END
        CASE
            WHEN COUNT(DISTINCT ORDER_ID) = 0 THEN 'Inactive'
            WHEN COUNT(DISTINCT ORDER_ID) = 1 THEN 'New'
            ELSE 'Repeat'
        END                                           AS customer_segment
    FROM silver.fact_orders
    WHERE ORDER_STATUS != 'CANCELLED'
    GROUP BY CUSTOMER_ID
) AS src
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
    src.first_order_date, src.last_order_date, src.customer_segment,
    current_timestamp()
);


-- ============================================================================
-- PART 2: Daily revenue calculation
-- Original: SELECT NVL(SUM(QUANTITY * UNIT_PRICE), 0) INTO v_total_rev
--           FROM SALES.ORDERS WHERE TRUNC(ORDER_DATE) = TRUNC(v_load_date)
-- ============================================================================

SELECT
    COALESCE(SUM(QUANTITY * UNIT_PRICE), 0) AS daily_revenue
FROM silver.fact_orders
WHERE CAST(ORDER_DATE AS DATE) = current_date()
  AND ORDER_STATUS != 'CANCELLED';


-- ============================================================================
-- PART 3: ETL audit log
-- Original: INSERT INTO SALES.ETL_LOG VALUES (...)
-- ============================================================================

INSERT INTO bronze.etl_log (
    PROCEDURE_NAME, EXECUTION_DATE, ROWS_AFFECTED, DAILY_REVENUE, STATUS
) VALUES (
    'SQL_SP_UPDATE_ORDER_STATS',
    current_timestamp(),
    -1,
    -1,
    'SUCCESS'
);


-- ============================================================================
-- PART 4: PySpark Notebook Wrapper (for EXCEPTION/DBMS_OUTPUT handling)
-- Embed in a notebook cell as %%pyspark:
-- ============================================================================
--
-- try:
--     spark.sql(merge_sql)  # MERGE from Part 1
--
--     # Oracle: SQL%ROWCOUNT -> Delta history metrics
--     history = spark.sql("DESCRIBE HISTORY gold.order_stats LIMIT 1")
--     metrics = history.select("operationMetrics").first()
--     if metrics and metrics["operationMetrics"]:
--         op = metrics["operationMetrics"]
--         updated  = op.get("numTargetRowsUpdated", "0")
--         inserted = op.get("numTargetRowsInserted", "0")
--         print(f"Order stats updated: {updated} updated, {inserted} inserted")
--
--     # Daily revenue
--     rev = spark.sql("SELECT COALESCE(SUM(QUANTITY*UNIT_PRICE),0) FROM silver.fact_orders WHERE CAST(ORDER_DATE AS DATE)=current_date() AND ORDER_STATUS!='CANCELLED'").first()[0]
--     print(f"Daily revenue: ${rev:,.2f}")
--
-- except Exception as e:
--     spark.sql("INSERT INTO bronze.etl_log VALUES ('SQL_SP_UPDATE_ORDER_STATS', current_timestamp(), 0, 0, 'FAILED')")
--     print(f"Error: {e}")
--     raise
