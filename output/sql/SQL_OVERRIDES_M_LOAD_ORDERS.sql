-- ============================================================================
-- SQL_OVERRIDES_M_LOAD_ORDERS.sql
-- Converted from: Informatica mapping M_LOAD_ORDERS inline SQL overrides
-- Agent: sql-migration
-- Date: 2026-03-23
-- ============================================================================


-- ============================================================================
-- OVERRIDE 1: Source Qualifier SQL Override (SQ_ORDERS)
-- Original Oracle:
--   SELECT ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE,
--          NVL(DISCOUNT_PCT, 0) AS DISCOUNT_PCT, ORDER_DATE, ORDER_STATUS, CHANNEL
--   FROM SALES.ORDERS
--   WHERE ORDER_DATE >= TO_DATE('$$LOAD_DATE', 'YYYY-MM-DD')
--     AND ORDER_STATUS != 'CANCELLED'
--
-- Conversions:
--   NVL(DISCOUNT_PCT, 0)              -> COALESCE(DISCOUNT_PCT, 0)
--   TO_DATE('$$LOAD_DATE', 'YYYY-MM-DD') -> to_date('${LOAD_DATE}', 'yyyy-MM-dd')
--   SALES.ORDERS                      -> bronze.orders
-- ============================================================================

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    QUANTITY,
    UNIT_PRICE,
    COALESCE(DISCOUNT_PCT, 0)   AS DISCOUNT_PCT,
    ORDER_DATE,
    ORDER_STATUS,
    CHANNEL
FROM bronze.orders
WHERE ORDER_DATE >= to_date('${LOAD_DATE}', 'yyyy-MM-dd')
  AND ORDER_STATUS != 'CANCELLED';


-- ============================================================================
-- OVERRIDE 2: Lookup SQL Override (LKP_PRODUCTS)
-- Original Oracle:
--   SELECT PRODUCT_ID, NVL(PRODUCT_NAME, 'Unknown') AS PRODUCT_NAME,
--          NVL(CATEGORY, 'Uncategorized') AS CATEGORY
--   FROM SALES.PRODUCTS
--
-- Conversions:
--   NVL(col, default)  -> COALESCE(col, default)
--   SALES.PRODUCTS     -> bronze.products
-- ============================================================================

SELECT
    PRODUCT_ID,
    COALESCE(PRODUCT_NAME, 'Unknown')       AS PRODUCT_NAME,
    COALESCE(CATEGORY, 'Uncategorized')     AS CATEGORY
FROM bronze.products;


-- ============================================================================
-- OVERRIDE 3: Pre-Session SQL (S_M_LOAD_ORDERS)
-- Original Oracle:
--   DELETE FROM SALES.FACT_ORDERS_STAGING
--   WHERE ORDER_DATE = TO_DATE('$$LOAD_DATE','YYYY-MM-DD')
--
-- Conversions:
--   TO_DATE('$$LOAD_DATE','YYYY-MM-DD') -> to_date('${LOAD_DATE}', 'yyyy-MM-dd')
--   SALES.FACT_ORDERS_STAGING           -> silver.fact_orders_staging
-- ============================================================================

DELETE FROM silver.fact_orders_staging
WHERE ORDER_DATE = to_date('${LOAD_DATE}', 'yyyy-MM-dd');


-- ============================================================================
-- OVERRIDE 4: Post-Session SQL (S_M_LOAD_ORDERS)
-- Original Oracle:
--   EXEC SALES.SP_UPDATE_ORDER_STATS;
--
-- Spark SQL has no EXEC equivalent. Options:
--   (a) Inline the MERGE from SQL_SP_UPDATE_ORDER_STATS.sql as a %%sql cell
--   (b) Call a utility notebook via %run or pipeline Notebook activity
-- ============================================================================

-- TODO: Wire post-session logic via pipeline activity or %run
-- See output/sql/SQL_SP_UPDATE_ORDER_STATS.sql for converted MERGE
