-- ============================================================================
-- SQL Overrides for mapping: M_LOAD_ORDERS
-- DB Type: ORACLE → Spark SQL
-- Date: 2026-03-26
-- Agent: sql-migration (automated)
-- ============================================================================

-- Override #1: Sql Query
-- Original:
--   SELECT ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, NVL(DISCOUNT_PCT, 0) AS DISCOUNT_PCT, ORDER_DATE, ORDER_STATUS, CHANNEL FROM SALES.ORDERS WHERE ORDER_DATE >= TO_DATE('$$LOAD_DATE', 'YYYY-MM-DD') AND ORDER_STATUS != 'CANCELLED'
-- Converted:
SELECT ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, COALESCE(DISCOUNT_PCT, 0) AS DISCOUNT_PCT, ORDER_DATE, ORDER_STATUS, CHANNEL FROM SALES.ORDERS WHERE ORDER_DATE >= TO_DATE('$$LOAD_DATE', 'yyyy-MM-dd') AND ORDER_STATUS != 'CANCELLED'

-- Override #2: Lookup Sql Override
-- Original:
--   SELECT PRODUCT_ID, NVL(PRODUCT_NAME, 'Unknown') AS PRODUCT_NAME, NVL(CATEGORY, 'Uncategorized') AS CATEGORY FROM SALES.PRODUCTS
-- Converted:
SELECT PRODUCT_ID, COALESCE(PRODUCT_NAME, 'Unknown') AS PRODUCT_NAME, COALESCE(CATEGORY, 'Uncategorized') AS CATEGORY FROM SALES.PRODUCTS
