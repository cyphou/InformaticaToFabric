-- ============================================================
-- Stored Procedure: SP_DB2_INVENTORY_REFRESH
-- Source DB: DB2 (z/OS or LUW)
-- Purpose: Refresh inventory snapshot with DB2-specific SQL
-- Tests: FETCH FIRST, VALUE, CURRENT DATE, CURRENT TIMESTAMP,
--        RRN, DAYS, DAYOFWEEK, WITH UR, CONCAT, DECIMAL,
--        GRAPHIC, VARGRAPHIC, OPTIMIZE FOR
-- ============================================================

-- Step 1: Get current inventory snapshot with isolation
SELECT
    i.product_id,
    VALUE(i.product_name, 'UNKNOWN') AS product_name,
    VALUE(i.category, 'UNCATEGORIZED') AS category,
    i.warehouse_id,
    i.quantity_on_hand,
    DECIMAL(i.unit_cost, 10, 2) AS unit_cost,
    DECIMAL(i.quantity_on_hand * i.unit_cost, 15, 2) AS inventory_value,
    RRN(i) AS row_sequence,
    CURRENT DATE AS snapshot_date,
    CURRENT TIMESTAMP AS snapshot_ts,
    DAYS(CURRENT DATE) - DAYS(i.last_restock_date) AS days_since_restock,
    DAYOFWEEK(i.last_restock_date) AS restock_weekday,
    CONCAT(CHAR(i.warehouse_id), CONCAT('-', i.product_id)) AS composite_key
FROM INVENTORY.STOCK_LEVELS i
WHERE i.quantity_on_hand > 0
  AND i.status = 'ACTIVE'
FETCH FIRST 5000 ROWS ONLY
WITH UR
OPTIMIZE FOR 1000 ROWS;

-- Step 2: Low stock alert query
SELECT
    product_id,
    product_name,
    quantity_on_hand,
    VALUE(reorder_point, 10) AS reorder_point,
    CASE
        WHEN quantity_on_hand <= VALUE(reorder_point, 10) * 0.25 THEN 'CRITICAL'
        WHEN quantity_on_hand <= VALUE(reorder_point, 10) * 0.50 THEN 'LOW'
        WHEN quantity_on_hand <= VALUE(reorder_point, 10) THEN 'REORDER'
        ELSE 'OK'
    END AS stock_status,
    CURRENT TIMESTAMP AS alert_ts
FROM INVENTORY.STOCK_LEVELS
WHERE quantity_on_hand <= VALUE(reorder_point, 10)
  AND status = 'ACTIVE'
FETCH FIRST 200 ROWS ONLY
WITH CS;

-- Step 3: Warehouse summary with GRAPHIC columns
SELECT
    w.warehouse_id,
    CAST(w.warehouse_name AS GRAPHIC(40)) AS warehouse_display,
    CAST(w.city AS VARGRAPHIC(100)) AS city_display,
    COUNT(*) AS total_products,
    SUM(DECIMAL(s.quantity_on_hand * s.unit_cost, 15, 2)) AS total_value,
    CURRENT DATE AS report_date
FROM INVENTORY.WAREHOUSES w
JOIN INVENTORY.STOCK_LEVELS s
    ON w.warehouse_id = s.warehouse_id
    AND s.status = 'ACTIVE'
GROUP BY w.warehouse_id, w.warehouse_name, w.city
FETCH FIRST 100 ROWS ONLY
WITH UR;

-- Step 4: Insert snapshot (with RR isolation)
INSERT INTO INVENTORY.DAILY_SNAPSHOTS (
    snapshot_date,
    warehouse_id,
    product_count,
    total_value,
    created_ts
)
SELECT
    CURRENT DATE,
    warehouse_id,
    COUNT(*),
    SUM(DECIMAL(quantity_on_hand * unit_cost, 15, 2)),
    CURRENT TIMESTAMP
FROM INVENTORY.STOCK_LEVELS
WHERE status = 'ACTIVE'
GROUP BY warehouse_id
WITH RR;
