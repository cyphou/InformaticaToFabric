-- ============================================================================
-- Converted from: C:\Users\pidoudet\OneDrive - Microsoft\Boulot\PBI SME\OracleToPostgre\InformaticaToDBFabric\input\sql\SP_DB2_INVENTORY_REFRESH.sql
-- DB Type: DB2
-- Conversion: DB2 → Spark SQL
-- Date: 2026-04-07
-- Agent: sql-migration (automated)
-- ============================================================================
-- Review all TODO comments before deploying to Fabric.
-- ============================================================================

-- ============================================================
-- Stored Procedure: SP_DB2_INVENTORY_REFRESH
-- Source DB: DB2 (z/OS or LUW)
-- Purpose: Refresh inventory snapshot with DB2-specific SQL
-- Tests: FETCH FIRST, VALUE, current_date(), current_timestamp(),
--        RRN, DAYS, DAYOFWEEK, -- WITH UR removed (Spark has no isolation levels in SQL), CONCAT, DECIMAL,
--        GRAPHIC, VARGRAPHIC, OPTIMIZE FOR
-- ============================================================

-- Step 1: Get current inventory snapshot with isolation
SELECT
    i.product_id,
    COALESCE(i.product_name, 'UNKNOWN') AS product_name,
    COALESCE(i.category, 'UNCATEGORIZED') AS category,
    i.warehouse_id,
    i.quantity_on_hand,
    DECIMAL(i.unit_cost, 10, 2) AS unit_cost,
    DECIMAL(i.quantity_on_hand * i.unit_cost, 15, 2) AS inventory_value,
    monotonically_increasing_id()  -- RRN(i) converted AS row_sequence,
    current_date() AS snapshot_date,
    current_timestamp() AS snapshot_ts,
    DATEDIFF(current_date(), current_date()) - DATEDIFF(current_date(), i.last_restock_date) AS days_since_restock,
    DAYOFWEEK(i.last_restock_date) AS restock_weekday,
    CONCAT(CHAR(i.warehouse_id), CONCAT('-', i.product_id)) AS composite_key
FROM INVENTORY.STOCK_LEVELS i
WHERE i.quantity_on_hand > 0
  AND i.status = 'ACTIVE'
LIMIT 5000
-- WITH UR removed (Spark has no isolation levels in SQL)
;

-- Step 2: Low stock alert query
SELECT
    product_id,
    product_name,
    quantity_on_hand,
    COALESCE(reorder_point, 10) AS reorder_point,
    CASE
        WHEN quantity_on_hand <= COALESCE(reorder_point, 10) * 0.25 THEN 'CRITICAL'
        WHEN quantity_on_hand <= COALESCE(reorder_point, 10) * 0.50 THEN 'LOW'
        WHEN quantity_on_hand <= COALESCE(reorder_point, 10) THEN 'REORDER'
        ELSE 'OK'
    END AS stock_status,
    current_timestamp() AS alert_ts
FROM INVENTORY.STOCK_LEVELS
WHERE quantity_on_hand <= COALESCE(reorder_point, 10)
  AND status = 'ACTIVE'
LIMIT 200
-- WITH CS removed;

-- Step 3: Warehouse summary with GRAPHIC columns
SELECT
    w.warehouse_id,
    CAST(w.warehouse_name AS STRING) AS warehouse_display,
    CAST(w.city AS STRING) AS city_display,
    COUNT(*) AS total_products,
    SUM(DECIMAL(s.quantity_on_hand * s.unit_cost, 15, 2)) AS total_value,
    current_date() AS report_date
FROM INVENTORY.WAREHOUSES w
JOIN INVENTORY.STOCK_LEVELS s
    ON w.warehouse_id = s.warehouse_id
    AND s.status = 'ACTIVE'
GROUP BY w.warehouse_id, w.warehouse_name, w.city
LIMIT 100
-- WITH UR removed (Spark has no isolation levels in SQL);

-- Step 4: Insert snapshot (-- WITH RR removed isolation)
INSERT INTO INVENTORY.DAILY_SNAPSHOTS (
    snapshot_date,
    warehouse_id,
    product_count,
    total_value,
    created_ts
)
SELECT
    current_date(),
    warehouse_id,
    COUNT(*),
    SUM(DECIMAL(quantity_on_hand * unit_cost, 15, 2)),
    current_timestamp()
FROM INVENTORY.STOCK_LEVELS
WHERE status = 'ACTIVE'
GROUP BY warehouse_id
-- WITH RR removed;
