-- Row count validation for MI_BULK_LOAD_PRODUCTS
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{ ref('mart_mi_bulk_load_products') }}
HAVING COUNT(*) = 0
