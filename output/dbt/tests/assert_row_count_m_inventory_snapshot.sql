-- Row count validation for m_inventory_snapshot
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{ ref('mart_m_inventory_snapshot') }}
HAVING COUNT(*) = 0
