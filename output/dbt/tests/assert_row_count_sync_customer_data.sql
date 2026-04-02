-- Row count validation for SYNC_CUSTOMER_DATA
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{ ref('mart_sync_customer_data') }}
HAVING COUNT(*) = 0
