-- Row count validation for m_sync_accounts
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{ ref('mart_m_sync_accounts') }}
HAVING COUNT(*) = 0
