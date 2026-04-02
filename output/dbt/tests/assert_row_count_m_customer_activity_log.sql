-- Row count validation for m_customer_activity_log
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{ ref('mart_m_customer_activity_log') }}
HAVING COUNT(*) = 0
