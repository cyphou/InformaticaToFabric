-- Row count validation for M_LOAD_CUSTOMERS
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{ ref('mart_m_load_customers') }}
HAVING COUNT(*) = 0
