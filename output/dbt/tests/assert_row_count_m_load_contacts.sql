-- Row count validation for m_load_contacts
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{ ref('mart_m_load_contacts') }}
HAVING COUNT(*) = 0
