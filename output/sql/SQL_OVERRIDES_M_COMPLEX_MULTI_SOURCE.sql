-- ============================================================================
-- SQL Overrides for mapping: M_COMPLEX_MULTI_SOURCE
-- DB Type: ORACLE → Spark SQL
-- Date: 2026-04-02
-- Agent: sql-migration (automated)
-- ============================================================================

-- Override #1: Sql Query
-- Original:
--   SELECT TXN_ID, ACCOUNT_ID, TXN_TYPE, AMOUNT, CURRENCY, TXN_DATE, DECODE(STATUS, 'C', 'COMPLETED', 'P', 'PENDING', 'F', 'FAILED', STATUS) AS STATUS, NVL(CHANNEL, 'UNKNOWN') AS CHANNEL, TAGS FROM FINANCE.TRANSACTIONS WHERE TXN_DATE >= ADD_MONTHS(SYSDATE, -3)
-- Converted:
SELECT TXN_ID, ACCOUNT_ID, TXN_TYPE, AMOUNT, CURRENCY, TXN_DATE, CASE WHEN STATUS = 'C' THEN 'COMPLETED' WHEN STATUS = 'P' THEN 'PENDING' WHEN STATUS = 'F' THEN 'FAILED' ELSE STATUS END AS STATUS, COALESCE(CHANNEL, 'UNKNOWN') AS CHANNEL, TAGS FROM FINANCE.TRANSACTIONS WHERE TXN_DATE >= ADD_MONTHS(current_timestamp(), -3)

-- Override #2: Source Filter
-- Original:
--   STATUS != 'X'
-- Converted:
STATUS != 'X'

-- Override #3: Lookup Sql Override
-- Original:
--   SELECT ACCOUNT_ID AS BLOCKED_ACCOUNT_ID, NVL(REASON, 'Unspecified') AS BLOCK_REASON FROM COMPLIANCE.FRAUD_BLACKLIST WHERE ACTIVE_FLAG = 'Y' AND EXPIRY_DATE > SYSDATE
-- Converted:
SELECT ACCOUNT_ID AS BLOCKED_ACCOUNT_ID, COALESCE(REASON, 'Unspecified') AS BLOCK_REASON FROM COMPLIANCE.FRAUD_BLACKLIST WHERE ACTIVE_FLAG = 'Y' AND EXPIRY_DATE > current_timestamp()
