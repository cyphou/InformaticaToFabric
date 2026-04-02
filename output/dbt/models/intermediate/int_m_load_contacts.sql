-- Intermediate model for: m_load_contacts
-- Transforms: EXP → port → FIL → LKP → AGG → DM
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["intermediate", "informatica"], schema="intermediate") }}

WITH source AS (
    SELECT * FROM {{ ref('stg_m_load_contacts') }}
)

, expressions_1 AS (
    -- Expression transform: derived columns
    -- TODO: Add CASE/COALESCE/CONCAT/CAST expressions
    SELECT
        *
        -- , CONCAT(first_name, ' ', last_name) AS full_name
        -- , COALESCE(email, 'unknown@example.com') AS email_clean
    FROM source
)
, filtered_2 AS (
    -- Filter transform
    -- TODO: Add WHERE condition from mapping
    SELECT * FROM expressions_1
    -- WHERE status = 'ACTIVE'
)
, with_lookup_3 AS (
    -- Lookup transform → LEFT JOIN
    SELECT
        s.*
        -- , lkp.lookup_value
    FROM filtered_2 s
    -- LEFT JOIN {{ ref('dim_lookup') }} lkp ON s.key = lkp.key
)
, aggregated_4 AS (
    -- Aggregator transform
    -- TODO: Add GROUP BY + aggregate functions
    SELECT
        -- group_key,
        -- SUM(amount) AS total_amount,
        -- COUNT(*) AS row_count
        *
    FROM with_lookup_3
    -- GROUP BY group_key
)
, masked_5 AS (
    -- Data masking transform
    SELECT
        *
        -- , MD5(CAST(sensitive_col AS STRING)) AS sensitive_col_hash
    FROM aggregated_4
)

SELECT * FROM masked_5