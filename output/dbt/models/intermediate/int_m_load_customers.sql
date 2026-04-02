-- Intermediate model for: M_LOAD_CUSTOMERS
-- Transforms: SQ → EXP → FIL
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["intermediate", "informatica"], schema="intermediate") }}

WITH source AS (
    SELECT * FROM {{ ref('stg_m_load_customers') }}
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

SELECT * FROM filtered_2