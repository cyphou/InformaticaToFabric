-- Intermediate model for: m_inventory_snapshot
-- Transforms: EXP → port
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["intermediate", "informatica"], schema="intermediate") }}

WITH source AS (
    SELECT * FROM {{ ref('stg_m_inventory_snapshot') }}
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

SELECT * FROM expressions_1