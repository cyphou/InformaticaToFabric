-- dbt model template for Informatica → Databricks migration
-- This template is used by run_dbt_migration.py as a reference pattern.
--
-- Layer: {{ layer }}  (staging | intermediate | marts)
-- Mapping: {{ mapping_name }}
-- Source: {{ source_table }}
-- Generated: {{ timestamp }}

{{ config(
    materialized="{{ materialization }}",
    tags=["{{ layer }}", "informatica"],
    schema="{{ target_schema }}"
) }}

{# ── Staging layer: direct source read ──────────────────── #}
{% if layer == 'staging' %}

SELECT *
FROM {{ source('{{ source_schema }}', '{{ source_table }}') }}

{# ── Intermediate layer: transformation logic ───────────── #}
{% elif layer == 'intermediate' %}

WITH source AS (
    SELECT * FROM {{ ref('stg_{{ model_name }}') }}
)

{# Expression transforms: derived columns #}
, expressions AS (
    SELECT
        *
        -- , CONCAT(first_name, ' ', last_name) AS full_name
        -- , COALESCE(status, 'UNKNOWN') AS status_clean
    FROM source
)

{# Filter transforms: WHERE clauses #}
, filtered AS (
    SELECT * FROM expressions
    -- WHERE is_active = TRUE
)

{# Lookup transforms: LEFT JOINs #}
, with_lookups AS (
    SELECT
        f.*
        -- , lkp.dimension_value
    FROM filtered f
    -- LEFT JOIN {{ ref('dim_lookup') }} lkp ON f.key = lkp.key
)

SELECT * FROM with_lookups

{# ── Marts layer: final business model ─────────────────── #}
{% elif layer == 'marts' %}

SELECT * FROM {{ ref('int_{{ model_name }}') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}

{% endif %}
