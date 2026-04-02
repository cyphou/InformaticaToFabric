-- Intermediate model for: MI_BULK_LOAD_PRODUCTS
-- Transforms: SQ → TGT
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["intermediate", "informatica"], schema="intermediate") }}

WITH source AS (
    SELECT * FROM {{ ref('stg_mi_bulk_load_products') }}
)


SELECT * FROM source