-- Intermediate model for: SYNC_CUSTOMER_DATA
-- Transforms: SQ → TGT
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["intermediate", "informatica"], schema="intermediate") }}

WITH source AS (
    SELECT * FROM {{ ref('stg_sync_customer_data') }}
)


SELECT * FROM source