-- Staging model for Informatica mapping: MI_BULK_LOAD_PRODUCTS
-- Source: S3_LANDING
-- Complexity: Simple
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["staging", "informatica"], schema="staging") }}

SELECT *
FROM {{ source('bronze', 's3_landing') }}