-- Staging model for Informatica mapping: m_inventory_snapshot
-- Source: src_silver_inventory
-- Complexity: Simple
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["staging", "informatica"], schema="staging") }}

SELECT *
FROM {{ source('bronze', 'src_silver_inventory') }}