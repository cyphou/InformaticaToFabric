-- Mart model for: m_inventory_snapshot
-- Target: tgt_gold_inventory_history
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="table", tags=["marts", "informatica"], schema="silver") }}

SELECT * FROM {{ ref('int_m_inventory_snapshot') }}