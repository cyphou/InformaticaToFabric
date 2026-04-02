-- Staging model for Informatica mapping: m_sync_accounts
-- Source: src_accounts
-- Complexity: Simple
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["staging", "informatica"], schema="staging") }}

SELECT *
FROM {{ source('bronze', 'src_accounts') }}