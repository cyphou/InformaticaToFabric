-- Mart model for: m_sync_accounts
-- Target: tgt_accounts
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="table", tags=["marts", "informatica"], schema="silver") }}

SELECT * FROM {{ ref('int_m_sync_accounts') }}