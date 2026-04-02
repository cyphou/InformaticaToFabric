-- Mart model for: m_load_contacts
-- Target: tgt_lh_contacts
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="table", tags=["marts", "informatica"], schema="silver") }}

SELECT * FROM {{ ref('int_m_load_contacts') }}