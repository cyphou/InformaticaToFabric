-- Staging model for Informatica mapping: m_load_contacts
-- Source: src_sf_contacts
-- Complexity: Medium
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["staging", "informatica"], schema="staging") }}

SELECT *
FROM {{ source('bronze', 'src_sf_contacts') }}