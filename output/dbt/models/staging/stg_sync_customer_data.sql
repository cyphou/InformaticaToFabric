-- Staging model for Informatica mapping: SYNC_CUSTOMER_DATA
-- Source: Oracle_CRM
-- Complexity: Simple
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["staging", "informatica"], schema="staging") }}

SELECT *
FROM {{ source('bronze', 'oracle_crm') }}