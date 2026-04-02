-- Staging model for Informatica mapping: M_LOAD_CUSTOMERS
-- Source: Oracle.SALES.CUSTOMERS
-- Complexity: Simple
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["staging", "informatica"], schema="staging") }}

SELECT *
FROM {{ source('sales', 'customers') }}