-- Mart model for: M_LOAD_CUSTOMERS
-- Target: DIM_CUSTOMER
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="table", tags=["marts", "informatica"], schema="silver") }}

SELECT * FROM {{ ref('int_m_load_customers') }}