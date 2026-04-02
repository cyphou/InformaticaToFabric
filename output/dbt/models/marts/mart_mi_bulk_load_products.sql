-- Mart model for: MI_BULK_LOAD_PRODUCTS
-- Target: Lakehouse_Bronze
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="table", tags=["marts", "informatica"], schema="silver") }}

SELECT * FROM {{ ref('int_mi_bulk_load_products') }}