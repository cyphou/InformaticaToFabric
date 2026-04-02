-- Mart model for: SYNC_CUSTOMER_DATA
-- Target: Lakehouse_Silver
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="table", tags=["marts", "informatica"], schema="silver") }}

SELECT * FROM {{ ref('int_sync_customer_data') }}