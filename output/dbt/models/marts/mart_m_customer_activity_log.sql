-- Mart model for: m_customer_activity_log
-- Target: tgt_bronze_events
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="table", tags=["marts", "informatica"], schema="silver") }}

SELECT * FROM {{ ref('int_m_customer_activity_log') }}