-- Staging model for Informatica mapping: m_customer_activity_log
-- Source: src_kafka_events
-- Complexity: Simple
-- Generated: 2026-04-02 12:27 UTC

{{ config(materialized="view", tags=["staging", "informatica"], schema="staging") }}

SELECT *
FROM {{ source('bronze', 'src_kafka_events') }}