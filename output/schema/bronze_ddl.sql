-- Table: main.bronze.tgt_bronze_cdc_events (from mapping m_cdc_order_pipeline)
CREATE TABLE IF NOT EXISTS main.bronze.tgt_bronze_cdc_events (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_cdc_order_pipeline'
;

-- Table: main.bronze.tgt_bronze_events (from mapping m_customer_activity_log)
CREATE TABLE IF NOT EXISTS main.bronze.tgt_bronze_events (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_customer_activity_log'
;