-- Table: main.silver.tgt_silver_orders (from mapping m_cdc_order_pipeline)
CREATE TABLE IF NOT EXISTS main.silver.tgt_silver_orders (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_cdc_order_pipeline'
;

-- Table: main.silver.tgt_silver_customer (from mapping m_customer_360)
CREATE TABLE IF NOT EXISTS main.silver.tgt_silver_customer (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_customer_360'
;

-- Table: main.silver.tgt_lh_contacts (from mapping m_load_contacts)
CREATE TABLE IF NOT EXISTS main.silver.tgt_lh_contacts (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_load_contacts'
;

-- Table: main.silver.tgt_accounts (from mapping m_sync_accounts)
CREATE TABLE IF NOT EXISTS main.silver.tgt_accounts (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_sync_accounts'
;

-- Table: main.silver.tgt_silver_inventory (from mapping m_realtime_inventory_scd2)
CREATE TABLE IF NOT EXISTS main.silver.tgt_silver_inventory (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_realtime_inventory_scd2'
;

-- Table: main.silver.tgt_alert_queue (from mapping m_realtime_inventory_scd2)
CREATE TABLE IF NOT EXISTS main.silver.tgt_alert_queue (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping m_realtime_inventory_scd2'
;

-- Table: main.silver.fact_txn_high (from mapping M_COMPLEX_MULTI_SOURCE)
CREATE TABLE IF NOT EXISTS main.silver.fact_txn_high (
    account_name STRING  -- from TXN_ID,
    amount_usd STRING  -- from TXN_ID,
    txn_date STRING  -- from TXN_ID,
    txn_id STRING  -- from TXN_ID,
    tag_value STRING  -- from TXN_ID,
    tag_index STRING  -- from TXN_ID,
    risk_category STRING  -- from TXN_ID,
    txn_rank STRING  -- from TXN_ID,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping M_COMPLEX_MULTI_SOURCE'
;

-- Table: main.silver.fact_txn_low (from mapping M_COMPLEX_MULTI_SOURCE)
CREATE TABLE IF NOT EXISTS main.silver.fact_txn_low (
    account_name STRING  -- from TXN_ID,
    amount_usd STRING  -- from TXN_ID,
    txn_date STRING  -- from TXN_ID,
    txn_id STRING  -- from TXN_ID,
    tag_value STRING  -- from TXN_ID,
    tag_index STRING  -- from TXN_ID,
    risk_category STRING  -- from TXN_ID,
    txn_rank STRING  -- from TXN_ID,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping M_COMPLEX_MULTI_SOURCE'
;

-- Table: main.silver.fact_txn_tags (from mapping M_COMPLEX_MULTI_SOURCE)
CREATE TABLE IF NOT EXISTS main.silver.fact_txn_tags (
    account_name STRING  -- from TXN_ID,
    amount_usd STRING  -- from TXN_ID,
    txn_date STRING  -- from TXN_ID,
    txn_id STRING  -- from TXN_ID,
    tag_value STRING  -- from TXN_ID,
    tag_index STRING  -- from TXN_ID,
    risk_category STRING  -- from TXN_ID,
    txn_rank STRING  -- from TXN_ID,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping M_COMPLEX_MULTI_SOURCE'
;

-- Table: main.silver.dim_customer (from mapping M_LOAD_CUSTOMERS)
CREATE TABLE IF NOT EXISTS main.silver.dim_customer (
    customer_id STRING  -- from CUSTOMER_ID,
    full_name STRING  -- from CUSTOMER_ID,
    email STRING  -- from CUSTOMER_ID,
    status_label STRING  -- from CUSTOMER_ID,
    credit_limit STRING  -- from CUSTOMER_ID,
    country_code STRING  -- from CUSTOMER_ID,
    etl_load_date STRING  -- from CUSTOMER_ID,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping M_LOAD_CUSTOMERS'
;

-- Table: main.silver.dim_employee (from mapping M_LOAD_EMPLOYEES)
CREATE TABLE IF NOT EXISTS main.silver.dim_employee (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping M_LOAD_EMPLOYEES'
;

-- Table: main.silver.fact_orders (from mapping M_LOAD_ORDERS)
CREATE TABLE IF NOT EXISTS main.silver.fact_orders (
    product_name STRING  -- from PRODUCT_ID,
    order_id STRING  -- from PRODUCT_ID,
    customer_id STRING  -- from PRODUCT_ID,
    category STRING  -- from PRODUCT_ID,
    quantity STRING  -- from PRODUCT_ID,
    unit_price STRING  -- from PRODUCT_ID,
    line_total STRING  -- from PRODUCT_ID,
    order_date STRING  -- from PRODUCT_ID,
    order_status STRING  -- from PRODUCT_ID,
    channel STRING  -- from PRODUCT_ID,
    total_orders STRING  -- from PRODUCT_ID,
    total_revenue STRING  -- from PRODUCT_ID,
    avg_order_value STRING  -- from PRODUCT_ID,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping M_LOAD_ORDERS'
;

-- Table: main.silver.dim_inventory (from mapping M_UPSERT_INVENTORY)
CREATE TABLE IF NOT EXISTS main.silver.dim_inventory (
    product_id STRING  -- from PRODUCT_ID,
    warehouse_id STRING  -- from PRODUCT_ID,
    qty_on_hand STRING  -- from PRODUCT_ID,
    qty_reserved STRING  -- from PRODUCT_ID,
    qty_available STRING  -- from PRODUCT_ID,
    last_updated STRING  -- from PRODUCT_ID,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping M_UPSERT_INVENTORY'
;

-- Table: main.silver.lakehouse_silver (from mapping SYNC_CUSTOMER_DATA)
CREATE TABLE IF NOT EXISTS main.silver.lakehouse_silver (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping SYNC_CUSTOMER_DATA'
;

-- Table: main.silver.lakehouse_bronze (from mapping MI_BULK_LOAD_PRODUCTS)
CREATE TABLE IF NOT EXISTS main.silver.lakehouse_bronze (
    id BIGINT,
    _etl_load_timestamp TIMESTAMP,
    _etl_source_mapping STRING
)
USING DELTA
COMMENT 'Migrated from Informatica mapping MI_BULK_LOAD_PRODUCTS'
;