-- Table: gold.agg_orders_by_customer (from mapping M_LOAD_ORDERS)
CREATE TABLE IF NOT EXISTS gold.agg_orders_by_customer (
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