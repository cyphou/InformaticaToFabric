# Test Matrix — Informatica to Fabric Validation

Generated: 2026-04-08 11:51 UTC

## Validation Notebooks

| # | Mapping | Target Table | Validation File | Levels |
|---|---------|-------------|-----------------|--------|
| 1 | m_cdc_order_pipeline | TGT_BRONZE_CDC_EVENTS | `VAL_TGT_BRONZE_CDC_EVENTS.py` | L1–L5 |
| 2 | m_cdc_order_pipeline | TGT_SILVER_ORDERS | `VAL_TGT_SILVER_ORDERS.py` | L1–L5 |
| 3 | m_cdc_order_pipeline | TGT_GOLD_DAILY_SUMMARY | `VAL_TGT_GOLD_DAILY_SUMMARY.py` | L1–L5 |
| 4 | m_customer_360 | TGT_SILVER_CUSTOMER | `VAL_TGT_SILVER_CUSTOMER.py` | L1–L5 |
| 5 | m_customer_360 | TGT_GOLD_CUSTOMER_360 | `VAL_TGT_GOLD_CUSTOMER_360.py` | L1–L5 |
| 6 | m_customer_activity_log | TGT_BRONZE_EVENTS | `VAL_TGT_BRONZE_EVENTS.py` | L1–L5 |
| 7 | m_load_contacts | TGT_LH_CONTACTS | `VAL_TGT_LH_CONTACTS.py` | L1–L5 |
| 8 | m_sync_accounts | TGT_ACCOUNTS | `VAL_TGT_ACCOUNTS.py` | L1–L5 |
| 9 | m_realtime_inventory_scd2 | TGT_SILVER_INVENTORY | `VAL_TGT_SILVER_INVENTORY.py` | L1–L5 |
| 10 | m_realtime_inventory_scd2 | TGT_GOLD_INVENTORY_DASHBOARD | `VAL_TGT_GOLD_INVENTORY_DASHBOARD.py` | L1–L5 |
| 11 | m_realtime_inventory_scd2 | TGT_ALERT_QUEUE | `VAL_TGT_ALERT_QUEUE.py` | L1–L5 |
| 12 | m_inventory_snapshot | TGT_GOLD_INVENTORY_HISTORY | `VAL_TGT_GOLD_INVENTORY_HISTORY.py` | L1–L5 |
| 13 | M_COMPLEX_MULTI_SOURCE | FACT_TXN_HIGH | `VAL_FACT_TXN_HIGH.py` | L1–L5 |
| 14 | M_COMPLEX_MULTI_SOURCE | FACT_TXN_LOW | `VAL_FACT_TXN_LOW.py` | L1–L5 |
| 15 | M_COMPLEX_MULTI_SOURCE | FACT_TXN_TAGS | `VAL_FACT_TXN_TAGS.py` | L1–L5 |
| 16 | M_LOAD_CUSTOMERS | DIM_CUSTOMER | `VAL_DIM_CUSTOMER.py` | L1–L5 |
| 17 | M_LOAD_EMPLOYEES | DIM_EMPLOYEE | `VAL_DIM_EMPLOYEE.py` | L1–L5 |
| 18 | M_LOAD_ORDERS | FACT_ORDERS | `VAL_FACT_ORDERS.py` | L1–L5 |
| 19 | M_LOAD_ORDERS | AGG_ORDERS_BY_CUSTOMER | `VAL_AGG_ORDERS_BY_CUSTOMER.py` | L1–L5 |
| 20 | M_UPSERT_INVENTORY | DIM_INVENTORY | `VAL_DIM_INVENTORY.py` | L1–L5 |
| 21 | SYNC_CUSTOMER_DATA | LAKEHOUSE_SILVER | `VAL_LAKEHOUSE_SILVER.py` | L1–L5 |
| 22 | MI_BULK_LOAD_PRODUCTS | LAKEHOUSE_BRONZE | `VAL_LAKEHOUSE_BRONZE.py` | L1–L5 |

## Validation Levels

| Level | Check | Description |
|-------|-------|-------------|
| L1 | Row Count | Source count == Target count |
| L2 | Checksum | MD5-based column hash comparison |
| L3 | Data Quality | NULL checks, key uniqueness |
| L4 | Key Sampling | Sample N keys and compare presence |
| L5 | Aggregate Comparison | SUM/MIN/MAX on numeric columns |

## How to Run

1. Upload validation notebooks to your Fabric workspace or Databricks workspace
2. Configure source JDBC connection strings in Cell 1
3. Run all cells — review OVERALL RESULT in final cell
4. Address any FAIL results before sign-off

## Notes

- `SKIPPED` results indicate a connection failure — not a data issue
- Expand `checksum_columns` and `nullable_not_allowed` in Cell 1 for deeper coverage
- For aggregate targets (gold layer), row count comparison may differ by design
