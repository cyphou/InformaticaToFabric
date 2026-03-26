# Test Matrix — Informatica to Fabric Validation

Generated: 2026-03-24 13:54 UTC

## Validation Notebooks

| # | Mapping | Target Table | Validation File | Levels |
|---|---------|-------------|-----------------|--------|
| 1 | m_load_contacts | TGT_LH_CONTACTS | `VAL_TGT_LH_CONTACTS.py` | L1–L5 |
| 2 | m_sync_accounts | TGT_ACCOUNTS | `VAL_TGT_ACCOUNTS.py` | L1–L5 |
| 3 | M_COMPLEX_MULTI_SOURCE | FACT_TXN_HIGH | `VAL_FACT_TXN_HIGH.py` | L1–L5 |
| 4 | M_COMPLEX_MULTI_SOURCE | FACT_TXN_LOW | `VAL_FACT_TXN_LOW.py` | L1–L5 |
| 5 | M_COMPLEX_MULTI_SOURCE | FACT_TXN_TAGS | `VAL_FACT_TXN_TAGS.py` | L1–L5 |
| 6 | M_LOAD_CUSTOMERS | DIM_CUSTOMER | `VAL_DIM_CUSTOMER.py` | L1–L5 |
| 7 | M_LOAD_EMPLOYEES | DIM_EMPLOYEE | `VAL_DIM_EMPLOYEE.py` | L1–L5 |
| 8 | M_LOAD_ORDERS | FACT_ORDERS | `VAL_FACT_ORDERS.py` | L1–L5 |
| 9 | M_LOAD_ORDERS | AGG_ORDERS_BY_CUSTOMER | `VAL_AGG_ORDERS_BY_CUSTOMER.py` | L1–L5 |
| 10 | M_UPSERT_INVENTORY | DIM_INVENTORY | `VAL_DIM_INVENTORY.py` | L1–L5 |
| 11 | SYNC_CUSTOMER_DATA | LAKEHOUSE_SILVER | `VAL_LAKEHOUSE_SILVER.py` | L1–L5 |
| 12 | MI_BULK_LOAD_PRODUCTS | LAKEHOUSE_BRONZE | `VAL_LAKEHOUSE_BRONZE.py` | L1–L5 |

## Validation Levels

| Level | Check | Description |
|-------|-------|-------------|
| L1 | Row Count | Source count == Target count |
| L2 | Checksum | MD5-based column hash comparison |
| L3 | Data Quality | NULL checks, key uniqueness |
| L4 | Key Sampling | Sample N keys and compare presence |
| L5 | Aggregate Comparison | SUM/MIN/MAX on numeric columns |

## How to Run

1. Upload validation notebooks to a Fabric workspace
2. Configure source JDBC connection strings in Cell 1
3. Run all cells — review OVERALL RESULT in final cell
4. Address any FAIL results before sign-off

## Notes

- `SKIPPED` results indicate a connection failure — not a data issue
- Expand `checksum_columns` and `nullable_not_allowed` in Cell 1 for deeper coverage
- For aggregate targets (gold layer), row count comparison may differ by design
