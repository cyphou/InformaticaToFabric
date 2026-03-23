# Test Matrix — Informatica to Fabric Validation

Generated: 2026-03-23 18:44 UTC

## Validation Notebooks

| # | Mapping | Target Table | Validation File | Levels |
|---|---------|-------------|-----------------|--------|
| 1 | m_load_contacts | TGT_LH_CONTACTS | `VAL_TGT_LH_CONTACTS.py` | L1–L3 |
| 2 | m_sync_accounts | TGT_ACCOUNTS | `VAL_TGT_ACCOUNTS.py` | L1–L3 |
| 3 | M_LOAD_CUSTOMERS | DIM_CUSTOMER | `VAL_DIM_CUSTOMER.py` | L1–L3 |
| 4 | M_LOAD_EMPLOYEES | DIM_EMPLOYEE | `VAL_DIM_EMPLOYEE.py` | L1–L3 |
| 5 | M_LOAD_ORDERS | FACT_ORDERS | `VAL_FACT_ORDERS.py` | L1–L3 |
| 6 | M_LOAD_ORDERS | AGG_ORDERS_BY_CUSTOMER | `VAL_AGG_ORDERS_BY_CUSTOMER.py` | L1–L3 |
| 7 | M_UPSERT_INVENTORY | DIM_INVENTORY | `VAL_DIM_INVENTORY.py` | L1–L3 |

## Validation Levels

| Level | Check | Description |
|-------|-------|-------------|
| L1 | Row Count | Source count == Target count |
| L2 | Checksum | MD5-based column hash comparison |
| L3 | Data Quality | NULL checks, key uniqueness |

## How to Run

1. Upload validation notebooks to a Fabric workspace
2. Configure source JDBC connection strings in Cell 1
3. Run all cells — review OVERALL RESULT in final cell
4. Address any FAIL results before sign-off

## Notes

- `SKIPPED` results indicate a connection failure — not a data issue
- Expand `checksum_columns` and `nullable_not_allowed` in Cell 1 for deeper coverage
- For aggregate targets (gold layer), row count comparison may differ by design
