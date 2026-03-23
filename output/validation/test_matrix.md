# Test Matrix — Informatica to Fabric Migration Validation

**Pipeline:** PL_WF_DAILY_SALES_LOAD
**Mappings:** M_LOAD_CUSTOMERS, M_LOAD_ORDERS, M_UPSERT_INVENTORY

---

## Target Table Validations

| # | Target Table | Validation Type | Mapping | Expected Behavior | Pass Criteria | Priority | Notebook |
|---|---|---|---|---|---|---|---|
| 1 | silver.dim_customer | Row Count | M_LOAD_CUSTOMERS | Overwrite — source count = target count | source_count == target_count | **P0** | VAL_DIM_CUSTOMER.py |
| 2 | silver.dim_customer | Column Checksum | M_LOAD_CUSTOMERS | Hash of key columns matches between source and target | source_checksum == target_checksum | **P0** | VAL_DIM_CUSTOMER.py |
| 3 | silver.dim_customer | Data Quality — NOT NULL | M_LOAD_CUSTOMERS | customer_id, first_name, last_name never null | null_count == 0 for each column | **P0** | VAL_DIM_CUSTOMER.py |
| 4 | silver.dim_customer | Data Quality — Key Uniqueness | M_LOAD_CUSTOMERS | customer_id is unique | total_rows == distinct_keys | **P0** | VAL_DIM_CUSTOMER.py |
| 5 | silver.dim_customer | Data Quality — Valid Status | M_LOAD_CUSTOMERS | status in (ACTIVE, INACTIVE, SUSPENDED, CLOSED) | invalid_count == 0 | **P1** | VAL_DIM_CUSTOMER.py |
| 6 | silver.dim_customer | Sample Comparison | M_LOAD_CUSTOMERS | 100 sampled records match field-by-field | diff_count == 0 | **P1** | VAL_DIM_CUSTOMER.py |
| 7 | silver.fact_orders | Row Count | M_LOAD_ORDERS | Append mode — target >= source | target_count >= source_count | **P0** | VAL_FACT_ORDERS.py |
| 8 | silver.fact_orders | Column Checksum | M_LOAD_ORDERS | Hash of key columns matches (full-load comparison) | source_checksum == target_checksum | **P0** | VAL_FACT_ORDERS.py |
| 9 | silver.fact_orders | Aggregate — total_amount | M_LOAD_ORDERS | Sum of order_amount matches source within tolerance | abs(source - target) <= 0.01 | **P0** | VAL_FACT_ORDERS.py |
| 10 | silver.fact_orders | Aggregate — total_quantity | M_LOAD_ORDERS | Sum of quantity matches source exactly | source_qty == target_qty | **P0** | VAL_FACT_ORDERS.py |
| 11 | silver.fact_orders | Data Quality — NOT NULL | M_LOAD_ORDERS | order_id, customer_id, product_id, order_date, quantity never null | null_count == 0 | **P0** | VAL_FACT_ORDERS.py |
| 12 | silver.fact_orders | Data Quality — Key Uniqueness | M_LOAD_ORDERS | order_id is unique | total_rows == distinct_keys | **P0** | VAL_FACT_ORDERS.py |
| 13 | silver.fact_orders | Data Quality — RI to dim_customer | M_LOAD_ORDERS | Every customer_id exists in silver.dim_customer | orphan_count == 0 | **P1** | VAL_FACT_ORDERS.py |
| 14 | silver.fact_orders | Data Quality — Value Range | M_LOAD_ORDERS | order_amount > 0, quantity > 0 | negative_count == 0 | **P1** | VAL_FACT_ORDERS.py |
| 15 | silver.fact_orders | Sample Comparison | M_LOAD_ORDERS | 100 sampled records match field-by-field | diff_count == 0 | **P1** | VAL_FACT_ORDERS.py |
| 16 | gold.agg_orders_by_customer | Row Count | M_LOAD_ORDERS | Overwrite — one row per customer, count matches source groups | source_groups == target_count | **P0** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 17 | gold.agg_orders_by_customer | Column Checksum | M_LOAD_ORDERS | Hash of (customer_id, total_orders, total_amount) matches | source_checksum == target_checksum | **P0** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 18 | gold.agg_orders_by_customer | Aggregate — Grand Totals | M_LOAD_ORDERS | Sum of total_amount and total_orders matches raw source | abs(diff) <= 0.01 for amounts; exact for counts | **P0** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 19 | gold.agg_orders_by_customer | Data Quality — NOT NULL | M_LOAD_ORDERS | customer_id, total_orders, total_amount never null | null_count == 0 | **P0** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 20 | gold.agg_orders_by_customer | Data Quality — Key Uniqueness | M_LOAD_ORDERS | customer_id is unique (aggregation key) | total_rows == distinct_keys | **P0** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 21 | gold.agg_orders_by_customer | Data Quality — Value Range | M_LOAD_ORDERS | total_orders > 0, total_amount > 0 | invalid_count == 0 | **P1** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 22 | gold.agg_orders_by_customer | Data Quality — RI to dim_customer | M_LOAD_ORDERS | Every customer_id exists in silver.dim_customer | orphan_count == 0 | **P1** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 23 | gold.agg_orders_by_customer | Sample Comparison | M_LOAD_ORDERS | 50 sampled customer aggregations match (with tolerance) | mismatch_count == 0 | **P1** | VAL_AGG_ORDERS_BY_CUSTOMER.py |
| 24 | silver.dim_inventory | Row Count (post-MERGE) | M_UPSERT_INVENTORY | After MERGE, target count matches source count | source_count == target_count | **P0** | VAL_DIM_INVENTORY.py |
| 25 | silver.dim_inventory | MERGE Verification | M_UPSERT_INVENTORY | Delta history shows MERGE with INSERT/UPDATE/DELETE metrics | MERGE operation found in history | **P0** | VAL_DIM_INVENTORY.py |
| 26 | silver.dim_inventory | Key Synchronization | M_UPSERT_INVENTORY | Post-MERGE: target keys == source keys (no extras, no missing) | extra == 0 AND missing == 0 | **P0** | VAL_DIM_INVENTORY.py |
| 27 | silver.dim_inventory | Column Checksum | M_UPSERT_INVENTORY | Hash of key columns matches after MERGE | source_checksum == target_checksum | **P0** | VAL_DIM_INVENTORY.py |
| 28 | silver.dim_inventory | Data Quality — NOT NULL | M_UPSERT_INVENTORY | inventory_id, product_id, warehouse_id, quantity_on_hand never null | null_count == 0 | **P0** | VAL_DIM_INVENTORY.py |
| 29 | silver.dim_inventory | Data Quality — Key Uniqueness | M_UPSERT_INVENTORY | inventory_id is unique | total_rows == distinct_keys | **P0** | VAL_DIM_INVENTORY.py |
| 30 | silver.dim_inventory | Data Quality — Value Range | M_UPSERT_INVENTORY | quantity_on_hand >= 0, reorder_point >= 0 | negative_count == 0 | **P1** | VAL_DIM_INVENTORY.py |
| 31 | silver.dim_inventory | Data Quality — Delta Format | M_UPSERT_INVENTORY | Table is in Delta format (required for MERGE) | isDeltaTable == True | **P1** | VAL_DIM_INVENTORY.py |
| 32 | silver.dim_inventory | Sample Comparison | M_UPSERT_INVENTORY | 100 sampled records match (excluding last_updated) | diff_count == 0 | **P1** | VAL_DIM_INVENTORY.py |

---

## Pipeline-Level Validations

| # | Check | Expected Behavior | Pass Criteria | Priority | Notebook |
|---|---|---|---|---|---|
| 33 | Target Tables Populated | All 4 target tables exist and have rows | all 4 tables row_count > 0 | **P0** | VAL_PIPELINE_EXECUTION.py |
| 34 | Execution Order | dim_customer loaded before fact_orders | dim_customer timestamp <= fact_orders timestamp | **P0** | VAL_PIPELINE_EXECUTION.py |
| 35 | Co-write: fact + agg | fact_orders and agg_orders written within 10 min | time_diff <= 600s | **P1** | VAL_PIPELINE_EXECUTION.py |
| 36 | IfCondition Branching | Customer success triggers inventory upsert | Both dim_customer and dim_inventory populated | **P1** | VAL_PIPELINE_EXECUTION.py |
| 37 | Full Pipeline Success | All notebooks ran, all targets populated | 4/4 tables populated | **P0** | VAL_PIPELINE_EXECUTION.py |
| 38 | Cross-Table RI: fact→dim | fact_orders.customer_id → dim_customer | orphan_count == 0 | **P0** | VAL_PIPELINE_EXECUTION.py |
| 39 | Cross-Table RI: agg→dim | agg_orders.customer_id → dim_customer | orphan_count == 0 | **P0** | VAL_PIPELINE_EXECUTION.py |

---

## Known Differences & Tolerances

| Category | Description | Handling |
|---|---|---|
| Floating-point precision | Oracle NUMBER vs Spark DOUBLE rounding | Tolerance of 0.01 on amount comparisons |
| NULL vs empty string | Oracle treats `''` as NULL; Spark does not | Document as known difference; exclude from strict checksum |
| Timestamps | last_updated / processing timestamps differ | Excluded from sample comparison on dim_inventory |
| Sequence values | Fabric may generate different surrogate keys | Compare on natural keys only |
| Append mode row counts | fact_orders accumulates across loads | target_count >= source_count (not equal) |
| MERGE timing | Delta MERGE metrics depend on history retention | Check last 5 operations; SKIPPED if unavailable |

---

## Priority Definitions

| Priority | Meaning | Action on Failure |
|---|---|---|
| **P0** | Critical — data correctness | Block deployment; investigate immediately |
| **P1** | Important — data quality | Log warning; investigate before production |
| **P2** | Nice-to-have — completeness | Document for review; non-blocking |

---

## Execution Instructions

Run validation notebooks in this order:

1. `VAL_DIM_CUSTOMER.py` — validate customer dimension
2. `VAL_FACT_ORDERS.py` — validate fact table (depends on dim_customer for RI check)
3. `VAL_AGG_ORDERS_BY_CUSTOMER.py` — validate gold aggregation
4. `VAL_DIM_INVENTORY.py` — validate inventory MERGE
5. `VAL_PIPELINE_EXECUTION.py` — validate end-to-end pipeline

All notebooks are **idempotent** and can be re-run safely.
