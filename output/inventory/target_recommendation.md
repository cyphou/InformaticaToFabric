# Migration Target Advisor — Databricks vs DBT

**Generated:** 2026-04-02 12:28 UTC

## Summary

| Metric | Value |
|--------|-------|
| Total mappings | 16 |
| DBT recommended | 3 (18.8%) |
| PySpark recommended | 6 (37.5%) |
| Either (flexible) | 7 |

## Recommendation

**Primary target: PySpark** — 6/16 mappings need PySpark.
Use `--target databricks` for the majority, optionally `--target auto` to mix.

## Per-Mapping Recommendations

| Mapping | Complexity | Target | Reason |
|---------|-----------|--------|--------|
| m_cdc_order_pipeline | Complex | 🐍 pyspark | Complex mapping with 9 transforms benefits from PySpark flexibility |
| m_customer_360 | Complex | 🐍 pyspark | Complex mapping with 10 transforms benefits from PySpark flexibility |
| m_customer_activity_log | Simple | 🔄 either | Compatible with both DBT and PySpark |
| m_load_contacts | Medium | 🐍 pyspark | Contains PySpark-required transforms: DM |
| m_sync_accounts | Simple | 🏗️ dbt | All transformations are SQL-translatable |
| m_realtime_inventory_scd2 | Complex | 🐍 pyspark | Contains PySpark-required transforms: NRM |
| m_inventory_snapshot | Simple | 🔄 either | Compatible with both DBT and PySpark |
| M_COMPLEX_MULTI_SOURCE | Complex | 🐍 pyspark | Contains PySpark-required transforms: NRM, SQLT |
| M_LOAD_CUSTOMERS | Simple | 🏗️ dbt | All transformations are SQL-translatable |
| M_LOAD_EMPLOYEES | Complex | 🐍 pyspark | Contains PySpark-required transforms: SQLT |
| M_LOAD_ORDERS | Complex | 🏗️ dbt | All transformations are SQL-translatable |
| M_UPSERT_INVENTORY | Complex | 🔄 either | Compatible with both DBT and PySpark |
| DQ_VALIDATE_EMAILS | Complex | 🔄 either | Compatible with both DBT and PySpark |
| DQ_STANDARDIZE_ADDRESSES | Complex | 🔄 either | Compatible with both DBT and PySpark |
| SYNC_CUSTOMER_DATA | Simple | 🔄 either | Compatible with both DBT and PySpark |
| MI_BULK_LOAD_PRODUCTS | Simple | 🔄 either | Compatible with both DBT and PySpark |
