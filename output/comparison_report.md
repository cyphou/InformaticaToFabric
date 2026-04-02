# Cross-Platform Comparison — Databricks PySpark vs DBT

**Generated:** 2026-04-02 12:28 UTC

## 1. Architecture Comparison

| Aspect | Databricks PySpark | DBT on Databricks |
|--------|-------------------|-------------------|
| **Execution engine** | Spark DataFrame API | SQL via dbt-databricks adapter |
| **Notebook API** | `dbutils.*` | dbt CLI / Cloud |
| **Secret management** | `dbutils.secrets.get()` | env_var / profiles.yml |
| **Table references** | `catalog.schema.table` | `{{ source() }}` / `{{ ref() }}` |
| **Schema evolution** | `MERGE INTO` / Delta | dbt `--full-refresh` / incremental |
| **Testing** | PySpark assertions | dbt tests (unique, not_null, accepted_values) |
| **Lineage** | Unity Catalog lineage | dbt docs lineage graph |
| **Orchestration** | Databricks Workflows (Jobs API) | Databricks Workflows + dbt task |
| **CI/CD** | Asset Bundles / REST API | dbt Cloud CI / dbt Core + Asset Bundles |
| **Cost model** | DBU (cluster hours) | DBU (SQL Warehouse hours) |

## 2. Transformation Mapping

| Informatica Transform | PySpark Equivalent | DBT Equivalent |
|----------------------|-------------------|----------------|
| Source Qualifier (SQ) | `spark.read.table()` | `{{ source('schema', 'table') }}` |
| Expression (EXP) | `.withColumn()` / `.select()` | `SELECT col, expr AS alias` |
| Filter (FIL) | `.filter()` / `.where()` | `WHERE condition` |
| Aggregator (AGG) | `.groupBy().agg()` | `GROUP BY` + aggregate functions |
| Joiner (JNR) | `.join()` | `JOIN` clause |
| Lookup (LKP) | `.join()` (broadcast) | `LEFT JOIN` / dbt macro |
| Router (RTR) | Multiple `.filter()` | `CASE WHEN` / dbt `UNION ALL` |
| Update Strategy (UPD) | `MERGE INTO` Delta | dbt incremental `merge` strategy |
| Rank (RNK) | `Window.orderBy()` | `ROW_NUMBER() OVER (...)` |
| Sorter (SRT) | `.orderBy()` | `ORDER BY` |
| Union (UNI) | `.union()` | `UNION ALL` |
| Sequence Gen (SEQ) | `monotonically_increasing_id()` | `ROW_NUMBER()` |
| Java/Custom (JTX/CT) | Custom PySpark UDF | ❌ Not supported (PySpark fallback) |
| HTTP | `requests` + PySpark | ❌ Not supported (PySpark fallback) |
| Stored Proc (SP) | `spark.sql('CALL ...')` | dbt `run-operation` or pre-hook |

## 3. Per-Mapping Target Suitability

| Mapping | Complexity | Transforms | Best Target | Reason |
|---------|-----------|-----------|-------------|--------|
| m_cdc_order_pipeline | Complex | JNR, LKP, EXP, port, RTR… | 🐍 pyspark | Complex mapping with 9 transforms benefits from PySpark flexibility |
| m_customer_360 | Complex | JNR, LKP, EXP, port, FIL… | 🐍 pyspark | Complex mapping with 10 transforms benefits from PySpark flexibility |
| m_customer_activity_log | Simple | EXP, port, FIL | 🔄 either | Compatible with both DBT and PySpark |
| m_load_contacts | Medium | EXP, port, FIL, LKP, AGG… | 🐍 pyspark | Contains PySpark-required transforms: DM |
| m_sync_accounts | Simple | EXP | 🏗️ dbt | All transformations are SQL-translatable |
| m_realtime_inventory_scd2 | Complex | JNR, LKP, EXP, port, FIL… | 🐍 pyspark | Contains PySpark-required transforms: NRM |
| m_inventory_snapshot | Simple | EXP, port | 🔄 either | Compatible with both DBT and PySpark |
| M_COMPLEX_MULTI_SOURCE | Complex | SQ, JNR, EXP, SQLT, LKP… | 🐍 pyspark | Contains PySpark-required transforms: NRM, SQLT |
| M_LOAD_CUSTOMERS | Simple | SQ, EXP, FIL | 🏗️ dbt | All transformations are SQL-translatable |
| M_LOAD_EMPLOYEES | Complex | SQ, EXP, FIL, SRT, LKP… | 🐍 pyspark | Contains PySpark-required transforms: SQLT |
| M_LOAD_ORDERS | Complex | SQ, LKP, EXP, AGG | 🏗️ dbt | All transformations are SQL-translatable |
| M_UPSERT_INVENTORY | Complex | SQ, EXP, UPD | 🔄 either | Compatible with both DBT and PySpark |
| DQ_VALIDATE_EMAILS | Complex | SQ, DQ, TGT | 🔄 either | Compatible with both DBT and PySpark |
| DQ_STANDARDIZE_ADDRESSES | Complex | SQ, DQ, TGT | 🔄 either | Compatible with both DBT and PySpark |
| SYNC_CUSTOMER_DATA | Simple | SQ, TGT | 🔄 either | Compatible with both DBT and PySpark |
| MI_BULK_LOAD_PRODUCTS | Simple | SQ, TGT | 🔄 either | Compatible with both DBT and PySpark |

## 4. Cost Projection (Estimated)

| Metric | PySpark on Databricks | DBT on Databricks |
|--------|----------------------|-------------------|
| Mappings suited | 13 | 10 |
| Est. daily DBU | 26.7 DBU (all-purpose cluster) | 16.0 DBU (SQL Warehouse) |
| Cost factor | 1.0× (baseline) | ~0.6× (SQL Warehouse cheaper) |
| Dev effort (PySpark) | 200 hours | — |
| Dev effort (DBT) | — | 100 hours |

## 5. Recommendation

**Recommended: `--target databricks`** — 6/16 mappings need PySpark.
Consider `--target auto` to use DBT for SQL-heavy subset.

```bash
informatica-to-fabric --target databricks --config migration.yaml
```

## 6. Artifact Comparison

**Databricks Notebooks:** 17 files, 41,096 bytes total
**DBT Models:** 4 files, 3,424 bytes total

### Notebooks vs DBT Models

| File | Notebooks | Size | DBT | Size |
|------|-----------|------|-----|------|
| .gitkeep | ✅ | 71B | — | — |
| NB_DQ_STANDARDIZE_ADDRESSES.py | ✅ | 1,301B | — | — |
| NB_DQ_VALIDATE_EMAILS.py | ✅ | 1,283B | — | — |
| NB_MI_BULK_LOAD_PRODUCTS.py | ✅ | 1,443B | — | — |
| NB_M_COMPLEX_MULTI_SOURCE.py | ✅ | 4,182B | — | — |
| NB_M_LOAD_CUSTOMERS.py | ✅ | 1,735B | — | — |
| NB_M_LOAD_EMPLOYEES.py | ✅ | 2,633B | — | — |
| NB_M_LOAD_ORDERS.py | ✅ | 2,816B | — | — |
| NB_M_UPSERT_INVENTORY.py | ✅ | 1,848B | — | — |
| NB_SYNC_CUSTOMER_DATA.py | ✅ | 1,434B | — | — |
| NB_m_cdc_order_pipeline.py | ✅ | 4,616B | — | — |
| NB_m_customer_360.py | ✅ | 4,903B | — | — |
| NB_m_customer_activity_log.py | ✅ | 1,975B | — | — |
| NB_m_inventory_snapshot.py | ✅ | 1,816B | — | — |
| NB_m_load_contacts.py | ✅ | 2,831B | — | — |
| NB_m_realtime_inventory_scd2.py | ✅ | 4,695B | — | — |
| NB_m_sync_accounts.py | ✅ | 1,514B | — | — |
| dbt_generation_summary.json | — | — | ✅ | 1,779B |
| dbt_project.yml | — | — | ✅ | 751B |
| packages.yml | — | — | ✅ | 195B |
| profiles.yml | — | — | ✅ | 699B |
