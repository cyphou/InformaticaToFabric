# Examples — Before & After Migration

This directory provides **concrete, browsable examples** of what Informatica exports look like and what the tool generates for Microsoft Fabric.

> All files below are **real outputs** from running the migration tool against the sample inputs in [`input/`](../input/).

---

## Quick Navigation

| Example | Input (Informatica) | Output (Fabric) | Complexity |
|---------|---------------------|------------------|------------|
| [1. Simple Mapping](#1-simple-mapping--m_load_customers) | [M_LOAD_CUSTOMERS.xml](../input/mappings/M_LOAD_CUSTOMERS.xml) | [NB_M_LOAD_CUSTOMERS.py](../output/notebooks/NB_M_LOAD_CUSTOMERS.py) | 🟢 Simple |
| [2. Complex Mapping](#2-complex-mapping--m_load_orders) | [M_LOAD_ORDERS.xml](../input/mappings/M_LOAD_ORDERS.xml) | [NB_M_LOAD_ORDERS.py](../output/notebooks/NB_M_LOAD_ORDERS.py) | 🟠 Complex |
| [3. Multi-Source + Mapplet + Router](#3-advanced-mapping--m_complex_multi_source) | [M_COMPLEX_MULTI_SOURCE.xml](../input/mappings/M_COMPLEX_MULTI_SOURCE.xml) | *(run tool to generate)* | 🔴 Advanced |
| [4. Workflow → Pipeline](#4-workflow--pipeline) | [WF_DAILY_SALES_LOAD.xml](../input/workflows/WF_DAILY_SALES_LOAD.xml) | [PL_WF_DAILY_SALES_LOAD.json](../output/pipelines/PL_WF_DAILY_SALES_LOAD.json) | 🟡 Medium |
| [5. Complex Workflow](#5-complex-workflow--wf_complex_finance_etl) | [WF_COMPLEX_FINANCE_ETL.xml](../input/workflows/WF_COMPLEX_FINANCE_ETL.xml) | *(run tool to generate)* | 🔴 Advanced |
| [6. Oracle SQL → Spark SQL](#6-oracle-sql--spark-sql) | [SP_CALC_RANKINGS.sql](../input/sql/SP_CALC_RANKINGS.sql) | [SQL_SP_CALC_RANKINGS.sql](../output/sql/SQL_SP_CALC_RANKINGS.sql) | 🟠 Complex |
| [7. Validation Notebook](#7-validation-notebook) | *(auto-generated)* | [VAL_DIM_CUSTOMER.py](../output/validation/VAL_DIM_CUSTOMER.py) | Auto |
| [8. Assessment Inventory](#8-assessment-inventory) | [input/](../input/) | [inventory.json](../output/inventory/inventory.json) | Auto |
| [9. IICS Taskflow](#9-iics-taskflow) | [IICS_TF_DAILY_CONTACTS_ETL.xml](../input/workflows/IICS_TF_DAILY_CONTACTS_ETL.xml) | [NB_m_load_contacts.py](../output/notebooks/NB_m_load_contacts.py) | 🟡 Medium |
| [10. Multi-DB SQL](#10-multi-db-sql-fixtures) | Teradata / DB2 / MySQL / PostgreSQL / SQL Server | *(run tool to generate)* | Varies |

---

## 1. Simple Mapping — M_LOAD_CUSTOMERS

**Pattern:** Source Qualifier → Expression → Filter → Target

### Input: PowerCenter XML

📄 [`input/mappings/M_LOAD_CUSTOMERS.xml`](../input/mappings/M_LOAD_CUSTOMERS.xml)

```xml
<!-- Source: Oracle SALES.CUSTOMERS (8 columns) -->
<SOURCE DATABASETYPE="Oracle" NAME="CUSTOMERS" OWNERNAME="SALES">
  <SOURCEFIELD NAME="CUSTOMER_ID" DATATYPE="number" PRECISION="10"/>
  <SOURCEFIELD NAME="FIRST_NAME" DATATYPE="varchar2" PRECISION="100"/>
  <SOURCEFIELD NAME="LAST_NAME" DATATYPE="varchar2" PRECISION="100"/>
  ...
</SOURCE>

<!-- Expression: Derive FULL_NAME and STATUS_LABEL -->
<TRANSFORMATION NAME="EXP_DERIVE_FIELDS" TYPE="Expression">
  <TRANSFORMFIELD NAME="FULL_NAME" EXPRESSION="FIRST_NAME || ' ' || LAST_NAME" PORTTYPE="OUTPUT"/>
  <TRANSFORMFIELD NAME="STATUS_LABEL"
    EXPRESSION="IIF(STATUS = 'A', 'Active', IIF(STATUS = 'I', 'Inactive', 'Unknown'))"
    PORTTYPE="OUTPUT"/>
</TRANSFORMATION>

<!-- Filter: Exclude unknowns -->
<TRANSFORMATION NAME="FIL_ACTIVE_CUSTOMERS" TYPE="Filter">
  <TABLEATTRIBUTE NAME="Filter Condition" VALUE="STATUS_LABEL != 'Unknown'"/>
</TRANSFORMATION>
```

### Output: Fabric Notebook (PySpark)

📄 [`output/notebooks/NB_M_LOAD_CUSTOMERS.py`](../output/notebooks/NB_M_LOAD_CUSTOMERS.py)

```python
# Notebook: NB_M_LOAD_CUSTOMERS
# Migrated from: Informatica mapping M_LOAD_CUSTOMERS
# Complexity: Simple
# Sources: Oracle.SALES.CUSTOMERS
# Targets: DIM_CUSTOMER

df_source = spark.table("bronze.customers")

df = df.withColumn("ETL_LOAD_DATE", current_timestamp())

df = df.filter(col("STATUS") != "INACTIVE")

df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.dim_customer")
```

---

## 2. Complex Mapping — M_LOAD_ORDERS

**Pattern:** Source Qualifier → Lookup → Expression → Aggregator → 2 Targets

### Input: PowerCenter XML

📄 [`input/mappings/M_LOAD_ORDERS.xml`](../input/mappings/M_LOAD_ORDERS.xml)

```xml
<!-- Source Qualifier with SQL Override (Oracle-specific) -->
<TRANSFORMATION NAME="SQ_ORDERS" TYPE="Source Qualifier">
  <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT ORDER_ID, CUSTOMER_ID, ...
    NVL(DISCOUNT_PCT, 0) AS DISCOUNT_PCT, ...
    FROM SALES.ORDERS
    WHERE ORDER_DATE >= TO_DATE('$$LOAD_DATE', 'YYYY-MM-DD')
    AND ORDER_STATUS != 'CANCELLED'"/>
</TRANSFORMATION>

<!-- Lookup: Product details (with NVL in SQL override) -->
<TRANSFORMATION NAME="LKP_PRODUCTS" TYPE="Lookup Procedure">
  <TABLEATTRIBUTE NAME="Lookup Sql Override"
    VALUE="SELECT PRODUCT_ID, NVL(PRODUCT_NAME, 'Unknown') ... FROM SALES.PRODUCTS"/>
</TRANSFORMATION>

<!-- Expression: Calculate line total with discount -->
<TRANSFORMATION NAME="EXP_CALC_TOTAL" TYPE="Expression">
  <TRANSFORMFIELD NAME="LINE_TOTAL"
    EXPRESSION="QUANTITY * UNIT_PRICE * (1 - DISCOUNT_PCT / 100)" PORTTYPE="OUTPUT"/>
</TRANSFORMATION>

<!-- Aggregator: Sum by customer -->
<TRANSFORMATION NAME="AGG_BY_CUSTOMER" TYPE="Aggregator">
  <TRANSFORMFIELD NAME="TOTAL_ORDERS" EXPRESSION="COUNT(ORDER_ID)" PORTTYPE="OUTPUT"/>
  <TRANSFORMFIELD NAME="TOTAL_REVENUE" EXPRESSION="SUM(LINE_TOTAL)" PORTTYPE="OUTPUT"/>
</TRANSFORMATION>
```

### Output: Fabric Notebook (PySpark)

📄 [`output/notebooks/NB_M_LOAD_ORDERS.py`](../output/notebooks/NB_M_LOAD_ORDERS.py)

```python
# Notebook: NB_M_LOAD_ORDERS
# Complexity: Complex
# Sources: Oracle.SALES.ORDERS, Oracle.SALES.PRODUCTS
# Targets: FACT_ORDERS, AGG_ORDERS_BY_CUSTOMER
# Flow: SQ → LKP → EXP → AGG

load_date = notebookutils.widgets.get("load_date")

df_source = spark.table("bronze.orders")
df_source_2 = spark.table("bronze.products")

# Lookup join (broadcast for small tables)
df = df.join(broadcast(df_lookup), on="LOOKUP_KEY", how="left")

# Aggregation
df_agg = df.groupBy("GROUP_KEY").agg(count("*").alias("RECORD_COUNT"))

# Dual target write
df.write.format("delta").saveAsTable("silver.fact_orders")
df_target_2.write.format("delta").saveAsTable("gold.agg_orders_by_customer")
```

### SQL Override Conversion

📄 [`output/sql/SQL_OVERRIDES_M_LOAD_ORDERS.sql`](../output/sql/SQL_OVERRIDES_M_LOAD_ORDERS.sql)

The Oracle `NVL()`, `TO_DATE()`, and `$$PARAM` references are automatically converted to Spark SQL equivalents.

---

## 3. Advanced Mapping — M_COMPLEX_MULTI_SOURCE

**Pattern:** 2 Sources → Joiner → Mapplet → SQL Transformation → Router → Rank / Normalizer → 3 Targets

📄 [`input/mappings/M_COMPLEX_MULTI_SOURCE.xml`](../input/mappings/M_COMPLEX_MULTI_SOURCE.xml)

This is the most complex mapping in the repo, featuring:

| Transformation | Purpose |
|---|---|
| **Joiner** (JNR_TXN_ACCT) | Join TRANSACTIONS with ACCOUNTS on account_id |
| **Mapplet** (MPLT_CURRENCY_CONVERT) | Reusable currency conversion (USD/EUR/GBP/JPY) |
| **SQL Transformation** (SQLT_RISK_SCORE) | Dynamic SQL-based risk scoring |
| **Lookup** (LKP_FRAUD_BLACKLIST) | Fraud blacklist check with SQL override |
| **Router** (RTR_VALUE_TIER) | Split by amount: HIGH_VALUE (≥5000) vs LOW_VALUE |
| **Rank** (RNK_TOP_TXN) | Top 10 transactions per account |
| **Normalizer** (NRM_TAGS) | Pipe-delimited tags → individual rows |

Three targets: `FACT_TXN_HIGH`, `FACT_TXN_LOW`, `FACT_TXN_TAGS`

> Run `informatica-to-fabric` or `@notebook-migration convert mapping M_COMPLEX_MULTI_SOURCE` to generate the output notebook.

---

## 4. Workflow → Pipeline

**Pattern:** Start → Session → Session → Decision → Session / Email → Scheduler

### Input: PowerCenter Workflow XML

📄 [`input/workflows/WF_DAILY_SALES_LOAD.xml`](../input/workflows/WF_DAILY_SALES_LOAD.xml)

```xml
<WORKFLOW NAME="WF_DAILY_SALES_LOAD">
  <!-- Sequential: Load Customers → Load Orders → Decision → Upsert Inventory -->
  <SESSION NAME="S_M_LOAD_CUSTOMERS" MAPPINGNAME="M_LOAD_CUSTOMERS"/>
  <SESSION NAME="S_M_LOAD_ORDERS" MAPPINGNAME="M_LOAD_ORDERS">
    <ATTRIBUTE NAME="Pre SQL" VALUE="DELETE FROM SALES.FACT_ORDERS_STAGING ..."/>
    <ATTRIBUTE NAME="Post SQL" VALUE="EXEC SALES.SP_UPDATE_ORDER_STATS;"/>
  </SESSION>
  <TASK NAME="DEC_CHECK_ORDERS" TYPE="Decision">
    <ATTRIBUTE NAME="Decision Expression" VALUE="$S_M_LOAD_ORDERS.TgtSuccessRows > 0"/>
  </TASK>
  <TASK NAME="EMAIL_FAILURE" TYPE="Email"/>

  <WORKFLOWLINK FROMTASK="S_M_LOAD_CUSTOMERS" TOTASK="S_M_LOAD_ORDERS"
    CONDITION="$S_M_LOAD_CUSTOMERS.Status = SUCCEEDED"/>
  <WORKFLOWLINK FROMTASK="DEC_CHECK_ORDERS" TOTASK="S_M_UPSERT_INVENTORY"
    CONDITION="$DEC_CHECK_ORDERS.condition = TRUE"/>

  <SCHEDULER NAME="SCHED_DAILY_0200">
    <ATTRIBUTE NAME="Repeat Type" VALUE="Every Day"/>
    <ATTRIBUTE NAME="Start Time" VALUE="02:00"/>
  </SCHEDULER>
</WORKFLOW>
```

### Output: Fabric Data Pipeline JSON

📄 [`output/pipelines/PL_WF_DAILY_SALES_LOAD.json`](../output/pipelines/PL_WF_DAILY_SALES_LOAD.json)

```json
{
  "name": "PL_WF_DAILY_SALES_LOAD",
  "properties": {
    "description": "Migrated from Informatica workflow WF_DAILY_SALES_LOAD. Original schedule: SCHED_DAILY_0200.",
    "activities": [
      {
        "name": "NB_M_LOAD_CUSTOMERS",
        "type": "TridentNotebook",
        "dependsOn": [],
        "typeProperties": {
          "notebook": { "referenceName": "NB_M_LOAD_CUSTOMERS", "type": "NotebookReference" }
        }
      },
      {
        "name": "NB_M_LOAD_ORDERS",
        "type": "TridentNotebook",
        "dependsOn": [
          { "activity": "NB_M_LOAD_CUSTOMERS", "dependencyConditions": ["Succeeded"] }
        ]
      },
      {
        "name": "DEC_CHECK_ORDERS",
        "type": "IfCondition",
        "dependsOn": [
          { "activity": "NB_M_LOAD_ORDERS", "dependencyConditions": ["Succeeded"] }
        ],
        "typeProperties": {
          "ifTrueActivities": [{ "name": "NB_M_UPSERT_INVENTORY", "type": "TridentNotebook" }],
          "ifFalseActivities": [{ "name": "Set_Skip_DEC_CHECK_ORDERS", "type": "SetVariable" }]
        }
      },
      {
        "name": "EMAIL_FAILURE",
        "type": "WebActivity",
        "dependsOn": [
          { "activity": "NB_M_LOAD_CUSTOMERS", "dependencyConditions": ["Failed"] }
        ]
      }
    ]
  }
}
```

**Key conversions:**
- Session → `TridentNotebook` activity
- Decision → `IfCondition` activity
- Email → `WebActivity` (Logic App webhook)
- Scheduler → Pipeline trigger (cron: `0 0 2 * * *`)
- Link conditions → `dependsOn` with `Succeeded`/`Failed`

---

## 5. Complex Workflow — WF_COMPLEX_FINANCE_ETL

📄 [`input/workflows/WF_COMPLEX_FINANCE_ETL.xml`](../input/workflows/WF_COMPLEX_FINANCE_ETL.xml)

This workflow exercises every PowerCenter task type:

| Task | Type | Fabric Equivalent |
|------|------|-------------------|
| TMR_WAIT_MARKET_CLOSE | **Timer** | Wait Activity |
| ASG_INIT_VARS | **Assignment** | Set Variable Activity |
| EVT_WAIT_UPSTREAM | **Event Wait** | Get Metadata + If Condition |
| S_LOAD_TRANSACTIONS, S_LOAD_BALANCES, S_LOAD_FX_RATES | **3 parallel Sessions** | 3 parallel Notebook Activities |
| DEC_VALIDATE_LOADS | **Decision** | If Condition Activity |
| WKL_RECONCILIATION | **Worklet** (nested sub-workflow) | Invoke Pipeline (child pipeline) |
| EVT_RAISE_COMPLETE | **Event Raise** | Set Variable Activity |
| EMAIL_SUCCESS / EMAIL_FAILURE | **Email** | Web Activity |
| SCHED_EOM_1730 | **Scheduler** (monthly, last business day) | Pipeline trigger |

> Run `@pipeline-migration convert workflow WF_COMPLEX_FINANCE_ETL` to generate the output.

---

## 6. Oracle SQL → Spark SQL

### Input: Oracle Stored Procedure

📄 [`input/sql/SP_CALC_RANKINGS.sql`](../input/sql/SP_CALC_RANKINGS.sql)

```sql
-- Oracle-specific: NVL, SYSDATE, ADD_MONTHS, MERGE INTO, analytics
SELECT
    CUSTOMER_ID,
    NVL(SUM(QUANTITY * UNIT_PRICE), 0) AS TOTAL_REVENUE,
    DENSE_RANK() OVER (ORDER BY SUM(...) DESC) AS REVENUE_RANK,
    NTILE(4) OVER (ORDER BY ...) AS QUARTILE,
    LAG(TOTAL_REVENUE, 1, 0) OVER (...) AS PREV_REVENUE,
    LEAD(TOTAL_REVENUE, 1, 0) OVER (...) AS NEXT_REVENUE
FROM SALES.ORDERS
WHERE ORDER_DATE >= ADD_MONTHS(SYSDATE, -12)
GROUP BY CUSTOMER_ID;
```

### Output: Spark SQL

📄 [`output/sql/SQL_SP_CALC_RANKINGS.sql`](../output/sql/SQL_SP_CALC_RANKINGS.sql)

```sql
-- Converted: Oracle → Spark SQL
-- NVL() → COALESCE()
-- SYSDATE → current_timestamp()
-- ADD_MONTHS() → add_months() (1:1)
-- Analytics (DENSE_RANK, NTILE, LAG, LEAD) → 1:1 in Spark SQL
-- MERGE INTO → Delta MERGE INTO

SELECT
    CUSTOMER_ID,
    COALESCE(SUM(QUANTITY * UNIT_PRICE), 0) AS TOTAL_REVENUE,
    DENSE_RANK() OVER (ORDER BY ... DESC) AS REVENUE_RANK,
    ...
FROM SALES.ORDERS
WHERE ORDER_DATE >= ADD_MONTHS(current_timestamp(), -12)
GROUP BY CUSTOMER_ID;
```

---

## 7. Validation Notebook

Automatically generated for every target table.

📄 [`output/validation/VAL_DIM_CUSTOMER.py`](../output/validation/VAL_DIM_CUSTOMER.py)

```python
# Validation Notebook: VAL_DIM_CUSTOMER
# Source: Oracle.SALES.CUSTOMERS → Target: silver.dim_customer

# Level 1: Row Count Comparison
source_count = df_source.count()
target_count = spark.table("silver.dim_customer").count()
results.append(("Row Count", "PASS" if source_count == target_count else "FAIL"))

# Level 2: Column Checksum (MD5)
df_src_hash = df_source.withColumn("row_hash", md5(concat_ws("|", *key_columns)))
df_tgt_hash = df_target.withColumn("row_hash", md5(concat_ws("|", *key_columns)))

# Level 3: Null/Duplicate Checks
null_counts = df_target.select([count(when(isnull(c), c)).alias(c) for c in nullable_not_allowed])
dup_count = df_target.groupBy(key_columns).count().filter("count > 1").count()
```

📄 See also: [`output/validation/test_matrix.md`](../output/validation/test_matrix.md) for the full pass/fail matrix.

---

## 8. Assessment Inventory

📄 [`output/inventory/inventory.json`](../output/inventory/inventory.json)

```json
{
  "generated": "2026-03-23",
  "source_platform": "Informatica PowerCenter",
  "target_platform": "Microsoft Fabric",
  "mappings": [
    {
      "name": "M_LOAD_CUSTOMERS",
      "sources": ["Oracle.SALES.CUSTOMERS"],
      "targets": ["DIM_CUSTOMER"],
      "transformations": ["SQ", "EXP", "FIL"],
      "complexity": "Simple",
      "has_sql_override": false
    },
    {
      "name": "M_LOAD_ORDERS",
      "sources": ["Oracle.SALES.ORDERS", "Oracle.SALES.PRODUCTS"],
      "targets": ["FACT_ORDERS", "AGG_ORDERS_BY_CUSTOMER"],
      "transformations": ["SQ", "LKP", "EXP", "AGG"],
      "complexity": "Complex",
      "has_sql_override": true
    }
  ]
}
```

📄 See also: [`output/inventory/complexity_report.md`](../output/inventory/complexity_report.md), [`output/inventory/dependency_dag.json`](../output/inventory/dependency_dag.json)

---

## 9. IICS Taskflow

📄 [`input/workflows/IICS_TF_DAILY_CONTACTS_ETL.xml`](../input/workflows/IICS_TF_DAILY_CONTACTS_ETL.xml)

IICS uses a different XML schema (`exportMetadata` / `dTemplate`):

```xml
<dTemplate name="TF_DAILY_CONTACTS_ETL" objectType="com.infa.deployment.taskflow">
  <dTemplate name="mt_load_contacts" objectType="com.infa.deployment.mappingtask">
    <dAttribute name="mappingName" value="m_load_contacts"/>
    <dAttribute name="connectionName" value="CONN_SALESFORCE_PROD"/>
  </dTemplate>
  <dTemplate name="gw_check_quality" objectType="com.infa.deployment.exclusivegateway">
    <dAttribute name="condition" value="row_count > 0"/>
  </dTemplate>
  <dLink from="Start" to="mt_load_contacts"/>
  <dLink from="mt_load_contacts" to="gw_check_quality"
    condition="$mt_load_contacts.status = SUCCEEDED"/>
</dTemplate>
```

Additional IICS fixtures:
- 📄 [`IICS_DQ_EMAIL_VALIDATION.xml`](../input/workflows/IICS_DQ_EMAIL_VALIDATION.xml) — Data Quality task with profiling, cleansing, dedup scoring
- 📄 [`IICS_AI_ORDER_EVENTS.xml`](../input/workflows/IICS_AI_ORDER_EVENTS.xml) — Application Integration with REST trigger, Kafka connector

---

## 10. Multi-DB SQL Fixtures

SQL sources from all 6 supported database types:

| File | DB Type | Key Patterns |
|------|---------|-------------|
| [`SP_CALC_RANKINGS.sql`](../input/sql/SP_CALC_RANKINGS.sql) | **Oracle** | NVL, SYSDATE, ADD_MONTHS, MERGE, DECODE, analytics |
| [`SP_SQLSERVER_CUSTOMER_MERGE.sql`](../input/sql/SP_SQLSERVER_CUSTOMER_MERGE.sql) | **SQL Server** | GETDATE, ISNULL, CROSS APPLY, STRING_AGG, TOP, MERGE, TRY/CATCH |
| [`SP_TERADATA_CUSTOMER_STATS.sql`](../input/sql/SP_TERADATA_CUSTOMER_STATS.sql) | **Teradata** | QUALIFY, SAMPLE, VOLATILE TABLE, SEL, COLLECT STATISTICS, FORMAT |
| [`SP_DB2_INVENTORY_REFRESH.sql`](../input/sql/SP_DB2_INVENTORY_REFRESH.sql) | **DB2** | FETCH FIRST, VALUE(), CURRENT DATE, RRN(), WITH UR, GRAPHIC |
| [`SP_MYSQL_USER_ANALYTICS.sql`](../input/sql/SP_MYSQL_USER_ANALYTICS.sql) | **MySQL** | IFNULL, NOW(), GROUP_CONCAT, DATE_FORMAT, backtick identifiers |
| [`SP_POSTGRESQL_REPORTING.sql`](../input/sql/SP_POSTGRESQL_REPORTING.sql) | **PostgreSQL** | ::casts, ILIKE, BIGSERIAL, RETURNING, ARRAY_AGG, GENERATE_SERIES |

Each file is designed to exercise the DB-specific patterns that the SQL migration engine detects and converts to Spark SQL.

---

## Session Configuration

📄 [`input/sessions/session_config.xml`](../input/sessions/session_config.xml)

Two session configs (`default_session_config` and `high_perf_session_config`) showing: DTM buffer sizes, Commit Intervals, Lookup/Sorter/Aggregator/Joiner cache sizes, Maximum Memory, Target Load Type, Recovery Strategy, and Pushdown Optimization settings.

These are mapped to Spark configuration equivalents during notebook generation.

---

## Running the Examples

```bash
# 1. Install
pip install -e ".[dev]"

# 2. Run full migration against all input/ fixtures
informatica-to-fabric

# 3. Review generated output
ls output/notebooks/    # PySpark notebooks
ls output/pipelines/    # Pipeline JSON
ls output/sql/          # Converted SQL
ls output/validation/   # Validation scripts
ls output/inventory/    # Assessment results

# 4. Run tests to verify
python -m pytest tests/ -v
```

Or use agents in VS Code Copilot Chat:

```
@migration-orchestrator start migration
```
