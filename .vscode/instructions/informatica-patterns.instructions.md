---
applyTo: "**"
---
# Informatica to Fabric Migration — Shared Patterns & Conventions

> **Shared rules inherited by all 6 agents.** This file defines naming conventions,
> transformation patterns, SQL conversion rules, and pipeline templates that every
> agent must follow consistently.

## Naming Conventions

| Artifact Type | Pattern | Example |
|---|---|---|
| Notebooks | `NB_<mapping_name>` | `NB_M_LOAD_CUSTOMERS` |
| Pipelines | `PL_<workflow_name>` | `PL_WF_DAILY_LOAD` |
| SQL scripts | `SQL_<original_name>` | `SQL_SP_CALC_REVENUE` |
| Validation | `VAL_<target_table>` | `VAL_DIM_CUSTOMER` |
| Inventory | `inventory.json` | `output/inventory/inventory.json` |
| Complexity | `complexity_report.md` | `output/inventory/complexity_report.md` |
| DAG | `dependency_dag.json` | `output/inventory/dependency_dag.json` |

## Lakehouse Layer Convention (Medallion Architecture)

| Layer | Purpose | Characteristics |
|---|---|---|
| 🥉 **Bronze** | Raw ingestion | Schema-on-read, mirrors source structure |
| 🥈 **Silver** | Cleansed & typed | Deduplicated, business keys applied, typed |
| 🥇 **Gold** | Business-ready | Aggregated, curated, ready for reporting |

## Informatica Transformation → PySpark Mapping Reference

### Source Qualifier (SQ)
```python
df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "schema.table").load()
# OR for Lakehouse sources:
df = spark.table("bronze.raw_table")
```

### Expression (EXP)
```python
df = df.withColumn("derived_col", expr("CASE WHEN status = 'A' THEN 'Active' ELSE 'Inactive' END"))
df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
```

### Filter (FIL)
```python
df = df.filter(col("is_active") == True)
df = df.where("amount > 0 AND status != 'DELETED'")
```

### Aggregator (AGG)
```python
df = df.groupBy("department_id").agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary")
)
```

### Joiner (JNR)
```python
df = df_left.join(df_right, df_left["key"] == df_right["key"], "inner")
# For master-detail: use "left" join
df = df_master.join(df_detail, "key", "left")
```

### Lookup (LKP)
```python
# Small lookup table — use broadcast join
from pyspark.sql.functions import broadcast
df = df.join(broadcast(df_lookup), "lookup_key", "left")

# Conditional lookup (with inequality)
df = df.join(df_lookup, (df["key"] == df_lookup["key"]) & (df["date"] >= df_lookup["eff_date"]), "left")
```

### Router (RTR)
```python
df_group1 = df.filter(col("region") == "NORTH")
df_group2 = df.filter(col("region") == "SOUTH")
df_default = df.filter(~col("region").isin("NORTH", "SOUTH"))
```

### Update Strategy (UPD)
```python
# Use Delta MERGE
spark.sql("""
    MERGE INTO silver.target AS t
    USING staging AS s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Sequence Generator
```python
from pyspark.sql.functions import monotonically_increasing_id
df = df.withColumn("seq_id", monotonically_increasing_id())
# OR use Delta identity columns for auto-increment
```

### Rank (RNK)
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
window = Window.partitionBy("group_col").orderBy(col("sort_col").desc())
df = df.withColumn("rank", row_number().over(window))
```

## Oracle SQL → Spark SQL Quick Reference
| Oracle | Spark SQL |
|---|---|
| `NVL(a,b)` | `COALESCE(a,b)` |
| `SYSDATE` | `current_timestamp()` |
| `DECODE(a,b,c,d)` | `CASE WHEN a=b THEN c ELSE d END` |
| `TO_DATE(s,'YYYYMMDD')` | `to_date(s,'yyyyMMdd')` |
| `TO_CHAR(d,'YYYY-MM-DD')` | `date_format(d,'yyyy-MM-dd')` |
| `ROWNUM` | `row_number() OVER(...)` |
| `SUBSTR(s,1,5)` | `substring(s,1,5)` |
| `INSTR(s,'x')` | `instr(s,'x')` (same in Spark) |
| `TRUNC(date)` | `date_trunc('day', date)` |
| `||` (concat) | `concat()` or `concat_ws()` |

## Pipeline Activity Patterns

### Notebook Activity
```json
{
  "type": "NotebookActivity",
  "name": "Run_NB_<mapping>",
  "dependsOn": [],
  "typeProperties": {
    "notebook": { "referenceName": "NB_<mapping>" },
    "parameters": {
      "load_date": { "value": "@pipeline().parameters.load_date", "type": "string" }
    }
  }
}
```

### If Condition (Decision replacement)
```json
{
  "type": "IfCondition",
  "name": "Check_<condition>",
  "typeProperties": {
    "expression": { "value": "@equals(activity('PrevStep').output.status, 'Succeeded')" },
    "ifTrueActivities": [],
    "ifFalseActivities": []
  }
}
```

## Error Handling Convention
- All notebooks should use try/except blocks for critical operations
- Pipeline-level: use `On Failure` dependency paths
- Log errors to a `_migration_log` Delta table in Gold lakehouse

## Lessons Learned (from migration execution)

### XML Parsing Pitfalls
- **SOURCE/TARGET at FOLDER level:** Some Informatica XML exports place `<SOURCE>` and `<TARGET>` elements as siblings of `<MAPPING>` at the `<FOLDER>` level, not nested inside `<MAPPING>`. Always search both locations.
- **IICS vs PowerCenter format:** IICS (Cloud) exports use different root tags (`exportMetadata`, `dTemplate`). Detect format before parsing; PowerCenter uses `POWERMART`, `REPOSITORY`, `FOLDER`.
- **IICS namespace clearing:** IICS XMLs may have `xmlns=""` on `weightedCSPackage` which clears the namespace for child elements. Use a `find_all()` helper that searches both namespaced and non-namespaced tags.
- **IICS Taskflows:** Parse `<dTemplate objectType="com.infa.deployment.taskflow">` for orchestration. Mapping tasks, command tasks, notification tasks, exclusive gateways, and timer events map to workflow equivalents.
- **Encoding issues:** Some exports include null bytes or invalid UTF-8. Use `errors="replace"` and strip `\x00`.

### Complexity Classification Insights
- **Update Strategy = Complex:** UPD transformations always require Delta MERGE, which is inherently complex. Weight UPD at 2 in complexity scoring, not 1.
- **SQL overrides escalate complexity:** Any mapping with SQL overrides should be at least Medium, even if transformations are simple.

### SQL Conversion Patterns
- **Oracle DATE includes time:** Oracle `DATE` type stores both date and time. Map to Spark `TIMESTAMP`, not `DATE`.
- **DECODE nesting:** Deeply nested `DECODE()` calls should be converted to `CASE WHEN` chains, not nested ternaries.
- **Post-session SQL:** `EXEC <stored_proc>` in post-session SQL needs special handling — it doesn't convert to a simple `%%sql` cell. Consider a separate notebook activity in the pipeline.
- **Global Temporary Tables:** `CREATE GLOBAL TEMPORARY TABLE` → `CREATE OR REPLACE TEMP VIEW`. `ON COMMIT PRESERVE/DELETE ROWS` clauses are removed (Spark temp views are session-scoped).
- **Materialized Views:** `CREATE MATERIALIZED VIEW` → Flag as TODO with Delta table + scheduled notebook refresh.
- **Database Links:** `@dblink` references → Flag as TODO with `spark.read.jdbc()` pointing to remote database.

### Session Config & Scheduling
- **Session config properties** (DTM buffer size, Commit Interval, Lookup Cache Size, etc.) are extracted and mapped to Spark equivalents in the inventory.
- **Scheduler conversion:** Informatica schedule names (DAILY, HOURLY, WEEKLY, MONTHLY) are converted to Fabric cron expressions and attached as `ScheduleTrigger` in pipeline JSON.
- **Custom schedules** with time patterns (e.g., `SCHED_06AM`, `RUN_2PM`, `LOAD_0600`) are inferred where possible.

### Pipeline Generation
- **Retry policies are required:** Every activity must have a `policy` block. Fabric defaults may not match Informatica behavior.
- **Worklet nesting:** Limit to 2 levels of `ExecutePipeline` nesting for maintainability. Flatten deeper hierarchies.
- **IICS Taskflows:** IICS uses exclusive gateways (→ IfCondition), parallel gateways (→ multiple dependsOn), and timer events (→ Wait Activity).

### Agent Collaboration
- **Sub-agents cannot write to arbitrary paths:** Each agent should only write to its designated output folder. Cross-agent file access is read-only.
- **Partial results are better than no results:** If one mapping fails to parse, save the rest. Use per-element try/except, not per-file.
