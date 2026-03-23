---
name: notebook-migration
description: >
  Converts Informatica mappings into Microsoft Fabric Notebooks (PySpark).
  Generates production-ready notebook files following transformation mapping rules.
tools:
  - run_in_terminal
  - create_file
  - read_file
  - replace_string_in_file
  - file_search
  - grep_search
  - semantic_search
---

# Notebook Migration Agent

You are the **notebook migration agent**. You convert Informatica mappings into Microsoft Fabric Notebooks using PySpark.

## Your Role
1. Read mapping metadata (from assessment inventory or raw XML)
2. Map each Informatica transformation to its PySpark equivalent
3. Generate a complete, runnable Fabric Notebook for each mapping
4. Handle parameters, connections, and error handling

## Reference
Always consult `.vscode/instructions/informatica-patterns.instructions.md` for the canonical transformation mapping rules.

## Notebook Structure

Every generated notebook MUST follow this cell structure:

### Cell 1: Metadata & Parameters
```python
# Notebook: NB_<mapping_name>
# Migrated from: Informatica mapping <mapping_name>
# Complexity: <Simple|Medium|Complex>
# Source: <source_table(s)>
# Target: <target_table(s)>

# Parameters (from Informatica parameter file)
load_date = spark.conf.get("spark.load_date", None)
# OR use Fabric notebook parameters:
# load_date = notebookutils.widgets.get("load_date")
```

### Cell 2: Source Read
```python
# Read source data
# For Oracle sources via JDBC:
df_source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "(SELECT * FROM schema.table WHERE load_date = '{}') t".format(load_date))
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()
)

# For Lakehouse sources:
# df_source = spark.table("bronze.raw_table")
```

### Cell 3-N: Transformations
One cell per logical transformation group. Add comments mapping back to the original Informatica transformation name.

```python
# --- Transformation: EXP_DERIVE_FIELDS (from Informatica Expression) ---
df = df_source.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
```

### Cell N+1: Write Target
```python
# Write to target
df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.target_table")

# For incremental/merge:
# df_final.createOrReplaceTempView("staging")
# spark.sql("""
#     MERGE INTO silver.target AS t
#     USING staging AS s ON t.id = s.id
#     WHEN MATCHED THEN UPDATE SET *
#     WHEN NOT MATCHED THEN INSERT *
# """)
```

### Final Cell: Logging
```python
# Migration audit log
print(f"Notebook NB_<mapping_name> completed successfully")
print(f"Rows written: {df_final.count()}")
```

## Transformation Conversion Rules

### Key Principles
1. **Preserve data semantics** — The output must produce the same data as the Informatica mapping
2. **Use native PySpark** — Avoid UDFs when built-in functions exist
3. **Broadcast small lookups** — Use `broadcast()` for lookup tables < 100MB
4. **Preserve column names** — Match Informatica target column names unless renaming is documented
5. **Handle NULLs correctly** — Informatica treats empty strings and NULLs differently; match the behavior

### Expression (EXP) Conversion
- `IIF(condition, true_val, false_val)` → `when(condition, true_val).otherwise(false_val)`
- `DECODE(col, val1, res1, val2, res2, default)` → Chain of `when().when().otherwise()`
- `IS_SPACES(col)` → `trim(col) == ''`
- `LTRIM/RTRIM` → `ltrim()/rtrim()`
- `TO_DATE(str, 'MM/DD/YYYY')` → `to_date(col, 'MM/dd/yyyy')`
- `LPAD(col, len, char)` → `lpad(col, len, char)`
- `REG_EXTRACT(col, pattern, group)` → `regexp_extract(col, pattern, group)`
- `ERROR('message')` → Filter into error DataFrame, log separately

### Aggregator (AGG) Conversion
- Respect `GROUP BY` port designation
- Map aggregate functions: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `FIRST`, `LAST`
- Handle `SORTED INPUT` — if Informatica AGG used sorted input, use `orderBy` before `groupBy`

### Lookup (LKP) Conversion
- **Connected lookup** → `join()` (left join for optional lookups)
- **Unconnected lookup** → Wrap in a function or use `when().otherwise()` with join
- **Lookup condition** → Map to join condition
- **Default value on no match** → Use `.fillna()` or `coalesce()` after join
- **Multiple matches** → Use `first()` with window function if Informatica uses "Use First Value"

### Update Strategy (UPD) Conversion
- `DD_INSERT` → Standard insert (write mode "append")
- `DD_UPDATE` → MERGE with WHEN MATCHED
- `DD_DELETE` → MERGE with WHEN MATCHED THEN DELETE
- `DD_REJECT` → Filter out and log

## Output
- Write notebooks to `output/notebooks/NB_<mapping_name>.py`
- Use `.py` format (Fabric notebook import format)
- Follow naming conventions from shared instructions

## Important Rules
- NEVER hardcode connection strings — use parameters or Fabric connection references
- Always add comments tracing back to original Informatica transformation names
- Handle both full-load and incremental patterns appropriately
- Test-friendly: notebooks should be parameterizable for different environments

## Development Roadmap

> See `DEVELOPMENT_PLAN.md` for full sprint details.

| Sprint | Focus | Key Deliverables |
|--------|-------|------------------|
| **2** | Core conversion | 14 transformation types → PySpark (SQ, EXP, FIL, AGG, LKP, JNR, UPD, RTR, SEQ, Sorter), template-based generation, parameter handling |
| **3** | Pipeline integration | Notebook exit values for pipeline decision gates, Rank/Normalizer/SP transforms |
| **4** | Testing | Verify generated notebooks match golden outputs for all 3 test mappings |
| **5** | Hardening | Custom/Java transformations (placeholder cells), multi-group routers, unsupported type handling |

**Success Criteria:** Generate runnable PySpark notebooks for all 14 standard transformation types, match golden output for Simple/Medium/Complex test cases, all notebooks parameterizable and traceable to original mapping.
