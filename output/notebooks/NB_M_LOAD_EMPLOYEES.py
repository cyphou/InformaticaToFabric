# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}
# METADATA_END

# CELL 1 — Metadata & Parameters
# Notebook: NB_M_LOAD_EMPLOYEES
# Migrated from: Informatica mapping M_LOAD_EMPLOYEES
# Complexity: Complex
# Sources: Oracle.HR.EMPLOYEES
# Targets: DIM_EMPLOYEE
# Flow: SQ → EXP → FIL → SRT → LKP → SQLT
# Generated: 2026-03-24

from pyspark.sql.functions import (
    col, lit, when, coalesce, concat_ws, current_timestamp,
    count, sum as _sum, avg as _avg, min as _min, max as _max,
    row_number, rank, dense_rank, broadcast, expr, md5
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
# COMMAND ----------

# CELL 2 — Source Read
# --- Source: Oracle.HR.EMPLOYEES ---
# Oracle: SELECT * FROM HR.EMPLOYEES
df_source = spark.table("bronze.employees")

# SQL Override detected — review converted SQL in output/sql/
# See: SQL_OVERRIDES_M_LOAD_EMPLOYEES.sql
# COMMAND ----------

# CELL 3 — Transformation: EXP
# --- Expression transformation ---
# Map Informatica ports to PySpark withColumn / select expressions
df = df.withColumn(
    "ETL_LOAD_DATE", current_timestamp()
)
# TODO: Add derived column expressions from mapping ports
# COMMAND ----------

# CELL 4 — Transformation: FIL
# --- Filter transformation ---
df = df.filter(
    col("STATUS") != "INACTIVE"  # TODO: Replace with actual filter condition
)
# COMMAND ----------

# CELL 5 — Transformation: SRT
# --- Sorter transformation ---
df = df.orderBy(col("SORT_COL").asc())  # TODO: Replace with actual sort columns
# COMMAND ----------

# CELL 6 — Transformation: LKP
# --- Lookup: LKP_DEPT_MGR ---
# Using broadcast join for lookup table (< 100MB)
df_lookup = spark.table("bronze.lookup_table")  # TODO: Replace with actual lookup table
df = df.join(
    broadcast(df_lookup),
    on="LOOKUP_KEY",  # TODO: Replace with actual lookup condition
    how="left"
)
# COMMAND ----------

# CELL 7 — Transformation: SQLT
# --- SQL Transformation → spark.sql() ---
df.createOrReplaceTempView("temp_input")
# df = spark.sql("SELECT * FROM temp_input WHERE ...")  # TODO: Add SQL logic
df = df
# COMMAND ----------

# CELL 8 — Target Write
# --- Target: DIM_EMPLOYEE → silver.dim_employee ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.dim_employee")

# COMMAND ----------

# CELL 9 — Audit Log
print(f"Notebook NB_M_LOAD_EMPLOYEES completed successfully")
print(f"Rows written: {df.count()}")
