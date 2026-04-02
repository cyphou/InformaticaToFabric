# Databricks notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_m_sync_accounts
# Migrated from: Informatica mapping m_sync_accounts
# Complexity: Simple
# Sources: src_accounts
# Targets: tgt_accounts
# Flow: EXP
# Generated: 2026-04-02

from pyspark.sql.functions import (
    col, lit, when, coalesce, concat_ws, current_timestamp,
    count, sum as _sum, avg as _avg, min as _min, max as _max,
    row_number, rank, dense_rank, broadcast, expr, md5
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
# COMMAND ----------

# CELL 2 — Source Read
# --- Source: src_accounts ---
# Oracle: SELECT * FROM src_accounts
df_source = spark.table("main.bronze.src_accounts")

# COMMAND ----------

# CELL 3 — Transformation: EXP
# --- Expression transformation ---
# Map Informatica ports to PySpark withColumn / select expressions
df = df_source.withColumn(
    "ETL_LOAD_DATE", current_timestamp()
)
# TODO: Add derived column expressions from mapping ports
# COMMAND ----------

# CELL 4 — Target Write
# --- Target: tgt_accounts → main.silver.tgt_accounts ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.tgt_accounts")

# COMMAND ----------

# CELL 5 — Audit Log
print(f"Notebook NB_m_sync_accounts completed successfully")
print(f"Rows written: {df.count()}")
