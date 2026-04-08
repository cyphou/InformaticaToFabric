# Databricks notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_m_inventory_snapshot
# Migrated from: Informatica mapping m_inventory_snapshot
# Complexity: Simple
# Sources: src_silver_inventory
# Targets: tgt_gold_inventory_history
# Flow: EXP → port
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
# --- Source: src_silver_inventory ---
# Oracle: SELECT * FROM src_silver_inventory
df_source = spark.table("main.bronze.src_silver_inventory")

# COMMAND ----------

# CELL 3 — Transformation: EXP
# --- Expression transformation ---
# Map Informatica ports to PySpark withColumn / select expressions
df = df_source.withColumn(
    "ETL_LOAD_DATE", current_timestamp()
)
# TODO: Add derived column expressions from mapping ports
# COMMAND ----------

# CELL 4 — Transformation: port
# --- port transformation (UNKNOWN) ---
# This transformation type (port) is not recognized.
# Manual conversion required.
df = df
# COMMAND ----------

# CELL 5 — Target Write
# --- Target: tgt_gold_inventory_history → main.gold.tgt_gold_inventory_history ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.tgt_gold_inventory_history")

# COMMAND ----------

# CELL 6 — Audit Log
print(f"Notebook NB_m_inventory_snapshot completed successfully")
print(f"Rows written: {df.count()}")
