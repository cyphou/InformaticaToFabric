# Databricks notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_m_customer_activity_log
# Migrated from: Informatica mapping m_customer_activity_log
# Complexity: Simple
# Sources: src_kafka_events
# Targets: tgt_bronze_events
# Flow: EXP → port → FIL
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
# --- Source: src_kafka_events ---
# Oracle: SELECT * FROM src_kafka_events
df_source = spark.table("main.bronze.src_kafka_events")

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

# CELL 5 — Transformation: FIL
# --- Filter transformation ---
df = df.filter(
    col("STATUS") != "INACTIVE"  # TODO: Replace with actual filter condition
)
# COMMAND ----------

# CELL 6 — Target Write
# --- Target: tgt_bronze_events → main.silver.tgt_bronze_events ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.tgt_bronze_events")

# COMMAND ----------

# CELL 7 — Audit Log
print(f"Notebook NB_m_customer_activity_log completed successfully")
print(f"Rows written: {df.count()}")
