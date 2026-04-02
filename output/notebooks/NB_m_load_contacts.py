# Databricks notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_m_load_contacts
# Migrated from: Informatica mapping m_load_contacts
# Complexity: Medium
# Sources: src_sf_contacts
# Targets: tgt_lh_contacts
# Flow: EXP → port → FIL → LKP → AGG → DM
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
# --- Source: src_sf_contacts ---
# Oracle: SELECT * FROM src_sf_contacts
df_source = spark.table("main.bronze.src_sf_contacts")

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

# CELL 6 — Transformation: LKP
# --- Lookup transformation ---
df_lookup = spark.table("main.bronze.lookup_table")  # TODO: Replace
df = df.join(broadcast(df_lookup), on="KEY", how="left")
# COMMAND ----------

# CELL 7 — Transformation: AGG
# --- Aggregator transformation ---
df_agg = df.groupBy(
    "GROUP_KEY"  # TODO: Replace with actual GROUP BY columns
).agg(
    count("*").alias("RECORD_COUNT"),
    # TODO: Add aggregate expressions from mapping ports
)
df = df_agg
# COMMAND ----------

# CELL 8 — Transformation: DM
# --- Data Masking transformation ---
# Approach 1: Hash-based masking
df = df.withColumn("MASKED_COL", md5(col("SENSITIVE_COL").cast("string")))  # TODO: Replace
# Approach 2: Partial masking
# df = df.withColumn("MASKED_EMAIL", regexp_replace(col("EMAIL"), "(?<=.{2}).(?=.*@)", "*"))
# COMMAND ----------

# CELL 9 — Target Write
# --- Target: tgt_lh_contacts → main.silver.tgt_lh_contacts ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.tgt_lh_contacts")

# COMMAND ----------

# CELL 10 — Audit Log
print(f"Notebook NB_m_load_contacts completed successfully")
print(f"Rows written: {df.count()}")
