# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_MI_BULK_LOAD_PRODUCTS
# Migrated from: Informatica mapping MI_BULK_LOAD_PRODUCTS
# Complexity: Simple
# Sources: S3_LANDING
# Targets: Lakehouse_Bronze
# Flow: SQ → TGT
# Generated: 2026-04-08

from pyspark.sql.functions import (
    col, lit, when, coalesce, concat_ws, current_timestamp,
    count, sum as _sum, avg as _avg, min as _min, max as _max,
    row_number, rank, dense_rank, broadcast, expr, md5
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Performance tuning (auto-generated based on mapping complexity)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
# COMMAND ----------

# CELL 2 — Source Read
# --- Source: S3_LANDING ---
# Oracle: SELECT * FROM S3_LANDING
df_source = spark.table("bronze.s3_landing")

# COMMAND ----------

# CELL 3 — Transformation: TGT
# --- TGT transformation (UNKNOWN) ---
# This transformation type (TGT) is not recognized.
# Manual conversion required.
df = df
# COMMAND ----------

# CELL 4 — Target Write
# --- Target: Lakehouse_Bronze → silver.lakehouse_bronze ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.lakehouse_bronze")

# COMMAND ----------

# CELL 5 — Audit Log
print(f"Notebook NB_MI_BULK_LOAD_PRODUCTS completed successfully")
print(f"Rows written: {df.count()}")
