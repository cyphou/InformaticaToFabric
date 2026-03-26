# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}
# METADATA_END

# CELL 1 — Metadata & Parameters
# Notebook: NB_SYNC_CUSTOMER_DATA
# Migrated from: Informatica mapping SYNC_CUSTOMER_DATA
# Complexity: Simple
# Sources: Oracle_CRM
# Targets: Lakehouse_Silver
# Flow: SQ → TGT
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
# --- Source: Oracle_CRM ---
# Oracle: SELECT * FROM Oracle_CRM
df_source = spark.table("bronze.oracle_crm")

# COMMAND ----------

# CELL 3 — Transformation: TGT
# --- TGT transformation (UNKNOWN) ---
# This transformation type (TGT) is not recognized.
# Manual conversion required.
df = df
# COMMAND ----------

# CELL 4 — Target Write
# --- Target: Lakehouse_Silver → silver.lakehouse_silver ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.lakehouse_silver")

# COMMAND ----------

# CELL 5 — Audit Log
print(f"Notebook NB_SYNC_CUSTOMER_DATA completed successfully")
print(f"Rows written: {df.count()}")
