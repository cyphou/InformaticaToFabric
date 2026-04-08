# Databricks notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_M_UPSERT_INVENTORY
# Migrated from: Informatica mapping M_UPSERT_INVENTORY
# Complexity: Complex
# Sources: Oracle.SALES.STG_INVENTORY
# Targets: DIM_INVENTORY
# Flow: SQ → EXP → UPD
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
# --- Source: Oracle.SALES.STG_INVENTORY ---
# Oracle: SELECT * FROM SALES.STG_INVENTORY
df_source = spark.table("main.bronze.stg_inventory")

# COMMAND ----------

# CELL 3 — Transformation: EXP
# --- Expression transformation ---
# Map Informatica ports to PySpark withColumn / select expressions
df = df.withColumn(
    "ETL_LOAD_DATE", current_timestamp()
)
# TODO: Add derived column expressions from mapping ports
# COMMAND ----------

# CELL 4 — Transformation: UPD
# --- Update Strategy → Delta MERGE ---
target_table = DeltaTable.forName(spark, "main.silver.dim_inventory")
target_table.alias('tgt').merge(
    df.alias('src'),
    'tgt.ID = src.ID'  # TODO: Replace with actual merge key
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
df = df  # Pass through for downstream
# COMMAND ----------

# CELL 5 — Target Write
# MERGE handled in Update Strategy cell above → main.silver.dim_inventory
# COMMAND ----------

# CELL 6 — Audit Log
print(f"Notebook NB_M_UPSERT_INVENTORY completed successfully")
print(f"Rows written: {df.count()}")
