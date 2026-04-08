# Databricks notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_m_cdc_order_pipeline
# Migrated from: Informatica mapping m_cdc_order_pipeline
# Complexity: Complex
# Sources: src_ods_orders, src_ods_order_lines, src_pg_products
# Targets: tgt_bronze_cdc_events, tgt_silver_orders, tgt_gold_daily_summary
# Flow: JNR → LKP → EXP → port → RTR → group → UPD → AGG → RNK
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
# --- Source: src_ods_orders ---
# Oracle: SELECT * FROM src_ods_orders
df_source = spark.table("main.bronze.src_ods_orders")

# --- Source: src_ods_order_lines ---
# Oracle: SELECT * FROM src_ods_order_lines
df_source_2 = spark.table("main.bronze.src_ods_order_lines")

# --- Source: src_pg_products ---
# Oracle: SELECT * FROM src_pg_products
df_source_3 = spark.table("main.bronze.src_pg_products")

# COMMAND ----------

# CELL 3 — Transformation: JNR
# --- Joiner transformation ---
# TODO: Identify master and detail sources from mapping
df = df_source.join(
    df_source_2,  # TODO: Replace with actual detail DataFrame
    on="JOIN_KEY",  # TODO: Replace with actual join condition
    how="inner"  # TODO: inner/left/right/full based on mapping
)
# COMMAND ----------

# CELL 4 — Transformation: LKP
# --- Lookup transformation ---
df_lookup = spark.table("main.bronze.lookup_table")  # TODO: Replace
df = df.join(broadcast(df_lookup), on="KEY", how="left")
# COMMAND ----------

# CELL 5 — Transformation: EXP
# --- Expression transformation ---
# Map Informatica ports to PySpark withColumn / select expressions
df = df.withColumn(
    "ETL_LOAD_DATE", current_timestamp()
)
# TODO: Add derived column expressions from mapping ports
# COMMAND ----------

# CELL 6 — Transformation: port
# --- port transformation (UNKNOWN) ---
# This transformation type (port) is not recognized.
# Manual conversion required.
df = df
# COMMAND ----------

# CELL 7 — Transformation: RTR
# --- Router transformation ---
# Create multiple DataFrames, one per output group
df_group_1 = df.filter(col("CATEGORY") == "A")  # TODO: Replace conditions
df_group_2 = df.filter(col("CATEGORY") == "B")
df_default = df.filter(~col("CATEGORY").isin("A", "B"))
df = df_group_1  # TODO: Select correct group for downstream
# COMMAND ----------

# CELL 8 — Transformation: group
# --- group transformation (UNKNOWN) ---
# This transformation type (group) is not recognized.
# Manual conversion required.
df = df
# COMMAND ----------

# CELL 9 — Transformation: UPD
# --- Update Strategy → Delta MERGE ---
target_table = DeltaTable.forName(spark, "main.silver.tgt_bronze_cdc_events")
target_table.alias('tgt').merge(
    df.alias('src'),
    'tgt.ID = src.ID'  # TODO: Replace with actual merge key
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
df = df  # Pass through for downstream
# COMMAND ----------

# CELL 10 — Transformation: AGG
# --- Aggregator transformation ---
df_agg = df.groupBy(
    "GROUP_KEY"  # TODO: Replace with actual GROUP BY columns
).agg(
    count("*").alias("RECORD_COUNT"),
    # TODO: Add aggregate expressions from mapping ports
)
df = df_agg
# COMMAND ----------

# CELL 11 — Transformation: RNK
# --- Rank transformation ---
w = Window.partitionBy("GROUP_COL").orderBy(col("RANK_COL").desc())  # TODO: Replace
df = df.withColumn("RANK", row_number().over(w))
# COMMAND ----------

# CELL 12 — Target Write
# MERGE handled in Update Strategy cell above → main.silver.tgt_bronze_cdc_events
# --- Target: tgt_silver_orders → main.silver.tgt_silver_orders ---
df_target_2 = df
df_target_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.tgt_silver_orders")

# --- Target: tgt_gold_daily_summary → main.gold.tgt_gold_daily_summary ---
df_target_3 = df
df_target_3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.tgt_gold_daily_summary")

# COMMAND ----------

# CELL 13 — Audit Log
print(f"Notebook NB_m_cdc_order_pipeline completed successfully")
print(f"Rows written: {df.count()}")
