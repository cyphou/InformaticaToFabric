# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_m_realtime_inventory_scd2
# Migrated from: Informatica mapping m_realtime_inventory_scd2
# Complexity: Complex
# Sources: src_sap_materials, src_sap_warehouse_bins, src_iot_sensors
# Targets: tgt_silver_inventory, tgt_gold_inventory_dashboard, tgt_alert_queue
# Flow: JNR → LKP → EXP → port → FIL → UNI → SRT → RTR → group → NRM
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
spark.conf.set("spark.sql.shuffle.partitions", "800")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.executor.cores", "4")
# COMMAND ----------

# CELL 2 — Source Read
# --- Source: src_sap_materials ---
# Oracle: SELECT * FROM src_sap_materials
df_source = spark.table("bronze.src_sap_materials")

# --- Source: src_sap_warehouse_bins ---
# Oracle: SELECT * FROM src_sap_warehouse_bins
df_source_2 = spark.table("bronze.src_sap_warehouse_bins")

# --- Source: src_iot_sensors ---
# Oracle: SELECT * FROM src_iot_sensors
df_source_3 = spark.table("bronze.src_iot_sensors")

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
df_lookup = spark.table("bronze.lookup_table")  # TODO: Replace
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

# CELL 7 — Transformation: FIL
# --- Filter transformation ---
df = df.filter(
    col("STATUS") != "INACTIVE"  # TODO: Replace with actual filter condition
)
# COMMAND ----------

# CELL 8 — Transformation: UNI
# --- Union transformation ---
# TODO: Ensure schemas match between DataFrames
df = df.unionByName(df_source_2, allowMissingColumns=True)
# COMMAND ----------

# CELL 9 — Transformation: SRT
# --- Sorter transformation ---
df = df.orderBy(col("SORT_COL").asc())  # TODO: Replace with actual sort columns
# COMMAND ----------

# CELL 10 — Transformation: RTR
# --- Router transformation ---
# Create multiple DataFrames, one per output group
df_group_1 = df.filter(col("CATEGORY") == "A")  # TODO: Replace conditions
df_group_2 = df.filter(col("CATEGORY") == "B")
df_default = df.filter(~col("CATEGORY").isin("A", "B"))
df = df_group_1  # TODO: Select correct group for downstream
# COMMAND ----------

# CELL 11 — Transformation: group
# --- group transformation (UNKNOWN) ---
# This transformation type (group) is not recognized.
# Manual conversion required.
df = df
# COMMAND ----------

# CELL 12 — Transformation: NRM
# --- Normalizer transformation → explode ---
from pyspark.sql.functions import explode, split
df = df.withColumn("NORMALIZED_COL", explode(split(col("ARRAY_COL"), ",")))  # TODO: Replace
# COMMAND ----------

# CELL 13 — Target Write
# --- Target: tgt_silver_inventory → silver.tgt_silver_inventory ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.tgt_silver_inventory")

# --- Target: tgt_gold_inventory_dashboard → gold.tgt_gold_inventory_dashboard ---
df_target_2 = df
df_target_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.tgt_gold_inventory_dashboard")

# --- Target: tgt_alert_queue → silver.tgt_alert_queue ---
df_target_3 = df
df_target_3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.tgt_alert_queue")

# COMMAND ----------

# CELL 14 — Audit Log
print(f"Notebook NB_m_realtime_inventory_scd2 completed successfully")
print(f"Rows written: {df.count()}")
