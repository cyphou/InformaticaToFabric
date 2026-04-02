# Databricks notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_m_customer_360
# Migrated from: Informatica mapping m_customer_360
# Complexity: Complex
# Sources: src_sf_contacts, src_sf_accounts, src_erp_customers, src_erp_transactions, src_snow_scores, src_api_firmographics
# Targets: tgt_silver_customer, tgt_gold_customer_360
# Flow: JNR → LKP → EXP → port → FIL → SRT → RTR → group → UPD → SEQ
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

# --- Source: src_sf_accounts ---
# Oracle: SELECT * FROM src_sf_accounts
df_source_2 = spark.table("main.bronze.src_sf_accounts")

# --- Source: src_erp_customers ---
# Oracle: SELECT * FROM src_erp_customers
df_source_3 = spark.table("main.bronze.src_erp_customers")

# --- Source: src_erp_transactions ---
# Oracle: SELECT * FROM src_erp_transactions
df_source_4 = spark.table("main.bronze.src_erp_transactions")

# --- Source: src_snow_scores ---
# Oracle: SELECT * FROM src_snow_scores
df_source_5 = spark.table("main.bronze.src_snow_scores")

# --- Source: src_api_firmographics ---
# Oracle: SELECT * FROM src_api_firmographics
df_source_6 = spark.table("main.bronze.src_api_firmographics")

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

# CELL 7 — Transformation: FIL
# --- Filter transformation ---
df = df.filter(
    col("STATUS") != "INACTIVE"  # TODO: Replace with actual filter condition
)
# COMMAND ----------

# CELL 8 — Transformation: SRT
# --- Sorter transformation ---
df = df.orderBy(col("SORT_COL").asc())  # TODO: Replace with actual sort columns
# COMMAND ----------

# CELL 9 — Transformation: RTR
# --- Router transformation ---
# Create multiple DataFrames, one per output group
df_group_1 = df.filter(col("CATEGORY") == "A")  # TODO: Replace conditions
df_group_2 = df.filter(col("CATEGORY") == "B")
df_default = df.filter(~col("CATEGORY").isin("A", "B"))
df = df_group_1  # TODO: Select correct group for downstream
# COMMAND ----------

# CELL 10 — Transformation: group
# --- group transformation (UNKNOWN) ---
# This transformation type (group) is not recognized.
# Manual conversion required.
df = df
# COMMAND ----------

# CELL 11 — Transformation: UPD
# --- Update Strategy → Delta MERGE ---
target_table = DeltaTable.forName(spark, "main.silver.tgt_silver_customer")
target_table.alias('tgt').merge(
    df.alias('src'),
    'tgt.ID = src.ID'  # TODO: Replace with actual merge key
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
df = df  # Pass through for downstream
# COMMAND ----------

# CELL 12 — Transformation: SEQ
# --- Sequence Generator ---
from pyspark.sql.functions import monotonically_increasing_id
df = df.withColumn("SEQ_ID", monotonically_increasing_id())
# COMMAND ----------

# CELL 13 — Target Write
# MERGE handled in Update Strategy cell above → main.silver.tgt_silver_customer
# --- Target: tgt_gold_customer_360 → main.gold.tgt_gold_customer_360 ---
df_target_2 = df
df_target_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.gold.tgt_gold_customer_360")

# COMMAND ----------

# CELL 14 — Audit Log
print(f"Notebook NB_m_customer_360 completed successfully")
print(f"Rows written: {df.count()}")
