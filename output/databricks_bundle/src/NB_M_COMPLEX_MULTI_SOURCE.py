# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_M_COMPLEX_MULTI_SOURCE
# Migrated from: Informatica mapping M_COMPLEX_MULTI_SOURCE
# Complexity: Complex
# Sources: Oracle.FINANCE.TRANSACTIONS, Oracle.FINANCE.ACCOUNTS
# Targets: FACT_TXN_HIGH, FACT_TXN_LOW, FACT_TXN_TAGS
# Flow: SQ → JNR → EXP → SQLT → LKP → RTR → RNK → NRM
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
# --- Source: Oracle.FINANCE.TRANSACTIONS ---
# Oracle: SELECT * FROM FINANCE.TRANSACTIONS
df_source = spark.table("bronze.transactions")

# --- Source: Oracle.FINANCE.ACCOUNTS ---
# Oracle: SELECT * FROM FINANCE.ACCOUNTS
df_source_2 = spark.table("bronze.accounts")

# SQL Override detected — review converted SQL in output/sql/
# See: SQL_OVERRIDES_M_COMPLEX_MULTI_SOURCE.sql
# COMMAND ----------

# CELL 3 — Transformation: JNR
# --- Joiner transformation ---
# TODO: Identify master and detail sources from mapping
df = df.join(
    df_source_2,  # TODO: Replace with actual detail DataFrame
    on="JOIN_KEY",  # TODO: Replace with actual join condition
    how="inner"  # TODO: inner/left/right/full based on mapping
)
# COMMAND ----------

# CELL 4 — Transformation: EXP
# --- Expression transformation ---
# Map Informatica ports to PySpark withColumn / select expressions
df = df.withColumn(
    "ETL_LOAD_DATE", current_timestamp()
)
# TODO: Add derived column expressions from mapping ports
# COMMAND ----------

# CELL 5 — Transformation: SQLT
# --- SQL Transformation → spark.sql() ---
df.createOrReplaceTempView("temp_input")
# df = spark.sql("SELECT * FROM temp_input WHERE ...")  # TODO: Add SQL logic
df = df
# COMMAND ----------

# CELL 6 — Transformation: LKP
# --- Lookup: LKP_FRAUD_BLACKLIST ---
# Using broadcast join for lookup table (< 100MB)
df_lookup = spark.table("bronze.lookup_table")  # TODO: Replace with actual lookup table
df = df.join(
    broadcast(df_lookup),
    on="LOOKUP_KEY",  # TODO: Replace with actual lookup condition
    how="left"
)
# COMMAND ----------

# CELL 7 — Transformation: RTR
# --- Router transformation ---
# Create multiple DataFrames, one per output group
df_group_1 = df.filter(col("CATEGORY") == "A")  # TODO: Replace conditions
df_group_2 = df.filter(col("CATEGORY") == "B")
df_default = df.filter(~col("CATEGORY").isin("A", "B"))
df = df_group_1  # TODO: Select correct group for downstream
# COMMAND ----------

# CELL 8 — Transformation: RNK
# --- Rank transformation ---
w = Window.partitionBy("GROUP_COL").orderBy(col("RANK_COL").desc())  # TODO: Replace
df = df.withColumn("RANK", row_number().over(w))
# COMMAND ----------

# CELL 9 — Transformation: NRM
# --- Normalizer transformation → explode ---
from pyspark.sql.functions import explode, split
df = df.withColumn("NORMALIZED_COL", explode(split(col("ARRAY_COL"), ",")))  # TODO: Replace
# COMMAND ----------

# CELL 10 — Target Write
# --- Target: FACT_TXN_HIGH → silver.fact_txn_high ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.fact_txn_high")

# --- Target: FACT_TXN_LOW → silver.fact_txn_low ---
df_target_2 = df
df_target_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.fact_txn_low")

# --- Target: FACT_TXN_TAGS → silver.fact_txn_tags ---
df_target_3 = df
df_target_3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.fact_txn_tags")

# COMMAND ----------

# CELL 11 — Audit Log
print(f"Notebook NB_M_COMPLEX_MULTI_SOURCE completed successfully")
print(f"Rows written: {df.count()}")
