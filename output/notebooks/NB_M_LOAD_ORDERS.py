# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}
# METADATA_END

# CELL 1 — Metadata & Parameters
# Notebook: NB_M_LOAD_ORDERS
# Migrated from: Informatica mapping M_LOAD_ORDERS
# Complexity: Complex
# Sources: Oracle.SALES.ORDERS, Oracle.SALES.PRODUCTS
# Targets: FACT_ORDERS, AGG_ORDERS_BY_CUSTOMER
# Flow: SQ → LKP → EXP → AGG
# Generated: 2026-03-23

from pyspark.sql.functions import (
    col, lit, when, coalesce, concat_ws, current_timestamp,
    count, sum as _sum, avg as _avg, min as _min, max as _max,
    row_number, rank, dense_rank, broadcast, expr, md5
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Parameters (from Informatica $$params / pipeline)
load_date = notebookutils.widgets.get("load_date")
# COMMAND ----------

# CELL 2 — Source Read
# --- Source: Oracle.SALES.ORDERS ---
# Oracle: SELECT * FROM SALES.ORDERS
df_source = spark.table("bronze.orders")

# --- Source: Oracle.SALES.PRODUCTS ---
# Oracle: SELECT * FROM SALES.PRODUCTS
df_source_2 = spark.table("bronze.products")

# SQL Override detected — review converted SQL in output/sql/
# See: SQL_OVERRIDES_M_LOAD_ORDERS.sql
# COMMAND ----------

# CELL 3 — Transformation: LKP
# --- Lookup: LKP_PRODUCTS ---
# Using broadcast join for lookup table (< 100MB)
df_lookup = spark.table("bronze.lookup_table")  # TODO: Replace with actual lookup table
df = df.join(
    broadcast(df_lookup),
    on="LOOKUP_KEY",  # TODO: Replace with actual lookup condition
    how="left"
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

# CELL 5 — Transformation: AGG
# --- Aggregator transformation ---
df_agg = df.groupBy(
    "GROUP_KEY"  # TODO: Replace with actual GROUP BY columns
).agg(
    count("*").alias("RECORD_COUNT"),
    # TODO: Add aggregate expressions from mapping ports
)
df = df_agg
# COMMAND ----------

# CELL 6 — Target Write
# --- Target: FACT_ORDERS → silver.fact_orders ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.fact_orders")

# --- Target: AGG_ORDERS_BY_CUSTOMER → gold.agg_orders_by_customer ---
df_target_2 = df
df_target_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.agg_orders_by_customer")

# COMMAND ----------

# CELL 7 — Audit Log
print(f"Notebook NB_M_LOAD_ORDERS completed successfully")
print(f"Rows written: {df.count()}")
