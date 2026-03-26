# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}
# METADATA_END

# CELL 1 — Metadata & Parameters
# Notebook: NB_M_LOAD_CUSTOMERS
# Migrated from: Informatica mapping M_LOAD_CUSTOMERS
# Complexity: Simple
# Sources: Oracle.SALES.CUSTOMERS
# Targets: DIM_CUSTOMER
# Flow: SQ → EXP → FIL
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
# --- Source: Oracle.SALES.CUSTOMERS ---
# Oracle: SELECT * FROM SALES.CUSTOMERS
df_source = spark.table("bronze.customers")

# COMMAND ----------

# CELL 3 — Transformation: EXP
# --- Expression transformation ---
# Map Informatica ports to PySpark withColumn / select expressions
df = df.withColumn(
    "ETL_LOAD_DATE", current_timestamp()
)
# TODO: Add derived column expressions from mapping ports
# COMMAND ----------

# CELL 4 — Transformation: FIL
# --- Filter transformation ---
df = df.filter(
    col("STATUS") != "INACTIVE"  # TODO: Replace with actual filter condition
)
# COMMAND ----------

# CELL 5 — Target Write
# --- Target: DIM_CUSTOMER → silver.dim_customer ---
df = df
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.dim_customer")

# COMMAND ----------

# CELL 6 — Audit Log
print(f"Notebook NB_M_LOAD_CUSTOMERS completed successfully")
print(f"Rows written: {df.count()}")
