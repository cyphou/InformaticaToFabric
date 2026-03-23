# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}
# METADATA_END

# CELL 1 - Metadata & Parameters
# Notebook: NB_M_LOAD_CUSTOMERS
# Migrated from: Informatica mapping M_LOAD_CUSTOMERS
# Complexity: Simple
# Source: SALES.CUSTOMERS (bronze.customers)
# Target: silver.dim_customer
# Flow: SQ_CUSTOMERS -> EXP_DERIVE_FIELDS -> FIL_ACTIVE_CUSTOMERS -> DIM_CUSTOMER

from pyspark.sql.functions import col, concat_ws, when, current_timestamp, lit

# CELL 2 - Source Read: SQ_CUSTOMERS
# --- Source Qualifier: SQ_CUSTOMERS (from Informatica Source Qualifier) ---
df_source = spark.table("bronze.customers")

# CELL 3 - Transformation: EXP_DERIVE_FIELDS (from Informatica Expression)
# --- Expression: EXP_DERIVE_FIELDS ---
# FULL_NAME = FIRST_NAME || ' ' || LAST_NAME
# STATUS_LABEL = IIF(STATUS='A','Active', IIF(STATUS='I','Inactive', IIF(STATUS='S','Suspended','Unknown')))
# ETL_LOAD_DATE = SYSDATE

df_derived = (
    df_source
    .withColumn("FULL_NAME", concat_ws(" ", col("FIRST_NAME"), col("LAST_NAME")))
    .withColumn(
        "STATUS_LABEL",
        when(col("STATUS") == "A", lit("Active"))
        .when(col("STATUS") == "I", lit("Inactive"))
        .when(col("STATUS") == "S", lit("Suspended"))
        .otherwise(lit("Unknown"))
    )
    .withColumn("ETL_LOAD_DATE", current_timestamp())
)

# CELL 4 - Transformation: FIL_ACTIVE_CUSTOMERS (from Informatica Filter)
# --- Filter: FIL_ACTIVE_CUSTOMERS ---
# Condition: STATUS_LABEL != 'Unknown'

df_filtered = df_derived.filter(col("STATUS_LABEL") != "Unknown")

# CELL 5 - Target Write: DIM_CUSTOMER
# --- Target: DIM_CUSTOMER -> silver.dim_customer (overwrite) ---

df_final = df_filtered

df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.dim_customer")

# CELL 6 - Audit Log
print(f"Notebook NB_M_LOAD_CUSTOMERS completed successfully")
print(f"Rows written: {df_final.count()}")
