# Fabric Notebook Template — Migrated from Informatica
# Notebook: NB_<MAPPING_NAME>
# Source Mapping: <MAPPING_NAME>
# Complexity: <Simple|Medium|Complex>
# Migration Date: <DATE>

# %% Cell 1: Parameters
# Mapped from Informatica parameter file: <param_file.prm>
# Uncomment and configure as needed

# load_date = notebookutils.widgets.get("load_date")  # Fabric parameter
# environment = notebookutils.widgets.get("environment")  # DEV / UAT / PROD

# %% Cell 2: Configuration
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Source configuration
# For Oracle JDBC sources:
# jdbc_url = "jdbc:oracle:thin:@<host>:<port>/<service>"
# jdbc_properties = {"user": "<user>", "password": notebookutils.credentials.getSecret("<vault>", "<secret>"), "driver": "oracle.jdbc.driver.OracleDriver"}

# For Lakehouse sources:
source_lakehouse = "Bronze"
target_lakehouse = "Silver"

# %% Cell 3: Read Source
# --- Source Qualifier: SQ_<source_name> ---

# Option A: Read from Lakehouse
df_source = spark.table(f"{source_lakehouse}.<source_table>")

# Option B: Read from Oracle via JDBC
# df_source = spark.read.jdbc(url=jdbc_url, table="<schema.table>", properties=jdbc_properties)

# Option C: Read with SQL override
# df_source = spark.read.jdbc(url=jdbc_url, table="(<SQL_OVERRIDE>) t", properties=jdbc_properties)

print(f"Source rows read: {df_source.count()}")

# %% Cell 4: Transformations
# --- Expression: EXP_<name> ---
# Map each Informatica port to a withColumn / select expression

# df = df_source.withColumn("derived_col", expr("..."))

# --- Filter: FIL_<name> ---
# df = df.filter(col("status") == "ACTIVE")

# --- Lookup: LKP_<name> ---
# df_lookup = spark.table(f"{source_lakehouse}.<lookup_table>")
# df = df.join(broadcast(df_lookup), df["key"] == df_lookup["key"], "left")

# --- Aggregator: AGG_<name> ---
# df = df.groupBy("group_col").agg(count("*").alias("cnt"), sum("amount").alias("total"))

# --- Joiner: JNR_<name> ---
# df = df_master.join(df_detail, "join_key", "inner")

# --- Router: RTR_<name> ---
# df_group1 = df.filter(col("category") == "A")
# df_group2 = df.filter(col("category") == "B")

# --- Update Strategy: UPD_<name> ---
# Handled in the write cell below (MERGE)

# %% Cell 5: Final Select / Rename
# Select and rename columns to match target schema
df_final = df_source  # Replace with actual transformed DataFrame

# df_final = df_final.select(
#     col("source_col1").alias("target_col1"),
#     col("source_col2").alias("target_col2"),
#     current_timestamp().alias("etl_load_timestamp")
# )

# %% Cell 6: Write Target

# Option A: Full overwrite
df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target_lakehouse}.<target_table>")

# Option B: Append
# df_final.write.format("delta").mode("append").saveAsTable(f"{target_lakehouse}.<target_table>")

# Option C: Merge (Upsert) — for Update Strategy
# df_final.createOrReplaceTempView("staging")
# spark.sql(f"""
#     MERGE INTO {target_lakehouse}.<target_table> AS t
#     USING staging AS s
#     ON t.<key> = s.<key>
#     WHEN MATCHED THEN UPDATE SET *
#     WHEN NOT MATCHED THEN INSERT *
# """)

print(f"Target rows written: {df_final.count()}")

# %% Cell 7: Post-processing & Audit
print(f"Notebook NB_<MAPPING_NAME> completed successfully")
