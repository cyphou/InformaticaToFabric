# Databricks notebook source

# METADATA_START
# {{"language_info":{{"name":"python"}},"kernel_info":{{"name":"databricks"}}}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_{notebook_name}
# Migrated from: Informatica mapping {mapping_name}
# Pattern: Databricks Auto Loader (cloudFiles) → Delta Sink
# Generated: {generated_date}

from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit
)

# --- Auto Loader Configuration ---
source_path = "{source_path}"  # Cloud storage path (abfss://, s3://, gs://)
schema_location = "{schema_location}"  # Schema evolution tracking

# COMMAND ----------

# CELL 2 — Read Stream with Auto Loader
df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "{file_format}")  # csv, json, parquet, avro
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 100)
    .load(source_path)
)

# Add file metadata
df_with_meta = (
    df_raw
    .withColumn("_source_file", input_file_name())
    .withColumn("_load_ts", current_timestamp())
)
# COMMAND ----------

# CELL 3 — Transformations
# TODO: Add mapping-specific transformations here
df_transformed = df_with_meta

# COMMAND ----------

# CELL 4 — Write Stream to Delta Lake
checkpoint_path = "{checkpoint_path}"
target_table = "{target_table}"

query = (
    df_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")  # Support schema evolution
    .trigger(availableNow=True)  # Process all available files then stop
    .toTable(target_table)
)

query.awaitTermination()
# COMMAND ----------

# CELL 5 — Audit
print(f"Auto Loader notebook NB_{notebook_name} completed")
print(f"Source: {{source_path}}")
print(f"Target: {{target_table}}")
