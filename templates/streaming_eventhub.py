# Fabric notebook source

# METADATA_START
# {{"language_info":{{"name":"python"}},"kernel_info":{{"name":"synapse_pyspark"}}}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_{notebook_name}
# Migrated from: Informatica mapping {mapping_name}
# Pattern: Structured Streaming — Azure Event Hub Source → Delta Sink
# Generated: {generated_date}

from pyspark.sql.functions import (
    col, from_json, current_timestamp, expr, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

# --- Event Hub Connection ---
# Connection string from Key Vault (never hard-code credentials)
eh_connection_string = {secret_get}
eh_consumer_group = "{consumer_group}"

eh_conf = {{
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_connection_string),
    "eventhubs.consumerGroup": eh_consumer_group,
    "maxEventsPerTrigger": 10000,
}}

# --- Message Schema ---
# TODO: Replace with actual message schema from source system
message_schema = StructType([
    StructField("id", LongType(), False),
    StructField("event_type", StringType(), True),
    StructField("payload", StringType(), True),
    StructField("event_time", TimestampType(), True),
])

# COMMAND ----------

# CELL 2 — Read Stream from Event Hub
df_raw = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
)

# Deserialize Event Hub body
df_parsed = (
    df_raw
    .withColumn("body_str", col("body").cast("string"))
    .withColumn("data", from_json(col("body_str"), message_schema))
    .select(
        col("enqueuedTime").alias("eh_enqueued_time"),
        col("offset").alias("eh_offset"),
        col("sequenceNumber").alias("eh_sequence"),
        "data.*",
    )
)
# COMMAND ----------

# CELL 3 — Transformations
# TODO: Add mapping-specific transformations here
df_transformed = df_parsed.withColumn("_load_ts", current_timestamp())

# COMMAND ----------

# CELL 4 — Watermark for Late Arrival Handling
df_watermarked = df_transformed.withWatermark("event_time", "{watermark_delay}")

# COMMAND ----------

# CELL 5 — Write Stream to Delta Lake
checkpoint_path = "{checkpoint_path}"
target_table = "{target_table}"

query = (
    df_watermarked.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="{trigger_interval}")
    .toTable(target_table)
)

query.awaitTermination()
# COMMAND ----------

# CELL 6 — Audit
print(f"Streaming notebook NB_{notebook_name} started")
print(f"Source: Azure Event Hub")
print(f"Target: {{target_table}}")
