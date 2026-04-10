# Fabric notebook source

# METADATA_START
# {{"language_info":{{"name":"python"}},"kernel_info":{{"name":"synapse_pyspark"}}}}

# CELL 1 — Metadata & Parameters
# Notebook: NB_{notebook_name}
# Migrated from: Informatica mapping {mapping_name}
# Pattern: Structured Streaming — Kafka Source → Delta Sink
# Generated: {generated_date}

from pyspark.sql.functions import (
    col, from_json, current_timestamp, expr, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

# --- Kafka Connection ---
kafka_bootstrap_servers = {secret_get}
kafka_topic = "{topic}"
consumer_group = "{consumer_group}"

# --- Message Schema ---
# TODO: Replace with actual message schema from source system
message_schema = StructType([
    StructField("id", LongType(), False),
    StructField("event_type", StringType(), True),
    StructField("payload", StringType(), True),
    StructField("event_time", TimestampType(), True),
])

# COMMAND ----------

# CELL 2 — Read Stream from Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("kafka.group.id", consumer_group)
    .option("startingOffsets", "latest")  # Use "earliest" for initial backfill
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 10000)  # Micro-batch size control
    .load()
)

# Deserialize Kafka value (key is typically partition key)
df_parsed = (
    df_raw
    .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value", "topic", "partition", "offset", "timestamp as kafka_timestamp")
    .withColumn("data", from_json(col("json_value"), message_schema))
    .select("kafka_key", "kafka_timestamp", "data.*")
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
print(f"Source: Kafka topic '{kafka_topic}'")
print(f"Target: {{target_table}}")
