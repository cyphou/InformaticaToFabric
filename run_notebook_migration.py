"""
Notebook Migration — Phase 2
Reads inventory.json + converted SQL and generates PySpark notebooks
for each Informatica mapping.

Outputs:
  output/notebooks/NB_<mapping_name>.py — one notebook per mapping

Usage:
    python run_notebook_migration.py
    python run_notebook_migration.py path/to/inventory.json
"""

import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "notebooks"
SQL_DIR = WORKSPACE / "output" / "sql"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _get_target():
    """Return the target platform ('fabric' or 'databricks')."""
    return os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")


def _get_catalog():
    """Return the Unity Catalog name for Databricks target."""
    return os.environ.get("INFORMATICA_DATABRICKS_CATALOG", "main")


def _table_ref(tier, table_name):
    """Return a fully-qualified table reference for the active target platform."""
    target = _get_target()
    if target == "databricks":
        catalog = _get_catalog()
        return f"{catalog}.{tier}.{table_name}"
    return f"{tier}.{table_name}"


def _widget_get(param_name):
    """Return the widget-get call for the active target platform."""
    target = _get_target()
    if target == "databricks":
        return f'dbutils.widgets.get("{param_name}")'
    return f'notebookutils.widgets.get("{param_name}")'


def _secret_get(vault_or_scope, secret_name):
    """Return the secret-retrieval call for the active target platform."""
    target = _get_target()
    if target == "databricks":
        return f'dbutils.secrets.get(scope="{vault_or_scope}", key="{secret_name}")'
    return f'notebookutils.credentials.getSecret("{vault_or_scope}", "{secret_name}")'

# Mapping from transformation abbreviation to PySpark code generation
TX_TEMPLATES = {
    "SQ": "source_qualifier",
    "EXP": "expression",
    "FIL": "filter",
    "AGG": "aggregator",
    "JNR": "joiner",
    "LKP": "lookup",
    "RTR": "router",
    "UPD": "update_strategy",
    "RNK": "rank",
    "SRT": "sorter",
    "UNI": "union",
    "NRM": "normalizer",
    "SEQ": "sequence_generator",
    "SP": "stored_procedure",
    "SQLT": "sql_transformation",
    "DM": "data_masking",
    "WSC": "web_service",
    "MPLT": "mapplet",
    # Sprint 26: Promoted from placeholder to template
    "JTX": "java_transformation",
    "CT": "custom_transformation",
    "HTTP": "http_transformation",
    "XMLG": "xml_generator",
    "XMLP": "xml_parser",
    "TC": "transaction_control",
    "ULKP": "unconnected_lookup",
    # Sprint 31: Remaining object gaps
    "EP": "external_procedure",
    "AEP": "advanced_external_procedure",
    "ASSOC": "association",
    "KEYGEN": "key_generator",
    "ADDRVAL": "address_validator",
}


def _cell_sep():
    return "\n# COMMAND ----------\n\n"


# ─────────────────────────────────────────────
#  Sprint 71: Spark Config Tuner
# ─────────────────────────────────────────────

def recommend_spark_config(mapping):
    """Generate spark.conf.set() calls tuned to mapping complexity.

    Simple:  2 cores, 200 shuffle partitions, AQE enabled
    Medium:  4 cores, 400 shuffle partitions, AQE enabled, broadcast 50MB
    Complex: 8 cores, 800 shuffle partitions, AQE enabled, broadcast 100MB
    Custom:  16 cores, auto-tuned, broadcast 256MB
    """
    complexity = mapping.get("complexity", "Simple")
    tx_count = len(mapping.get("transformations", []))
    source_count = len(mapping.get("sources", []))

    configs = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    }

    if complexity == "Custom" or tx_count > 15:
        configs.update({
            "spark.sql.shuffle.partitions": "800",
            "spark.sql.autoBroadcastJoinThreshold": "268435456",
            "spark.executor.memory": "8g",
            "spark.executor.cores": "4",
        })
    elif complexity == "Complex" or tx_count > 10:
        configs.update({
            "spark.sql.shuffle.partitions": "800",
            "spark.sql.autoBroadcastJoinThreshold": "104857600",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "4",
        })
    elif complexity == "Medium" or tx_count > 5:
        configs.update({
            "spark.sql.shuffle.partitions": "400",
            "spark.sql.autoBroadcastJoinThreshold": "52428800",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        })
    else:
        configs.update({
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.autoBroadcastJoinThreshold": "10485760",
        })

    if source_count > 3:
        configs["spark.sql.shuffle.partitions"] = str(
            max(int(configs.get("spark.sql.shuffle.partitions", "200")), 600)
        )

    return configs


def detect_broadcast_candidates(mapping):
    """Detect lookup tables that should use broadcast() hints.

    LKP transforms with small-dimension tables (< 100MB estimated) → broadcast.
    Returns list of lookup table references.
    """
    candidates = []
    lookups = mapping.get("lookup_conditions", [])
    txs = mapping.get("transformations", [])

    if "LKP" in txs or "ULKP" in txs:
        for lkp in lookups:
            candidates.append({
                "lookup": lkp.get("lookup", "unknown"),
                "condition": lkp.get("condition", ""),
                "hint": "broadcast",
                "reason": "Lookup table — typically < 100MB; use broadcast join",
            })

    if not lookups and ("LKP" in txs or "ULKP" in txs):
        candidates.append({
            "lookup": "lookup_table",
            "condition": "",
            "hint": "broadcast",
            "reason": "LKP transform detected — add broadcast() around lookup DataFrame",
        })

    return candidates


def recommend_materialization(mapping, dag_info=None):
    """Recommend .cache(), .persist(), or Delta checkpoint based on DAG reuse.

    If a table is read >2 times in the DAG → cache it.
    If a mapping has >10 transforms → persist intermediate results.
    """
    recommendations = []
    txs = mapping.get("transformations", [])
    targets = mapping.get("targets", [])

    # Multi-target mappings: intermediate results reused across targets
    if len(targets) > 1:
        recommendations.append({
            "action": "cache",
            "target": "df (intermediate)",
            "reason": f"Multi-target mapping ({len(targets)} targets) — cache intermediate DataFrame",
        })

    # Complex pipelines: persist after expensive operations
    expensive_ops = {"JNR", "AGG", "RNK", "UNI", "NRM"}
    expense_count = sum(1 for t in txs if t in expensive_ops)
    if expense_count >= 3:
        recommendations.append({
            "action": "persist",
            "target": "df (post-join/agg)",
            "reason": f"{expense_count} expensive operations — persist after joins/aggregations",
        })

    # Long pipelines: checkpoint to break lineage
    if len(txs) > 10:
        recommendations.append({
            "action": "checkpoint",
            "target": "df (mid-pipeline)",
            "reason": f"Long pipeline ({len(txs)} transforms) — Delta checkpoint to break lineage",
        })

    return recommendations


def _metadata_cell(mapping):
    """Generate the metadata + imports cell."""
    name = mapping["name"]
    complexity = mapping.get("complexity", "Unknown")
    sources = ", ".join(mapping.get("sources", []))
    targets = ", ".join(mapping.get("targets", []))
    txs = " → ".join(mapping.get("transformations", []))
    params = mapping.get("parameters", [])

    lines = [
        f"# {'Databricks' if _get_target() == 'databricks' else 'Fabric'} notebook source",
        "",
        "# METADATA_START",
    ]
    if _get_target() == "databricks":
        lines.append('# {"language_info":{"name":"python"},"kernel_info":{"name":"python3"}}')
    else:
        lines.append('# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}')
    lines.extend([
        "",
        "# CELL 1 — Metadata & Parameters",
        f"# Notebook: NB_{name}",
        f"# Migrated from: Informatica mapping {name}",
        f"# Complexity: {complexity}",
        f"# Sources: {sources}",
        f"# Targets: {targets}",
        f"# Flow: {txs}",
        f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "",
        "from pyspark.sql.functions import (",
        "    col, lit, when, coalesce, concat_ws, current_timestamp,",
        "    count, sum as _sum, avg as _avg, min as _min, max as _max,",
        "    row_number, rank, dense_rank, broadcast, expr, md5",
        ")",
        "from pyspark.sql.window import Window",
        "from delta.tables import DeltaTable",
    ])

    if params:
        lines.append("")
        lines.append("# Parameters (from Informatica $$params / pipeline)")
        for p in params:
            safe_name = p.replace("$$", "").lower()
            lines.append(f'{safe_name} = {_widget_get(safe_name)}')

    # Sprint 71: Spark config tuning
    spark_configs = recommend_spark_config(mapping)
    if spark_configs:
        lines.append("")
        lines.append("# Performance tuning (auto-generated based on mapping complexity)")
        for key, value in spark_configs.items():
            lines.append(f'spark.conf.set("{key}", "{value}")')

    return "\n".join(lines)


def _source_cell(mapping, cell_num):
    """Generate the source read cell."""
    sources = mapping.get("sources", [])
    lines = [f"# CELL {cell_num} — Source Read"]

    for i, src in enumerate(sources):
        parts = src.split(".")
        if len(parts) >= 3:
            schema_table = f"{parts[1]}.{parts[2]}"
            lakehouse_table = _table_ref("bronze", parts[2].lower())
        elif len(parts) == 2:
            schema_table = src
            lakehouse_table = _table_ref("bronze", parts[1].lower())
        else:
            schema_table = src
            lakehouse_table = _table_ref("bronze", src.lower())

        var_name = "df_source" if i == 0 else f"df_source_{i + 1}"
        lines.append(f"# --- Source: {src} ---")
        lines.append(f"# Oracle: SELECT * FROM {schema_table}")
        lines.append(f'{var_name} = spark.table("{lakehouse_table}")')
        lines.append("")

    # SQL overrides
    overrides = mapping.get("sql_overrides", [])
    sq_overrides = [o for o in overrides if o.get("type") == "Sql Query"]
    if sq_overrides:
        lines.append("# SQL Override detected — review converted SQL in output/sql/")
        lines.append(f"# See: SQL_OVERRIDES_{mapping['name']}.sql")

    return "\n".join(lines)


def _transformation_cell(tx_type, idx, mapping, cell_num):
    """Generate a cell for a single transformation type."""
    name = mapping["name"]
    lines = [f"# CELL {cell_num} — Transformation: {tx_type}"]
    prev_df = "df_source" if idx == 0 else "df"

    if tx_type == "SQ":
        return None  # Handled by source cell

    elif tx_type == "EXP":
        lines.extend([
            "# --- Expression transformation ---",
            "# Map Informatica ports to PySpark withColumn / select expressions",
            f"df = {prev_df}.withColumn(",
            '    "ETL_LOAD_DATE", current_timestamp()',
            ")",
            "# TODO: Add derived column expressions from mapping ports",
        ])

    elif tx_type == "FIL":
        lines.extend([
            "# --- Filter transformation ---",
            f"df = {prev_df}.filter(",
            '    col("STATUS") != "INACTIVE"  # TODO: Replace with actual filter condition',
            ")",
        ])

    elif tx_type == "AGG":
        lines.extend([
            "# --- Aggregator transformation ---",
            f"df_agg = {prev_df}.groupBy(",
            '    "GROUP_KEY"  # TODO: Replace with actual GROUP BY columns',
            ").agg(",
            '    count("*").alias("RECORD_COUNT"),',
            '    # TODO: Add aggregate expressions from mapping ports',
            ")",
            "df = df_agg",
        ])

    elif tx_type == "JNR":
        lines.extend([
            "# --- Joiner transformation ---",
            "# TODO: Identify master and detail sources from mapping",
            f"df = {prev_df}.join(",
            '    df_source_2,  # TODO: Replace with actual detail DataFrame',
            '    on="JOIN_KEY",  # TODO: Replace with actual join condition',
            '    how="inner"  # TODO: inner/left/right/full based on mapping',
            ")",
        ])

    elif tx_type == "LKP":
        lookups = mapping.get("lookup_conditions", [])
        if lookups:
            lkp_name = lookups[0].get("lookup", "LKP_TABLE")
            lines.extend([
                f"# --- Lookup: {lkp_name} ---",
                "# Using broadcast join for lookup table (< 100MB)",
                f'df_lookup = spark.table("{_table_ref("bronze", "lookup_table")}")  # TODO: Replace with actual lookup table',
                f"df = {prev_df}.join(",
                '    broadcast(df_lookup),',
                '    on="LOOKUP_KEY",  # TODO: Replace with actual lookup condition',
                '    how="left"',
                ")",
            ])
        else:
            lines.extend([
                "# --- Lookup transformation ---",
                f'df_lookup = spark.table("{_table_ref("bronze", "lookup_table")}")  # TODO: Replace',
                f"df = {prev_df}.join(broadcast(df_lookup), on=\"KEY\", how=\"left\")",
            ])

    elif tx_type == "RTR":
        lines.extend([
            "# --- Router transformation ---",
            "# Create multiple DataFrames, one per output group",
            f'df_group_1 = {prev_df}.filter(col("CATEGORY") == "A")  # TODO: Replace conditions',
            f'df_group_2 = {prev_df}.filter(col("CATEGORY") == "B")',
            f'df_default = {prev_df}.filter(~col("CATEGORY").isin("A", "B"))',
            "df = df_group_1  # TODO: Select correct group for downstream",
        ])

    elif tx_type == "UPD":
        targets = mapping.get("targets", ["target_table"])
        target = targets[0].lower() if targets else "target_table"
        merge_table = _table_ref("silver", target)
        lines.extend([
            "# --- Update Strategy → Delta MERGE ---",
            f'target_table = DeltaTable.forName(spark, "{merge_table}")',
            "target_table.alias('tgt').merge(",
            f"    {prev_df}.alias('src'),",
            "    'tgt.ID = src.ID'  # TODO: Replace with actual merge key",
            ").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()",
            f"df = {prev_df}  # Pass through for downstream",
        ])

    elif tx_type == "RNK":
        lines.extend([
            "# --- Rank transformation ---",
            'w = Window.partitionBy("GROUP_COL").orderBy(col("RANK_COL").desc())  # TODO: Replace',
            f'df = {prev_df}.withColumn("RANK", row_number().over(w))',
        ])

    elif tx_type == "SRT":
        lines.extend([
            "# --- Sorter transformation ---",
            f'df = {prev_df}.orderBy(col("SORT_COL").asc())  # TODO: Replace with actual sort columns',
        ])

    elif tx_type == "UNI":
        lines.extend([
            "# --- Union transformation ---",
            "# TODO: Ensure schemas match between DataFrames",
            f"df = {prev_df}.unionByName(df_source_2, allowMissingColumns=True)",
        ])

    elif tx_type == "NRM":
        lines.extend([
            "# --- Normalizer transformation → explode ---",
            "from pyspark.sql.functions import explode, split",
            f'df = {prev_df}.withColumn("NORMALIZED_COL", explode(split(col("ARRAY_COL"), ",")))  # TODO: Replace',
        ])

    elif tx_type == "SEQ":
        lines.extend([
            "# --- Sequence Generator ---",
            "from pyspark.sql.functions import monotonically_increasing_id",
            f'df = {prev_df}.withColumn("SEQ_ID", monotonically_increasing_id())',
        ])

    elif tx_type == "SP":
        lines.extend([
            "# --- Stored Procedure → inline Spark SQL ---",
            f"# See converted SQL: output/sql/SQL_*_{name}.sql",
            '# spark.sql("MERGE INTO silver.table ...")',
            f"df = {prev_df}",
        ])

    elif tx_type == "SQLT":
        lines.extend([
            "# --- SQL Transformation → spark.sql() ---",
            f'{prev_df}.createOrReplaceTempView("temp_input")',
            '# df = spark.sql("SELECT * FROM temp_input WHERE ...")  # TODO: Add SQL logic',
            f"df = {prev_df}",
        ])

    elif tx_type == "DM":
        lines.extend([
            "# --- Data Masking transformation ---",
            "# Approach 1: Hash-based masking",
            f'df = {prev_df}.withColumn("MASKED_COL", md5(col("SENSITIVE_COL").cast("string")))  # TODO: Replace',
            "# Approach 2: Partial masking",
            '# df = df.withColumn("MASKED_EMAIL", regexp_replace(col("EMAIL"), "(?<=.{2}).(?=.*@)", "*"))',
        ])

    elif tx_type == "WSC":
        lines.extend([
            "# --- Web Service Consumer → requests UDF ---",
            "import requests",
            "from pyspark.sql.functions import udf",
            "from pyspark.sql.types import StringType",
            "",
            "# Define a UDF that calls an external web service",
            "# TODO: Configure URL, headers, authentication, and retry logic",
            "@udf(returnType=StringType())",
            "def call_web_service(input_value):",
            '    try:',
            '        resp = requests.get(',
            '            "https://api.example.com/data",  # TODO: Replace with actual URL',
            '            params={"key": input_value},',
            '            headers={"Content-Type": "application/json"},',
            '            timeout=30',
            '        )',
            '        resp.raise_for_status()',
            '        return resp.text',
            '    except Exception as e:',
            '        return None',
            "",
            f'df = {prev_df}.withColumn("ws_result", call_web_service(col("INPUT_COL")))  # TODO: Replace INPUT_COL',
        ])

    elif tx_type == "MPLT":
        lines.extend([
            "# --- Mapplet (expanded) ---",
            "# Reusable transformation logic from Informatica Mapplet.",
            "# Inner transformations have been expanded inline above.",
            f"df = {prev_df}",
        ])

    elif tx_type == "JTX":
        lines.extend([
            "# --- Java Transformation → PySpark UDF ---",
            "# The original Informatica Java transform embedded custom Java code.",
            "# Rewrite the logic as a Python UDF below.",
            "from pyspark.sql.functions import udf",
            "from pyspark.sql.types import StringType  # TODO: Match actual return type",
            "",
            "@udf(returnType=StringType())",
            "def java_transform_udf(input_col):",
            '    \"\"\"Rewrite the Java transformation logic in Python.',
            '    ',
            '    Original Java class: TODO — paste class name from mapping XML.',
            '    Input ports: TODO — list input port names.',
            '    Output ports: TODO — list output port names.',
            '    \"\"\"',
            '    # TODO: Implement the Java logic in Python',
            '    result = input_col  # Placeholder — replace with actual logic',
            '    return result',
            "",
            f'df = {prev_df}.withColumn("JTX_OUTPUT", java_transform_udf(col("INPUT_COL")))  # TODO: Replace columns',
        ])

    elif tx_type == "CT":
        lines.extend([
            "# --- Custom Transformation → pandas UDF ---",
            "# The original Informatica Custom transform used C/C++ code.",
            "# Rewrite as a vectorized pandas UDF for performance.",
            "import pandas as pd",
            "from pyspark.sql.functions import pandas_udf",
            "from pyspark.sql.types import StringType  # TODO: Match actual return type",
            "",
            "@pandas_udf(StringType())",
            "def custom_transform_udf(series: pd.Series) -> pd.Series:",
            '    \"\"\"Vectorized replacement for Custom transformation.',
            '    ',
            '    Original module: TODO — paste DLL/SO name from mapping XML.',
            '    Input ports: TODO — list input port names.',
            '    Output ports: TODO — list output port names.',
            '    \"\"\"',
            '    # TODO: Implement the C/C++ logic as vectorized pandas operations',
            '    return series  # Placeholder — replace with actual logic',
            "",
            f'df = {prev_df}.withColumn("CT_OUTPUT", custom_transform_udf(col("INPUT_COL")))  # TODO: Replace columns',
        ])

    elif tx_type == "HTTP":
        lines.extend([
            "# --- HTTP Transformation → requests UDF with retry ---",
            "import requests",
            "from requests.adapters import HTTPAdapter",
            "from urllib3.util.retry import Retry",
            "from pyspark.sql.functions import udf",
            "from pyspark.sql.types import StringType",
            "",
            "# Configure retry strategy",
            "_session = requests.Session()",
            "_retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])",
            "_session.mount('https://', HTTPAdapter(max_retries=_retry))",
            "_session.mount('http://', HTTPAdapter(max_retries=_retry))",
            "",
            "@udf(returnType=StringType())",
            "def http_transform(input_value):",
            '    \"\"\"Call external HTTP endpoint — replaces Informatica HTTP transformation.',
            '    TODO: Configure URL, method (GET/POST), headers, and body template.',
            '    \"\"\"',
            '    try:',
            '        resp = _session.get(',
            '            "https://api.example.com/endpoint",  # TODO: Replace URL',
            '            params={"input": input_value},',
            '            timeout=30',
            '        )',
            '        resp.raise_for_status()',
            '        return resp.text',
            '    except Exception:',
            '        return None',
            "",
            f'df = {prev_df}.withColumn("HTTP_RESULT", http_transform(col("URL_INPUT")))  # TODO: Replace columns',
        ])

    elif tx_type == "XMLG":
        lines.extend([
            "# --- XML Generator → PySpark XML construction ---",
            "from pyspark.sql.functions import concat, format_string, col, lit",
            "",
            "# Build XML string from DataFrame columns",
            "# TODO: Adjust tag names and columns to match original XML Generator ports",
            f"df = {prev_df}.withColumn(",
            '    "xml_output",',
            '    concat(',
            '        lit("<record>"),',
            '        lit("<id>"), col("ID").cast("string"), lit("</id>"),  # TODO: Replace',
            '        lit("<name>"), col("NAME").cast("string"), lit("</name>"),  # TODO: Replace',
            '        lit("</record>")',
            '    )',
            ")",
            "# For file output: df.select('xml_output').write.text('output_path')",
        ])

    elif tx_type == "XMLP":
        lines.extend([
            "# --- XML Parser → spark.read.format('xml') ---",
            "# Option 1: Parse XML column from existing DataFrame",
            "from pyspark.sql.functions import from_json, col",
            "from pyspark.sql.types import StructType, StructField, StringType",
            "",
            "# TODO: Define the XML schema matching the original XML Parser ports",
            "xml_schema = StructType([",
            '    StructField("id", StringType(), True),',
            '    StructField("name", StringType(), True),',
            '    # TODO: Add fields matching the XML Parser output ports',
            "])",
            "",
            "# Option 2: Read XML files directly (requires spark-xml package)",
            "# df_xml = spark.read.format('com.databricks.spark.xml')",
            "#     .option('rowTag', 'record')  # TODO: Replace with actual row tag",
            "#     .schema(xml_schema)",
            "#     .load('path/to/xml/files')",
            "",
            f"df = {prev_df}  # TODO: Replace with parsed XML DataFrame",
        ])

    elif tx_type == "TC":
        lines.extend([
            "# --- Transaction Control → Delta ACID pattern ---",
            "# Informatica TC_COMMIT / TC_ROLLBACK → Delta Lake atomic writes",
            "# Delta Lake provides ACID guarantees on every write operation.",
            "#",
            "# Migration checklist:",
            "#   1. TC_COMMIT_BEFORE  → write previous batch, then continue",
            "#   2. TC_COMMIT_AFTER   → write current batch after processing",
            "#   3. TC_ROLLBACK       → no write; Delta auto-rolls back on failure",
            "#   4. Effective commit  → every .write / .save / MERGE is atomic",
            "#   5. Batch size        → use foreachBatch for micro-commits",
            "#",
            "from delta.tables import DeltaTable",
            "",
            f"target_table = '{_table_ref('silver', 'target_table')}'  # TODO: Replace with actual table",
            "",
            "# Pattern A — single atomic write (most common, replaces TC_COMMIT_AFTER):",
            "try:",
            f"    {prev_df}.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)",
            f'    print(f"Transaction committed: {{target_table}}")',
            "except Exception as e:",
            f'    print(f"Transaction failed — Delta automatically rolled back: {{e}}")',
            "    raise",
            "",
            "# Pattern B — batch micro-commits (replaces TC_COMMIT_BEFORE with row intervals):",
            "# batch_size = 10000  # TODO: Match original Informatica commit interval",
            f"# for i in range(0, {prev_df}.count(), batch_size):",
            f"#     batch = {prev_df}.limit(batch_size).offset(i)  # pseudo-code; use repartition",
            "#     batch.write.format('delta').mode('append').saveAsTable(target_table)",
            "",
            "# Pattern C — merge with retry (replaces TC_ROLLBACK on conflict):",
            "# delta_tbl = DeltaTable.forName(spark, target_table)",
            f"# delta_tbl.alias('tgt').merge({prev_df}.alias('src'), 'tgt.ID = src.ID')",
            "#     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()",
            "",
            f"df = {prev_df}",
        ])

    elif tx_type == "ULKP":
        lines.extend([
            "# --- Unconnected Lookup → broadcast join + when/otherwise ---",
            "# Informatica ULKP: called from expressions via :LKP.lookup_name(key)",
            "# Pattern: Pre-load lookup table, broadcast join, then use when().",
            "",
            "# Load lookup table (broadcast for performance < 100MB)",
            f'df_ulkp = spark.table("{_table_ref("bronze", "lookup_table")}")  # TODO: Replace with actual lookup table',
            'ulkp_key = "LOOKUP_KEY"  # TODO: Replace with actual key column',
            'ulkp_value = "LOOKUP_VALUE"  # TODO: Replace with actual return column',
            'default_value = None  # TODO: Set default value on no-match',
            "",
            f"df = {prev_df}.join(",
            "    broadcast(df_ulkp.select(col(ulkp_key), col(ulkp_value).alias('_ulkp_val'))),",
            "    on=col('JOIN_KEY') == col(ulkp_key),  # TODO: Replace JOIN_KEY",
            "    how='left'",
            ")",
            'df = df.withColumn("LOOKUP_RESULT", coalesce(col("_ulkp_val"), lit(default_value)))',
            'df = df.drop("_ulkp_val", ulkp_key)',
        ])

    # --- Sprint 31: Remaining Object Gap Templates ---

    elif tx_type == "EP":
        lines.extend([
            "# --- External Procedure → Python subprocess / UDF ---",
            "# The original Informatica External Procedure called an external program.",
            "# Rewrite as a Python subprocess call or native PySpark logic.",
            "import subprocess",
            "from pyspark.sql.functions import udf",
            "from pyspark.sql.types import StringType  # TODO: Match actual return type",
            "",
            "@udf(returnType=StringType())",
            "def external_proc_udf(input_col):",
            '    \"\"\"Calls external program — replace with actual logic.',
            '    ',
            '    Original procedure: TODO — paste procedure name from mapping XML.',
            '    Input ports: TODO — list input port names.',
            '    Output ports: TODO — list output port names.',
            '    \"\"\"',
            '    # Option 1: Call external executable',
            '    # result = subprocess.run(["program", str(input_col)], capture_output=True, text=True, timeout=30)',
            '    # return result.stdout.strip()',
            '    # Option 2: Rewrite in Python',
            '    return input_col  # Placeholder',
            "",
            f'df = {prev_df}.withColumn("EP_OUTPUT", external_proc_udf(col("INPUT_COL")))  # TODO: Replace columns',
        ])

    elif tx_type == "AEP":
        lines.extend([
            "# --- Advanced External Procedure → Python library call ---",
            "# The original used a C/C++/Java shared library (.dll/.so/.jar).",
            "# Port the logic to Python or wrap via ctypes/JNI.",
            "from pyspark.sql.functions import udf",
            "from pyspark.sql.types import StringType  # TODO: Match actual return type",
            "",
            "# Option 1: ctypes for C/C++ library",
            "# import ctypes",
            "# lib = ctypes.CDLL('path/to/library.so')",
            "",
            "# Option 2: jpype for Java library",
            "# import jpype",
            "",
            "@udf(returnType=StringType())",
            "def advanced_ext_proc_udf(input_col):",
            '    \"\"\"Advanced External Procedure — rewrite native library logic.',
            '    ',
            '    Original library: TODO — paste DLL/SO/JAR name from mapping.',
            '    Function: TODO — paste function/method name.',
            '    \"\"\"',
            '    return input_col  # Placeholder — replace with actual logic',
            "",
            f'df = {prev_df}.withColumn("AEP_OUTPUT", advanced_ext_proc_udf(col("INPUT_COL")))  # TODO: Replace columns',
        ])

    elif tx_type == "ASSOC":
        lines.extend([
            "# --- Association → PySpark window-based grouping ---",
            "# Informatica Association groups related records (e.g., duplicate detection).",
            "# Replicate with window functions or groupBy.",
            "from pyspark.sql.window import Window",
            "from pyspark.sql.functions import row_number, col, dense_rank",
            "",
            "# Define grouping criteria (replace with actual association logic)",
            'group_cols = ["MATCH_KEY"]  # TODO: Replace with actual matching columns',
            'rank_col = "CONFIDENCE_SCORE"  # TODO: Replace with ranking column',
            "",
            f"w = Window.partitionBy(*group_cols).orderBy(col(rank_col).desc())",
            f'df = {prev_df}.withColumn("ASSOC_GROUP_RANK", dense_rank().over(w))',
            '# Keep best match per group:',
            '# df = df.filter(col("ASSOC_GROUP_RANK") == 1)',
        ])

    elif tx_type == "KEYGEN":
        lines.extend([
            "# --- Key Generator → PySpark surrogate key ---",
            "# Generates unique surrogate keys for dimension tables.",
            "from pyspark.sql.functions import monotonically_increasing_id, sha2, concat_ws, col",
            "",
            "# Option 1: Sequential-style IDs (not globally unique across partitions)",
            f'df = {prev_df}.withColumn("GENERATED_KEY", monotonically_increasing_id())',
            "",
            "# Option 2: Hash-based key (deterministic, globally unique)",
            '# natural_key_cols = ["COL1", "COL2"]  # TODO: Replace with natural key columns',
            '# df = df.withColumn("GENERATED_KEY", sha2(concat_ws("|", *[col(c).cast("string") for c in natural_key_cols]), 256))',
        ])

    elif tx_type == "ADDRVAL":
        lines.extend([
            "# --- Address Validator → Azure Maps API / regex ---",
            "# Informatica Address Validator uses third-party data to standardize/validate.",
            "# Replace with Azure Maps Geocoding API or regex-based validation.",
            "import requests",
            "from pyspark.sql.functions import udf, col, regexp_replace, upper, trim",
            "from pyspark.sql.types import StringType",
            "",
            "# Option 1: Azure Maps API (requires subscription key)",
            f"# AZURE_MAPS_KEY = {_secret_get('keyvault', 'azure-maps-key')}",
            "# @udf(returnType=StringType())",
            "# def validate_address(address):",
            '#     resp = requests.get(',
            '#         "https://atlas.microsoft.com/search/address/json",',
            '#         params={"api-version": "1.0", "query": address, "subscription-key": AZURE_MAPS_KEY},',
            '#         timeout=10',
            '#     )',
            '#     if resp.ok and resp.json().get("results"):",',
            '#         return resp.json()["results"][0].get("address", {}).get("freeformAddress", address)',
            '#     return address',
            "",
            "# Option 2: Basic regex standardization",
            f"df = {prev_df}.withColumn(",
            '    "STD_ADDRESS",',
            '    upper(trim(regexp_replace(col("ADDRESS"), r"\\s+", " ")))  # TODO: Replace ADDRESS column',
            ")",
        ])

    else:
        # Truly unknown types
        lines.extend([
            f"# --- {tx_type} transformation (UNKNOWN) ---",
            f"# This transformation type ({tx_type}) is not recognized.",
            "# Manual conversion required.",
            f"df = {prev_df}",
        ])

    return "\n".join(lines)


def _target_cell(mapping, cell_num):
    """Generate the target write cell."""
    targets = mapping.get("targets", [])
    has_upd = "UPD" in mapping.get("transformations", [])
    lines = [f"# CELL {cell_num} — Target Write"]

    for i, target in enumerate(targets):
        target_lower = target.lower()
        # Determine lakehouse tier from context
        if "agg" in target_lower or "gold" in target_lower:
            tier = "gold"
        elif "dim" in target_lower or "fact" in target_lower:
            tier = "silver"
        else:
            tier = "silver"

        table_name = _table_ref(tier, target_lower)

        if has_upd and i == 0:
            lines.append(f"# MERGE handled in Update Strategy cell above → {table_name}")
        else:
            df_name = "df" if i == 0 else f"df_target_{i + 1}"
            lines.extend([
                f"# --- Target: {target} → {table_name} ---",
                f'{df_name} = df',
                f'{df_name}.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("{table_name}")',
                "",
            ])

    return "\n".join(lines)


def _audit_cell(mapping, cell_num):
    """Generate the audit log cell."""
    name = mapping["name"]
    return (
        f"# CELL {cell_num} — Audit Log\n"
        f'print(f"Notebook NB_{name} completed successfully")\n'
        f'print(f"Rows written: {{df.count()}}")\n'
    )


# ─────────────────────────────────────────────
#  Sprint 80-81: Streaming & CDC Notebook Generation
# ─────────────────────────────────────────────

def _cdc_merge_cell(mapping, cell_num):
    """Generate a Delta MERGE cell driven by CDC metadata.

    Uses the mapping's cdc info (merge_keys, update_strategy) to produce
    a proper MERGE INTO with WHEN MATCHED / WHEN NOT MATCHED / WHEN NOT
    MATCHED BY SOURCE clauses.
    """
    cdc_info = mapping.get("cdc", {})
    us = cdc_info.get("update_strategy", {})
    merge_keys = cdc_info.get("merge_keys", [])
    cdc_sources = cdc_info.get("cdc_sources", [])
    cdc_type = cdc_info.get("cdc_type", "upsert_only")

    targets = mapping.get("targets", [])
    # Pick the first non-bronze target for the merge
    merge_target = None
    for t in targets:
        tl = t.lower()
        if "silver" in tl or "fact" in tl or "dim" in tl:
            merge_target = t
            break
    if not merge_target and targets:
        merge_target = targets[0]
    merge_target = merge_target or "target_table"

    target_ref = _table_ref("silver", merge_target.lower())

    # Determine CDC operation column
    cdc_col = "CDC_OPERATION"
    if cdc_sources:
        cdc_col = cdc_sources[0].get("cdc_column", "CDC_OPERATION") or "CDC_OPERATION"

    # Build merge key condition
    if merge_keys:
        merge_cond = " AND ".join(f"tgt.{k} = src.{k}" for k in merge_keys)
    else:
        merge_cond = "tgt.ID = src.ID  # TODO: Replace with actual merge key(s)"

    lines = [
        f"# CELL {cell_num} — CDC MERGE INTO (Sprint 81)",
        f"# CDC type: {cdc_type}",
        f"# Merge keys: {', '.join(merge_keys) if merge_keys else 'TODO'}",
        f"# CDC column: {cdc_col}",
        "",
        f'target_table = DeltaTable.forName(spark, "{target_ref}")',
        "",
        "# Separate CDC operations",
        f'df_inserts = df.filter(col("{cdc_col}") == "I")',
        f'df_updates = df.filter(col("{cdc_col}") == "U")',
    ]

    if cdc_type == "full_cdc":
        lines.append(f'df_deletes = df.filter(col("{cdc_col}") == "D")')

    lines.extend([
        "",
        "# Upsert: inserts + updates via MERGE",
        "df_upsert = df_inserts.unionByName(df_updates, allowMissingColumns=True)",
        "",
        "target_table.alias('tgt').merge(",
        "    df_upsert.alias('src'),",
        f"    '{merge_cond}'",
        ").whenMatchedUpdateAll()",
        ".whenNotMatchedInsertAll()",
    ])

    if cdc_type == "full_cdc":
        lines.extend([
            "# Full CDC: also handle deletes",
            ".execute()",
            "",
            "# Apply deletes separately (soft-delete or hard-delete)",
            "# Option 1: Soft delete (recommended for audit trail)",
            "# target_table.alias('tgt').merge(",
            "#     df_deletes.alias('src'),",
            f"#     '{merge_cond}'",
            '# ).whenMatchedUpdate(set={{"is_deleted": "lit(True)", "_deleted_ts": "current_timestamp()"}}).execute()',
            "",
            "# Option 2: Hard delete",
            "target_table.alias('tgt').merge(",
            "    df_deletes.alias('src'),",
            f"    '{merge_cond}'",
            ").whenMatchedDelete().execute()",
        ])
    else:
        lines.append(".execute()")

    lines.append("")
    lines.append("df = df  # Pass through for downstream")

    return "\n".join(lines)


def _change_feed_reader_cell(mapping, cell_num):
    """Generate a Databricks Change Data Feed (CDF) reader cell.

    Reads changes from a Delta table with CDF enabled:
      spark.readStream.option("readChangeFeed", "true")
    """
    targets = mapping.get("targets", [])
    source_table = _table_ref("silver", (targets[0].lower() if targets else "source_table"))

    lines = [
        f"# CELL {cell_num} — Change Data Feed Reader (Sprint 81)",
        "# Read incremental changes from Delta table with CDF enabled",
        "# Enable CDF on the source table first:",
        f'# spark.sql("ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")',
        "",
        f"df_changes = (",
        f"    spark.readStream",
        f'    .format("delta")',
        f'    .option("readChangeFeed", "true")',
        f'    .option("startingVersion", 0)  # Or use startingTimestamp',
        f'    .table("{source_table}")',
        ")",
        "",
        "# CDF provides: _change_type (insert, update_preimage, update_postimage, delete),",
        "#               _commit_version, _commit_timestamp",
        'df_cdc = df_changes.filter(col("_change_type").isin("insert", "update_postimage"))',
    ]

    return "\n".join(lines)


def _streaming_source_cell(mapping, cell_num):
    """Generate a Structured Streaming source read cell based on streaming metadata."""
    streaming_info = mapping.get("streaming", {})
    sources = streaming_info.get("streaming_sources", [])

    if not sources:
        return None

    src = sources[0]
    src_type = src.get("type", "").lower()
    topic = src.get("topic", "streaming-topic")
    src_name = src.get("name", "streaming_source")

    lines = [f"# CELL {cell_num} — Streaming Source Read (Sprint 80)"]

    if "kafka" in src_type:
        lines.extend([
            f"# --- Streaming Source: {src_name} (Kafka) ---",
            f"kafka_servers = {_secret_get('keyvault', 'kafka-bootstrap-servers')}",
            "",
            "from pyspark.sql.functions import from_json",
            "from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType",
            "",
            "# TODO: Replace with actual message schema",
            "message_schema = StructType([",
            '    StructField("id", LongType(), False),',
            '    StructField("event_type", StringType(), True),',
            '    StructField("payload", StringType(), True),',
            '    StructField("event_time", TimestampType(), True),',
            "])",
            "",
            "df_raw = (",
            "    spark.readStream",
            '    .format("kafka")',
            '    .option("kafka.bootstrap.servers", kafka_servers)',
            f'    .option("subscribe", "{topic}")',
            '    .option("startingOffsets", "latest")',
            '    .option("failOnDataLoss", "false")',
            '    .option("maxOffsetsPerTrigger", 10000)',
            "    .load()",
            ")",
            "",
            "df_source = (",
            '    df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_value", "timestamp as kafka_ts")',
            '    .withColumn("data", from_json(col("json_value"), message_schema))',
            '    .select("kafka_ts", "data.*")',
            ")",
        ])
    elif any(kw in src_type for kw in ("eventhub", "event hub", "servicebus")):
        lines.extend([
            f"# --- Streaming Source: {src_name} (Event Hub) ---",
            f"eh_conn_string = {_secret_get('keyvault', 'eventhub-connection-string')}",
            "",
            "from pyspark.sql.functions import from_json",
            "from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType",
            "",
            "eh_conf = {",
            '    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_conn_string),',
            '    "maxEventsPerTrigger": 10000,',
            "}",
            "",
            "df_raw = (",
            "    spark.readStream",
            '    .format("eventhubs")',
            "    .options(**eh_conf)",
            "    .load()",
            ")",
            "",
            'df_source = df_raw.withColumn("body_str", col("body").cast("string"))',
            "# TODO: Parse body JSON with from_json and actual schema",
        ])
    elif "mqtt" in src_type:
        lines.extend([
            f"# --- Streaming Source: {src_name} (MQTT → Kafka bridge) ---",
            "# MQTT sources typically require a bridge to Kafka or Event Hub",
            "# Configure an IoT Hub or MQTT-to-Kafka connector, then read from the topic",
            f"kafka_servers = {_secret_get('keyvault', 'kafka-bootstrap-servers')}",
            "",
            "df_source = (",
            "    spark.readStream",
            '    .format("kafka")',
            '    .option("kafka.bootstrap.servers", kafka_servers)',
            f'    .option("subscribe", "{topic}")',
            '    .option("startingOffsets", "latest")',
            "    .load()",
            '    .selectExpr("CAST(value AS STRING) as payload", "timestamp as event_time")',
            ")",
        ])
    else:
        lines.extend([
            f"# --- Streaming Source: {src_name} ({src_type}) ---",
            "# TODO: Configure streaming source connection",
            f"# Original Informatica source type: {src_type}",
            'df_source = spark.readStream.format("delta").table("bronze.streaming_source")  # Placeholder',
        ])

    return "\n".join(lines)


def _streaming_sink_cell(mapping, cell_num):
    """Generate a Delta streaming sink cell with checkpoint."""
    name = mapping["name"]
    targets = mapping.get("targets", [])
    target = targets[0].lower() if targets else "streaming_target"
    target_ref = _table_ref("bronze", target)

    # Determine output mode based on CDC
    cdc_info = mapping.get("cdc", {})
    has_cdc = cdc_info.get("is_cdc", False)

    lines = [
        f"# CELL {cell_num} — Streaming Sink (Sprint 80)",
        f'checkpoint_path = "abfss://checkpoints@storage.dfs.core.windows.net/{name}"',
        "",
    ]

    if has_cdc:
        lines.extend([
            "# CDC streaming: use foreachBatch for MERGE pattern",
            "def merge_batch(batch_df, batch_id):",
            f'    target_table = DeltaTable.forName(spark, "{_table_ref("silver", target)}")',
            "    target_table.alias('tgt').merge(",
            "        batch_df.alias('src'),",
            "        'tgt.ID = src.ID'  # TODO: Replace with actual merge key",
            "    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()",
            "",
            "query = (",
            "    df.writeStream",
            '    .foreachBatch(merge_batch)',
            '    .option("checkpointLocation", checkpoint_path)',
            '    .trigger(processingTime="30 seconds")',
            "    .start()",
            ")",
        ])
    else:
        lines.extend([
            "query = (",
            "    df.writeStream",
            '    .format("delta")',
            '    .outputMode("append")',
            '    .option("checkpointLocation", checkpoint_path)',
            '    .trigger(processingTime="30 seconds")',
            f'    .toTable("{target_ref}")',
            ")",
        ])

    lines.extend([
        "",
        "# For long-running streams, use query.awaitTermination()",
        "# For one-shot processing, use .trigger(availableNow=True)",
    ])

    return "\n".join(lines)


def generate_notebook(mapping):
    """Generate a complete notebook .py file for a mapping."""
    cells = []

    streaming_info = mapping.get("streaming", {})
    cdc_info = mapping.get("cdc", {})
    is_streaming = streaming_info.get("is_streaming", False)
    is_cdc = cdc_info.get("is_cdc", False)

    # Cell 1: Metadata + imports
    cells.append(_metadata_cell(mapping))

    cell_num = 2

    # Cell 2: Source read — streaming or batch
    if is_streaming:
        streaming_cell = _streaming_source_cell(mapping, cell_num)
        if streaming_cell:
            cells.append(streaming_cell)
        else:
            cells.append(_source_cell(mapping, cell_num))
    else:
        cells.append(_source_cell(mapping, cell_num))
    cell_num += 1

    # Transformation cells (skip SQ — handled in source cell)
    txs = mapping.get("transformations", [])
    for i, tx in enumerate(txs):
        if tx == "SQ":
            continue
        # Use CDC-aware MERGE for UPD when CDC is detected
        if tx == "UPD" and is_cdc:
            cells.append(_cdc_merge_cell(mapping, cell_num))
            cell_num += 1
            continue
        cell_content = _transformation_cell(tx, i, mapping, cell_num)
        if cell_content:
            cells.append(cell_content)
            cell_num += 1

    # Target write cell — streaming sink or batch
    if is_streaming:
        cells.append(_streaming_sink_cell(mapping, cell_num))
    else:
        cells.append(_target_cell(mapping, cell_num))
    cell_num += 1

    # Change Data Feed reader cell (Databricks only, CDC mappings)
    if is_cdc and _get_target() == "databricks":
        cells.append(_change_feed_reader_cell(mapping, cell_num))
        cell_num += 1

    # Sprint 39: PII scanning cell (if PII columns detected)
    pii_columns = mapping.get("pii_columns", [])
    if pii_columns:
        pii_lines = [f"# CELL {cell_num} — PII Data Scanning (Sprint 39)"]
        pii_lines.append("# Auto-detected PII columns — apply masking or encryption before production use.")
        pii_lines.append("from pyspark.sql.functions import md5, sha2, regexp_replace, lit")
        pii_lines.append("")
        for pii in pii_columns:
            col_name = pii.get("field", "UNKNOWN").split(".")[-1]
            category = pii.get("pii_category", "UNKNOWN")
            sensitivity = pii.get("sensitivity", "Internal")
            pii_lines.append(f'# PII: {col_name} → {category} ({sensitivity})')
            if sensitivity == "Highly Confidential":
                pii_lines.append(f'# df = df.withColumn("{col_name}", sha2(col("{col_name}").cast("string"), 256))')
            else:
                pii_lines.append(f'# df = df.withColumn("{col_name}", md5(col("{col_name}").cast("string")))')
        cells.append("\n".join(pii_lines))
        cell_num += 1

    # Audit cell
    cells.append(_audit_cell(mapping, cell_num))

    return _cell_sep().join(cells)


def _write_notebook(mapping_and_output):
    """Write a single notebook — for use with ProcessPoolExecutor."""
    mapping, out_dir = mapping_and_output
    name = mapping["name"]
    out_path = Path(out_dir) / f"NB_{name}.py"
    content = generate_notebook(mapping)
    out_path.write_text(content, encoding="utf-8")
    return name, mapping.get("complexity", "?"), len(mapping.get("transformations", []))


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    # Sprint 37: Check for --parallel flag (passed via env or argv)
    parallel = "--parallel" in sys.argv

    target = _get_target()
    target_label = "Databricks (Unity Catalog)" if target == "databricks" else "Fabric"

    print("=" * 60)
    print(f"  Notebook Migration — Phase 2 [{target_label}]")
    if parallel:
        print("  (Parallel generation enabled)")
    print("=" * 60)
    print()

    mappings = inv.get("mappings", [])
    generated = 0

    if parallel and len(mappings) > 4:
        # Use ProcessPoolExecutor for large batches
        work_items = [(m, str(OUTPUT_DIR)) for m in mappings]
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(_write_notebook, wi) for wi in work_items]
            for future in as_completed(futures):
                name, complexity, tx_count = future.result()
                generated += 1
                print(f"  ✅ NB_{name}.py ({complexity}, {tx_count} transformations)")
    else:
        for m in mappings:
            name = m["name"]
            out_path = OUTPUT_DIR / f"NB_{name}.py"
            content = generate_notebook(m)
            out_path.write_text(content, encoding="utf-8")
            generated += 1

            tx_count = len(m.get("transformations", []))
            complexity = m.get("complexity", "?")
            print(f"  ✅ NB_{name}.py ({complexity}, {tx_count} transformations)")

    print()
    print("=" * 60)
    print(f"  Notebooks generated: {generated}")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
