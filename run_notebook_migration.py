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
import sys
from pathlib import Path
from datetime import datetime, timezone

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "notebooks"
SQL_DIR = WORKSPACE / "output" / "sql"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
    # Placeholders
    "JTX": "placeholder",
    "CT": "placeholder",
    "HTTP": "placeholder",
    "XMLG": "placeholder",
    "XMLP": "placeholder",
    "TC": "placeholder",
}


def _cell_sep():
    return "\n# COMMAND ----------\n\n"


def _metadata_cell(mapping):
    """Generate the metadata + imports cell."""
    name = mapping["name"]
    complexity = mapping.get("complexity", "Unknown")
    sources = ", ".join(mapping.get("sources", []))
    targets = ", ".join(mapping.get("targets", []))
    txs = " → ".join(mapping.get("transformations", []))
    params = mapping.get("parameters", [])

    lines = [
        "# Fabric notebook source",
        "",
        "# METADATA_START",
        '# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}',
        "# METADATA_END",
        "",
        f"# CELL 1 — Metadata & Parameters",
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
    ]

    if params:
        lines.append("")
        lines.append("# Parameters (from Informatica $$params / pipeline)")
        for p in params:
            safe_name = p.replace("$$", "").lower()
            lines.append(f'{safe_name} = notebookutils.widgets.get("{safe_name}")')

    return "\n".join(lines)


def _source_cell(mapping, cell_num):
    """Generate the source read cell."""
    sources = mapping.get("sources", [])
    lines = [f"# CELL {cell_num} — Source Read"]

    for i, src in enumerate(sources):
        parts = src.split(".")
        if len(parts) >= 3:
            schema_table = f"{parts[1]}.{parts[2]}"
            lakehouse_table = f"bronze.{parts[2].lower()}"
        elif len(parts) == 2:
            schema_table = src
            lakehouse_table = f"bronze.{parts[1].lower()}"
        else:
            schema_table = src
            lakehouse_table = f"bronze.{src.lower()}"

        var_name = f"df_source" if i == 0 else f"df_source_{i + 1}"
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
                'df_lookup = spark.table("bronze.lookup_table")  # TODO: Replace with actual lookup table',
                f"df = {prev_df}.join(",
                '    broadcast(df_lookup),',
                '    on="LOOKUP_KEY",  # TODO: Replace with actual lookup condition',
                '    how="left"',
                ")",
            ])
        else:
            lines.extend([
                "# --- Lookup transformation ---",
                'df_lookup = spark.table("bronze.lookup_table")  # TODO: Replace',
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
        lines.extend([
            "# --- Update Strategy → Delta MERGE ---",
            f'target_table = DeltaTable.forName(spark, "silver.{target}")',
            f"target_table.alias('tgt').merge(",
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
            "# --- Web Service Consumer → TODO: Replace with pipeline Web Activity ---",
            "# Option 1: Pipeline Web Activity (preferred for external API calls)",
            "# Option 2: Python requests UDF (for inline processing)",
            "# import requests",
            "# response = requests.get('https://api.example.com/data')  # TODO: Configure",
            f"df = {prev_df}",
        ])

    elif tx_type == "MPLT":
        lines.extend([
            "# --- Mapplet (expanded) ---",
            "# Reusable transformation logic from Informatica Mapplet.",
            "# Inner transformations have been expanded inline above.",
            f"df = {prev_df}",
        ])

    else:
        # Placeholder for unknown/custom types
        lines.extend([
            f"# --- {tx_type} transformation (PLACEHOLDER) ---",
            f"# TODO: Manual conversion required for {tx_type}",
            f"#   This transformation type requires manual review and PySpark implementation.",
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
            lakehouse = "gold"
        elif "dim" in target_lower or "fact" in target_lower:
            lakehouse = "silver"
        else:
            lakehouse = "silver"

        table_name = f"{lakehouse}.{target_lower}"

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


def generate_notebook(mapping):
    """Generate a complete notebook .py file for a mapping."""
    cells = []

    # Cell 1: Metadata + imports
    cells.append(_metadata_cell(mapping))

    cell_num = 2

    # Cell 2: Source read
    cells.append(_source_cell(mapping, cell_num))
    cell_num += 1

    # Transformation cells (skip SQ — handled in source cell)
    txs = mapping.get("transformations", [])
    for i, tx in enumerate(txs):
        if tx == "SQ":
            continue
        cell_content = _transformation_cell(tx, i, mapping, cell_num)
        if cell_content:
            cells.append(cell_content)
            cell_num += 1

    # Target write cell
    cells.append(_target_cell(mapping, cell_num))
    cell_num += 1

    # Audit cell
    cells.append(_audit_cell(mapping, cell_num))

    return _cell_sep().join(cells)


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, "r", encoding="utf-8") as f:
        inv = json.load(f)

    print("=" * 60)
    print("  Notebook Migration — Phase 2")
    print("=" * 60)
    print()

    mappings = inv.get("mappings", [])
    generated = 0

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
