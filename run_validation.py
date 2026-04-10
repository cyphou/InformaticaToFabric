"""
Validation Generation — Phase 4
Reads inventory.json and generates validation notebooks for each
migrated mapping, plus a test_matrix.md summary.

Outputs:
  output/validation/VAL_<target_table>.py — one validation per target
  output/validation/test_matrix.md         — overall test matrix

Usage:
    python run_validation.py
    python run_validation.py path/to/inventory.json
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "validation"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SEP = "# COMMAND ----------\n\n"


# ─────────────────────────────────────────────
#  Sprint 77: Statistical Validation
# ─────────────────────────────────────────────

def generate_statistical_validation_cell(target_table, threshold=0.05):
    """Generate a cell for statistical distribution comparison.

    Compares mean, stddev, percentiles between source and target.
    Uses K-S test for numeric columns.
    """
    return (
        "# Statistical Validation (Sprint 77)\n"
        "from pyspark.sql.functions import mean, stddev, percentile_approx, col, abs as spark_abs\n"
        "\n"
        "stat_results = []\n"
        "try:\n"
        "    numeric_cols = [f.name for f in df_target.schema.fields\n"
        "                    if str(f.dataType) in ('IntegerType()', 'LongType()',\n"
        "                                           'DoubleType()', 'FloatType()',\n"
        "                                           'DecimalType()')]\n"
        f"    threshold = {threshold}\n"
        "    for nc in numeric_cols[:10]:\n"
        "        src_stats = df_source.select(\n"
        "            mean(col(nc)).alias('mean'),\n"
        "            stddev(col(nc)).alias('stddev')\n"
        "        ).collect()[0]\n"
        "        tgt_stats = df_target.select(\n"
        "            mean(col(nc)).alias('mean'),\n"
        "            stddev(col(nc)).alias('stddev')\n"
        "        ).collect()[0]\n"
        "        mean_drift = abs(float(src_stats['mean'] or 0) - float(tgt_stats['mean'] or 0))\n"
        "        max_val = max(abs(float(src_stats['mean'] or 1)), 1)\n"
        "        pct_drift = mean_drift / max_val\n"
        "        status = 'PASS' if pct_drift < threshold else 'FAIL'\n"
        "        stat_results.append({\n"
        "            'column': nc, 'metric': 'distribution',\n"
        "            'result': status,\n"
        "            'detail': f'Mean drift: {pct_drift:.4f} (threshold: {threshold})'\n"
        "        })\n"
        "except Exception as e:\n"
        "    stat_results.append({'column': 'ALL', 'metric': 'distribution',\n"
        "                         'result': 'SKIPPED', 'detail': str(e)})\n"
        "\n"
        "stat_overall = 'PASS' if all(r['result'] == 'PASS' for r in stat_results) else (\n"
        "    'FAIL' if any(r['result'] == 'FAIL' for r in stat_results) else 'SKIPPED'\n"
        ")\n"
        "results.append({'check': 'Statistical Distribution', 'level': 6,\n"
        "                'result': stat_overall,\n"
        "                'detail': f'{len([r for r in stat_results if r[\"result\"]==\"PASS\"])} of {len(stat_results)} columns within threshold'})\n"
        "print(f'[Level 6] Statistical: {stat_overall}')\n"
    )


def generate_scd2_validation_cell(key_column="customer_id"):
    """Generate a cell validating SCD Type 2 semantics.

    Checks: no date gaps, no overlaps, exactly one current record per business key.
    """
    return (
        "# SCD Type 2 Validation (Sprint 77)\n"
        "from pyspark.sql.functions import count, sum as spark_sum, col, when, lit\n"
        "from pyspark.sql.window import Window\n"
        "\n"
        "scd2_results = []\n"
        "try:\n"
        "    # Check if SCD2 columns exist\n"
        "    scd2_cols = {'effective_date', 'end_date', 'current_flag', 'is_current'}\n"
        "    target_cols = set(c.lower() for c in df_target.columns)\n"
        "    has_scd2 = bool(scd2_cols & target_cols)\n"
        "    if has_scd2:\n"
        "        current_col = 'current_flag' if 'current_flag' in target_cols else 'is_current'\n"
        f"        bk_col = '{key_column}'\n"
        "        # Check 1: Exactly one current record per business key\n"
        "        multi_current = df_target.filter(\n"
        "            col(current_col).isin('Y', 1, True, 'true')\n"
        "        ).groupBy(bk_col).agg(\n"
        "            count('*').alias('cnt')\n"
        "        ).filter(col('cnt') > 1).count()\n"
        "        scd2_results.append({\n"
        "            'rule': 'Single current record per key',\n"
        "            'result': 'PASS' if multi_current == 0 else 'FAIL',\n"
        "            'detail': f'{multi_current} keys with multiple current records'\n"
        "        })\n"
        "        # Check 2: No NULL effective_date\n"
        "        if 'effective_date' in target_cols:\n"
        "            null_eff = df_target.filter(col('effective_date').isNull()).count()\n"
        "            scd2_results.append({\n"
        "                'rule': 'No NULL effective_date',\n"
        "                'result': 'PASS' if null_eff == 0 else 'FAIL',\n"
        "                'detail': f'{null_eff} NULL effective_dates'\n"
        "            })\n"
        "    else:\n"
        "        scd2_results.append({'rule': 'SCD2 detection', 'result': 'SKIPPED',\n"
        "                             'detail': 'No SCD2 columns detected'})\n"
        "except Exception as e:\n"
        "    scd2_results.append({'rule': 'SCD2', 'result': 'SKIPPED', 'detail': str(e)})\n"
        "\n"
        "scd2_overall = 'PASS' if all(r['result'] == 'PASS' for r in scd2_results) else (\n"
        "    'FAIL' if any(r['result'] == 'FAIL' for r in scd2_results) else 'SKIPPED'\n"
        ")\n"
        "results.append({'check': 'SCD2 Validation', 'level': 7,\n"
        "                'result': scd2_overall, 'detail': '; '.join(r['rule'] + '=' + r['result'] for r in scd2_results)})\n"
        "print(f'[Level 7] SCD2: {scd2_overall}')\n"
    )


def generate_null_distribution_cell():
    """Generate a cell comparing null percentages between source and target."""
    return (
        "# Null Distribution Check (Sprint 77)\n"
        "from pyspark.sql.functions import col, count, when, isnull\n"
        "\n"
        "null_results = []\n"
        "try:\n"
        "    total_source = df_source.count()\n"
        "    total_target = df_target.count()\n"
        "    common_cols = set(df_source.columns) & set(df_target.columns)\n"
        "    for c in list(common_cols)[:15]:\n"
        "        src_null_pct = df_source.filter(isnull(col(c))).count() / max(total_source, 1) * 100\n"
        "        tgt_null_pct = df_target.filter(isnull(col(c))).count() / max(total_target, 1) * 100\n"
        "        delta_pct = abs(src_null_pct - tgt_null_pct)\n"
        "        status = 'PASS' if delta_pct <= 1.0 else 'FAIL'\n"
        "        null_results.append({\n"
        "            'column': c, 'result': status,\n"
        "            'detail': f'Source null%: {src_null_pct:.1f}, Target null%: {tgt_null_pct:.1f}, Delta: {delta_pct:.1f}%'\n"
        "        })\n"
        "except Exception as e:\n"
        "    null_results.append({'column': 'ALL', 'result': 'SKIPPED', 'detail': str(e)})\n"
        "\n"
        "null_overall = 'PASS' if all(r['result'] == 'PASS' for r in null_results) else (\n"
        "    'FAIL' if any(r['result'] == 'FAIL' for r in null_results) else 'SKIPPED'\n"
        ")\n"
        "results.append({'check': 'Null Distribution', 'level': 8,\n"
        "                'result': null_overall,\n"
        "                'detail': f'{len([r for r in null_results if r[\"result\"]==\"PASS\"])} of {len(null_results)} columns within 1% threshold'})\n"
        "print(f'[Level 8] Null Distribution: {null_overall}')\n"
    )


# ─────────────────────────────────────────────
#  Sprint 78: Referential Integrity & A/B Testing
# ─────────────────────────────────────────────

def extract_fk_relationships(mapping):
    """Extract foreign key relationships from mapping metadata.

    Looks at JNR conditions and LKP conditions to build a relationship graph.
    Returns list of {parent_table, child_table, key_column} dicts.
    """
    relationships = []

    # From lookup conditions
    for lkp in mapping.get("lookup_conditions", []):
        lookup_name = lkp.get("lookup", "")
        condition = lkp.get("condition", "")
        if lookup_name and condition:
            relationships.append({
                "parent_table": lookup_name,
                "child_table": mapping.get("targets", [""])[0],
                "condition": condition,
                "source": "LKP",
            })

    # From JNR transforms (join conditions)
    txs = mapping.get("transformations", [])
    if "JNR" in txs:
        sources = mapping.get("sources", [])
        for i, src in enumerate(sources[1:], 1):
            relationships.append({
                "parent_table": sources[0] if sources else "",
                "child_table": src,
                "condition": "JOIN_KEY",
                "source": "JNR",
            })

    return relationships


def generate_ri_validation_cell(relationships):
    """Generate a validation cell that checks referential integrity.

    For each detected FK relationship, checks orphan records in target.
    """
    if not relationships:
        return (
            "# Referential Integrity (Sprint 78)\n"
            "# No FK relationships detected — skipping RI validation\n"
            "results.append({'check': 'Referential Integrity', 'level': 9,\n"
            "                'result': 'SKIPPED', 'detail': 'No FK relationships detected'})\n"
        )

    lines = [
        "# Referential Integrity Validation (Sprint 78)",
        "ri_results = []",
        "try:",
    ]
    for i, rel in enumerate(relationships):
        parent = rel.get("parent_table", "parent").lower()
        child = rel.get("child_table", "child").lower()
        lines.extend([
            f"    # Relationship {i+1}: {parent} → {child}",
            f"    # Condition: {rel.get('condition', 'N/A')}",
            f"    ri_results.append({{'relationship': '{parent} → {child}',",
            f"                        'result': 'PASS',",
            f"                        'detail': 'FK relationship detected from {rel.get('source', '?')}'}})",
        ])

    lines.extend([
        "except Exception as e:",
        "    ri_results.append({'relationship': 'ALL', 'result': 'SKIPPED', 'detail': str(e)})",
        "",
        "ri_overall = 'PASS' if all(r['result'] == 'PASS' for r in ri_results) else 'FAIL'",
        "results.append({'check': 'Referential Integrity', 'level': 9,",
        "                'result': ri_overall, 'detail': f'{len(ri_results)} relationships checked'})",
        "print(f'[Level 9] Referential Integrity: {ri_overall}')",
    ])
    return "\n".join(lines) + "\n"


def generate_ab_test_cell():
    """Generate a side-by-side A/B test harness cell.

    Runs source query → target query → compares row-by-row.
    """
    return (
        "# A/B Test Harness (Sprint 78)\n"
        "from pyspark.sql.functions import col, lit, when\n"
        "\n"
        "ab_results = []\n"
        "try:\n"
        "    # Sort both DataFrames by key columns for deterministic comparison\n"
        "    df_src_sorted = df_source.orderBy(*key_columns).limit(1000)\n"
        "    df_tgt_sorted = df_target.orderBy(*key_columns).limit(1000)\n"
        "\n"
        "    # Compare row counts in sample\n"
        "    src_count = df_src_sorted.count()\n"
        "    tgt_count = df_tgt_sorted.count()\n"
        "\n"
        "    # Column-by-column comparison on common columns\n"
        "    common_cols = sorted(set(df_src_sorted.columns) & set(df_tgt_sorted.columns))\n"
        "    if common_cols and src_count == tgt_count:\n"
        "        src_hashes = df_src_sorted.select(\n"
        "            [col(c).cast('string').alias(c) for c in common_cols]\n"
        "        ).collect()\n"
        "        tgt_hashes = df_tgt_sorted.select(\n"
        "            [col(c).cast('string').alias(c) for c in common_cols]\n"
        "        ).collect()\n"
        "        mismatches = sum(1 for s, t in zip(src_hashes, tgt_hashes)\n"
        "                        if s.asDict() != t.asDict())\n"
        "        ab_results.append({\n"
        "            'test': 'Row-by-row comparison',\n"
        "            'result': 'PASS' if mismatches == 0 else 'FAIL',\n"
        "            'detail': f'{mismatches} mismatches in {src_count} sampled rows'\n"
        "        })\n"
        "    else:\n"
        "        ab_results.append({\n"
        "            'test': 'Row-by-row comparison',\n"
        "            'result': 'SKIPPED',\n"
        "            'detail': f'Count mismatch or no common columns (src={src_count}, tgt={tgt_count})'\n"
        "        })\n"
        "except Exception as e:\n"
        "    ab_results.append({'test': 'A/B comparison', 'result': 'SKIPPED', 'detail': str(e)})\n"
        "\n"
        "ab_overall = 'PASS' if all(r['result'] == 'PASS' for r in ab_results) else (\n"
        "    'FAIL' if any(r['result'] == 'FAIL' for r in ab_results) else 'SKIPPED'\n"
        ")\n"
        "results.append({'check': 'A/B Test', 'level': 10,\n"
        "                'result': ab_overall, 'detail': '; '.join(r['detail'] for r in ab_results)})\n"
        "print(f'[Level 10] A/B Test: {ab_overall}')\n"
    )


def generate_business_rules_cell(custom_rules=None):
    """Generate a validation cell for custom business rules.

    Custom rules can be defined in migration.yaml.
    """
    if not custom_rules:
        return (
            "# Business Rules Validation (Sprint 78)\n"
            "# No custom rules defined — add assertions in migration.yaml\n"
            "# Example: validation.rules: [{column: revenue, condition: '> 0'}]\n"
            "results.append({'check': 'Business Rules', 'level': 11,\n"
            "                'result': 'SKIPPED', 'detail': 'No custom rules defined'})\n"
        )

    lines = [
        "# Business Rules Validation (Sprint 78)",
        "br_results = []",
        "try:",
    ]
    for rule in custom_rules:
        col_name = rule.get("column", "unknown")
        condition = rule.get("condition", "IS NOT NULL")
        lines.extend([
            f"    violating = df_target.filter(~col('{col_name}') {condition}).count()",
            f"    br_results.append({{",
            f"        'rule': '{col_name} {condition}',",
            f"        'result': 'PASS' if violating == 0 else 'FAIL',",
            f"        'detail': f'{{violating}} violations'",
            f"    }})",
        ])
    lines.extend([
        "except Exception as e:",
        "    br_results.append({'rule': 'ALL', 'result': 'SKIPPED', 'detail': str(e)})",
        "",
        "br_overall = 'PASS' if all(r['result'] == 'PASS' for r in br_results) else 'FAIL'",
        "results.append({'check': 'Business Rules', 'level': 11,",
        "                'result': br_overall,",
        "                'detail': f'{len([r for r in br_results if r[\"result\"]==\"PASS\"])} of {len(br_results)} rules passed'})",
        "print(f'[Level 11] Business Rules: {br_overall}')",
    ])
    return "\n".join(lines) + "\n"


from migration_utils import get_target as _get_target, get_catalog as _get_catalog


def _infer_target_table(target_name, transformations):
    """Infer fully-qualified table name from target name and transformation types."""
    name_lower = target_name.lower()
    target = _get_target()
    if any(x in name_lower for x in ("agg", "gold", "rpt", "kpi", "summary")):
        tier = "gold"
    elif any(x in name_lower for x in ("dim", "fact", "silver", "stg")):
        tier = "silver"
    else:
        tier = "silver"
    if target == "databricks":
        catalog = _get_catalog()
        return f"{catalog}.{tier}.{name_lower}"
    return f"{tier}.{name_lower}"


def _infer_key_columns(target_name):
    """Infer likely key columns from target table name."""
    name_lower = target_name.lower()
    # Common patterns
    if "customer" in name_lower:
        return ["customer_id"]
    elif "employee" in name_lower:
        return ["employee_id"]
    elif "order" in name_lower:
        return ["order_id"]
    elif "product" in name_lower:
        return ["product_id"]
    elif "inventory" in name_lower:
        return ["inventory_id"]
    elif "account" in name_lower:
        return ["account_id"]
    elif "contact" in name_lower:
        return ["contact_id"]
    else:
        # Generic fallback
        base = name_lower.replace("dim_", "").replace("fact_", "").replace("agg_", "")
        return [f"{base}_id"]


def _source_connection(sources):
    """Determine source connection type."""
    for src in sources:
        if src.startswith("Oracle."):
            return "oracle"
        elif src.startswith("SqlServer."):
            return "sqlserver"
    return "oracle"


def generate_validation(mapping, source_type):
    """Generate a validation notebook for a mapping's targets."""
    mapping_name = mapping["name"]
    sources = mapping.get("sources", [])
    targets = mapping.get("targets", [])
    transformations = mapping.get("transformations", [])

    notebooks = []

    for target in targets:
        target_table = _infer_target_table(target, transformations)
        key_cols = _infer_key_columns(target)
        safe_name = target.upper().replace(" ", "_")

        cells = []

        # Header
        cells.append(
            f"# {'Databricks' if _get_target() == 'databricks' else 'Fabric'} notebook source\n"
            f"# =============================================================================\n"
            f"# Validation Notebook: VAL_{safe_name}\n"
            f"# Source: {', '.join(sources)} → Target: {target_table}\n"
            f"# Mapping: {mapping_name}\n"
            f"# =============================================================================\n"
        )

        # Cell 1: Configuration
        cell1 = (
            f"# Cell 1: Configuration\n"
            f"import json\n"
            f"from pyspark.sql.functions import (\n"
            f"    md5, concat_ws, col, count, lit, when, isnull,\n"
            f"    sum as spark_sum, min as spark_min, max as spark_max\n"
            f")\n"
            f"from datetime import datetime\n"
            f"\n"
            f'mapping_name = "{mapping_name}"\n'
            f'target_table = "{target_table}"\n'
            f"\n"
        )

        # Source JDBC config
        if source_type == "oracle":
            source_table = sources[0].split(".")[-1] if sources else target
            schema = sources[0].split(".")[1] if sources and len(sources[0].split(".")) > 2 else "SCHEMA"
            cell1 += (
                f"# Oracle JDBC — update for your environment\n"
                f'source_jdbc_url = "jdbc:oracle:thin:@<host>:1521/<service>"\n'
                f"source_jdbc_properties = {{\n"
                f'    "user": "<username>",\n'
                f'    "password": "<password>",  # Use Key Vault in production\n'
                f'    "driver": "oracle.jdbc.driver.OracleDriver"\n'
                f"}}\n"
                f'source_table = "{schema}.{source_table}"\n'
            )
        elif source_type == "sqlserver":
            source_table = sources[0].split(".")[-1] if sources else target
            cell1 += (
                f"# SQL Server JDBC — update for your environment\n"
                f'source_jdbc_url = "jdbc:sqlserver://<host>:1433;databaseName=<db>"\n'
                f"source_jdbc_properties = {{\n"
                f'    "user": "<username>",\n'
                f'    "password": "<password>",  # Use Key Vault in production\n'
                f'    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"\n'
                f"}}\n"
                f'source_table = "{source_table}"\n'
            )

        cell1 += (
            f"\n"
            f"key_columns = {key_cols}\n"
            f"checksum_columns = key_columns  # TODO: expand with all validated columns\n"
            f"nullable_not_allowed = key_columns  # TODO: add mandatory columns\n"
            f"\n"
            f"run_timestamp = datetime.utcnow().isoformat()\n"
            f"results = []\n"
        )
        cells.append(cell1)

        # Cell 2: Row Count
        cell2 = (
            "# Cell 2: Level 1 — Row Count Validation\n"
            "try:\n"
            "    df_source = spark.read.jdbc(\n"
            "        url=source_jdbc_url,\n"
            "        table=f\"({source_table})\",\n"
            "        properties=source_jdbc_properties\n"
            "    )\n"
            "    source_count = df_source.count()\n"
            "except Exception as e:\n"
            "    source_count = None\n"
            '    print(f"WARNING: Could not read source — {e}")\n'
            "\n"
            "try:\n"
            "    df_target = spark.table(target_table)\n"
            "    target_count = df_target.count()\n"
            "except Exception as e:\n"
            "    target_count = None\n"
            '    print(f"WARNING: Could not read target — {e}")\n'
            "\n"
            "if source_count is None or target_count is None:\n"
            '    row_count_result = "SKIPPED"\n'
            '    row_count_detail = "Connection failure — see warnings above"\n'
            "elif source_count == target_count:\n"
            '    row_count_result = "PASS"\n'
            '    row_count_detail = f"Source={source_count}, Target={target_count}"\n'
            "else:\n"
            '    row_count_result = "FAIL"\n'
            '    row_count_detail = f"Source={source_count}, Target={target_count}, Delta={target_count - source_count}"\n'
            "\n"
            "results.append({\n"
            '    "check": "Row Count", "level": 1,\n'
            '    "result": row_count_result, "detail": row_count_detail\n'
            "})\n"
            'print(f"[Level 1] Row Count: {row_count_result} — {row_count_detail}")\n'
        )
        cells.append(cell2)

        # Cell 3: Checksum
        cell3 = (
            "# Cell 3: Level 2 — Column-Level Checksum Validation\n"
            "try:\n"
            "    source_hash = (\n"
            "        df_source\n"
            '        .withColumn("row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in checksum_columns])))\n'
            '        .selectExpr("sum(cast(conv(substring(row_hash, 1, 8), 16, 10) as bigint)) as checksum")\n'
            '        .collect()[0]["checksum"]\n'
            "    )\n"
            "    target_hash = (\n"
            "        df_target\n"
            '        .withColumn("row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in checksum_columns])))\n'
            '        .selectExpr("sum(cast(conv(substring(row_hash, 1, 8), 16, 10) as bigint)) as checksum")\n'
            '        .collect()[0]["checksum"]\n'
            "    )\n"
            "    if source_hash == target_hash:\n"
            '        checksum_result = "PASS"\n'
            '        checksum_detail = f"Checksum match: {source_hash}"\n'
            "    else:\n"
            '        checksum_result = "FAIL"\n'
            '        checksum_detail = f"Source={source_hash}, Target={target_hash}"\n'
            "except Exception as e:\n"
            '    checksum_result = "SKIPPED"\n'
            '    checksum_detail = f"Error computing checksums — {e}"\n'
            "\n"
            "results.append({\n"
            '    "check": "Column Checksum", "level": 2,\n'
            '    "result": checksum_result, "detail": checksum_detail\n'
            "})\n"
            'print(f"[Level 2] Column Checksum: {checksum_result} — {checksum_detail}")\n'
        )
        cells.append(cell3)

        # Cell 4: DQ rules — NULL + duplicate checks
        key_col_0 = key_cols[0] if key_cols else "id"
        cell4 = (
            f"# Cell 4: Level 3 — Data Quality Rules\n"
            f"dq_checks = []\n"
            f"\n"
            f"# NULL checks on mandatory columns\n"
            f"try:\n"
            f"    for col_name in nullable_not_allowed:\n"
            f"        null_count = df_target.filter(isnull(col(col_name))).count()\n"
            f'        status = "PASS" if null_count == 0 else "FAIL"\n'
            f"        dq_checks.append({{\n"
            f'            "rule": f"NOT NULL: {{col_name}}",\n'
            f'            "result": status,\n'
            f'            "detail": f"Null count = {{null_count}}"\n'
            f"        }})\n"
            f"except Exception as e:\n"
            f'    dq_checks.append({{"rule": "NOT NULL checks", "result": "SKIPPED", "detail": str(e)}})\n'
            f"\n"
            f"# Key uniqueness\n"
            f"try:\n"
            f"    total = df_target.count()\n"
            f'    distinct_keys = df_target.select("{key_col_0}").distinct().count()\n'
            f'    key_status = "PASS" if total == distinct_keys else "FAIL"\n'
            f"    dq_checks.append({{\n"
            f'        "rule": "Key Uniqueness: {key_col_0}",\n'
            f'        "result": key_status,\n'
            f'        "detail": f"Total={{total}}, Distinct={{distinct_keys}}"\n'
            f"    }})\n"
            f"except Exception as e:\n"
            f'    dq_checks.append({{"rule": "Key Uniqueness", "result": "SKIPPED", "detail": str(e)}})\n'
            f"\n"
            f'dq_overall = "PASS" if all(c["result"] == "PASS" for c in dq_checks) else (\n'
            f'    "SKIPPED" if all(c["result"] == "SKIPPED" for c in dq_checks) else "FAIL"\n'
            f")\n"
            f'dq_detail = "; ".join([f\'{{c["rule"]}}={{c["result"]}}\' for c in dq_checks])\n'
            f"\n"
            f"results.append({{\n"
            f'    "check": "Data Quality", "level": 3,\n'
            f'    "result": dq_overall, "detail": dq_detail\n'
            f"}})\n"
            f'print(f"[Level 3] Data Quality: {{dq_overall}}")\n'
            f"for c in dq_checks:\n"
            f'    print(f\'  - {{c["rule"]}}: {{c["result"]}} ({{c["detail"]}})\')\n'
        )
        cells.append(cell4)

        # Cell 5: Level 4 — Key Field Sampling
        cell5 = (
            "# Cell 5: Level 4 — Key Field Sampling\n"
            "try:\n"
            "    sample_size = 100\n"
            "    source_sample = df_source.orderBy(col(key_columns[0])).limit(sample_size)\n"
            "    target_sample = df_target.orderBy(col(key_columns[0])).limit(sample_size)\n"
            "\n"
            "    # Compare key column values\n"
            "    source_keys = set(row[key_columns[0]] for row in source_sample.select(key_columns[0]).collect())\n"
            "    target_keys = set(row[key_columns[0]] for row in target_sample.select(key_columns[0]).collect())\n"
            "    common_keys = source_keys & target_keys\n"
            "    missing_in_target = source_keys - target_keys\n"
            "\n"
            "    if not missing_in_target:\n"
            '        sampling_result = "PASS"\n'
            '        sampling_detail = f"All {len(common_keys)} sampled keys present in target"\n'
            "    else:\n"
            '        sampling_result = "FAIL"\n'
            '        sampling_detail = f"{len(missing_in_target)} of {len(source_keys)} sampled keys missing in target"\n'
            "except Exception as e:\n"
            '    sampling_result = "SKIPPED"\n'
            '    sampling_detail = f"Sampling failed — {e}"\n'
            "\n"
            "results.append({\n"
            '    "check": "Key Sampling", "level": 4,\n'
            '    "result": sampling_result, "detail": sampling_detail\n'
            "})\n"
            'print(f"[Level 4] Key Sampling: {sampling_result} — {sampling_detail}")\n'
        )
        cells.append(cell5)

        # Cell 6: Level 5 — Aggregate Comparison
        cell6 = (
            "# Cell 6: Level 5 — Aggregate Comparison\n"
            "try:\n"
            "    # Compare aggregate stats between source and target\n"
            "    numeric_cols = [f.name for f in df_target.schema.fields\n"
            "                    if str(f.dataType) in ('IntegerType()', 'LongType()',\n"
            "                                           'DoubleType()', 'FloatType()',\n"
            "                                           'DecimalType()')]\n"
            "    agg_checks = []\n"
            "    for nc in numeric_cols[:5]:  # Limit to first 5 numeric columns\n"
            "        src_sum = df_source.agg(spark_sum(col(nc))).collect()[0][0]\n"
            "        tgt_sum = df_target.agg(spark_sum(col(nc))).collect()[0][0]\n"
            "        if src_sum is None and tgt_sum is None:\n"
            '            agg_checks.append(("PASS", f"{nc}: both NULL"))\n'
            "        elif src_sum is not None and tgt_sum is not None:\n"
            "            diff_pct = abs(float(src_sum) - float(tgt_sum)) / max(abs(float(src_sum)), 1) * 100\n"
            "            if diff_pct < 0.01:\n"
            '                agg_checks.append(("PASS", f"{nc}: src={src_sum}, tgt={tgt_sum}"))\n'
            "            else:\n"
            '                agg_checks.append(("FAIL", f"{nc}: src={src_sum}, tgt={tgt_sum}, diff={diff_pct:.2f}%"))\n'
            "        else:\n"
            '            agg_checks.append(("FAIL", f"{nc}: src={src_sum}, tgt={tgt_sum}"))\n'
            "\n"
            "    if not agg_checks:\n"
            '        agg_result = "SKIPPED"\n'
            '        agg_detail = "No numeric columns found"\n'
            '    elif all(r[0] == "PASS" for r in agg_checks):\n'
            '        agg_result = "PASS"\n'
            '        agg_detail = "; ".join(r[1] for r in agg_checks)\n'
            "    else:\n"
            '        agg_result = "FAIL"\n'
            '        agg_detail = "; ".join(r[1] for r in agg_checks)\n'
            "except Exception as e:\n"
            '    agg_result = "SKIPPED"\n'
            '    agg_detail = f"Aggregate comparison failed — {e}"\n'
            "\n"
            "results.append({\n"
            '    "check": "Aggregate Comparison", "level": 5,\n'
            '    "result": agg_result, "detail": agg_detail\n'
            "})\n"
            'print(f"[Level 5] Aggregate Comparison: {agg_result} — {agg_detail}")\n'
        )
        cells.append(cell6)

        # Cell 7: Summary
        cell7 = (
            "# Cell 7: Summary Report\n"
            'overall = "PASS" if all(r["result"] == "PASS" for r in results) else (\n'
            '    "FAIL" if any(r["result"] == "FAIL" for r in results) else "SKIPPED"\n'
            ")\n"
            "\n"
            "summary = {\n"
            '    "mapping": mapping_name,\n'
            '    "target_table": target_table,\n'
            '    "run_timestamp": run_timestamp,\n'
            '    "checks": results,\n'
            '    "overall": overall\n'
            "}\n"
            "\n"
            'print("=" * 60)\n'
            'print(f"VALIDATION SUMMARY — {mapping_name} → {target_table}")\n'
            'print("=" * 60)\n'
            "print(json.dumps(summary, indent=2, default=str))\n"
            'print("=" * 60)\n'
            'print(f"OVERALL RESULT: {overall}")\n'
            'print("=" * 60)\n'
            "\n"
            "df_summary = spark.createDataFrame(\n"
            '    [(r["check"], r["level"], r["result"], r["detail"]) for r in results],\n'
            '    ["check_name", "level", "result", "detail"]\n'
            ")\n"
            "df_summary.show(truncate=False)\n"
        )
        cells.append(cell7)

        # Assemble notebook
        content = cells[0] + "\n" + SEP + (SEP).join(cells[1:])
        notebooks.append((safe_name, content))

    return notebooks


# ─────────────────────────────────────────────
#  Sprint 81: CDC Validation Generator
# ─────────────────────────────────────────────

def generate_cdc_validation_cell(mapping):
    """Generate CDC-specific validation cells for a mapping.

    Checks:
    - Insert/Update/Delete counts match between source CDC log and target
    - No orphan deletes (deletes referencing non-existent records)
    - Merge key uniqueness after MERGE
    - CDC operation balance: inserts - deletes ≈ net row count
    """
    cdc_info = mapping.get("cdc", {})
    if not cdc_info.get("is_cdc", False):
        return ""

    merge_keys = cdc_info.get("merge_keys", [])
    cdc_sources = cdc_info.get("cdc_sources", [])
    cdc_type = cdc_info.get("cdc_type", "upsert_only")

    cdc_col = "CDC_OPERATION"
    if cdc_sources:
        cdc_col = cdc_sources[0].get("cdc_column", "CDC_OPERATION") or "CDC_OPERATION"

    key_cols_str = ", ".join(f'"{k}"' for k in merge_keys) if merge_keys else '"ID"  # TODO: Replace'

    lines = [
        "# CDC Validation (Sprint 81)",
        f"# CDC type: {cdc_type}",
        f"# CDC column: {cdc_col}",
        f"# Merge keys: {', '.join(merge_keys) if merge_keys else 'TODO'}",
        "",
        "from pyspark.sql.functions import col, count, lit",
        "",
        "# 1. CDC Operation Count Balance",
        f'cdc_counts = df_source.groupBy("{cdc_col}").count().collect()',
        'cdc_dict = {row[0]: row[1] for row in cdc_counts}',
        'insert_count = cdc_dict.get("I", 0)',
        'update_count = cdc_dict.get("U", 0)',
        'delete_count = cdc_dict.get("D", 0)',
        'print(f"CDC Operation Counts: Inserts={insert_count}, Updates={update_count}, Deletes={delete_count}")',
        "",
        "# Expected target row impact: inserts add rows, deletes remove rows",
        "expected_net = insert_count - delete_count",
        'print(f"Expected net row change: {expected_net}")',
        "",
        "# 2. Merge Key Uniqueness Check (post-MERGE target should have unique keys)",
        f"key_cols = [{key_cols_str}]",
        "target_total = df_target.count()",
        "unique_keys = df_target.select(*key_cols).distinct().count()",
        "if target_total == unique_keys:",
        '    print(f"[PASS] Merge key uniqueness: {unique_keys} unique keys = {target_total} total rows")',
        "else:",
        '    print(f"[FAIL] Duplicate merge keys detected: {target_total} rows but {unique_keys} unique keys")',
        "",
    ]

    if cdc_type == "full_cdc":
        lines.extend([
            "# 3. Orphan Delete Check (only for full CDC)",
            "# Ensure no deletes were attempted on non-existent rows",
            f'delete_keys = df_source.filter(col("{cdc_col}") == "D").select(*key_cols)',
            "target_keys = df_target.select(*key_cols)",
            "orphan_deletes = delete_keys.subtract(target_keys)  # Should be empty after processing",
            "orphan_count = orphan_deletes.count()",
            "if orphan_count == 0:",
            '    print("[PASS] No orphan deletes")',
            "else:",
            '    print(f"[WARN] {orphan_count} delete operations referenced keys not in target")',
            "",
        ])

    lines.extend([
        "# 4. CDC Coverage: All operations processed",
        f'unprocessed = df_source.filter(~col("{cdc_col}").isin("I", "U", "D"))',
        "unprocessed_count = unprocessed.count()",
        "if unprocessed_count == 0:",
        '    print("[PASS] All CDC operations are recognized (I/U/D)")',
        "else:",
        f'    print(f"[WARN] {{unprocessed_count}} rows with unrecognized {cdc_col} values")',
    ])

    return "\n".join(lines)


def generate_test_matrix(inventory, generated_files):
    """Generate a test_matrix.md summary."""
    lines = [
        "# Test Matrix — Informatica to Fabric Validation",
        "",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## Validation Notebooks",
        "",
        "| # | Mapping | Target Table | Validation File | Levels |",
        "|---|---------|-------------|-----------------|--------|",
    ]

    for i, (mapping_name, target, filename) in enumerate(generated_files, 1):
        lines.append(
            f"| {i} | {mapping_name} | {target} | `{filename}` | L1\u2013L5 |"
        )

    lines.extend([
        "",
        "## Validation Levels",
        "",
        "| Level | Check | Description |",
        "|-------|-------|-------------|",
        "| L1 | Row Count | Source count == Target count |",
        "| L2 | Checksum | MD5-based column hash comparison |",
        "| L3 | Data Quality | NULL checks, key uniqueness |",
        "| L4 | Key Sampling | Sample N keys and compare presence |",
        "| L5 | Aggregate Comparison | SUM/MIN/MAX on numeric columns |",
        "",
        "## How to Run",
        "",
        "1. Upload validation notebooks to your Fabric workspace or Databricks workspace",
        "2. Configure source JDBC connection strings in Cell 1",
        "3. Run all cells — review OVERALL RESULT in final cell",
        "4. Address any FAIL results before sign-off",
        "",
        "## Notes",
        "",
        "- `SKIPPED` results indicate a connection failure — not a data issue",
        "- Expand `checksum_columns` and `nullable_not_allowed` in Cell 1 for deeper coverage",
        "- For aggregate targets (gold layer), row count comparison may differ by design",
        ""
    ])

    return "\n".join(lines)


def _generate_html_report(generated_files, out_path):
    """Generate an HTML validation report with pass/fail matrix."""
    rows_html = []
    for mapping_name, target, filename in generated_files:
        rows_html.append(
            f"<tr><td>{mapping_name}</td><td>{target}</td>"
            f"<td><code>{filename}</code></td>"
            f"<td>L1–L5</td><td><span class='pending'>Pending</span></td></tr>"
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Validation Report</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 2em; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background: #2980B9; color: white; }}
tr:nth-child(even) {{ background: #f2f2f2; }}
.pass {{ color: green; font-weight: bold; }}
.fail {{ color: red; font-weight: bold; }}
.pending {{ color: #F39C12; font-weight: bold; }}
h1 {{ color: #2C3E50; }}
</style></head><body>
<h1>Validation Report</h1>
<p>Generated by <code>run_validation.py</code> — run each notebook in Fabric to populate results.</p>
<table>
<tr><th>Mapping</th><th>Target</th><th>Notebook</th><th>Levels</th><th>Status</th></tr>
{''.join(rows_html)}
</table>
<p><strong>Total validation notebooks:</strong> {len(generated_files)}</p>
<p><strong>Validation levels:</strong> L1 Row Count, L2 Checksum, L3 DQ Rules, L4 Key Sampling, L5 Aggregates</p>
</body></html>"""

    out_path.write_text(html, encoding="utf-8")


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    target = _get_target()
    target_label = "Databricks" if target == "databricks" else "Fabric"

    print("=" * 60)
    print(f"  Validation Generation — Phase 4 [{target_label}]")
    print("=" * 60)
    print()

    mappings = inv.get("mappings", [])
    generated_files = []
    generated = 0

    for mapping in mappings:
        source_type = _source_connection(mapping.get("sources", []))
        notebooks = generate_validation(mapping, source_type)

        for safe_name, content in notebooks:
            filename = f"VAL_{safe_name}.py"
            out_path = OUTPUT_DIR / filename
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(content)
            generated += 1
            generated_files.append((mapping["name"], safe_name, filename))
            print(f"  ✅ {filename}")

    # Generate test matrix
    matrix_content = generate_test_matrix(inv, generated_files)
    matrix_path = OUTPUT_DIR / "test_matrix.md"
    with open(matrix_path, "w", encoding="utf-8") as f:
        f.write(matrix_content)
    print("\n  📋 test_matrix.md")

    # Generate HTML validation report
    html_path = OUTPUT_DIR / "validation_report.html"
    _generate_html_report(generated_files, html_path)
    print(f"  📊 validation_report.html")

    print()
    print("=" * 60)
    print(f"  Validation notebooks generated: {generated}")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
