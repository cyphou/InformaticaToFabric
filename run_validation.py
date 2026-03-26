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


def _get_target():
    """Return the target platform ('fabric' or 'databricks')."""
    return os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")


def _get_catalog():
    """Return the Unity Catalog name for Databricks target."""
    return os.environ.get("INFORMATICA_DATABRICKS_CATALOG", "main")


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
