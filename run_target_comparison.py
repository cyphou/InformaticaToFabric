"""
Sprint 45 — Cross-Platform Target Comparison & Migration Advisor

Generates:
  1. output/comparison_report.md  — side-by-side Databricks vs DBT comparison
  2. output/inventory/target_recommendation.md — per-mapping target recommendation
  3. output/manifest.json — unified deployment manifest (both targets)

Usage:
    python run_target_comparison.py
    python run_target_comparison.py --inventory output/inventory/inventory.json
    python run_target_comparison.py --advisor-only
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output"
INVENTORY_PATH = OUTPUT_DIR / "inventory" / "inventory.json"


# ─────────────────────────────────────────────
#  Migration Advisor — Databricks vs DBT
# ─────────────────────────────────────────────

# Transformations that are SQL-friendly → lean DBT
_SQL_FRIENDLY_TX = {"SQ", "FIL", "EXP", "AGG", "LKP", "SRT", "UNI", "ULKP"}

# Transformations that need PySpark → lean Databricks notebooks
_PYSPARK_TX = {
    "JTX", "CT", "HTTP", "XMLG", "XMLP", "WSC", "SP", "SQLT",
    "EP", "AEP", "NRM", "DM", "KEYGEN", "ADDRVAL", "TC",
}

# Complexity weights for cost estimation
_COMPLEXITY_DBU = {"Simple": 0.5, "Medium": 1.2, "Complex": 2.5}
_COMPLEXITY_HOURS = {"Simple": 2, "Medium": 8, "Complex": 20}


def classify_mapping_target(mapping):
    """Classify a mapping as 'dbt', 'pyspark', or 'either'.

    Returns (recommendation, reason) tuple.
    """
    tx_set = set(mapping.get("transformations", []))
    has_sql_override = mapping.get("has_sql_override", False)
    has_stored_proc = mapping.get("has_stored_proc", False)
    complexity = mapping.get("complexity", "Medium")

    pyspark_tx = tx_set & _PYSPARK_TX
    sql_tx = tx_set & _SQL_FRIENDLY_TX
    total = len(tx_set)

    # Stored procedures → always PySpark (custom logic)
    if has_stored_proc:
        return "pyspark", "Uses stored procedures requiring PySpark execution"

    # Java/Custom/HTTP/XML transforms → PySpark
    if pyspark_tx:
        return "pyspark", f"Contains PySpark-required transforms: {', '.join(sorted(pyspark_tx))}"

    # Pure SQL-friendly transformations → DBT
    if total > 0 and sql_tx == tx_set:
        return "dbt", "All transformations are SQL-translatable"

    # SQL override without complex transforms → DBT
    if has_sql_override and not pyspark_tx:
        return "dbt", "Has SQL overrides, no PySpark-only transforms"

    # Complex with many transforms → might benefit from PySpark
    if complexity == "Complex" and total > 6:
        return "pyspark", f"Complex mapping with {total} transforms benefits from PySpark flexibility"

    # Default: either works
    if sql_tx and len(sql_tx) >= total * 0.7:
        return "dbt", "Majority SQL-friendly transforms — DBT preferred"

    return "either", "Compatible with both DBT and PySpark"


def generate_advisor_report(inventory, output_path=None):
    """Generate per-mapping target recommendation report.

    Returns dict with recommendations and summary.
    """
    mappings = inventory.get("mappings", [])
    recommendations = []
    counts = {"dbt": 0, "pyspark": 0, "either": 0}

    for m in mappings:
        target, reason = classify_mapping_target(m)
        counts[target] += 1
        recommendations.append({
            "mapping": m["name"],
            "complexity": m.get("complexity", "Medium"),
            "transforms": m.get("transformations", []),
            "recommended_target": target,
            "reason": reason,
        })

    total = len(mappings)
    summary = {
        "total_mappings": total,
        "dbt_recommended": counts["dbt"],
        "pyspark_recommended": counts["pyspark"],
        "either": counts["either"],
        "dbt_percentage": round(counts["dbt"] / total * 100, 1) if total else 0,
        "pyspark_percentage": round(counts["pyspark"] / total * 100, 1) if total else 0,
    }

    # Build markdown report
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Migration Target Advisor — Databricks vs DBT",
        "",
        f"**Generated:** {ts}",
        "",
        "## Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total mappings | {total} |",
        f"| DBT recommended | {counts['dbt']} ({summary['dbt_percentage']}%) |",
        f"| PySpark recommended | {counts['pyspark']} ({summary['pyspark_percentage']}%) |",
        f"| Either (flexible) | {counts['either']} |",
        "",
        "## Recommendation",
        "",
    ]

    if counts["dbt"] > counts["pyspark"]:
        lines.append(f"**Primary target: DBT** — {counts['dbt']}/{total} mappings are SQL-translatable.")
        lines.append("Use `--target dbt` for the majority, with PySpark notebooks for complex mappings.")
    elif counts["pyspark"] > counts["dbt"]:
        lines.append(f"**Primary target: PySpark** — {counts['pyspark']}/{total} mappings need PySpark.")
        lines.append("Use `--target databricks` for the majority, optionally `--target auto` to mix.")
    else:
        lines.append("**Mixed workload** — use `--target auto` to let the tool route per mapping.")

    lines.extend([
        "",
        "## Per-Mapping Recommendations",
        "",
        "| Mapping | Complexity | Target | Reason |",
        "|---------|-----------|--------|--------|",
    ])
    for rec in recommendations:
        emoji = {"dbt": "🏗️", "pyspark": "🐍", "either": "🔄"}[rec["recommended_target"]]
        lines.append(
            f"| {rec['mapping']} | {rec['complexity']} | {emoji} {rec['recommended_target']} | {rec['reason']} |"
        )
    lines.append("")

    report_text = "\n".join(lines)
    out = output_path or (OUTPUT_DIR / "inventory" / "target_recommendation.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(report_text)

    result = {"recommendations": recommendations, "summary": summary, "report_path": str(out)}
    return result


# ─────────────────────────────────────────────
#  Target Comparison Report
# ─────────────────────────────────────────────

def _count_artifacts(base_dir):
    """Count artifacts in a directory and return {filename: size_bytes}."""
    artifacts = {}
    if base_dir.exists():
        for f in sorted(base_dir.iterdir()):
            if f.is_file():
                artifacts[f.name] = f.stat().st_size
    return artifacts


def _artifact_diff(dir_a, dir_b, label_a="Databricks", label_b="DBT"):
    """Compare artifacts between two output directories."""
    arts_a = _count_artifacts(dir_a)
    arts_b = _count_artifacts(dir_b)
    all_names = sorted(set(arts_a) | set(arts_b))
    rows = []
    for name in all_names:
        in_a = "✅" if name in arts_a else "—"
        in_b = "✅" if name in arts_b else "—"
        size_a = f"{arts_a[name]:,}B" if name in arts_a else "—"
        size_b = f"{arts_b[name]:,}B" if name in arts_b else "—"
        rows.append(f"| {name} | {in_a} | {size_a} | {in_b} | {size_b} |")
    return rows


def generate_comparison_report(inventory, databricks_dir=None, dbt_dir=None, output_path=None):
    """Generate side-by-side comparison of Databricks vs DBT migration outputs.

    Can compare either actual output dirs or generate a theoretical comparison.
    Returns the report path.
    """
    mappings = inventory.get("mappings", [])
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        "# Cross-Platform Comparison — Databricks PySpark vs DBT",
        "",
        f"**Generated:** {ts}",
        "",
        "## 1. Architecture Comparison",
        "",
        "| Aspect | Databricks PySpark | DBT on Databricks |",
        "|--------|-------------------|-------------------|",
        "| **Execution engine** | Spark DataFrame API | SQL via dbt-databricks adapter |",
        "| **Notebook API** | `dbutils.*` | dbt CLI / Cloud |",
        "| **Secret management** | `dbutils.secrets.get()` | env_var / profiles.yml |",
        "| **Table references** | `catalog.schema.table` | `{{ source() }}` / `{{ ref() }}` |",
        "| **Schema evolution** | `MERGE INTO` / Delta | dbt `--full-refresh` / incremental |",
        "| **Testing** | PySpark assertions | dbt tests (unique, not_null, accepted_values) |",
        "| **Lineage** | Unity Catalog lineage | dbt docs lineage graph |",
        "| **Orchestration** | Databricks Workflows (Jobs API) | Databricks Workflows + dbt task |",
        "| **CI/CD** | Asset Bundles / REST API | dbt Cloud CI / dbt Core + Asset Bundles |",
        "| **Cost model** | DBU (cluster hours) | DBU (SQL Warehouse hours) |",
        "",
        "## 2. Transformation Mapping",
        "",
        "| Informatica Transform | PySpark Equivalent | DBT Equivalent |",
        "|----------------------|-------------------|----------------|",
        "| Source Qualifier (SQ) | `spark.read.table()` | `{{ source('schema', 'table') }}` |",
        "| Expression (EXP) | `.withColumn()` / `.select()` | `SELECT col, expr AS alias` |",
        "| Filter (FIL) | `.filter()` / `.where()` | `WHERE condition` |",
        "| Aggregator (AGG) | `.groupBy().agg()` | `GROUP BY` + aggregate functions |",
        "| Joiner (JNR) | `.join()` | `JOIN` clause |",
        "| Lookup (LKP) | `.join()` (broadcast) | `LEFT JOIN` / dbt macro |",
        "| Router (RTR) | Multiple `.filter()` | `CASE WHEN` / dbt `UNION ALL` |",
        "| Update Strategy (UPD) | `MERGE INTO` Delta | dbt incremental `merge` strategy |",
        "| Rank (RNK) | `Window.orderBy()` | `ROW_NUMBER() OVER (...)` |",
        "| Sorter (SRT) | `.orderBy()` | `ORDER BY` |",
        "| Union (UNI) | `.union()` | `UNION ALL` |",
        "| Sequence Gen (SEQ) | `monotonically_increasing_id()` | `ROW_NUMBER()` |",
        "| Java/Custom (JTX/CT) | Custom PySpark UDF | ❌ Not supported (PySpark fallback) |",
        "| HTTP | `requests` + PySpark | ❌ Not supported (PySpark fallback) |",
        "| Stored Proc (SP) | `spark.sql('CALL ...')` | dbt `run-operation` or pre-hook |",
        "",
        "## 3. Per-Mapping Target Suitability",
        "",
        "| Mapping | Complexity | Transforms | Best Target | Reason |",
        "|---------|-----------|-----------|-------------|--------|",
    ]

    dbt_count = 0
    pyspark_count = 0
    either_count = 0
    total_dbu_est = 0.0

    for m in mappings:
        target, reason = classify_mapping_target(m)
        emoji = {"dbt": "🏗️", "pyspark": "🐍", "either": "🔄"}[target]
        tx_str = ", ".join(m.get("transformations", [])[:5])
        if len(m.get("transformations", [])) > 5:
            tx_str += "…"
        lines.append(f"| {m['name']} | {m.get('complexity', '?')} | {tx_str} | {emoji} {target} | {reason} |")

        if target == "dbt":
            dbt_count += 1
        elif target == "pyspark":
            pyspark_count += 1
        else:
            either_count += 1

        complexity = m.get("complexity", "Medium")
        total_dbu_est += _COMPLEXITY_DBU.get(complexity, 1.0)

    total = len(mappings)
    lines.extend([
        "",
        "## 4. Cost Projection (Estimated)",
        "",
        "| Metric | PySpark on Databricks | DBT on Databricks |",
        "|--------|----------------------|-------------------|",
        f"| Mappings suited | {pyspark_count + either_count} | {dbt_count + either_count} |",
        f"| Est. daily DBU | {total_dbu_est:.1f} DBU (all-purpose cluster) | {total_dbu_est * 0.6:.1f} DBU (SQL Warehouse) |",
        f"| Cost factor | 1.0× (baseline) | ~0.6× (SQL Warehouse cheaper) |",
        f"| Dev effort (PySpark) | {sum(_COMPLEXITY_HOURS.get(m.get('complexity','Medium'), 8) for m in mappings)} hours | — |",
        f"| Dev effort (DBT) | — | {sum(max(1, _COMPLEXITY_HOURS.get(m.get('complexity','Medium'), 8) // 2) for m in mappings)} hours |",
        "",
        "## 5. Recommendation",
        "",
    ])

    if dbt_count > pyspark_count:
        lines.extend([
            f"**Recommended: `--target auto` (hybrid)** — {dbt_count}/{total} mappings are DBT-suitable,",
            f"{pyspark_count}/{total} need PySpark. The `auto` mode routes each mapping optimally.",
            "",
            "```bash",
            "informatica-to-fabric --target auto --config migration.yaml",
            "```",
        ])
    else:
        lines.extend([
            f"**Recommended: `--target databricks`** — {pyspark_count}/{total} mappings need PySpark.",
            "Consider `--target auto` to use DBT for SQL-heavy subset.",
            "",
            "```bash",
            "informatica-to-fabric --target databricks --config migration.yaml",
            "```",
        ])

    # Artifact comparison if dual-target dirs exist
    db_notebooks = databricks_dir / "notebooks" if databricks_dir else OUTPUT_DIR / "notebooks"
    dbt_models = dbt_dir / "dbt" if dbt_dir else OUTPUT_DIR / "dbt"
    if db_notebooks.exists() or dbt_models.exists():
        lines.extend([
            "",
            "## 6. Artifact Comparison",
            "",
        ])
        if db_notebooks.exists():
            nb_files = _count_artifacts(db_notebooks)
            lines.append(f"**Databricks Notebooks:** {len(nb_files)} files, "
                         f"{sum(nb_files.values()):,} bytes total")
        if dbt_models.exists():
            dbt_files = _count_artifacts(dbt_models)
            lines.append(f"**DBT Models:** {len(dbt_files)} files, "
                         f"{sum(dbt_files.values()):,} bytes total")

        # Detailed diff
        if db_notebooks.exists() and dbt_models.exists():
            lines.extend([
                "",
                "### Notebooks vs DBT Models",
                "",
                f"| File | Notebooks | Size | DBT | Size |",
                f"|------|-----------|------|-----|------|",
            ])
            lines.extend(_artifact_diff(db_notebooks, dbt_models, "Notebook", "DBT"))

    lines.append("")
    report_text = "\n".join(lines)

    out = output_path or (OUTPUT_DIR / "comparison_report.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(report_text)

    return {
        "report_path": str(out),
        "dbt_suited": dbt_count,
        "pyspark_suited": pyspark_count,
        "either": either_count,
        "total": total,
        "est_daily_dbu_pyspark": round(total_dbu_est, 1),
        "est_daily_dbu_dbt": round(total_dbu_est * 0.6, 1),
    }


# ─────────────────────────────────────────────
#  Unified Deployment Manifest
# ─────────────────────────────────────────────

def generate_unified_manifest(inventory, targets=None, output_path=None):
    """Generate a unified manifest covering multiple target platforms.

    Scans output dirs for each target and builds a combined deployment plan.
    Returns the manifest dict.
    """
    if targets is None:
        targets = ["databricks", "dbt"]

    ts = datetime.now(timezone.utc).isoformat()
    manifest = {
        "schema_version": "2.0",
        "generated": ts,
        "targets": targets,
        "total_mappings": len(inventory.get("mappings", [])),
        "total_workflows": len(inventory.get("workflows", [])),
        "artifacts": {},
        "deployment_order": [],
    }

    artifact_dirs = {
        "notebooks": OUTPUT_DIR / "notebooks",
        "dbt": OUTPUT_DIR / "dbt",
        "pipelines": OUTPUT_DIR / "pipelines",
        "sql": OUTPUT_DIR / "sql",
        "schema": OUTPUT_DIR / "schema",
        "autosys": OUTPUT_DIR / "autosys",
        "validation": OUTPUT_DIR / "validation",
        "scripts": OUTPUT_DIR / "scripts",
    }

    order_idx = 1
    for category, directory in artifact_dirs.items():
        if not directory.exists():
            continue
        files = sorted(f for f in directory.iterdir() if f.is_file())
        if not files:
            continue

        target_tag = "dbt" if category == "dbt" else "databricks"
        if category in ("sql", "schema", "validation", "scripts", "autosys", "pipelines"):
            target_tag = "shared"

        entries = []
        for f in files:
            try:
                rel_path = str(f.relative_to(WORKSPACE))
            except ValueError:
                rel_path = str(f)
            entry = {
                "name": f.stem,
                "file": f.name,
                "path": rel_path,
                "size_bytes": f.stat().st_size,
                "target": target_tag,
                "deploy_order": order_idx,
            }
            entries.append(entry)
            manifest["deployment_order"].append(f"{category}/{f.name}")
            order_idx += 1

        manifest["artifacts"][category] = {
            "count": len(entries),
            "total_bytes": sum(e["size_bytes"] for e in entries),
            "files": entries,
        }

    manifest["total_artifacts"] = sum(
        cat["count"] for cat in manifest["artifacts"].values()
    )

    out = output_path or (OUTPUT_DIR / "manifest.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    return manifest


# ─────────────────────────────────────────────
#  Dual-Target Generation (--target all)
# ─────────────────────────────────────────────

def run_dual_target(skip_phases=None):
    """Run the migration for both databricks and dbt targets sequentially.

    Returns dict with results for each target.
    """
    import importlib

    results = {}
    for target in ("databricks", "dbt"):
        os.environ["INFORMATICA_MIGRATION_TARGET"] = "databricks"
        os.environ["INFORMATICA_DBT_MODE"] = "" if target == "databricks" else "dbt"
        os.environ["INFORMATICA_DATABRICKS_CATALOG"] = os.environ.get(
            "INFORMATICA_DATABRICKS_CATALOG", "main"
        )

        print(f"\n{'='*60}")
        print(f"  Dual-Target Run: {target.upper()}")
        print(f"{'='*60}\n")

        phase_modules = [
            "run_notebook_migration",
            "run_pipeline_migration",
        ]
        if target == "dbt":
            phase_modules.append("run_dbt_migration")

        for mod_name in phase_modules:
            if skip_phases and mod_name in skip_phases:
                continue
            try:
                saved_argv = sys.argv
                sys.argv = [mod_name + ".py"]
                mod = importlib.import_module(mod_name)
                importlib.reload(mod)
                mod.main()
                sys.argv = saved_argv
            except (SystemExit, Exception) as exc:
                sys.argv = saved_argv
                print(f"  ⚠️  {mod_name} ({target}): {exc}")

        results[target] = {"status": "ok", "modules": phase_modules}

    return results


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sprint 45 — Cross-Platform Comparison & Migration Advisor")
    parser.add_argument("--inventory", type=str, default=None,
                        help="Path to inventory.json")
    parser.add_argument("--advisor-only", action="store_true",
                        help="Generate only the advisor report")
    parser.add_argument("--comparison-only", action="store_true",
                        help="Generate only the comparison report")
    parser.add_argument("--manifest-only", action="store_true",
                        help="Generate only the unified manifest")
    parser.add_argument("--dual-target", action="store_true",
                        help="Run migration for both databricks and dbt targets")
    args = parser.parse_args()

    inv_path = Path(args.inventory) if args.inventory else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: inventory not found at {inv_path}")
        print("Run assessment first: python run_assessment.py")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    print("=" * 60)
    print("  Sprint 45 — Cross-Platform Comparison & Advisor")
    print("=" * 60)

    if args.dual_target:
        print("\n[1/4] Running dual-target migration (Databricks + DBT)...")
        dual_results = run_dual_target()
        print(f"  ✅ Dual-target complete: {list(dual_results.keys())}")
    else:
        dual_results = None

    if not args.comparison_only and not args.manifest_only:
        print("\n[Advisor] Generating target recommendations...")
        advisor = generate_advisor_report(inventory)
        print(f"  ✅ {advisor['summary']['total_mappings']} mappings analyzed")
        print(f"     DBT: {advisor['summary']['dbt_recommended']} | "
              f"PySpark: {advisor['summary']['pyspark_recommended']} | "
              f"Either: {advisor['summary']['either']}")
        print(f"  → {advisor['report_path']}")

    if not args.advisor_only and not args.manifest_only:
        print("\n[Comparison] Generating cross-platform comparison...")
        comparison = generate_comparison_report(inventory)
        print(f"  ✅ Comparison report: {comparison['report_path']}")
        print(f"     DBT-suited: {comparison['dbt_suited']} | "
              f"PySpark-suited: {comparison['pyspark_suited']} | "
              f"Either: {comparison['either']}")

    if not args.advisor_only and not args.comparison_only:
        print("\n[Manifest] Generating unified deployment manifest...")
        manifest = generate_unified_manifest(inventory)
        print(f"  ✅ Manifest: {manifest['total_artifacts']} artifacts across "
              f"{len(manifest['artifacts'])} categories")

    print("\n" + "=" * 60)
    print("  Sprint 45 complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
