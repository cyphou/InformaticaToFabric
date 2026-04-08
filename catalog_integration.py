"""
Data Catalog Integration — Sprint 79
Publishes migration metadata and lineage to Microsoft Purview (Fabric)
and Unity Catalog (Databricks) for enterprise data discovery.

Outputs:
    output/catalog/purview_entities.json   — Purview-compatible entity JSON
    output/catalog/uc_lineage.sql          — Unity Catalog lineage SQL
    output/catalog/column_lineage.json     — Column-level lineage export
    output/catalog/impact_analysis.md      — Impact analysis report

Usage:
    python catalog_integration.py
    python catalog_integration.py path/to/inventory.json
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "catalog"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _get_target():
    return os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")


def _get_catalog():
    return os.environ.get("INFORMATICA_DATABRICKS_CATALOG", "main")


# ─────────────────────────────────────────────
#  Purview Metadata Generation
# ─────────────────────────────────────────────

def generate_purview_entities(inventory):
    """Generate Purview-compatible JSON for scanned entities.

    Uses Apache Atlas v2 API format for:
    - Tables (source and target)
    - Columns
    - Lineage edges (source → target)
    """
    entities = []
    relationships = []

    for mapping in inventory.get("mappings", []):
        mapping_name = mapping["name"]

        # Source table entities
        for source in mapping.get("sources", []):
            parts = source.split(".")
            table_name = parts[-1] if parts else source
            schema_name = parts[-2] if len(parts) > 1 else "default"
            db_name = parts[0] if len(parts) > 2 else "source_db"

            entity = {
                "typeName": "azure_sql_table",
                "attributes": {
                    "qualifiedName": f"{db_name}.{schema_name}.{table_name}",
                    "name": table_name,
                    "description": f"Source table from Informatica mapping {mapping_name}",
                    "dbName": db_name,
                    "schemaName": schema_name,
                },
                "status": "ACTIVE",
                "createdBy": "informatica_migration",
            }
            entities.append(entity)

        # Target table entities
        for target in mapping.get("targets", []):
            target_lower = target.lower()
            if any(x in target_lower for x in ("agg", "gold", "rpt", "kpi")):
                tier = "gold"
            elif any(x in target_lower for x in ("dim", "fact", "silver")):
                tier = "silver"
            else:
                tier = "silver"

            entity = {
                "typeName": "azure_datalake_gen2_path",
                "attributes": {
                    "qualifiedName": f"lakehouse.{tier}.{target_lower}",
                    "name": target_lower,
                    "description": f"Migrated from Informatica mapping {mapping_name}",
                    "schemaName": tier,
                },
                "status": "ACTIVE",
                "createdBy": "informatica_migration",
            }
            entities.append(entity)

        # Lineage edges
        for source in mapping.get("sources", []):
            for target in mapping.get("targets", []):
                parts = source.split(".")
                source_qn = ".".join(parts) if parts else source
                target_qn = f"lakehouse.silver.{target.lower()}"

                relationships.append({
                    "typeName": "DataSet_DataSet_Lineage",
                    "attributes": {},
                    "end1": {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": source_qn}},
                    "end2": {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": target_qn}},
                    "propagatedClassifications": [],
                    "createdBy": "informatica_migration",
                    "label": f"ETL: {mapping_name}",
                })

    return {
        "entities": {"entities": entities},
        "relationships": relationships,
        "metadata": {
            "generated": datetime.now(timezone.utc).isoformat(),
            "tool": "informatica-to-fabric",
            "entity_count": len(entities),
            "lineage_count": len(relationships),
        },
    }


# ─────────────────────────────────────────────
#  Unity Catalog Lineage
# ─────────────────────────────────────────────

def generate_unity_catalog_lineage(inventory):
    """Generate SQL statements for Unity Catalog lineage and tags.

    Produces:
    - ALTER TABLE SET TAGS for migration metadata
    - COMMENT ON for table descriptions
    - Lineage registration via UC audit
    """
    catalog = _get_catalog()
    statements = []

    for mapping in inventory.get("mappings", []):
        mapping_name = mapping["name"]
        complexity = mapping.get("complexity", "Unknown")

        for target in mapping.get("targets", []):
            target_lower = target.lower()
            if any(x in target_lower for x in ("agg", "gold", "rpt")):
                tier = "gold"
            elif any(x in target_lower for x in ("dim", "fact")):
                tier = "silver"
            else:
                tier = "silver"

            fqn = f"{catalog}.{tier}.{target_lower}"

            # Sanitize values to prevent SQL injection in generated statements
            safe_mapping = mapping_name.replace("'", "''")
            safe_complexity = complexity.replace("'", "''")
            safe_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            # SET TAGS
            statements.append(
                f"ALTER TABLE {fqn} SET TAGS ("
                f"'migration_source' = 'informatica', "
                f"'source_mapping' = '{safe_mapping}', "
                f"'complexity' = '{safe_complexity}', "
                f"'migration_date' = '{safe_date}'"
                f");"
            )

            # COMMENT
            sources = ", ".join(mapping.get("sources", []))
            safe_sources = sources.replace("'", "''")
            statements.append(
                f"COMMENT ON TABLE {fqn} IS "
                f"'Migrated from Informatica mapping {safe_mapping}. Sources: {safe_sources}';"
            )

    return statements


# ─────────────────────────────────────────────
#  Column-Level Lineage Export
# ─────────────────────────────────────────────

def export_column_lineage(inventory):
    """Export field-level lineage from inventory.

    Returns list of {source_table, source_column, target_table, target_column, transforms} dicts.
    """
    lineage_entries = []

    for mapping in inventory.get("mappings", []):
        mapping_name = mapping["name"]
        field_lineage = mapping.get("field_lineage", [])

        for entry in field_lineage:
            lineage_entries.append({
                "mapping": mapping_name,
                "source_table": entry.get("source_instance", ""),
                "source_column": entry.get("source_field", ""),
                "target_table": entry.get("target_instance", ""),
                "target_column": entry.get("target_field", ""),
                "transforms": entry.get("transforms", []),
                "expression": entry.get("expression", ""),
            })

    return lineage_entries


# ─────────────────────────────────────────────
#  Impact Analysis
# ─────────────────────────────────────────────

def generate_impact_analysis(inventory, source_table):
    """Trace all downstream targets for a given source table.

    Returns markdown report showing impact chain.
    """
    affected_mappings = []
    affected_targets = set()

    for mapping in inventory.get("mappings", []):
        sources = mapping.get("sources", [])
        for src in sources:
            if source_table.lower() in src.lower():
                affected_mappings.append(mapping["name"])
                affected_targets.update(mapping.get("targets", []))
                break

    lines = [
        f"# Impact Analysis: {source_table}",
        "",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## Summary",
        "",
        f"- **Source table:** {source_table}",
        f"- **Affected mappings:** {len(affected_mappings)}",
        f"- **Downstream targets:** {len(affected_targets)}",
        "",
    ]

    if affected_mappings:
        lines.extend([
            "## Affected Mappings",
            "",
            "| # | Mapping | Targets |",
            "|---|---------|---------|",
        ])
        for i, mapping_name in enumerate(affected_mappings, 1):
            mapping = next((m for m in inventory.get("mappings", []) if m["name"] == mapping_name), {})
            targets = ", ".join(mapping.get("targets", []))
            lines.append(f"| {i} | {mapping_name} | {targets} |")

        lines.extend([
            "",
            "## Downstream Targets",
            "",
        ])
        for target in sorted(affected_targets):
            lines.append(f"- `{target}`")
    else:
        lines.append("No mappings found referencing this source table.")

    return "\n".join(lines)


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    target = _get_target()

    print("=" * 60)
    print(f"  Data Catalog Integration — Sprint 79 [{target}]")
    print("=" * 60)
    print()

    # 1. Purview metadata
    purview_data = generate_purview_entities(inv)
    purview_path = OUTPUT_DIR / "purview_entities.json"
    purview_path.write_text(json.dumps(purview_data, indent=2), encoding="utf-8")
    print(f"  ✅ Purview entities: {purview_data['metadata']['entity_count']} entities, "
          f"{purview_data['metadata']['lineage_count']} lineage edges")

    # 2. Unity Catalog lineage SQL
    uc_statements = generate_unity_catalog_lineage(inv)
    uc_path = OUTPUT_DIR / "uc_lineage.sql"
    uc_path.write_text("\n\n".join(uc_statements), encoding="utf-8")
    print(f"  ✅ Unity Catalog lineage: {len(uc_statements)} SQL statements")

    # 3. Column-level lineage
    col_lineage = export_column_lineage(inv)
    col_path = OUTPUT_DIR / "column_lineage.json"
    col_path.write_text(json.dumps(col_lineage, indent=2), encoding="utf-8")
    print(f"  ✅ Column lineage: {len(col_lineage)} field mappings")

    # 4. Impact analysis for all source tables
    all_sources = set()
    for m in inv.get("mappings", []):
        for src in m.get("sources", []):
            all_sources.add(src)

    if all_sources:
        first_source = sorted(all_sources)[0]
        impact = generate_impact_analysis(inv, first_source)
        impact_path = OUTPUT_DIR / "impact_analysis.md"
        impact_path.write_text(impact, encoding="utf-8")
        print(f"  ✅ Impact analysis: {first_source}")

    print()
    print("=" * 60)
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
