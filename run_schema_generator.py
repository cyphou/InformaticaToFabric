"""
Schema Generator — Sprint 27
Extracts target table definitions from mapping XML and generates
Delta Lake CREATE TABLE DDL organized by lakehouse tier (Bronze/Silver/Gold).

Outputs:
  output/schema/bronze_ddl.sql   — Bronze layer DDL
  output/schema/silver_ddl.sql   — Silver layer DDL
  output/schema/gold_ddl.sql     — Gold layer DDL
  output/schema/setup_workspace.py — PySpark notebook to create all tables

Usage:
    python run_schema_generator.py
    python run_schema_generator.py path/to/inventory.json
"""

import json
import re
import sys
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "schema"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
#  Source DB type → Delta Lake type mapping
# ─────────────────────────────────────────────

# Maps source database types (all dialects) to Delta Lake types
TYPE_MAP = {
    # Oracle
    "VARCHAR2": "STRING",
    "NVARCHAR2": "STRING",
    "CHAR": "STRING",
    "NCHAR": "STRING",
    "CLOB": "STRING",
    "NCLOB": "STRING",
    "LONG": "STRING",
    "RAW": "BINARY",
    "LONG RAW": "BINARY",
    "BLOB": "BINARY",
    "NUMBER": "DECIMAL(38,10)",
    "FLOAT": "DOUBLE",
    "BINARY_FLOAT": "FLOAT",
    "BINARY_DOUBLE": "DOUBLE",
    "DATE": "TIMESTAMP",
    "TIMESTAMP": "TIMESTAMP",
    "INTERVAL": "STRING",
    "ROWID": "STRING",
    "XMLTYPE": "STRING",
    # SQL Server
    "NVARCHAR": "STRING",
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "NTEXT": "STRING",
    "INT": "INT",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",
    "BIT": "BOOLEAN",
    "DECIMAL": "DECIMAL(38,10)",
    "NUMERIC": "DECIMAL(38,10)",
    "MONEY": "DECIMAL(19,4)",
    "SMALLMONEY": "DECIMAL(10,4)",
    "REAL": "FLOAT",
    "DATETIME": "TIMESTAMP",
    "DATETIME2": "TIMESTAMP",
    "SMALLDATETIME": "TIMESTAMP",
    "DATETIMEOFFSET": "TIMESTAMP",
    "TIME": "STRING",
    "UNIQUEIDENTIFIER": "STRING",
    "VARBINARY": "BINARY",
    "IMAGE": "BINARY",
    "SQL_VARIANT": "STRING",
    # Teradata
    "BYTEINT": "TINYINT",
    "INTEGER": "INT",
    "GRAPHIC": "STRING",
    "VARGRAPHIC": "STRING",
    "BYTE": "BINARY",
    "VARBYTE": "BINARY",
    "PERIOD": "STRING",
    # DB2
    "DBCLOB": "STRING",
    "DECFLOAT": "DOUBLE",
    "GRAPHIC_DB2": "STRING",
    # MySQL
    "MEDIUMTEXT": "STRING",
    "LONGTEXT": "STRING",
    "TINYTEXT": "STRING",
    "ENUM": "STRING",
    "SET": "STRING",
    "MEDIUMINT": "INT",
    "MEDIUMBLOB": "BINARY",
    "LONGBLOB": "BINARY",
    "TINYBLOB": "BINARY",
    "JSON": "STRING",
    "YEAR": "INT",
    # PostgreSQL
    "SERIAL": "INT",
    "BIGSERIAL": "BIGINT",
    "SMALLSERIAL": "SMALLINT",
    "BYTEA": "BINARY",
    "BOOLEAN": "BOOLEAN",
    "UUID": "STRING",
    "INET": "STRING",
    "CIDR": "STRING",
    "MACADDR": "STRING",
    "JSONB": "STRING",
    "TSQUERY": "STRING",
    "TSVECTOR": "STRING",
    "HSTORE": "STRING",
    "ARRAY": "STRING",
    # Generic
    "STRING": "STRING",
    "DOUBLE": "DOUBLE",
}

# Regex for types with precision/scale: NUMBER(10,2), VARCHAR2(100), etc.
_PRECISION_RE = re.compile(r'^(\w+)\s*\(([^)]+)\)$', re.IGNORECASE)


def map_type_to_delta(source_type, db_type="oracle"):
    """Map a source database type to Delta Lake type."""
    if not source_type:
        return "STRING"

    source_upper = source_type.strip().upper()

    # Handle types with precision: NUMBER(10,2) → DECIMAL(10,2)
    m = _PRECISION_RE.match(source_upper)
    if m:
        base = m.group(1)
        params = m.group(2)
        if base in ("NUMBER", "DECIMAL", "NUMERIC"):
            return f"DECIMAL({params})"
        if base in ("VARCHAR2", "NVARCHAR2", "VARCHAR", "NVARCHAR", "CHAR", "NCHAR"):
            return "STRING"
        if base in ("RAW", "VARBINARY"):
            return "BINARY"
        if base in ("TIMESTAMP",):
            return "TIMESTAMP"
        if base in ("FLOAT",):
            return "DOUBLE"

    # Direct lookup (strip precision for lookup)
    base_type = source_upper.split("(")[0].strip()
    if base_type in TYPE_MAP:
        return TYPE_MAP[base_type]

    return "STRING"  # Safe fallback


def infer_lakehouse_tier(target_name):
    """Infer Bronze/Silver/Gold tier from target table name."""
    name_lower = target_name.lower()
    if any(x in name_lower for x in ("agg_", "gold_", "rpt_", "kpi_", "summary_")):
        return "gold"
    elif any(x in name_lower for x in ("raw_", "bronze_", "src_", "stg_")):
        return "bronze"
    return "silver"


def infer_partition_key(target_name, columns):
    """Suggest a partition key based on table name and columns."""
    # Look for date-like columns
    col_names = [c["name"].lower() for c in columns]
    for date_col in ("load_date", "etl_date", "created_date", "event_date", "transaction_date"):
        if date_col in col_names:
            return date_col
    # Fallback — no partitioning
    return None


def extract_target_schemas(inventory):
    """Extract target table schemas from inventory mappings.

    Builds table definitions from mapping target names and field lineage.
    Returns list of table dicts.
    """
    tables = {}

    for mapping in inventory.get("mappings", []):
        for target in mapping.get("targets", []):
            if target in tables:
                continue

            # Infer columns from field lineage (target fields)
            columns = []
            seen_cols = set()
            for entry in mapping.get("field_lineage", []):
                if entry.get("target_instance", "").replace("TGT_", "").replace("tgt_", "") in target or True:
                    col_name = entry.get("target_field", "")
                    if col_name and col_name not in seen_cols:
                        columns.append({
                            "name": col_name,
                            "type": "STRING",  # Default; real type from XML TARGET element
                            "source": entry.get("source_field", ""),
                        })
                        seen_cols.add(col_name)

            # If no lineage, create minimal schema
            if not columns:
                columns = [{"name": "id", "type": "BIGINT", "source": ""}]

            tier = infer_lakehouse_tier(target)
            partition_key = infer_partition_key(target, columns)

            tables[target] = {
                "name": target,
                "tier": tier,
                "columns": columns,
                "partition_key": partition_key,
                "mapping": mapping["name"],
                "complexity": mapping.get("complexity", "Unknown"),
            }

    return list(tables.values())


def generate_ddl(table):
    """Generate Delta Lake CREATE TABLE DDL for a table."""
    tier = table["tier"]
    name = table["name"].lower()
    fqn = f"{tier}.{name}"

    lines = [f"-- Table: {fqn} (from mapping {table['mapping']})"]
    lines.append(f"CREATE TABLE IF NOT EXISTS {fqn} (")

    col_defs = []
    for col in table["columns"]:
        delta_type = map_type_to_delta(col["type"])
        comment = f"  -- from {col['source']}" if col["source"] else ""
        col_defs.append(f"    {col['name'].lower()} {delta_type}{comment}")

    # Add audit columns
    col_defs.append("    _etl_load_timestamp TIMESTAMP")
    col_defs.append("    _etl_source_mapping STRING")

    lines.append(",\n".join(col_defs))
    lines.append(")")
    lines.append("USING DELTA")

    if table.get("partition_key"):
        lines.append(f"PARTITIONED BY ({table['partition_key']})")

    mapping_name = table.get("mapping", "unknown")
    lines.append(f"COMMENT 'Migrated from Informatica mapping {mapping_name}'")
    lines.append(";")

    return "\n".join(lines)


def generate_setup_notebook(tables):
    """Generate a PySpark setup notebook that creates all lakehouses and tables."""
    cells = []

    cells.append(
        "# Fabric notebook source\n"
        "# Setup Workspace — Auto-generated by run_schema_generator.py\n"
        "# Creates all lakehouse schemas and tables for the migration.\n"
        "#\n"
        "# Run this notebook once in your Fabric workspace to set up the target schema.\n"
    )

    cells.append(
        "# COMMAND ----------\n\n"
        "# Cell 1: Create Schemas\n"
        "for schema_name in ['bronze', 'silver', 'gold']:\n"
        "    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')\n"
        "    print(f'Schema {schema_name} ready')\n"
    )

    # Group tables by tier
    by_tier = {"bronze": [], "silver": [], "gold": []}
    for t in tables:
        by_tier.setdefault(t["tier"], []).append(t)

    for tier in ["bronze", "silver", "gold"]:
        tier_tables = by_tier.get(tier, [])
        if not tier_tables:
            continue

        ddl_statements = []
        for t in tier_tables:
            ddl = generate_ddl(t)
            ddl_statements.append(ddl)

        ddl_block = "\n\n".join(ddl_statements)

        cells.append(
            f"# COMMAND ----------\n\n"
            f"# Cell: Create {tier.title()} Tables ({len(tier_tables)} tables)\n"
            f"ddl_statements = \"\"\"\n{ddl_block}\n\"\"\"\n"
            f"\n"
            f"for stmt in ddl_statements.split(';'):\n"
            f"    stmt = stmt.strip()\n"
            f"    if stmt and not stmt.startswith('--'):\n"
            f"        try:\n"
            f"            spark.sql(stmt)\n"
            f"        except Exception as e:\n"
            f"            print(f'Warning: {{e}}')\n"
            f"\n"
            f"print(f'{tier.title()} layer: {len(tier_tables)} tables created')\n"
        )

    cells.append(
        "# COMMAND ----------\n\n"
        "# Cell: Summary\n"
        f"print('Setup complete. Total tables: {len(tables)}')\n"
        "for schema_name in ['bronze', 'silver', 'gold']:\n"
        "    tables_list = spark.sql(f'SHOW TABLES IN {schema_name}').collect()\n"
        "    print(f'  {schema_name}: {len(tables_list)} tables')\n"
    )

    return "\n\n".join(cells)


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    print("=" * 60)
    print("  Schema Generator — Sprint 27")
    print("=" * 60)
    print()

    tables = extract_target_schemas(inv)
    print(f"  Target tables found: {len(tables)}")

    # Group by tier
    by_tier = {"bronze": [], "silver": [], "gold": []}
    for t in tables:
        by_tier.setdefault(t["tier"], []).append(t)

    # Write DDL files per tier
    for tier in ["bronze", "silver", "gold"]:
        tier_tables = by_tier.get(tier, [])
        if not tier_tables:
            continue

        ddl_content = "\n\n".join(generate_ddl(t) for t in tier_tables)
        ddl_path = OUTPUT_DIR / f"{tier}_ddl.sql"
        ddl_path.write_text(ddl_content, encoding="utf-8")
        print(f"  {tier.title()}: {len(tier_tables)} tables → {ddl_path.name}")

    # Write setup notebook
    notebook_content = generate_setup_notebook(tables)
    notebook_path = OUTPUT_DIR / "setup_workspace.py"
    notebook_path.write_text(notebook_content, encoding="utf-8")
    print(f"  Setup notebook → {notebook_path.name}")

    print()
    print("=" * 60)
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
