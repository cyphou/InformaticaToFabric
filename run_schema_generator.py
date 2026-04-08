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
import os
import re
import sys
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "schema"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _get_target():
    """Return the target platform ('fabric' or 'databricks')."""
    return os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")


def _get_catalog():
    """Return the Unity Catalog name for Databricks target."""
    return os.environ.get("INFORMATICA_DATABRICKS_CATALOG", "main")

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
    target = _get_target()
    if target == "databricks":
        catalog = _get_catalog()
        fqn = f"{catalog}.{tier}.{name}"
    else:
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
    """Generate a PySpark setup notebook that creates all schemas and tables."""
    cells = []
    target = _get_target()

    if target == "databricks":
        catalog = _get_catalog()
        cells.append(
            "# Databricks notebook source\n"
            "# Setup Workspace — Auto-generated by run_schema_generator.py\n"
            f"# Creates Unity Catalog '{catalog}' schemas and tables for the migration.\n"
            "#\n"
            "# Run this notebook once in your Databricks workspace to set up the target schema.\n"
        )
        cells.append(
            "# COMMAND ----------\n\n"
            "# Cell 1: Create Catalog & Schemas\n"
            f"spark.sql('CREATE CATALOG IF NOT EXISTS {catalog}')\n"
            f"spark.sql('USE CATALOG {catalog}')\n"
            "for schema_name in ['bronze', 'silver', 'gold']:\n"
            f"    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{{schema_name}}')\n"
            "    print(f'Schema {schema_name} ready')\n"
        )
    else:
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

    if target == "databricks":
        catalog = _get_catalog()
        cells.append(
            "# COMMAND ----------\n\n"
            "# Cell: Summary\n"
            f"print('Setup complete. Total tables: {len(tables)}')\n"
            f"spark.sql('USE CATALOG {catalog}')\n"
            "for schema_name in ['bronze', 'silver', 'gold']:\n"
            f"    tables_list = spark.sql(f'SHOW TABLES IN {catalog}.{{schema_name}}').collect()\n"
            "    print(f'  {schema_name}: {len(tables_list)} tables')\n"
        )
    else:
        cells.append(
            "# COMMAND ----------\n\n"
            "# Cell: Summary\n"
            f"print('Setup complete. Total tables: {len(tables)}')\n"
            "for schema_name in ['bronze', 'silver', 'gold']:\n"
            "    tables_list = spark.sql(f'SHOW TABLES IN {schema_name}').collect()\n"
            "    print(f'  {schema_name}: {len(tables_list)} tables')\n"
        )

    return "\n\n".join(cells)


# ─────────────────────────────────────────────
#  Sprint 69: Platform-Native Features
# ─────────────────────────────────────────────

def recommend_storage_target(mapping):
    """Recommend Lakehouse vs Warehouse for a mapping based on its characteristics.

    Lakehouse (Spark ETL): complex transforms, PySpark-heavy, multi-source, unstructured
    Warehouse (T-SQL): simple SQL, aggregation-heavy, BI-facing, structured
    """
    transforms = set(mapping.get("transformations", []))
    complexity = mapping.get("complexity", "Simple")
    has_sql = mapping.get("has_sql_override", False)
    sources = mapping.get("sources", [])

    warehouse_score = 0
    lakehouse_score = 0

    # SQL-heavy? → Warehouse
    if has_sql:
        warehouse_score += 2
    if mapping.get("has_stored_proc", False):
        warehouse_score += 3

    # Simple aggregation? → Warehouse
    if transforms <= {"SQ", "AGG", "FIL", "SRT", "EXP"}:
        warehouse_score += 2

    # Complex transforms? → Lakehouse
    complex_tx = {"JNR", "LKP", "MPLT", "NRM", "RTR", "SP", "UPD", "DM", "SQLT"}
    if transforms & complex_tx:
        lakehouse_score += len(transforms & complex_tx)

    # Multi-source? → Lakehouse
    if len(sources) > 2:
        lakehouse_score += 2

    # High complexity? → Lakehouse
    if complexity in ("Complex", "Custom"):
        lakehouse_score += 2
    elif complexity == "Simple":
        warehouse_score += 1

    recommendation = "warehouse" if warehouse_score > lakehouse_score else "lakehouse"
    return {
        "mapping": mapping.get("name", ""),
        "recommendation": recommendation,
        "lakehouse_score": lakehouse_score,
        "warehouse_score": warehouse_score,
        "reason": (
            "SQL-heavy / aggregation / BI-facing → Warehouse"
            if recommendation == "warehouse"
            else "Complex transforms / multi-source / PySpark → Lakehouse"
        ),
    }


def generate_warehouse_ddl(table):
    """Generate Fabric Warehouse DDL (T-SQL CREATE TABLE) for a table."""
    tier = table["tier"]
    name = table["name"].lower()
    fqn = f"[{tier}].[{name}]"

    lines = [f"-- Table: {fqn} (from mapping {table['mapping']})"]
    lines.append(f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{name}' AND schema_id = SCHEMA_ID('{tier}'))")
    lines.append(f"CREATE TABLE {fqn} (")

    col_defs = []
    for col in table["columns"]:
        tsql_type = _map_type_to_tsql(col["type"])
        comment = f"  -- from {col['source']}" if col["source"] else ""
        col_defs.append(f"    [{col['name'].lower()}] {tsql_type}{comment}")

    col_defs.append("    [_etl_load_timestamp] DATETIME2")
    col_defs.append("    [_etl_source_mapping] NVARCHAR(255)")

    lines.append(",\n".join(col_defs))
    lines.append(");")
    lines.append(f"-- Migrated from Informatica mapping {table.get('mapping', 'unknown')}")

    return "\n".join(lines)


def _map_type_to_tsql(source_type):
    """Map a source type to T-SQL type for Fabric Warehouse."""
    tsql_map = {
        "STRING": "NVARCHAR(4000)",
        "BIGINT": "BIGINT",
        "INT": "INT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "BOOLEAN": "BIT",
        "FLOAT": "FLOAT",
        "DOUBLE": "FLOAT",
        "DECIMAL(38,10)": "DECIMAL(38,10)",
        "DECIMAL(19,4)": "DECIMAL(19,4)",
        "DECIMAL(10,4)": "DECIMAL(10,4)",
        "TIMESTAMP": "DATETIME2",
        "DATE": "DATE",
        "BINARY": "VARBINARY(MAX)",
    }
    return tsql_map.get(source_type.upper(), "NVARCHAR(4000)")


def generate_sql_warehouse_ddl(table, catalog="main"):
    """Generate Databricks SQL Warehouse DDL with optimization hints.

    Adds CLUSTER BY and Z-ORDER recommendations.
    """
    tier = table["tier"]
    name = table["name"].lower()
    fqn = f"{catalog}.{tier}.{name}"

    lines = [f"-- SQL Warehouse optimized: {fqn} (from mapping {table['mapping']})"]
    lines.append(f"CREATE TABLE IF NOT EXISTS {fqn} (")

    col_defs = []
    for col in table["columns"]:
        delta_type = map_type_to_delta(col["type"])
        col_defs.append(f"    {col['name'].lower()} {delta_type}")

    col_defs.append("    _etl_load_timestamp TIMESTAMP")
    col_defs.append("    _etl_source_mapping STRING")

    lines.append(",\n".join(col_defs))
    lines.append(")")
    lines.append("USING DELTA")

    if table.get("partition_key"):
        lines.append(f"PARTITIONED BY ({table['partition_key']})")

    # CLUSTER BY recommendation for SQL Warehouse
    id_cols = [c["name"] for c in table["columns"]
               if any(k in c["name"].lower() for k in ("_id", "_key", "_code"))]
    if id_cols:
        cluster_cols = ", ".join(c.lower() for c in id_cols[:4])
        lines.append(f"CLUSTER BY ({cluster_cols})")

    # Z-ORDER annotation
    date_cols = [c["name"] for c in table["columns"]
                 if any(k in c["name"].lower() for k in ("date", "timestamp", "time"))]
    if date_cols:
        zorder_cols = ", ".join(c.lower() for c in date_cols[:2])
        lines.append(f"-- OPTIMIZE {fqn} ZORDER BY ({zorder_cols})")

    lines.append(f"COMMENT 'Migrated from Informatica mapping {table.get('mapping', 'unknown')}'")
    lines.append(";")

    return "\n".join(lines)


def generate_onelake_shortcuts(inventory):
    """Generate OneLake shortcut definitions for cross-workspace table references.

    Detects DB link patterns (Sprint 33 GTT/MV/DB link detection) and generates
    shortcut JSON for replacing them with OneLake shortcuts.
    """
    shortcuts = []
    for mapping in inventory.get("mappings", []):
        for source in mapping.get("sources", []):
            # DB link pattern: schema.table@dblink → shortcut
            if "@" in source:
                parts = source.split("@")
                table_ref = parts[0]
                db_link = parts[1] if len(parts) > 1 else ""
                shortcuts.append({
                    "name": table_ref.replace(".", "_").lower(),
                    "type": "onelake_shortcut",
                    "source_database_link": db_link,
                    "source_table": table_ref,
                    "target_lakehouse": "migration_lakehouse",
                    "target_path": f"Tables/{table_ref.replace('.', '_').lower()}",
                    "mapping": mapping["name"],
                })

        # Cross-database references
        for source in mapping.get("sources", []):
            parts = source.split(".")
            if len(parts) >= 3:  # e.g., Oracle.SCHEMA.TABLE or DB.SCHEMA.TABLE
                shortcuts.append({
                    "name": parts[-1].lower(),
                    "type": "onelake_shortcut",
                    "source_system": parts[0],
                    "source_schema": parts[1] if len(parts) > 2 else "",
                    "source_table": parts[-1],
                    "target_lakehouse": "migration_lakehouse",
                    "target_path": f"Tables/{parts[-1].lower()}",
                    "mapping": mapping["name"],
                })

    return shortcuts


def generate_delta_sharing_config(inventory, provider_name="migration_provider"):
    """Generate Delta Sharing provider + recipient configuration.

    Creates sharing definitions for tables that need cross-workspace access.
    """
    catalog = _get_catalog()
    tables = extract_target_schemas(inventory)
    gold_tables = [t for t in tables if t["tier"] == "gold"]

    config = {
        "provider": {
            "name": provider_name,
            "catalog": catalog,
        },
        "shares": [
            {
                "name": "migration_gold_share",
                "description": "Gold-tier tables from Informatica migration",
                "tables": [
                    {
                        "schema": "gold",
                        "table": t["name"].lower(),
                        "history_sharing": False,
                    }
                    for t in gold_tables
                ],
            },
        ],
        "recipients": [
            {
                "name": "downstream_consumer",
                "comment": "Add recipient workspace/account details",
                "sharing_code": "{{ SHARING_CODE }}",
            },
        ],
        "grant_sql": [
            f"CREATE SHARE IF NOT EXISTS migration_gold_share;",
        ] + [
            f"ALTER SHARE migration_gold_share ADD TABLE {catalog}.gold.{t['name'].lower()};"
            for t in gold_tables
        ] + [
            "CREATE RECIPIENT IF NOT EXISTS downstream_consumer;",
            "GRANT SELECT ON SHARE migration_gold_share TO RECIPIENT downstream_consumer;",
        ],
    }
    return config


def generate_mirroring_config(inventory):
    """Generate Fabric Mirroring configuration for on-prem source databases.

    Detects source database types and generates mirroring setup recommendations.
    """
    sources_by_db = {}
    for mapping in inventory.get("mappings", []):
        for source in mapping.get("sources", []):
            parts = source.split(".")
            if parts:
                db_type = parts[0] if len(parts) > 1 else "Unknown"
                sources_by_db.setdefault(db_type, set()).add(source)

    configs = []
    supported_mirrors = {"Oracle", "SQL Server", "PostgreSQL", "Azure SQL", "Cosmos DB"}
    for db_type, tables in sources_by_db.items():
        # Normalize
        db_norm = db_type
        for s in supported_mirrors:
            if s.lower().replace(" ", "") in db_type.lower().replace(" ", ""):
                db_norm = s
                break
        is_supported = db_norm in supported_mirrors
        configs.append({
            "source_database": db_norm,
            "supported": is_supported,
            "table_count": len(tables),
            "tables": sorted(tables),
            "recommendation": (
                f"Enable Fabric Mirroring for {db_norm} — near-real-time replication to OneLake"
                if is_supported
                else f"{db_norm} not supported for Mirroring — use Dataflow Gen2 or gateway"
            ),
        })

    return {
        "mirroring_candidates": configs,
        "total_source_tables": sum(len(t) for t in sources_by_db.values()),
    }


# ─────────────────────────────────────────────
#  Sprint 71: Partition Strategy Recommender
# ─────────────────────────────────────────────

def recommend_partition_strategy(table):
    """Recommend optimal partition strategy for a Delta table.

    Analyzes column names and types to suggest hash/range partitioning
    and PARTITIONED BY column selection.
    """
    columns = table.get("columns", [])
    name_lower = table.get("name", "").lower()
    col_names = {c["name"].lower(): c for c in columns}

    # Priority 1: Date columns for range partitioning (time-series data)
    date_candidates = []
    for cn in col_names:
        if any(k in cn for k in ("load_date", "etl_date", "created_date", "event_date",
                                  "transaction_date", "order_date", "effective_date",
                                  "updated_date", "modified_date", "process_date")):
            date_candidates.append(cn)

    # Priority 2: High-cardinality ID columns for hash partitioning
    id_candidates = []
    for cn in col_names:
        if cn.endswith("_id") or cn.endswith("_key") or cn.endswith("_code"):
            id_candidates.append(cn)

    # Priority 3: Category columns for list partitioning
    category_candidates = []
    for cn in col_names:
        if any(k in cn for k in ("region", "country", "category", "type", "status",
                                  "department", "tenant")):
            category_candidates.append(cn)

    strategy = {
        "table": table.get("name", ""),
        "tier": table.get("tier", "silver"),
        "partition_type": "none",
        "partition_columns": [],
        "zorder_columns": [],
        "estimated_benefit": "low",
    }

    if date_candidates:
        strategy["partition_type"] = "range"
        strategy["partition_columns"] = date_candidates[:1]
        strategy["estimated_benefit"] = "high"
    elif category_candidates and id_candidates:
        strategy["partition_type"] = "hash"
        strategy["partition_columns"] = category_candidates[:1]
        strategy["estimated_benefit"] = "medium"
    elif id_candidates:
        strategy["partition_type"] = "hash"
        strategy["partition_columns"] = id_candidates[:1]
        strategy["estimated_benefit"] = "medium"

    # Z-ORDER: best for filter/join columns that aren't partition keys
    zorder_candidates = []
    partition_set = set(strategy["partition_columns"])
    for cn in id_candidates + date_candidates:
        if cn not in partition_set and cn not in zorder_candidates:
            zorder_candidates.append(cn)
    strategy["zorder_columns"] = zorder_candidates[:4]

    return strategy


def generate_optimize_statements(tables):
    """Generate OPTIMIZE and ZORDER SQL statements for all tables.

    Returns a list of SQL strings for post-load optimization.
    """
    target = _get_target()
    catalog = _get_catalog() if target == "databricks" else None
    statements = []

    for table in tables:
        strategy = recommend_partition_strategy(table)
        tier = table.get("tier", "silver")
        name = table.get("name", "").lower()
        if target == "databricks":
            fqn = f"{catalog}.{tier}.{name}"
        else:
            fqn = f"{tier}.{name}"

        if strategy["zorder_columns"]:
            zorder_cols = ", ".join(strategy["zorder_columns"])
            statements.append(f"OPTIMIZE {fqn} ZORDER BY ({zorder_cols});")
        else:
            statements.append(f"OPTIMIZE {fqn};")

        # ANALYZE TABLE for statistics
        statements.append(f"ANALYZE TABLE {fqn} COMPUTE STATISTICS FOR ALL COLUMNS;")

    return statements


def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"ERROR: {inv_path} not found. Run run_assessment.py first.")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    target = _get_target()
    target_label = "Unity Catalog" if target == "databricks" else "Lakehouse"

    print("=" * 60)
    print(f"  Schema Generator — Sprint 27 [{target_label}]")
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
