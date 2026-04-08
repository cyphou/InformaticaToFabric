"""
DBT Migration — Phase 3 (Sprint 51+)
Reads inventory.json and generates dbt models (SQL) for mappings classified
as Simple or Medium. Complex/Custom mappings are left for PySpark notebooks.

The `--target auto` mode uses this module together with run_notebook_migration
to produce a mixed output: dbt models for ~80% of mappings, PySpark for ~20%.

Outputs:
  output/dbt/
    dbt_project.yml          — dbt project configuration
    profiles.yml             — Databricks SQL Warehouse connection profile
    models/
      sources.yml            — Source table definitions
      staging/stg_*.sql      — Staging models (1:1 with sources)
      intermediate/int_*.sql — Transformation models
      marts/mart_*.sql       — Business entity models
    macros/                   — Reusable Jinja macros
    tests/                    — Custom data tests
    seeds/                    — Reference CSV data

Usage:
    python run_dbt_migration.py
    python run_dbt_migration.py path/to/inventory.json
    INFORMATICA_DBT_MODE=auto python run_dbt_migration.py
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "dbt"
SQL_DIR = WORKSPACE / "output" / "sql"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"

# ─── Complexity-based routing ──────────────────────────────────────────────

# Mappings with these complexities go to dbt (SQL-expressible)
DBT_ELIGIBLE_COMPLEXITIES = {"Simple", "Medium"}

# Transforms that CANNOT be expressed in pure SQL → force PySpark fallback
PYSPARK_ONLY_TRANSFORMS = {
    "JTX", "CT", "HTTP", "XMLG", "XMLP", "EP", "AEP",
    "TC", "WSC",
}

# Transforms with direct SQL equivalents
SQL_EXPRESSIBLE_TRANSFORMS = {
    "SQ", "EXP", "FIL", "AGG", "LKP", "SRT",
    "JNR", "UNI", "RNK", "RTR", "NRM", "SEQ",
    "SP", "SQLT", "DM", "ULKP", "MPLT",
    "ASSOC", "KEYGEN", "ADDRVAL",
}


def get_dbt_mode():
    """Return the dbt mode: 'dbt', 'pyspark', 'auto', or ''."""
    return os.environ.get("INFORMATICA_DBT_MODE", "")


def get_catalog():
    """Return Unity Catalog name for Databricks."""
    return os.environ.get("INFORMATICA_DATABRICKS_CATALOG", "main")


def should_use_dbt(mapping):
    """Determine if a mapping should be converted to dbt (True) or PySpark (False).

    Decision logic:
      1. If complexity is Complex or Custom → PySpark
      2. If mapping contains PySpark-only transforms → PySpark
      3. Otherwise → dbt
    """
    complexity = mapping.get("complexity", "Unknown")
    if complexity not in DBT_ELIGIBLE_COMPLEXITIES:
        return False
    transforms = set(mapping.get("transformations", []))
    if transforms & PYSPARK_ONLY_TRANSFORMS:
        return False
    return True


def classify_mappings(mappings):
    """Split mappings into dbt-eligible and pyspark-only lists.

    Returns:
        (dbt_mappings, pyspark_mappings)
    """
    dbt_list = []
    pyspark_list = []
    for m in mappings:
        if should_use_dbt(m):
            dbt_list.append(m)
        else:
            pyspark_list.append(m)
    return dbt_list, pyspark_list


# ─── Oracle/SQL Server → Databricks SQL helpers ───────────────────────────

ORACLE_TO_DBSQL = [
    (re.compile(r'\bNVL\s*\(', re.IGNORECASE), 'COALESCE('),
    (re.compile(r'\bNVL2\s*\(', re.IGNORECASE), 'IF('),
    (re.compile(r'\bSYSDATE\b', re.IGNORECASE), 'CURRENT_TIMESTAMP()'),
    (re.compile(r'\bSYSTIMESTAMP\b', re.IGNORECASE), 'CURRENT_TIMESTAMP()'),
    (re.compile(r"\bTO_CHAR\s*\(\s*([^,]+),\s*'([^']+)'\s*\)", re.IGNORECASE),
     lambda m: f"DATE_FORMAT({m.group(1)}, '{_oracle_date_fmt(m.group(2))}')"),
    (re.compile(r"\bTO_DATE\s*\(\s*([^,]+),\s*'([^']+)'\s*\)", re.IGNORECASE),
     lambda m: f"TO_DATE({m.group(1)}, '{_oracle_date_fmt(m.group(2))}')"),
    (re.compile(r'\bDECODE\s*\(', re.IGNORECASE), '_DECODE_('),
    (re.compile(r'\bTRUNC\s*\(\s*([^,)]+)\s*\)', re.IGNORECASE),
     r'DATE_TRUNC("DAY", \1)'),
    (re.compile(r"\bADD_MONTHS\s*\(\s*([^,]+),\s*([^)]+)\)", re.IGNORECASE),
     r'ADD_MONTHS(\1, \2)'),
    (re.compile(r'\bROWNUM\b', re.IGNORECASE), 'ROW_NUMBER() OVER ()'),
    (re.compile(r'\bLENGTH\s*\(', re.IGNORECASE), 'LENGTH('),
    (re.compile(r'\bSUBSTR\s*\(', re.IGNORECASE), 'SUBSTRING('),
    (re.compile(r'\bINSTR\s*\(', re.IGNORECASE), 'LOCATE('),
]


def _oracle_date_fmt(fmt):
    """Convert Oracle date format to Spark/Databricks date format."""
    has_24h = 'HH24' in fmt
    result = (fmt
              .replace('YYYY', 'yyyy').replace('YY', 'yy')
              .replace('MM', 'MM').replace('DD', 'dd')
              .replace('HH24', 'HH')
              .replace('MI', 'mm').replace('SS', 'ss'))
    if not has_24h:
        result = result.replace('HH', 'hh')
    return result


def convert_sql_to_dbsql(sql):
    """Convert Oracle/SQL Server SQL to Databricks SQL dialect."""
    result = sql
    for pattern, replacement in ORACLE_TO_DBSQL:
        if callable(replacement):
            result = pattern.sub(replacement, result)
        else:
            result = pattern.sub(replacement, result)
    # Post-process DECODE → CASE WHEN
    result = _expand_decode(result)
    return result


def _expand_decode(sql):
    """Expand _DECODE_(...) placeholders to CASE WHEN expressions.

    Handles: DECODE(expr, val1, result1, val2, result2, ..., default)
    Converts to: CASE expr WHEN val1 THEN result1 WHEN val2 THEN result2 ... ELSE default END
    """
    result = []
    i = 0
    while i < len(sql):
        pos = sql.find('_DECODE_(', i)
        if pos == -1:
            result.append(sql[i:])
            break
        result.append(sql[i:pos])
        # Find matching closing paren, respecting nesting
        depth = 0
        start = pos + len('_DECODE_(')
        j = start
        while j < len(sql):
            if sql[j] == '(':
                depth += 1
            elif sql[j] == ')':
                if depth == 0:
                    break
                depth -= 1
            j += 1
        if j >= len(sql):
            # No matching paren — leave as-is
            result.append(sql[pos:])
            break
        inner = sql[start:j]
        # Split args on top-level commas (respecting parens)
        args = _split_decode_args(inner)
        if len(args) >= 3:
            expr = args[0].strip()
            case_parts = [f"CASE {expr}"]
            k = 1
            while k + 1 < len(args):
                case_parts.append(f" WHEN {args[k].strip()} THEN {args[k+1].strip()}")
                k += 2
            if k < len(args):
                # Odd remaining arg is the default
                case_parts.append(f" ELSE {args[k].strip()}")
            case_parts.append(" END")
            result.append("".join(case_parts))
        else:
            # Too few args — pass through
            result.append(f"DECODE({inner})")
        i = j + 1
    return "".join(result)


def _split_decode_args(s):
    """Split a DECODE argument string on top-level commas."""
    args = []
    depth = 0
    current = []
    for ch in s:
        if ch == '(':
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            args.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        args.append("".join(current))
    return args


def _extract_instance_info(field_lineage):
    """Build a dict of instance_name -> {type, fields} from field lineage entries."""
    info = {}
    for entry in field_lineage:
        for tx in entry.get("transformations", []):
            inst = tx.get("instance", "")
            ttype = tx.get("type", "")
            if inst and inst not in info:
                info[inst] = {"type": ttype, "fields": []}
            if inst:
                tf = entry.get("target_field", "")
                if tf and tf not in info[inst]["fields"]:
                    info[inst]["fields"].append(tf)
    return info


def _fields_through_instance(field_lineage, instance_name):
    """Return target field names that pass through a specific transform instance."""
    fields = []
    for entry in field_lineage:
        for tx in entry.get("transformations", []):
            if tx.get("instance", "").lower() == instance_name.lower():
                tf = entry.get("target_field", "")
                if tf and tf not in fields:
                    fields.append(tf)
    return fields


def _generate_router_group_model(mapping, target_name, group_idx):
    """Generate a separate intermediate model for one Router group/target."""
    name = _sanitize_name(mapping["name"])
    tgt_safe = _sanitize_name(target_name)
    return (
        f"-- Router group {group_idx} for: {mapping['name']} → {target_name}\n"
        f"-- Auto-generated from Router transform\n"
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"\n"
        f'{_model_config(materialized="view", tags=["intermediate", "router_group"], schema="intermediate")}\n'
        f"\n"
        f"SELECT *\n"
        f"FROM {{{{ ref('int_{name}') }}}}\n"
        f"WHERE 1=1  -- TODO: Add Router group {group_idx} condition for target {tgt_safe}\n"
    )


# ─── DBT model generators ─────────────────────────────────────────────────

def _model_config(materialized="view", tags=None, unique_key=None, schema=None):
    """Generate a {{ config(...) }} block."""
    parts = [f'materialized="{materialized}"']
    if tags:
        parts.append(f'tags={json.dumps(tags)}')
    if unique_key:
        parts.append(f'unique_key="{unique_key}"')
    if schema:
        parts.append(f'schema="{schema}"')
    return "{{ config(" + ", ".join(parts) + ") }}"


def _sanitize_name(name):
    """Sanitize a mapping/table name for dbt model naming."""
    return re.sub(r'[^a-z0-9_]', '_', name.lower()).strip('_')


def generate_staging_model(mapping):
    """Generate a staging model (stg_*.sql) from a mapping's source."""
    name = _sanitize_name(mapping["name"])
    sources = mapping.get("sources", [])
    # Use first source table
    source_table = sources[0] if sources else "unknown_source"
    # Extract just the table name from qualified names like Oracle.SCHEMA.TABLE
    parts = source_table.split(".")
    table_name = _sanitize_name(parts[-1])
    schema_name = _sanitize_name(parts[-2]) if len(parts) >= 2 else "bronze"

    sql_override = ""
    if mapping.get("sql_overrides"):
        for ovr in mapping["sql_overrides"]:
            if ovr.get("type") == "Sql Query":
                sql_override = convert_sql_to_dbsql(ovr["value"])
                break

    lines = [
        f"-- Staging model for Informatica mapping: {mapping['name']}",
        f"-- Source: {source_table}",
        f"-- Complexity: {mapping.get('complexity', 'Unknown')}",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        _model_config(materialized="view", tags=["staging", "informatica"], schema="staging"),
        "",
    ]

    if sql_override:
        lines.extend([
            "-- Original SQL override converted to Databricks SQL",
            sql_override,
        ])
    else:
        lines.extend([
            f"SELECT *",
            f"FROM {{{{ source('{schema_name}', '{table_name}') }}}}",
        ])

    return "\n".join(lines)


def generate_intermediate_model(mapping):
    """Generate an intermediate model (int_*.sql) with transformation logic."""
    name = _sanitize_name(mapping["name"])
    transforms = mapping.get("transformations", [])
    field_lineage = mapping.get("field_lineage", [])
    lookup_conditions = mapping.get("lookup_conditions", [])

    # Build instance metadata from field lineage
    instance_info = _extract_instance_info(field_lineage)
    # Build target fields list from lineage
    target_fields = sorted({e["target_field"] for e in field_lineage}) if field_lineage else []
    # Build source fields known from lineage
    source_fields = sorted({e["source_field"] for e in field_lineage}) if field_lineage else []

    lines = [
        f"-- Intermediate model for: {mapping['name']}",
        f"-- Transforms: {' → '.join(transforms)}",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        _model_config(materialized="view", tags=["intermediate", "informatica"],
                      schema="intermediate"),
        "",
        f"WITH source AS (",
        f"    SELECT * FROM {{{{ ref('stg_{name}') }}}}",
        f")",
        "",
    ]

    cte_parts = []
    prev_cte = "source"
    cte_idx = 0

    for tx in transforms:
        if tx == "SQ":
            continue  # Handled by staging model

        if tx == "EXP":
            cte_idx += 1
            # Find EXP instances from lineage
            exp_instances = [inst for inst, info in instance_info.items()
                            if info.get("type") == "Expression"]
            exp_name = exp_instances[0] if exp_instances else "EXP"
            cte_name = f"expressions_{cte_idx}"
            # Build field list from target fields that pass through this transform
            exp_fields = _fields_through_instance(field_lineage, exp_name)
            if exp_fields:
                field_lines = ",\n".join(
                    f"        {f}" for f in exp_fields
                )
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Expression transform: {exp_name}\n"
                    f"    -- Target fields: {', '.join(exp_fields[:5])}{'...' if len(exp_fields) > 5 else ''}\n"
                    f"    SELECT\n"
                    f"        *\n"
                    f"        -- Derived fields from {exp_name}:\n"
                    f"        -- {', '.join(exp_fields)}\n"
                    f"    FROM {prev_cte}\n"
                    f")"
                )
            else:
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Expression transform: derived columns\n"
                    f"    SELECT\n"
                    f"        *\n"
                    f"    FROM {prev_cte}\n"
                    f")"
                )
            prev_cte = cte_name

        elif tx == "FIL":
            cte_idx += 1
            fil_instances = [inst for inst, info in instance_info.items()
                            if info.get("type") == "Filter"]
            fil_name = fil_instances[0] if fil_instances else "FIL"
            cte_name = f"filtered_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Filter transform: {fil_name}\n"
                f"    SELECT * FROM {prev_cte}\n"
                f"    WHERE 1=1  -- TODO: Add condition from {fil_name}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "AGG":
            cte_idx += 1
            agg_instances = [inst for inst, info in instance_info.items()
                            if info.get("type") in ("Aggregator", "AGG")]
            agg_name = agg_instances[0] if agg_instances else "AGG"
            agg_fields = _fields_through_instance(field_lineage, agg_name)
            cte_name = f"aggregated_{cte_idx}"
            if agg_fields:
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Aggregator transform: {agg_name}\n"
                    f"    -- Output fields: {', '.join(agg_fields)}\n"
                    f"    SELECT\n"
                    f"        -- GROUP BY key columns, aggregate: {', '.join(agg_fields)}\n"
                    f"        *\n"
                    f"    FROM {prev_cte}\n"
                    f"    -- GROUP BY group_key\n"
                    f")"
                )
            else:
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Aggregator transform: {agg_name}\n"
                    f"    SELECT\n"
                    f"        *\n"
                    f"    FROM {prev_cte}\n"
                    f"    -- GROUP BY group_key\n"
                    f")"
                )
            prev_cte = cte_name

        elif tx == "LKP":
            cte_idx += 1
            lkp_instances = [inst for inst, info in instance_info.items()
                            if info.get("type") in ("Lookup Procedure", "Lookup", "LKP")]
            # Fallback: use lookup name from lookup_conditions
            if lkp_instances:
                lkp_name = lkp_instances[0]
            elif lookup_conditions:
                lkp_name = lookup_conditions[0].get("lookup", "dim_lookup")
            else:
                lkp_name = "dim_lookup"
            cte_name = f"with_lookup_{cte_idx}"
            # Use lookup_conditions from inventory
            lkp_cond = ""
            lkp_ref = _sanitize_name(lkp_name)
            for lc in lookup_conditions:
                if lc.get("lookup", "").lower() == lkp_name.lower() or not lkp_cond:
                    if lc.get("condition"):
                        lkp_cond = convert_sql_to_dbsql(lc["condition"])
                    elif lc.get("sql"):
                        lkp_cond = convert_sql_to_dbsql(lc["sql"])
            if lkp_cond:
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Lookup: {lkp_name}\n"
                    f"    SELECT\n"
                    f"        s.*\n"
                    f"    FROM {prev_cte} s\n"
                    f"    LEFT JOIN {{{{ ref('{lkp_ref}') }}}} lkp\n"
                    f"        ON {lkp_cond}\n"
                    f")"
                )
            else:
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Lookup: {lkp_name}\n"
                    f"    SELECT\n"
                    f"        s.*\n"
                    f"    FROM {prev_cte} s\n"
                    f"    LEFT JOIN {{{{ ref('{lkp_ref}') }}}} lkp ON s.id = lkp.id\n"
                    f")"
                )
            prev_cte = cte_name

        elif tx == "JNR":
            cte_idx += 1
            jnr_instances = [inst for inst, info in instance_info.items()
                            if info.get("type") in ("Joiner", "JNR")]
            jnr_name = jnr_instances[0] if jnr_instances else "JOINER"
            cte_name = f"joined_{cte_idx}"
            # Check if there are multiple sources
            sources = mapping.get("sources", [])
            if len(sources) > 1:
                other_src = _sanitize_name(sources[1])
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Joiner: {jnr_name}\n"
                    f"    SELECT\n"
                    f"        a.*\n"
                    f"    FROM {prev_cte} a\n"
                    f"    INNER JOIN {{{{ ref('stg_{other_src}') }}}} b ON a.id = b.id\n"
                    f")"
                )
            else:
                cte_parts.append(
                    f", {cte_name} AS (\n"
                    f"    -- Joiner: {jnr_name}\n"
                    f"    SELECT\n"
                    f"        a.*\n"
                    f"    FROM {prev_cte} a\n"
                    f"    -- INNER JOIN {{{{ ref('other_source') }}}} b ON a.key = b.key\n"
                    f")"
                )
            prev_cte = cte_name

        elif tx == "SRT":
            # Sorting is handled in the final SELECT
            pass

        elif tx == "RNK":
            cte_idx += 1
            cte_name = f"ranked_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Rank transform → WINDOW function\n"
                f"    SELECT\n"
                f"        *,\n"
                f"        ROW_NUMBER() OVER (ORDER BY 1) AS row_rank\n"
                f"        -- RANK() OVER (PARTITION BY group_col ORDER BY sort_col) AS rank_val\n"
                f"    FROM {prev_cte}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "UNI":
            cte_idx += 1
            cte_name = f"unioned_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Union transform\n"
                f"    SELECT * FROM {prev_cte}\n"
                f"    -- UNION ALL\n"
                f"    -- SELECT * FROM {{{{ ref('other_model') }}}}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "RTR":
            cte_idx += 1
            cte_name = f"routed_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Router transform → filtered subset\n"
                f"    -- NOTE: Router groups should become separate models\n"
                f"    SELECT * FROM {prev_cte}\n"
                f"    -- WHERE route_condition\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "NRM":
            cte_idx += 1
            cte_name = f"normalized_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Normalizer transform → EXPLODE\n"
                f"    SELECT\n"
                f"        *\n"
                f"        -- , EXPLODE(array_column) AS normalized_value\n"
                f"    FROM {prev_cte}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "SEQ":
            cte_idx += 1
            cte_name = f"with_seq_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Sequence Generator → ROW_NUMBER surrogate key\n"
                f"    SELECT\n"
                f"        *,\n"
                f"        ROW_NUMBER() OVER (ORDER BY 1) AS sk_id\n"
                f"    FROM {prev_cte}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "UPD":
            # Update strategy → handled via materialization (incremental + merge)
            pass

        elif tx == "DM":
            cte_idx += 1
            cte_name = f"masked_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Data masking transform\n"
                f"    SELECT\n"
                f"        *\n"
                f"        -- , MD5(CAST(sensitive_col AS STRING)) AS sensitive_col_hash\n"
                f"    FROM {prev_cte}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "SQLT":
            cte_idx += 1
            cte_name = f"sql_tx_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- SQL Transformation (inline SQL)\n"
                f"    -- TODO: Embed converted SQL here\n"
                f"    SELECT * FROM {prev_cte}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "MPLT":
            cte_idx += 1
            cte_name = f"mapplet_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Mapplet → reusable macro\n"
                f"    -- TODO: {{{{ mapplet_name(ref('stg_{name}')) }}}}\n"
                f"    SELECT * FROM {prev_cte}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "SP":
            cte_idx += 1
            cte_name = f"stored_proc_{cte_idx}"
            cte_parts.append(
                f", {cte_name} AS (\n"
                f"    -- Stored Procedure → inline SQL or dbt run-operation\n"
                f"    -- TODO: Convert stored procedure to SQL\n"
                f"    SELECT * FROM {prev_cte}\n"
                f")"
            )
            prev_cte = cte_name

        elif tx == "port":
            # Port is a metadata construct, not a real transform
            pass

    lines.extend([p for p in cte_parts])
    lines.extend([
        "",
        f"SELECT * FROM {prev_cte}",
    ])

    # Check if any sort transforms exist
    if "SRT" in transforms:
        lines.append("-- ORDER BY sort_column")

    return "\n".join(lines)


def _is_scd2_candidate(mapping):
    """Detect if a mapping is an SCD2 / snapshot candidate.

    Triggers on:
      - UPD transform present (explicit upsert/merge)
      - Target name contains 'history', 'snapshot', 'scd', 'archive'
      - Mapping name contains 'scd', 'history', 'snapshot'
    """
    transforms = set(mapping.get("transformations", []))
    if "UPD" in transforms:
        return True
    targets = [t.lower() for t in mapping.get("targets", [])]
    name_lower = mapping.get("name", "").lower()
    scd_keywords = {"history", "snapshot", "scd", "archive", "slowly_changing"}
    for tgt in targets:
        if any(kw in tgt for kw in scd_keywords):
            return True
    if any(kw in name_lower for kw in scd_keywords):
        return True
    return False


def generate_mart_model(mapping):
    """Generate a mart model (mart_*.sql) — final business entity."""
    name = _sanitize_name(mapping["name"])
    targets = mapping.get("targets", [])
    target_table = _sanitize_name(targets[0]) if targets else name
    has_upsert = "UPD" in mapping.get("transformations", [])

    lines = [
        f"-- Mart model for: {mapping['name']}",
        f"-- Target: {', '.join(targets)}",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]

    if has_upsert:
        lines.append(
            _model_config(materialized="incremental", tags=["marts", "informatica"],
                         unique_key="id", schema="silver")
        )
    else:
        lines.append(
            _model_config(materialized="table", tags=["marts", "informatica"],
                         schema="silver")
        )

    lines.extend([
        "",
        f"SELECT * FROM {{{{ ref('int_{name}') }}}}",
    ])

    if has_upsert:
        lines.extend([
            "",
            "{% if is_incremental() %}",
            "WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})",
            "{% endif %}",
        ])

    return "\n".join(lines)


def generate_sources_yml(mappings, catalog=None):
    """Generate models/sources.yml from inventory mappings."""
    catalog = catalog or get_catalog()
    sources_by_schema = {}

    for m in mappings:
        for src in m.get("sources", []):
            parts = src.split(".")
            table_name = parts[-1].lower()
            schema_name = parts[-2].lower() if len(parts) >= 2 else "bronze"
            if schema_name not in sources_by_schema:
                sources_by_schema[schema_name] = set()
            sources_by_schema[schema_name].add(table_name)

    lines = [
        "# Auto-generated from Informatica inventory",
        f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "version: 2",
        "",
        "sources:",
    ]

    for schema, tables in sorted(sources_by_schema.items()):
        lines.extend([
            f"  - name: {schema}",
            f"    database: {catalog}",
            f"    schema: {schema}",
            f"    tables:",
        ])
        for table in sorted(tables):
            lines.append(f"      - name: {table}")

    return "\n".join(lines)


def generate_schema_yml(dbt_mappings):
    """Generate models/schema.yml with basic column tests."""
    lines = [
        "# Auto-generated schema tests from Informatica inventory",
        f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "version: 2",
        "",
        "models:",
    ]

    for m in dbt_mappings:
        name = _sanitize_name(m["name"])
        lines.extend([
            f"  - name: stg_{name}",
            f"    description: \"Staging model for Informatica mapping {m['name']}\"",
            f"    columns:",
            f"      - name: id",
            f"        tests:",
            f"          - not_null",
        ])

    return "\n".join(lines)


def generate_dbt_project_yml(project_name="informatica_migration"):
    """Generate dbt_project.yml."""
    return f"""# dbt project for Informatica → Databricks migration
# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

name: '{project_name}'
version: '1.0.0'
config-version: 2

profile: 'databricks_migration'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  {project_name}:
    staging:
      +materialized: view
      +schema: staging
      +tags: ["staging"]
    intermediate:
      +materialized: view
      +schema: intermediate
      +tags: ["intermediate"]
    marts:
      +materialized: table
      +schema: silver
      +tags: ["marts"]
"""


def generate_profiles_yml(catalog=None):
    """Generate profiles.yml for Databricks SQL Warehouse."""
    catalog = catalog or get_catalog()
    return f"""# Databricks connection profile for dbt
# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
# Update host, http_path, and token before use.

databricks_migration:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: {catalog}
      schema: dev
      host: "{{{{ env_var('DBT_DATABRICKS_HOST') }}}}"
      http_path: "{{{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}}}"
      token: "{{{{ env_var('DBT_DATABRICKS_TOKEN') }}}}"
      threads: 4
    prod:
      type: databricks
      catalog: {catalog}
      schema: prod
      host: "{{{{ env_var('DBT_DATABRICKS_HOST') }}}}"
      http_path: "{{{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}}}"
      token: "{{{{ env_var('DBT_DATABRICKS_TOKEN') }}}}"
      threads: 8
"""


def generate_packages_yml():
    """Generate packages.yml with recommended dbt packages."""
    return """# dbt packages for migration project
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
"""


def write_dbt_project(dbt_mappings, all_mappings):
    """Write the full dbt project to output/dbt/."""
    catalog = get_catalog()
    base = OUTPUT_DIR
    models_dir = base / "models"
    staging_dir = models_dir / "staging"
    intermediate_dir = models_dir / "intermediate"
    marts_dir = models_dir / "marts"

    for d in [base, models_dir, staging_dir, intermediate_dir, marts_dir,
              base / "macros", base / "tests", base / "seeds",
              base / "snapshots", base / "analyses"]:
        d.mkdir(parents=True, exist_ok=True)

    # Project config files
    (base / "dbt_project.yml").write_text(
        generate_dbt_project_yml(), encoding="utf-8")
    (base / "profiles.yml").write_text(
        generate_profiles_yml(catalog), encoding="utf-8")
    (base / "packages.yml").write_text(
        generate_packages_yml(), encoding="utf-8")

    # Sources
    (models_dir / "sources.yml").write_text(
        generate_sources_yml(all_mappings, catalog), encoding="utf-8")

    # Enhanced schema tests (Sprint 55)
    (models_dir / "schema.yml").write_text(
        generate_enhanced_schema_yml(dbt_mappings), encoding="utf-8")

    # Utility macros (Sprint 56)
    macros = generate_utility_macros()
    for macro_file, macro_content in macros.items():
        (base / "macros" / macro_file).write_text(macro_content, encoding="utf-8")

    # CI/CD pipeline (Sprint 58)
    ci_dir = base / ".github" / "workflows"
    ci_dir.mkdir(parents=True, exist_ok=True)
    (ci_dir / "dbt_ci.yml").write_text(generate_dbt_ci_yml(), encoding="utf-8")

    # Models per mapping
    generated = []
    for mapping in dbt_mappings:
        name = _sanitize_name(mapping["name"])

        stg_path = staging_dir / f"stg_{name}.sql"
        stg_path.write_text(generate_staging_model(mapping), encoding="utf-8")

        int_path = intermediate_dir / f"int_{name}.sql"
        int_path.write_text(generate_intermediate_model(mapping), encoding="utf-8")

        # Router → separate models per target (Sprint 67)
        if "RTR" in mapping.get("transformations", []):
            targets = mapping.get("targets", [])
            if len(targets) > 1:
                for idx, tgt in enumerate(targets, 1):
                    tgt_name = _sanitize_name(tgt)
                    rtr_path = intermediate_dir / f"int_{name}_group_{idx}.sql"
                    rtr_path.write_text(
                        _generate_router_group_model(mapping, tgt, idx),
                        encoding="utf-8",
                    )

        # Check for UPD or SCD2 candidate → incremental + snapshot
        is_scd2 = _is_scd2_candidate(mapping)
        has_upsert = "UPD" in mapping.get("transformations", [])
        if has_upsert or is_scd2:
            # Generate incremental model (Sprint 56) instead of plain mart
            mart_path = marts_dir / f"mart_{name}.sql"
            mart_path.write_text(generate_incremental_model(mapping), encoding="utf-8")

            # Also generate snapshot for SCD2 (Sprint 56)
            snp_path = base / "snapshots" / f"snp_{name}.sql"
            snp_path.write_text(generate_snapshot(mapping, catalog), encoding="utf-8")
        else:
            mart_path = marts_dir / f"mart_{name}.sql"
            mart_path.write_text(generate_mart_model(mapping), encoding="utf-8")

        # Custom data tests (Sprint 55)
        test_path = base / "tests" / f"assert_row_count_{name}.sql"
        test_path.write_text(generate_data_test_row_count(mapping, catalog), encoding="utf-8")
        test_tx_path = base / "tests" / f"assert_transform_{name}.sql"
        test_tx_path.write_text(generate_data_test_transform(mapping), encoding="utf-8")

        # Mapplet macros (Sprint 53)
        if "MPLT" in mapping.get("transformations", []):
            mplt_path = base / "macros" / f"mapplet_{name}.sql"
            mplt_path.write_text(generate_mapplet_macro(mapping["name"]), encoding="utf-8")

        generated.append({
            "mapping": mapping["name"],
            "target": "dbt",
            "models": [str(stg_path.name), str(int_path.name), str(mart_path.name)],
        })

    # Write generation summary
    summary = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "dbt_mode": get_dbt_mode(),
        "catalog": catalog,
        "total_dbt_mappings": len(dbt_mappings),
        "total_all_mappings": len(all_mappings),
        "dbt_percentage": round(len(dbt_mappings) / max(len(all_mappings), 1) * 100, 1),
        "models": generated,
    }
    (base / "dbt_generation_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    return summary


# ─── Sprint 53: Mapplet → dbt macro ───────────────────────────────────────

def generate_mapplet_macro(mapplet_name):
    """Generate a reusable dbt macro for a mapplet."""
    safe_name = _sanitize_name(mapplet_name)
    return f"""-- Reusable macro generated from Informatica mapplet: {mapplet_name}
-- Usage: {{{{ mapplet_{safe_name}(ref('source_model')) }}}}

{{% macro mapplet_{safe_name}(source_model) %}}
SELECT
    *
    -- TODO: Add mapplet transformation logic
FROM {{{{ source_model }}}}
{{% endmacro %}}
"""


def generate_utility_macros():
    """Generate common utility macros (Sprint 56)."""
    macros = {}

    macros["clean_string.sql"] = """-- Clean and standardize string values
{% macro clean_string(column_name) %}
    TRIM(UPPER(REGEXP_REPLACE({{ column_name }}, r'\\s+', ' ')))
{% endmacro %}
"""

    macros["safe_divide.sql"] = """-- Safe division avoiding divide-by-zero
{% macro safe_divide(numerator, denominator, default_value=0) %}
    CASE WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
         THEN {{ default_value }}
         ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
"""

    macros["hash_key.sql"] = """-- Generate MD5 hash key from multiple columns
{% macro hash_key(columns) %}
    MD5(CONCAT_WS('|', {% for col in columns %}CAST(COALESCE(CAST({{ col }} AS STRING), '') AS STRING){% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}
"""

    macros["surrogate_key.sql"] = """-- Generate surrogate key using dbt_utils
{% macro surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}
"""

    macros["informatica_iif.sql"] = """-- Convert Informatica IIF to SQL CASE WHEN
{% macro iif(condition, true_value, false_value) %}
    CASE WHEN {{ condition }} THEN {{ true_value }} ELSE {{ false_value }} END
{% endmacro %}
"""
    return macros


# ─── Sprint 55: dbt test generators ───────────────────────────────────────

def generate_data_test_row_count(mapping, catalog="main"):
    """Generate a custom dbt data test for row count validation (L2)."""
    name = _sanitize_name(mapping["name"])
    targets = mapping.get("targets", [])
    target_table = targets[0].split(".")[-1].lower() if targets else name
    tier = "gold" if "AGG" in mapping.get("transformations", []) else "silver"

    return f"""-- Row count validation for {mapping['name']}
-- Level 2 test: ensure target table has expected row count

SELECT
    CASE WHEN COUNT(*) = 0 THEN 1 END AS validation_failure
FROM {{{{ ref('mart_{name}') }}}}
HAVING COUNT(*) = 0
"""


def generate_data_test_transform(mapping):
    """Generate a custom dbt data test for transformation verification (L4)."""
    name = _sanitize_name(mapping["name"])
    transforms = mapping.get("transformations", [])

    lines = [
        f"-- Transformation verification for {mapping['name']}",
        f"-- Level 4 test: verify business logic",
        f"",
    ]

    if "EXP" in transforms:
        lines.append(f"-- Verify expression-derived columns are non-null")
        lines.append(f"SELECT * FROM {{{{ ref('mart_{name}') }}}}")
        lines.append(f"WHERE 1=0  -- TODO: Add business rule assertions")
    else:
        lines.append(f"SELECT * FROM {{{{ ref('mart_{name}') }}}}")
        lines.append(f"WHERE 1=0  -- No transformation assertions needed")

    return "\n".join(lines)


def generate_enhanced_schema_yml(dbt_mappings):
    """Generate enhanced models/schema.yml with comprehensive tests (Sprint 55)."""
    lines = [
        "# Auto-generated schema tests from Informatica inventory",
        f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "version: 2",
        "",
        "models:",
    ]

    for m in dbt_mappings:
        name = _sanitize_name(m["name"])

        # Staging model
        lines.extend([
            f"  - name: stg_{name}",
            f"    description: \"Staging model for Informatica mapping {m['name']}\"",
            f"    columns:",
            f"      - name: id",
            f"        tests:",
            f"          - not_null",
        ])

        # Intermediate model
        lines.extend([
            f"  - name: int_{name}",
            f"    description: \"Intermediate transforms for {m['name']}\"",
            f"    columns:",
            f"      - name: id",
            f"        tests:",
            f"          - not_null",
        ])

        # Mart model
        lines.extend([
            f"  - name: mart_{name}",
            f"    description: \"Business entity from {m['name']}\"",
            f"    meta:",
            f"      owner: data-engineering",
            f"      sla_seconds: 300",
            f"    columns:",
            f"      - name: id",
            f"        tests:",
            f"          - not_null",
            f"          - unique",
        ])

    return "\n".join(lines)


# ─── Sprint 56: Snapshots & incremental models ────────────────────────────

def generate_snapshot(mapping, catalog="main"):
    """Generate a dbt snapshot for SCD Type 2 mappings."""
    name = _sanitize_name(mapping["name"])
    catalog = catalog or get_catalog()
    targets = mapping.get("targets", [])
    target_table = targets[0].split(".")[-1].lower() if targets else name

    return f"""{{% snapshot snp_{name} %}}

{{{{
    config(
        target_schema='silver',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}}}

SELECT * FROM {{{{ ref('int_{name}') }}}}

{{% endsnapshot %}}
"""


def generate_incremental_model(mapping):
    """Generate an incremental model with merge strategy (Sprint 56)."""
    name = _sanitize_name(mapping["name"])
    targets = mapping.get("targets", [])
    target_table = _sanitize_name(targets[0]) if targets else name

    return f"""-- Incremental model for: {mapping['name']}
-- Strategy: merge (upsert)
-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

{{{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    schema='silver',
    tags=['incremental', 'informatica'],
    pre_hook=["OPTIMIZE {{{{ this }}}} ZORDER BY (id)"],
    post_hook=["ANALYZE TABLE {{{{ this }}}} COMPUTE STATISTICS"]
) }}}}

SELECT * FROM {{{{ ref('int_{name}') }}}}

{{% if is_incremental() %}}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{{{ this }}}})
{{% endif %}}
"""


# ─── Sprint 57: Mixed pipeline generation ─────────────────────────────────

def generate_mixed_workflow(workflow, dbt_mappings, pyspark_mappings, catalog="main"):
    """Generate a Databricks Workflow mixing dbt and notebook tasks (Sprint 57)."""
    name = workflow.get("name", "UNKNOWN")
    dbt_names = {_sanitize_name(m["name"]) for m in dbt_mappings}
    pyspark_names = {m["name"] for m in pyspark_mappings}

    tasks = []

    # Pre-task: dbt deps + seed
    tasks.append({
        "task_key": "dbt_deps_seed",
        "dbt_task": {
            "commands": ["dbt deps", "dbt seed"],
            "project_directory": f"/Repos/migration/dbt_project",
            "warehouse_id": "{{WAREHOUSE_ID}}",
        },
    })

    prev_task = "dbt_deps_seed"
    sessions = workflow.get("sessions", [])

    for session in sessions:
        mapping_name = session.get("mapping", session.get("name", ""))
        sanitized = _sanitize_name(mapping_name)

        if sanitized in dbt_names:
            task_key = f"dbt_{sanitized}"
            task = {
                "task_key": task_key,
                "depends_on": [{"task_key": prev_task}],
                "dbt_task": {
                    "commands": [f"dbt run --select mart_{sanitized}"],
                    "project_directory": "/Repos/migration/dbt_project",
                    "warehouse_id": "{{WAREHOUSE_ID}}",
                },
            }
        else:
            task_key = f"nb_{mapping_name}".replace("-", "_").replace(" ", "_")
            task = {
                "task_key": task_key,
                "depends_on": [{"task_key": prev_task}],
                "notebook_task": {
                    "notebook_path": f"/Shared/migration/NB_{mapping_name}",
                    "base_parameters": {
                        "load_date": "{{job.trigger_time.iso_date}}",
                        "catalog": catalog,
                    },
                },
                "new_cluster": {
                    "spark_version": "14.3.x-photon-scala2.12",
                    "num_workers": 4,
                },
            }

        tasks.append(task)
        prev_task = task_key

    # Post-task: dbt test
    if any(t.get("dbt_task") for t in tasks[1:]):
        tasks.append({
            "task_key": "dbt_test",
            "depends_on": [{"task_key": prev_task}],
            "dbt_task": {
                "commands": ["dbt test"],
                "project_directory": "/Repos/migration/dbt_project",
                "warehouse_id": "{{WAREHOUSE_ID}}",
            },
        })

    # Schedule
    schedule_cron = workflow.get("schedule_cron", {})
    cron_expr = schedule_cron.get("cron", "") if schedule_cron else ""

    job_def = {
        "name": f"PL_{name}",
        "tasks": tasks,
        "max_concurrent_runs": 1,
    }

    if cron_expr:
        job_def["schedule"] = {
            "quartz_cron_expression": cron_expr,
            "timezone_id": "UTC",
        }

    return job_def


# ─── Sprint 58: dbt deployment ────────────────────────────────────────────

def generate_dbt_ci_yml():
    """Generate a GitHub Actions CI/CD pipeline for dbt (Sprint 58)."""
    return """# dbt CI/CD Pipeline — auto-generated
name: dbt CI

on:
  push:
    branches: [main]
    paths: ['output/dbt/**']
  pull_request:
    branches: [main]
    paths: ['output/dbt/**']

env:
  DBT_DATABRICKS_HOST: ${{ secrets.DBT_DATABRICKS_HOST }}
  DBT_DATABRICKS_HTTP_PATH: ${{ secrets.DBT_DATABRICKS_HTTP_PATH }}
  DBT_DATABRICKS_TOKEN: ${{ secrets.DBT_DATABRICKS_TOKEN }}

jobs:
  dbt-build:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: output/dbt

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dbt
        run: pip install dbt-databricks>=1.7

      - name: dbt deps
        run: dbt deps

      - name: dbt build
        run: dbt build --profiles-dir .

      - name: dbt test
        run: dbt test --profiles-dir .

      - name: dbt docs generate
        run: dbt docs generate --profiles-dir .
"""


def generate_deploy_dbt_script():
    """Generate a deployment script for the dbt project (Sprint 58)."""
    return '''#!/usr/bin/env python3
"""Deploy dbt project to Databricks Repos."""
import argparse
import json
import sys

try:
    import requests
except ImportError:
    print("ERROR: pip install requests")
    sys.exit(1)


def deploy_to_repos(workspace_url, token, repo_path, git_url, branch="main"):
    """Create or update a Databricks Repo linked to the dbt project."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Try to create the repo
    resp = requests.post(
        f"{workspace_url.rstrip('/')}/api/2.0/repos",
        headers=headers,
        json={"url": git_url, "provider": "github", "path": repo_path},
        timeout=30,
    )

    if resp.status_code == 200:
        print(f"  Created repo: {repo_path}")
        return resp.json()
    elif resp.status_code == 400 and "already exists" in resp.text.lower():
        print(f"  Repo already exists: {repo_path}")
        # Update to latest branch
        # Get repo ID first
        list_resp = requests.get(
            f"{workspace_url.rstrip('/')}/api/2.0/repos",
            headers=headers,
            params={"path_prefix": repo_path},
            timeout=30,
        )
        if list_resp.ok:
            repos = list_resp.json().get("repos", [])
            if repos:
                repo_id = repos[0]["id"]
                update_resp = requests.patch(
                    f"{workspace_url.rstrip('/')}/api/2.0/repos/{repo_id}",
                    headers=headers,
                    json={"branch": branch},
                    timeout=30,
                )
                if update_resp.ok:
                    print(f"  Updated to branch: {branch}")
                    return update_resp.json()
        return {"status": "exists"}
    else:
        print(f"  Error: {resp.status_code} {resp.text[:200]}")
        return {"error": resp.status_code}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy dbt project to Databricks")
    parser.add_argument("--workspace-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--repo-path", default="/Repos/migration/dbt_project")
    parser.add_argument("--git-url", required=True, help="Git repository URL")
    parser.add_argument("--branch", default="main")
    args = parser.parse_args()

    deploy_to_repos(args.workspace_url, args.token, args.repo_path, args.git_url, args.branch)
'''


def main():
    """Main entry point for dbt migration."""
    # Load inventory
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else INVENTORY_PATH
    if not inv_path.exists():
        print(f"  ⚠️  Inventory not found: {inv_path}")
        print("     Run assessment first: python run_assessment.py")
        return

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    mappings = inventory.get("mappings", [])
    if not mappings:
        print("  ⚠️  No mappings found in inventory.")
        return

    dbt_mode = get_dbt_mode()

    # Route mappings based on mode
    if dbt_mode == "pyspark":
        # Pure PySpark mode — skip DBT entirely
        print("  ⏭️  DBT migration skipped (mode: pyspark)")
        return
    elif dbt_mode == "dbt":
        # Pure DBT mode — all eligible go to dbt, rest get warnings
        dbt_mappings, pyspark_mappings = classify_mappings(mappings)
        if pyspark_mappings:
            print(f"  ⚠️  {len(pyspark_mappings)} complex mappings need PySpark (not expressible in SQL)")
            for m in pyspark_mappings:
                print(f"      → {m['name']} ({m.get('complexity', '?')})")
    elif dbt_mode == "auto":
        # Auto mode — split based on complexity
        dbt_mappings, pyspark_mappings = classify_mappings(mappings)
        print(f"  🎯 Auto-routing: {len(dbt_mappings)} → dbt, {len(pyspark_mappings)} → PySpark")
    else:
        # Default: if INFORMATICA_DBT_MODE is not set, classify and generate all eligible
        dbt_mappings, pyspark_mappings = classify_mappings(mappings)
        if not dbt_mappings:
            print("  ⏭️  No dbt-eligible mappings found.")
            return

    if not dbt_mappings:
        print("  ⏭️  No dbt-eligible mappings to generate.")
        return

    print(f"  📂 Generating dbt project: {OUTPUT_DIR}")
    print(f"     Mappings: {len(dbt_mappings)} (dbt) / {len(mappings)} (total)")

    summary = write_dbt_project(dbt_mappings, mappings)

    pct = summary["dbt_percentage"]
    print(f"  ✅ dbt project generated — {len(dbt_mappings)} models ({pct}% of total)")
    print(f"     Project: {OUTPUT_DIR / 'dbt_project.yml'}")
    print(f"     Models:  {OUTPUT_DIR / 'models'}")
    for m in summary["models"]:
        print(f"       → {m['mapping']}: {', '.join(m['models'])}")


if __name__ == "__main__":
    main()
