"""
Databricks Deployment — Upload Migration Artifacts
Deploys generated notebooks, workflows (Jobs), and SQL to an Azure
Databricks workspace using the Databricks REST API.

Prerequisites:
  pip install databricks-sdk   (optional — falls back to requests)
  Or: pip install requests

Authentication:
  Provide a Databricks personal access token (PAT) via:
    --token flag, DATABRICKS_TOKEN env var, or migration.yaml

Configuration:
  Set values in migration.yaml or pass via CLI flags:
    --workspace-url   Databricks workspace URL  (e.g. https://adb-123.azuredatabricks.net)
    --token           Personal access token
    --notebook-path   Workspace path for notebooks (default: /Shared/migration)
    --dry-run         Preview actions without making API calls

Outputs:
  output/databricks_deployment_log.json — deployment results per artifact

Usage:
    python deploy_to_databricks.py --workspace-url https://adb-xxx.azuredatabricks.net --token dapi...
    python deploy_to_databricks.py --workspace-url https://adb-xxx.azuredatabricks.net --dry-run
"""

import argparse
import base64
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests as _requests
except ImportError:
    _requests = None

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output"


# ─────────────────────────────────────────────
#  Authentication & HTTP helpers
# ─────────────────────────────────────────────

def _headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _api_request(method, url, token, json_body=None, timeout=60):
    """Make an authenticated Databricks API request with retry on 429."""
    if _requests is None:
        print("ERROR: requests not installed. Run: pip install requests")
        sys.exit(1)
    resp = _requests.request(method, url, headers=_headers(token), json=json_body, timeout=timeout)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "30"))
        print(f"    ⏳ Rate limited — waiting {retry_after}s...")
        time.sleep(retry_after)
        resp = _requests.request(method, url, headers=_headers(token), json=json_body, timeout=timeout)
    return resp


# ─────────────────────────────────────────────
#  Notebook Deployment
# ─────────────────────────────────────────────

def deploy_notebooks(workspace_url, token, notebook_path="/Shared/migration", dry_run=False):
    """Deploy all NB_*.py notebooks to the Databricks workspace."""
    nb_dir = OUTPUT_DIR / "notebooks"
    nb_files = sorted(nb_dir.glob("NB_*.py"))
    results = []

    print(f"\n  📓 Deploying {len(nb_files)} notebooks to {notebook_path}/...")

    for nb_path in nb_files:
        display_name = nb_path.stem
        remote_path = f"{notebook_path}/{display_name}"
        content_b64 = base64.b64encode(nb_path.read_bytes()).decode("utf-8")

        if dry_run:
            print(f"    [DRY-RUN] Would import notebook: {remote_path}")
            results.append({"artifact": display_name, "type": "Notebook", "status": "dry-run", "path": remote_path})
            continue

        url = f"{workspace_url.rstrip('/')}/api/2.0/workspace/import"
        payload = {
            "path": remote_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "content": content_b64,
            "overwrite": True,
        }
        resp = _api_request("POST", url, token, payload)

        if resp.status_code == 200:
            print(f"    ✅ {display_name} → {remote_path}")
            results.append({"artifact": display_name, "type": "Notebook", "status": "created", "path": remote_path})
        else:
            msg = resp.text[:200] if resp.text else str(resp.status_code)
            print(f"    ❌ {display_name}: {resp.status_code} {msg}")
            results.append({"artifact": display_name, "type": "Notebook", "status": "error", "code": resp.status_code})

    return results


# ─────────────────────────────────────────────
#  Workflow (Jobs) Deployment
# ─────────────────────────────────────────────

def deploy_workflows(workspace_url, token, dry_run=False):
    """Deploy all PL_*.json workflow definitions as Databricks Jobs."""
    pl_dir = OUTPUT_DIR / "pipelines"
    pl_files = sorted(pl_dir.glob("PL_*.json"))
    results = []

    print(f"\n  ⚡ Deploying {len(pl_files)} workflows as Databricks Jobs...")

    for pl_path in pl_files:
        display_name = pl_path.stem
        try:
            job_def = json.loads(pl_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"    ❌ {display_name}: invalid JSON — {exc}")
            results.append({"artifact": display_name, "type": "Job", "status": "error", "error": str(exc)})
            continue

        if dry_run:
            print(f"    [DRY-RUN] Would create job: {display_name}")
            results.append({"artifact": display_name, "type": "Job", "status": "dry-run"})
            continue

        url = f"{workspace_url.rstrip('/')}/api/2.1/jobs/create"
        resp = _api_request("POST", url, token, job_def)

        if resp.status_code == 200:
            job_id = resp.json().get("job_id", "unknown")
            print(f"    ✅ {display_name} → job_id={job_id}")
            results.append({"artifact": display_name, "type": "Job", "status": "created", "job_id": job_id})
        else:
            msg = resp.text[:200] if resp.text else str(resp.status_code)
            print(f"    ❌ {display_name}: {resp.status_code} {msg}")
            results.append({"artifact": display_name, "type": "Job", "status": "error", "code": resp.status_code})

    return results


# ─────────────────────────────────────────────
#  SQL Script Deployment (as Notebooks)
# ─────────────────────────────────────────────

def deploy_sql_scripts(workspace_url, token, notebook_path="/Shared/migration/sql", dry_run=False):
    """Deploy SQL scripts as Databricks SQL notebooks."""
    sql_dir = OUTPUT_DIR / "sql"
    sql_files = sorted(sql_dir.glob("SQL_*.sql"))
    results = []

    print(f"\n  🗄️  Deploying {len(sql_files)} SQL scripts as notebooks...")

    for sql_path in sql_files:
        display_name = f"SQL_{sql_path.stem}"
        remote_path = f"{notebook_path}/{display_name}"

        sql_content = sql_path.read_text(encoding="utf-8")
        nb_content = f"-- Databricks notebook source\n-- Converted SQL: {sql_path.name}\n\n-- COMMAND ----------\n\n{sql_content}\n"
        content_b64 = base64.b64encode(nb_content.encode("utf-8")).decode("utf-8")

        if dry_run:
            print(f"    [DRY-RUN] Would import SQL notebook: {remote_path}")
            results.append({"artifact": display_name, "type": "SQL Notebook", "status": "dry-run", "path": remote_path})
            continue

        url = f"{workspace_url.rstrip('/')}/api/2.0/workspace/import"
        payload = {
            "path": remote_path,
            "format": "SOURCE",
            "language": "SQL",
            "content": content_b64,
            "overwrite": True,
        }
        resp = _api_request("POST", url, token, payload)

        if resp.status_code == 200:
            print(f"    ✅ {display_name} → {remote_path}")
            results.append({"artifact": display_name, "type": "SQL Notebook", "status": "created", "path": remote_path})
        else:
            msg = resp.text[:200] if resp.text else str(resp.status_code)
            print(f"    ❌ {display_name}: {resp.status_code} {msg}")
            results.append({"artifact": display_name, "type": "SQL Notebook", "status": "error", "code": resp.status_code})

    return results


# ─────────────────────────────────────────────
#  Secret Scope Setup
# ─────────────────────────────────────────────

def setup_secret_scope(workspace_url, token, scope_name, secrets=None, dry_run=False):
    """Create a Databricks secret scope and populate secrets."""
    secrets = secrets or {}
    results = []

    print(f"\n  🔐 Setting up secret scope: {scope_name}")

    if dry_run:
        print(f"    [DRY-RUN] Would create scope: {scope_name}")
        for key in secrets:
            print(f"    [DRY-RUN] Would add secret: {key}")
        return [{"action": "setup-secrets", "scope": scope_name, "status": "dry-run", "count": len(secrets)}]

    # Create scope
    url = f"{workspace_url.rstrip('/')}/api/2.0/secrets/scopes/create"
    resp = _api_request("POST", url, token, {"scope": scope_name})
    if resp.status_code == 200:
        print(f"    ✅ Created scope: {scope_name}")
        results.append({"action": "create-scope", "scope": scope_name, "status": "created"})
    elif resp.status_code == 400 and "RESOURCE_ALREADY_EXISTS" in resp.text:
        print(f"    ⚠️  Scope {scope_name} already exists")
        results.append({"action": "create-scope", "scope": scope_name, "status": "exists"})
    else:
        print(f"    ❌ Failed to create scope: {resp.status_code}")
        results.append({"action": "create-scope", "scope": scope_name, "status": "error", "code": resp.status_code})
        return results

    # Add secrets
    for key, value in secrets.items():
        url = f"{workspace_url.rstrip('/')}/api/2.0/secrets/put"
        resp = _api_request("POST", url, token, {
            "scope": scope_name,
            "key": key,
            "string_value": value,
        })
        if resp.status_code == 200:
            print(f"    ✅ Secret: {key}")
            results.append({"action": "put-secret", "key": key, "status": "created"})
        else:
            print(f"    ❌ Secret {key}: {resp.status_code}")
            results.append({"action": "put-secret", "key": key, "status": "error", "code": resp.status_code})

    return results


# ─────────────────────────────────────────────
#  Unity Catalog Permissions Generator
# ─────────────────────────────────────────────

def generate_uc_permissions(inventory_path=None, catalog="main"):
    """Generate Unity Catalog GRANT SQL statements from inventory."""
    inv_path = inventory_path or (OUTPUT_DIR / "inventory" / "inventory.json")
    if not inv_path.exists():
        print("  ⚠️  inventory.json not found — skipping UC permissions")
        return ""

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    schemas = set()
    tables = set()

    for mapping in inventory.get("mappings", []):
        for target in mapping.get("targets", []):
            table_name = target.split(".")[-1].lower()
            complexity = mapping.get("complexity", "Simple")
            tier = "gold" if any(t in mapping.get("transformations", []) for t in ["AGG", "RNK"]) else "silver"
            schemas.add(tier)
            tables.add(f"{tier}.{table_name}")

    lines = [
        f"-- Unity Catalog Permissions for catalog: {catalog}",
        f"-- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        f"-- Catalog-level grants",
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `data-engineers`;",
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `data-analysts`;",
        "",
    ]

    for schema in sorted(schemas):
        lines.append(f"-- Schema: {catalog}.{schema}")
        lines.append(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `data-engineers`;")
        lines.append(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `data-analysts`;")
        lines.append(f"GRANT CREATE TABLE ON SCHEMA {catalog}.{schema} TO `data-engineers`;")
        lines.append("")

    for table in sorted(tables):
        full_name = f"{catalog}.{table}"
        lines.append(f"GRANT SELECT ON TABLE {full_name} TO `data-analysts`;")
        lines.append(f"GRANT MODIFY ON TABLE {full_name} TO `data-engineers`;")

    lines.append("")
    lines.append("-- Function grants for UDFs")
    lines.append(f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {catalog}.silver TO `data-engineers`;")
    if "gold" in schemas:
        lines.append(f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {catalog}.gold TO `data-engineers`;")
    lines.append("")

    sql = "\n".join(lines)

    out_dir = OUTPUT_DIR / "scripts"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "uc_permissions.sql"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(sql)

    print(f"  📝 Unity Catalog permissions → {out_path.name} ({len(tables)} tables, {len(schemas)} schemas)")
    return sql


# ─────────────────────────────────────────────
#  Cluster Configuration Recommender
# ─────────────────────────────────────────────

def recommend_cluster_config(inventory_path=None):
    """Recommend Databricks cluster configuration based on migration inventory."""
    inv_path = inventory_path or (OUTPUT_DIR / "inventory" / "inventory.json")
    if not inv_path.exists():
        return {"error": "inventory.json not found"}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    mappings = inventory.get("mappings", [])
    total = len(mappings)
    complexities = {}
    for m in mappings:
        c = m.get("complexity", "Simple")
        complexities[c] = complexities.get(c, 0) + 1

    has_agg = any("AGG" in m.get("transformations", []) for m in mappings)
    has_join = any("JNR" in m.get("transformations", []) for m in mappings)
    has_lookup = any("LKP" in m.get("transformations", []) for m in mappings)
    complex_count = complexities.get("Complex", 0) + complexities.get("Custom", 0)

    # Size recommendation
    if total <= 10 and complex_count == 0:
        driver = "Standard_DS3_v2"
        worker = "Standard_DS3_v2"
        min_workers = 1
        max_workers = 2
        profile = "small"
    elif total <= 50 or complex_count <= 5:
        driver = "Standard_DS4_v2"
        worker = "Standard_DS4_v2"
        min_workers = 2
        max_workers = 8
        profile = "medium"
    else:
        driver = "Standard_DS5_v2"
        worker = "Standard_DS5_v2"
        min_workers = 4
        max_workers = 16
        profile = "large"

    # Photon recommendation
    use_photon = has_agg or has_join or complex_count > 3

    config = {
        "profile": profile,
        "total_mappings": total,
        "complexity_breakdown": complexities,
        "recommendation": {
            "driver_node_type": driver,
            "worker_node_type": worker,
            "autoscale": {"min_workers": min_workers, "max_workers": max_workers},
            "enable_photon": use_photon,
            "spark_version": "14.3.x-scala2.12" if not use_photon else "14.3.x-photon-scala2.12",
            "runtime_engine": "PHOTON" if use_photon else "STANDARD",
        },
        "rationale": {
            "profile": f"{profile} profile selected for {total} mappings with {complex_count} complex/custom",
            "photon": f"Photon {'enabled' if use_photon else 'disabled'} — {'aggregations/joins detected' if use_photon else 'simple transformations only'}",
            "autoscale": f"{min_workers}-{max_workers} workers for {'ETL-heavy' if has_agg else 'standard'} workload",
        },
    }

    out_dir = OUTPUT_DIR / "inventory"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "cluster_config.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"  📊 Cluster config → {out_path.name} (profile: {profile}, photon: {use_photon})")
    return config


# ─────────────────────────────────────────────
#  Sprint 47 — Unity Catalog Lineage Metadata
# ─────────────────────────────────────────────

def generate_uc_lineage(inventory_path=None, catalog="main"):
    """Generate Unity Catalog lineage metadata JSON for migrated tables."""
    inv_path = inventory_path or (OUTPUT_DIR / "inventory" / "inventory.json")
    if not inv_path.exists():
        return {"error": "inventory.json not found"}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    lineage_entries = []
    for mapping in inventory.get("mappings", []):
        entry = {
            "mapping_name": mapping["name"],
            "source_tables": [],
            "target_tables": [],
            "transformations": mapping.get("transformations", []),
            "notebook": f"NB_{mapping['name']}",
        }
        for src in mapping.get("sources", []):
            parts = src.split(".")
            table_name = parts[-1].lower()
            schema_name = parts[-2].lower() if len(parts) >= 2 else "bronze"
            entry["source_tables"].append({
                "catalog": catalog,
                "schema": schema_name,
                "table": table_name,
                "full_name": f"{catalog}.{schema_name}.{table_name}",
            })
        for tgt in mapping.get("targets", []):
            table_name = tgt.split(".")[-1].lower()
            has_agg = "AGG" in mapping.get("transformations", [])
            tier = "gold" if has_agg else "silver"
            entry["target_tables"].append({
                "catalog": catalog,
                "schema": tier,
                "table": table_name,
                "full_name": f"{catalog}.{tier}.{table_name}",
            })
        lineage_entries.append(entry)

    lineage = {
        "catalog": catalog,
        "generated": datetime.now(timezone.utc).isoformat(),
        "total_mappings": len(lineage_entries),
        "lineage": lineage_entries,
    }

    out_dir = OUTPUT_DIR / "inventory"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "uc_lineage.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lineage, f, indent=2, ensure_ascii=False)

    print(f"  📊 UC lineage → {out_path.name} ({len(lineage_entries)} mappings)")
    return lineage


# ─────────────────────────────────────────────
#  Sprint 47 — DLT Notebook Generation
# ─────────────────────────────────────────────

def generate_dlt_notebook(mapping, catalog="main"):
    """Generate a Delta Live Tables notebook for a mapping."""
    name = mapping["name"]
    sources = mapping.get("sources", [])
    targets = mapping.get("targets", [])
    transforms = mapping.get("transformations", [])

    source_table = sources[0] if sources else "unknown_source"
    parts = source_table.split(".")
    table_name = parts[-1].lower()
    schema_name = parts[-2].lower() if len(parts) >= 2 else "bronze"

    target_table = targets[0].split(".")[-1].lower() if targets else table_name
    has_agg = "AGG" in transforms
    tier = "gold" if has_agg else "silver"

    lines = [
        f"# Databricks notebook source",
        f"# DLT Pipeline — {name}",
        f"# Generated from Informatica mapping: {name}",
        f"# Transforms: {' → '.join(transforms)}",
        f"",
        f"import dlt",
        f"from pyspark.sql.functions import *",
        f"",
        f"# --- Source Table ---",
        f'@dlt.table(',
        f'    name="raw_{table_name}",',
        f'    comment="Raw ingestion from {source_table}",',
        f'    table_properties={{"quality": "bronze"}}',
        f')',
        f'def raw_{table_name}():',
        f'    return spark.table("{catalog}.{schema_name}.{table_name}")',
        f'',
    ]

    # Add quality expectations
    lines.extend([
        f"# --- Quality-checked table ---",
        f'@dlt.table(',
        f'    name="clean_{table_name}",',
        f'    comment="Quality-checked {table_name}",',
        f'    table_properties={{"quality": "silver"}}',
        f')',
        f'@dlt.expect_or_drop("valid_record", "id IS NOT NULL")',
        f'def clean_{table_name}():',
        f'    return dlt.read("raw_{table_name}")',
        f'',
    ])

    # Add target/mart table
    lines.extend([
        f"# --- Target: {target_table} ---",
        f'@dlt.table(',
        f'    name="{target_table}",',
        f'    comment="Final mart table for {name}",',
        f'    table_properties={{"quality": "{tier}"}}',
        f')',
        f'def {target_table}():',
        f'    df = dlt.read("clean_{table_name}")',
    ])

    # Add transforms
    if "FIL" in transforms:
        lines.append(f'    # Filter transform')
        lines.append(f'    # df = df.filter(col("status") == "ACTIVE")')
    if "EXP" in transforms:
        lines.append(f'    # Expression transform')
        lines.append(f'    # df = df.withColumn("load_date", current_timestamp())')
    if "AGG" in transforms:
        lines.append(f'    # Aggregation (implement GROUP BY)')
        lines.append(f'    # df = df.groupBy("key").agg(count("*").alias("cnt"))')

    lines.append(f'    return df')

    return "\n".join(lines)


def generate_dlt_notebooks(inventory_path=None, catalog="main"):
    """Generate DLT notebooks for all mappings."""
    inv_path = inventory_path or (OUTPUT_DIR / "inventory" / "inventory.json")
    if not inv_path.exists():
        return []

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    out_dir = OUTPUT_DIR / "notebooks" / "dlt"
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = []
    for mapping in inventory.get("mappings", []):
        nb_content = generate_dlt_notebook(mapping, catalog)
        nb_path = out_dir / f"DLT_{mapping['name']}.py"
        nb_path.write_text(nb_content, encoding="utf-8")
        generated.append(nb_path.name)

    print(f"  📓 DLT notebooks → {len(generated)} files in {out_dir.name}/")
    return generated


# ─────────────────────────────────────────────
#  Sprint 47 — Cluster Policy Recommender
# ─────────────────────────────────────────────

def recommend_cluster_policies(inventory_path=None):
    """Recommend cluster policies based on workload patterns."""
    inv_path = inventory_path or (OUTPUT_DIR / "inventory" / "inventory.json")
    if not inv_path.exists():
        return {"error": "inventory.json not found"}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    mappings = inventory.get("mappings", [])
    policies = []

    # Classify workloads
    etl_heavy = [m for m in mappings if any(t in m.get("transformations", [])
                 for t in ["AGG", "JNR", "LKP", "UPD"])]
    ml_workloads = [m for m in mappings if any(t in m.get("transformations", [])
                    for t in ["JTX", "CT", "HTTP"])]
    sql_heavy = [m for m in mappings if m.get("complexity") in ("Simple", "Medium")
                 and not any(t in m.get("transformations", []) for t in ["JTX", "CT"])]

    if etl_heavy:
        policies.append({
            "name": "etl-migration-policy",
            "type": "job",
            "description": f"ETL workload policy for {len(etl_heavy)} mappings with joins/aggregations",
            "settings": {
                "node_type_id": "Standard_DS4_v2",
                "driver_node_type_id": "Standard_DS4_v2",
                "autoscale": {"min_workers": 2, "max_workers": 8},
                "runtime_engine": "PHOTON",
                "spark_version": "14.3.x-photon-scala2.12",
            },
            "applicable_mappings": [m["name"] for m in etl_heavy],
        })

    if ml_workloads:
        policies.append({
            "name": "ml-migration-policy",
            "type": "interactive",
            "description": f"ML/custom workload policy for {len(ml_workloads)} complex mappings",
            "settings": {
                "node_type_id": "Standard_NC6s_v3",
                "driver_node_type_id": "Standard_DS5_v2",
                "num_workers": 4,
                "runtime_engine": "STANDARD",
                "spark_version": "14.3.x-gpu-ml-scala2.12",
            },
            "applicable_mappings": [m["name"] for m in ml_workloads],
        })

    if sql_heavy:
        policies.append({
            "name": "sql-warehouse-policy",
            "type": "sql_warehouse",
            "description": f"SQL Warehouse for {len(sql_heavy)} simple/medium SQL-heavy mappings",
            "settings": {
                "warehouse_type": "PRO",
                "cluster_size": "Small" if len(sql_heavy) < 20 else "Medium",
                "auto_stop_mins": 15,
                "enable_photon": True,
                "channel": "CHANNEL_NAME_CURRENT",
            },
            "applicable_mappings": [m["name"] for m in sql_heavy],
        })

    result = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "total_mappings": len(mappings),
        "policies": policies,
    }

    out_dir = OUTPUT_DIR / "inventory"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "cluster_policies.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"  📊 Cluster policies → {len(policies)} policies recommended")
    return result


# ─────────────────────────────────────────────
#  Sprint 47 — SQL Dashboard Queries
# ─────────────────────────────────────────────

def generate_sql_dashboard_queries(inventory_path=None, catalog="main"):
    """Generate Databricks SQL dashboard queries from validation templates."""
    inv_path = inventory_path or (OUTPUT_DIR / "inventory" / "inventory.json")
    if not inv_path.exists():
        return []

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    queries = []
    for mapping in inventory.get("mappings", []):
        for tgt in mapping.get("targets", []):
            table_name = tgt.split(".")[-1].lower()
            has_agg = "AGG" in mapping.get("transformations", [])
            tier = "gold" if has_agg else "silver"
            full_name = f"{catalog}.{tier}.{table_name}"

            queries.append({
                "name": f"row_count_{table_name}",
                "description": f"Row count for {full_name}",
                "query": f"SELECT COUNT(*) AS row_count FROM {full_name}",
                "visualization": "counter",
            })
            queries.append({
                "name": f"null_check_{table_name}",
                "description": f"NULL percentage per column in {full_name}",
                "query": (
                    f"SELECT\n"
                    f"  COUNT(*) AS total_rows,\n"
                    f"  COUNT(*) - COUNT(id) AS null_id_count\n"
                    f"FROM {full_name}"
                ),
                "visualization": "table",
            })

    out_dir = OUTPUT_DIR / "databricks" / "dashboards"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "validation_queries.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"queries": queries, "generated": datetime.now(timezone.utc).isoformat()},
                  f, indent=2, ensure_ascii=False)

    print(f"  📊 SQL dashboard queries → {len(queries)} queries generated")
    return queries


# ─────────────────────────────────────────────
#  Sprint 47 — Advanced Workflow Features
# ─────────────────────────────────────────────

def generate_advanced_workflow(workflow, mappings_by_name=None, catalog="main"):
    """Generate advanced Databricks Workflow with job clusters, repair config, conditions."""
    mappings_by_name = mappings_by_name or {}
    name = workflow.get("name", "UNKNOWN")
    sessions = workflow.get("sessions", [])

    # Build job clusters
    job_clusters = [{
        "job_cluster_key": "migration_cluster",
        "new_cluster": {
            "spark_version": "14.3.x-photon-scala2.12",
            "node_type_id": "Standard_DS4_v2",
            "autoscale": {"min_workers": 2, "max_workers": 8},
            "runtime_engine": "PHOTON",
            "spark_conf": {
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.delta.autoCompact.enabled": "true",
            },
        },
    }]

    tasks = []
    prev_task = None

    for session in sessions:
        mapping_name = session.get("mapping", session.get("name", ""))
        task_key = f"task_{mapping_name}".replace("-", "_").replace(" ", "_")

        task = {
            "task_key": task_key,
            "job_cluster_key": "migration_cluster",
            "notebook_task": {
                "notebook_path": f"/Shared/migration/NB_{mapping_name}",
                "base_parameters": {
                    "load_date": "{{job.trigger_time.iso_date}}",
                    "catalog": catalog,
                },
            },
            "timeout_seconds": 3600,
            "max_retries": 1,
            "retry_on_timeout": False,
        }

        if prev_task:
            task["depends_on"] = [{"task_key": prev_task}]

        tasks.append(task)
        prev_task = task_key

    # Schedule
    schedule_cron = workflow.get("schedule_cron", {})
    cron_expr = schedule_cron.get("cron", "") if schedule_cron else ""

    job_def = {
        "name": f"WF_{name}",
        "job_clusters": job_clusters,
        "tasks": tasks,
        "max_concurrent_runs": 1,
        "run_as": {"user_name": "migration-service@company.com"},
        "queue": {"enabled": True},
        "health": {
            "rules": [{
                "metric": "RUN_DURATION_SECONDS",
                "op": "GREATER_THAN",
                "value": 7200,
            }],
        },
    }

    if cron_expr:
        job_def["schedule"] = {
            "quartz_cron_expression": cron_expr,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        }

    return job_def


# ─────────────────────────────────────────────
#  Sprint 48 — Deployment Cost Estimator
# ─────────────────────────────────────────────

def estimate_dbu_cost(inventory_path=None):
    """Estimate Databricks DBU cost for the migration workload."""
    inv_path = inventory_path or (OUTPUT_DIR / "inventory" / "inventory.json")
    if not inv_path.exists():
        return {"error": "inventory.json not found"}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    mappings = inventory.get("mappings", [])
    estimates = []
    total_dbu = 0.0

    for m in mappings:
        complexity = m.get("complexity", "Simple")
        transforms = m.get("transformations", [])

        # Base DBU per run
        if complexity == "Simple":
            base_dbu = 0.5
        elif complexity == "Medium":
            base_dbu = 1.5
        elif complexity == "Complex":
            base_dbu = 4.0
        else:
            base_dbu = 6.0

        # Adjustment factors
        if "AGG" in transforms or "JNR" in transforms:
            base_dbu *= 1.3
        if "LKP" in transforms:
            base_dbu *= 1.2
        if "UPD" in transforms:
            base_dbu *= 1.1

        estimates.append({
            "mapping": m["name"],
            "complexity": complexity,
            "estimated_dbu_per_run": round(base_dbu, 2),
            "estimated_monthly_dbu": round(base_dbu * 30, 2),
        })
        total_dbu += base_dbu

    result = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "total_mappings": len(mappings),
        "estimated_daily_dbu": round(total_dbu, 2),
        "estimated_monthly_dbu": round(total_dbu * 30, 2),
        "dbu_rate_usd": 0.55,
        "estimated_monthly_cost_usd": round(total_dbu * 30 * 0.55, 2),
        "per_mapping": estimates,
    }

    out_dir = OUTPUT_DIR / "inventory"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "cost_estimate.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"  💰 Cost estimate → ${result['estimated_monthly_cost_usd']}/month ({result['estimated_monthly_dbu']} DBU)")
    return result


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deploy migration artifacts to Azure Databricks")
    parser.add_argument("--workspace-url", required=True, help="Databricks workspace URL (e.g. https://adb-xxx.azuredatabricks.net)")
    parser.add_argument("--token", default=None, help="Databricks personal access token (or set DATABRICKS_TOKEN env var)")
    parser.add_argument("--notebook-path", default="/Shared/migration", help="Workspace path for notebooks (default: /Shared/migration)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making API calls")
    parser.add_argument("--only", choices=["notebooks", "workflows", "sql", "all"], default="all",
                        help="Deploy only specific artifact type (default: all)")
    parser.add_argument("--setup-secrets", action="store_true", help="Create secret scope from migration.yaml")
    parser.add_argument("--generate-permissions", action="store_true", help="Generate Unity Catalog GRANT statements")
    parser.add_argument("--recommend-cluster", action="store_true", help="Generate cluster configuration recommendation")
    args = parser.parse_args()

    if _requests is None:
        print("ERROR: requests not installed. Run: pip install requests")
        sys.exit(1)

    # Resolve token
    import os
    token = args.token or os.environ.get("DATABRICKS_TOKEN", "")
    if not token and not args.dry_run:
        print("ERROR: No token provided. Use --token or set DATABRICKS_TOKEN env var.")
        sys.exit(1)

    print()
    print("╔" + "═" * 58 + "╗")
    print("║" + "  Databricks Deployment".center(58) + "║")
    print("╚" + "═" * 58 + "╝")
    print(f"  Workspace: {args.workspace_url}")
    print(f"  Notebook path: {args.notebook_path}")
    print(f"  Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print(f"  Scope: {args.only}")

    all_results = []

    # Optional: generate permissions
    if args.generate_permissions:
        generate_uc_permissions()

    # Optional: recommend cluster config
    if args.recommend_cluster:
        recommend_cluster_config()

    # Optional: setup secrets
    if args.setup_secrets:
        try:
            import yaml
            config_path = WORKSPACE / "migration.yaml"
            with open(config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
            db_config = config.get("databricks", {})
            scope_name = db_config.get("secret_scope", "migration-scope")
            # Collect secrets from source config
            secrets = {}
            source = config.get("source", {})
            if source.get("jdbc_url"):
                secrets["source-jdbc-url"] = source["jdbc_url"]
            if source.get("username"):
                secrets["source-username"] = source["username"]
            all_results.extend(setup_secret_scope(args.workspace_url, token, scope_name, secrets, args.dry_run))
        except ImportError:
            print("  ⚠️  PyYAML not installed — skipping secret setup")
        except FileNotFoundError:
            print("  ⚠️  migration.yaml not found — skipping secret setup")

    # Deploy artifacts
    if args.only in ("all", "notebooks"):
        all_results.extend(deploy_notebooks(args.workspace_url, token, args.notebook_path, args.dry_run))

    if args.only in ("all", "workflows"):
        all_results.extend(deploy_workflows(args.workspace_url, token, args.dry_run))

    if args.only in ("all", "sql"):
        all_results.extend(deploy_sql_scripts(args.workspace_url, token, f"{args.notebook_path}/sql", args.dry_run))

    # Write deployment log
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "workspace_url": args.workspace_url,
        "notebook_path": args.notebook_path,
        "dry_run": args.dry_run,
        "results": all_results,
    }
    log_path = OUTPUT_DIR / "databricks_deployment_log.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

    # Summary
    created = sum(1 for r in all_results if r.get("status") == "created")
    errors = sum(1 for r in all_results if r.get("status") == "error")
    dry = sum(1 for r in all_results if r.get("status") == "dry-run")

    print()
    print("╔" + "═" * 58 + "╗")
    if args.dry_run:
        print("║" + f"  Dry-run complete: {dry} artifacts would be deployed".center(58) + "║")
    else:
        print("║" + f"  Created: {created}  Errors: {errors}".center(58) + "║")
    print("║" + f"  Log: {log_path.name}".center(58) + "║")
    print("╚" + "═" * 58 + "╝")


if __name__ == "__main__":
    main()
