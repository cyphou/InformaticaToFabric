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
