"""
Fabric Deployment — Upload Migration Artifacts
Deploys generated notebooks, pipelines, and SQL to a Microsoft Fabric
workspace using the Fabric REST API.

Prerequisites:
  pip install azure-identity requests

Authentication:
  Uses DefaultAzureCredential (Azure CLI, Managed Identity, or env vars).
  Log in with: az login

Configuration:
  Set values in migration.yaml (Sprint 11) or pass via CLI flags:
    --workspace-id   Fabric workspace GUID
    --dry-run        Preview actions without making API calls

Outputs:
  output/deployment_log.json — deployment results per artifact

Usage:
    python deploy_to_fabric.py --workspace-id <GUID>
    python deploy_to_fabric.py --workspace-id <GUID> --dry-run
    python deploy_to_fabric.py --workspace-id <GUID> --only notebooks
    python deploy_to_fabric.py --workspace-id <GUID> --only pipelines
"""

import argparse
import base64
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from azure.identity import DefaultAzureCredential
except ImportError:
    DefaultAzureCredential = None

try:
    import requests
except ImportError:
    requests = None

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output"

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def _get_token():
    """Acquire an access token using DefaultAzureCredential."""
    if DefaultAzureCredential is None:
        print("ERROR: azure-identity not installed. Run: pip install azure-identity")
        sys.exit(1)
    credential = DefaultAzureCredential()
    token_obj = credential.get_token(FABRIC_SCOPE)
    return token_obj


def _get_valid_token(token_obj):
    """Return a valid access token string, refreshing if expired."""
    if token_obj is None:
        return None
    # Azure AccessToken has an expires_on field (Unix timestamp)
    if hasattr(token_obj, "expires_on"):
        now = datetime.now(timezone.utc).timestamp()
        # Refresh if token expires within 5 minutes
        if token_obj.expires_on - now < 300:
            print("  🔄 Token expiring soon — refreshing...")
            credential = DefaultAzureCredential()
            token_obj = credential.get_token(FABRIC_SCOPE)
            print("  ✅ Token refreshed")
    return token_obj


def _headers(token_obj):
    """Build HTTP headers with a valid access token (refreshing if needed)."""
    if token_obj is None:
        return {"Content-Type": "application/json"}
    refreshed = _get_valid_token(token_obj)
    token_str = refreshed.token if hasattr(refreshed, "token") else str(refreshed)
    return {
        "Authorization": f"Bearer {token_str}",
        "Content-Type": "application/json",
    }


def _read_as_base64(file_path):
    """Read a file and return base64-encoded content."""
    content = file_path.read_bytes()
    return base64.b64encode(content).decode("utf-8")


# ─────────────────────────────────────────────
#  Notebook Deployment
# ─────────────────────────────────────────────

def deploy_notebooks(workspace_id, token, dry_run=False):
    """Deploy all NB_*.py notebooks to the Fabric workspace."""
    nb_dir = OUTPUT_DIR / "notebooks"
    nb_files = sorted(nb_dir.glob("NB_*.py"))
    results = []

    print(f"\n  📓 Deploying {len(nb_files)} notebooks...")

    for nb_path in nb_files:
        display_name = nb_path.stem  # NB_M_LOAD_CUSTOMERS
        payload = {
            "displayName": display_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": _read_as_base64(nb_path),
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }

        if dry_run:
            print(f"    [DRY-RUN] Would create notebook: {display_name}")
            results.append({"artifact": display_name, "type": "Notebook", "status": "dry-run"})
            continue

        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        resp = requests.post(url, headers=_headers(token), json=payload, timeout=60)

        if resp.status_code in (200, 201, 202):
            item_id = resp.json().get("id", "unknown")
            print(f"    ✅ {display_name} → {item_id}")
            results.append({"artifact": display_name, "type": "Notebook", "status": "created", "id": item_id})
        elif resp.status_code == 409:
            print(f"    ⚠️  {display_name} already exists — skipping")
            results.append({"artifact": display_name, "type": "Notebook", "status": "exists"})
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            print(f"    ⏳ Rate limited — waiting {retry_after}s...")
            time.sleep(retry_after)
            # Retry once
            resp = requests.post(url, headers=_headers(token), json=payload, timeout=60)
            if resp.status_code in (200, 201, 202):
                item_id = resp.json().get("id", "unknown")
                print(f"    ✅ {display_name} → {item_id} (after retry)")
                results.append({"artifact": display_name, "type": "Notebook", "status": "created", "id": item_id})
            else:
                print(f"    ❌ {display_name}: {resp.status_code} {resp.text[:100]}")
                results.append({"artifact": display_name, "type": "Notebook", "status": "error", "code": resp.status_code})
        else:
            print(f"    ❌ {display_name}: {resp.status_code} {resp.text[:100]}")
            results.append({"artifact": display_name, "type": "Notebook", "status": "error", "code": resp.status_code})

    return results


# ─────────────────────────────────────────────
#  Pipeline Deployment
# ─────────────────────────────────────────────

def deploy_pipelines(workspace_id, token, dry_run=False):
    """Deploy all PL_*.json pipelines to the Fabric workspace."""
    pl_dir = OUTPUT_DIR / "pipelines"
    pl_files = sorted(pl_dir.glob("PL_*.json"))
    results = []

    print(f"\n  ⚡ Deploying {len(pl_files)} pipelines...")

    for pl_path in pl_files:
        display_name = pl_path.stem  # PL_WF_DAILY_SALES_LOAD
        payload = {
            "displayName": display_name,
            "type": "DataPipeline",
            "definition": {
                "parts": [
                    {
                        "path": "pipeline-content.json",
                        "payload": _read_as_base64(pl_path),
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }

        if dry_run:
            print(f"    [DRY-RUN] Would create pipeline: {display_name}")
            results.append({"artifact": display_name, "type": "DataPipeline", "status": "dry-run"})
            continue

        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        resp = requests.post(url, headers=_headers(token), json=payload, timeout=60)

        if resp.status_code in (200, 201, 202):
            item_id = resp.json().get("id", "unknown")
            print(f"    ✅ {display_name} → {item_id}")
            results.append({"artifact": display_name, "type": "DataPipeline", "status": "created", "id": item_id})
        elif resp.status_code == 409:
            print(f"    ⚠️  {display_name} already exists — skipping")
            results.append({"artifact": display_name, "type": "DataPipeline", "status": "exists"})
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            print(f"    ⏳ Rate limited — waiting {retry_after}s...")
            time.sleep(retry_after)
            resp = requests.post(url, headers=_headers(token), json=payload, timeout=60)
            if resp.status_code in (200, 201, 202):
                item_id = resp.json().get("id", "unknown")
                print(f"    ✅ {display_name} → {item_id} (after retry)")
                results.append({"artifact": display_name, "type": "DataPipeline", "status": "created", "id": item_id})
            else:
                print(f"    ❌ {display_name}: {resp.status_code} {resp.text[:100]}")
                results.append({"artifact": display_name, "type": "DataPipeline", "status": "error", "code": resp.status_code})
        else:
            print(f"    ❌ {display_name}: {resp.status_code} {resp.text[:100]}")
            results.append({"artifact": display_name, "type": "DataPipeline", "status": "error", "code": resp.status_code})

    return results


# ─────────────────────────────────────────────
#  SQL Script Deployment (as Notebooks)
# ─────────────────────────────────────────────

def deploy_sql_scripts(workspace_id, token, dry_run=False):
    """Deploy SQL scripts as Fabric Notebooks with %%sql magic cells."""
    sql_dir = OUTPUT_DIR / "sql"
    sql_files = sorted(sql_dir.glob("SQL_*.sql"))
    results = []

    print(f"\n  🗄️  Deploying {len(sql_files)} SQL scripts as notebooks...")

    for sql_path in sql_files:
        display_name = f"SQL_{sql_path.stem}"

        # Wrap SQL in a notebook cell with %%sql magic
        sql_content = sql_path.read_text(encoding="utf-8")
        nb_content = f"# Fabric notebook source\n# Converted SQL: {sql_path.name}\n\n# COMMAND ----------\n\n%%sql\n{sql_content}\n"
        encoded = base64.b64encode(nb_content.encode("utf-8")).decode("utf-8")

        payload = {
            "displayName": display_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": encoded,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }

        if dry_run:
            print(f"    [DRY-RUN] Would create SQL notebook: {display_name}")
            results.append({"artifact": display_name, "type": "Notebook (SQL)", "status": "dry-run"})
            continue

        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
        resp = requests.post(url, headers=_headers(token), json=payload, timeout=60)

        if resp.status_code in (200, 201, 202):
            item_id = resp.json().get("id", "unknown")
            print(f"    ✅ {display_name} → {item_id}")
            results.append({"artifact": display_name, "type": "Notebook (SQL)", "status": "created", "id": item_id})
        elif resp.status_code == 409:
            print(f"    ⚠️  {display_name} already exists — skipping")
            results.append({"artifact": display_name, "type": "Notebook (SQL)", "status": "exists"})
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            print(f"    ⏳ Rate limited — waiting {retry_after}s...")
            time.sleep(retry_after)
            resp = requests.post(url, headers=_headers(token), json=payload, timeout=60)
            if resp.status_code in (200, 201, 202):
                item_id = resp.json().get("id", "unknown")
                print(f"    ✅ {display_name} → {item_id} (after retry)")
                results.append({"artifact": display_name, "type": "Notebook (SQL)", "status": "created", "id": item_id})
            else:
                print(f"    ❌ {display_name}: {resp.status_code} {resp.text[:100]}")
                results.append({"artifact": display_name, "type": "Notebook (SQL)", "status": "error", "code": resp.status_code})
        else:
            print(f"    ❌ {display_name}: {resp.status_code} {resp.text[:100]}")
            results.append({"artifact": display_name, "type": "Notebook (SQL)", "status": "error", "code": resp.status_code})

    return results


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deploy migration artifacts to Microsoft Fabric")
    parser.add_argument("--workspace-id", required=True, help="Fabric workspace GUID")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making API calls")
    parser.add_argument("--only", choices=["notebooks", "pipelines", "sql", "all"], default="all",
                        help="Deploy only specific artifact type (default: all)")
    args = parser.parse_args()

    if requests is None:
        print("ERROR: requests not installed. Run: pip install requests")
        sys.exit(1)

    print()
    print("╔" + "═" * 58 + "╗")
    print("║" + "  Fabric Deployment".center(58) + "║")
    print("╚" + "═" * 58 + "╝")
    print(f"  Workspace: {args.workspace_id}")
    print(f"  Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print(f"  Scope: {args.only}")

    token = None
    if not args.dry_run:
        print("\n  🔑 Authenticating...")
        token = _get_token()
        print("  ✅ Token acquired")

    all_results = []

    if args.only in ("all", "notebooks"):
        all_results.extend(deploy_notebooks(args.workspace_id, token, args.dry_run))

    if args.only in ("all", "pipelines"):
        all_results.extend(deploy_pipelines(args.workspace_id, token, args.dry_run))

    if args.only in ("all", "sql"):
        all_results.extend(deploy_sql_scripts(args.workspace_id, token, args.dry_run))

    # Write deployment log
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "workspace_id": args.workspace_id,
        "dry_run": args.dry_run,
        "results": all_results,
    }
    log_path = OUTPUT_DIR / "deployment_log.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

    # Summary
    created = sum(1 for r in all_results if r["status"] == "created")
    exists = sum(1 for r in all_results if r["status"] == "exists")
    errors = sum(1 for r in all_results if r["status"] == "error")
    dry = sum(1 for r in all_results if r["status"] == "dry-run")

    print()
    print("╔" + "═" * 58 + "╗")
    if args.dry_run:
        print("║" + f"  Dry-run complete: {dry} artifacts would be deployed".center(58) + "║")
    else:
        print("║" + f"  Created: {created}  Exists: {exists}  Errors: {errors}".center(58) + "║")
    print("║" + f"  Log: {log_path.name}".center(58) + "║")
    print("╚" + "═" * 58 + "╝")


if __name__ == "__main__":
    main()
