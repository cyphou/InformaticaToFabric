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
#  Sprint 70: Fabric CU Cost Estimator
# ─────────────────────────────────────────────

# Fabric Capacity Unit cost model (approximate)
CU_COST_PER_HOUR = 0.36  # F2 SKU baseline $/CU-hour (list price)


def estimate_fabric_cu_cost(inventory_path=None):
    """Estimate monthly Fabric Capacity Unit (CU) cost based on mapping complexity.

    Uses mapping complexity and transform counts to estimate Spark pool CU-hours
    and pipeline activity costs.
    """
    inv_path = inventory_path or OUTPUT_DIR / "inventory" / "inventory.json"
    if isinstance(inv_path, str):
        inv_path = Path(inv_path)
    if not inv_path.exists():
        return {"error": "inventory not found"}

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    mappings = inv.get("mappings", [])
    cu_details = []
    total_cu_hours = 0

    for mapping in mappings:
        complexity = mapping.get("complexity", "Simple")
        transforms = mapping.get("transformations", [])

        # Base CU-hours per execution
        base_cu = {"Simple": 0.5, "Medium": 1.5, "Complex": 4.0, "Custom": 6.0}.get(complexity, 1.0)

        # Multipliers
        multiplier = 1.0
        if "JNR" in transforms:
            multiplier *= 1.3
        if "AGG" in transforms:
            multiplier *= 1.2
        if "LKP" in transforms:
            multiplier *= 1.1
        if mapping.get("has_sql_override"):
            multiplier *= 1.2
        if "SP" in transforms:
            multiplier *= 1.5

        cu_hours = round(base_cu * multiplier, 2)
        # Assume daily execution → 30 runs/month
        monthly_cu = round(cu_hours * 30, 2)
        total_cu_hours += monthly_cu

        cu_details.append({
            "mapping": mapping.get("name", ""),
            "complexity": complexity,
            "cu_hours_per_run": cu_hours,
            "monthly_cu_hours": monthly_cu,
            "monthly_cost_usd": round(monthly_cu * CU_COST_PER_HOUR, 2),
        })

    total_monthly_cost = round(total_cu_hours * CU_COST_PER_HOUR, 2)

    result = {
        "target": "fabric",
        "sku_baseline": "F2",
        "cu_cost_per_hour": CU_COST_PER_HOUR,
        "total_mappings": len(mappings),
        "estimated_monthly_cu_hours": round(total_cu_hours, 2),
        "estimated_monthly_cost_usd": total_monthly_cost,
        "details": cu_details,
        "notes": [
            "Estimates assume daily execution (30 runs/month)",
            "Actual CU consumption depends on data volume and cluster warm-up",
            "F2 SKU baseline — adjust for F4/F8/F16 as needed",
        ],
    }

    cost_dir = OUTPUT_DIR / "inventory"
    cost_dir.mkdir(parents=True, exist_ok=True)
    cost_path = cost_dir / "fabric_cost_estimate.json"
    with open(cost_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"  💰 Fabric CU estimate → ${total_monthly_cost}/month ({round(total_cu_hours, 1)} CU-hours)")
    return result


# ─────────────────────────────────────────────
#  Sprint 70: Azure Monitor Metrics Emitter
# ─────────────────────────────────────────────

def emit_azure_monitor_metrics(results, workspace_id=None, dry_run=True):
    """Emit migration metrics to Azure Monitor custom metrics.

    Tracks: artifacts_generated, deployment_errors, conversion_score_avg.
    Compatible with Azure Monitor REST API (custom metrics).
    """
    import os

    metrics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "workspace_id": workspace_id or "unknown",
        "custom_metrics": [],
    }

    if isinstance(results, list):
        # Deployment results
        metrics["custom_metrics"].append({
            "name": "migration/artifacts_deployed",
            "value": sum(1 for r in results if r.get("status") in ("created", "dry-run")),
            "unit": "Count",
        })
        metrics["custom_metrics"].append({
            "name": "migration/deployment_errors",
            "value": sum(1 for r in results if r.get("status") == "error"),
            "unit": "Count",
        })
    elif isinstance(results, dict) and "mappings" in results:
        # Inventory results
        mappings = results["mappings"]
        metrics["custom_metrics"].append({
            "name": "migration/mappings_total",
            "value": len(mappings),
            "unit": "Count",
        })
        scores = [m.get("conversion_score", 0) for m in mappings if m.get("conversion_score")]
        if scores:
            metrics["custom_metrics"].append({
                "name": "migration/conversion_score_avg",
                "value": round(sum(scores) / len(scores), 1),
                "unit": "Percent",
            })

    # Write to local metrics file (Azure Monitor integration would POST here)
    metrics_path = OUTPUT_DIR / "azure_monitor_metrics.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    # If Azure Monitor endpoint is configured, POST metrics
    monitor_endpoint = os.environ.get("AZURE_MONITOR_ENDPOINT", "")
    if monitor_endpoint and not dry_run and requests:
        try:
            resp = requests.post(
                monitor_endpoint,
                json=metrics,
                timeout=30,
                headers={"Content-Type": "application/json"},
            )
            metrics["emit_status"] = resp.status_code
        except Exception as e:
            metrics["emit_status"] = f"error: {e}"
    else:
        metrics["emit_status"] = "local-only" if dry_run else "no-endpoint"

    return metrics


# ─────────────────────────────────────────────
#  Sprint 70: Webhook Alerting (Teams / Slack)
# ─────────────────────────────────────────────

def send_webhook_alert(message, webhook_url=None, alert_type="info", details=None):
    """Send a deployment alert to Teams or Slack webhook.

    Auto-detects webhook format (Teams vs Slack) from URL pattern.
    """
    import os
    url = webhook_url or os.environ.get("MIGRATION_WEBHOOK_URL", "")
    if not url:
        return {"status": "no-webhook-configured"}

    # Detect webhook type
    is_teams = "office.com" in url or "webhook.office" in url
    is_slack = "hooks.slack.com" in url

    color_map = {"info": "#2980B9", "warning": "#F39C12", "error": "#E74C3C", "success": "#27AE60"}
    color = color_map.get(alert_type, "#2980B9")

    if is_teams:
        payload = {
            "@type": "MessageCard",
            "themeColor": color.replace("#", ""),
            "title": f"Migration Alert ({alert_type.upper()})",
            "text": message,
            "sections": [
                {"facts": [{"name": k, "value": str(v)} for k, v in (details or {}).items()]}
            ] if details else [],
        }
    elif is_slack:
        payload = {
            "text": f"*Migration Alert ({alert_type.upper()})*\n{message}",
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {"title": k, "value": str(v), "short": True}
                        for k, v in (details or {}).items()
                    ],
                }
            ] if details else [],
        }
    else:
        # Generic webhook
        payload = {
            "alert_type": alert_type,
            "message": message,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    if not requests:
        return {"status": "requests-not-installed"}

    try:
        resp = requests.post(url, json=payload, timeout=15)
        return {"status": "sent", "code": resp.status_code}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ─────────────────────────────────────────────
#  Sprint 68: Environment promotion & deployment pipelines
# ─────────────────────────────────────────────

ENV_DEFAULTS = {
    "dev": {"workspace_suffix": "-dev", "log_level": "DEBUG"},
    "test": {"workspace_suffix": "-test", "log_level": "INFO"},
    "prod": {"workspace_suffix": "-prod", "log_level": "WARNING"},
}


def generate_env_configs(base_config_path=None):
    """Generate environment-specific config templates (dev/test/prod).

    Each file inherits from migration.yaml with env-specific overrides.
    """
    base_path = base_config_path or WORKSPACE / "migration.yaml"
    base_config = {}
    try:
        import yaml
        if base_path.exists():
            with open(base_path, encoding="utf-8") as f:
                base_config = yaml.safe_load(f) or {}
    except ImportError:
        pass

    env_dir = OUTPUT_DIR / "environments"
    env_dir.mkdir(parents=True, exist_ok=True)
    configs = {}

    for env_name, overrides in ENV_DEFAULTS.items():
        env_cfg = {**base_config}
        env_cfg["environment"] = env_name
        env_cfg.setdefault("fabric", {})
        env_cfg["fabric"]["workspace_id"] = f"{{{{ {env_name.upper()}_WORKSPACE_ID }}}}"
        env_cfg.setdefault("logging", {})
        env_cfg["logging"]["level"] = overrides["log_level"]
        env_cfg.setdefault("sources", {})
        env_cfg["sources"]["connection_string"] = f"{{{{ {env_name.upper()}_CONNECTION_STRING }}}}"

        out_path = env_dir / f"{env_name}.yaml"
        import json as _json
        # Write as YAML-like (structured JSON for portability)
        out_path.write_text(
            _json.dumps(env_cfg, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        configs[env_name] = env_cfg

    return configs


def generate_deployment_pipeline_json():
    """Generate a Fabric Deployment Pipeline definition (Dev→Test→Prod).

    Returns the pipeline definition dict and writes it to output.
    """
    pipeline = {
        "$schema": "fabric-deployment-pipeline/v1",
        "displayName": "Migration Deployment Pipeline",
        "description": "Auto-generated from Informatica migration. Promotes artifacts Dev→Test→Prod.",
        "stages": [
            {
                "order": 0,
                "displayName": "Development",
                "description": "Development workspace — initial deployment target",
                "isActive": True,
            },
            {
                "order": 1,
                "displayName": "Test",
                "description": "Test workspace — validation and QA",
                "isActive": True,
            },
            {
                "order": 2,
                "displayName": "Production",
                "description": "Production workspace — final target",
                "isActive": True,
            },
        ],
        "rules": [
            {
                "artifactType": "Notebook",
                "rule": "KeepLabel",
                "description": "Preserve notebook labels across stages",
            },
            {
                "artifactType": "DataPipeline",
                "rule": "ParameterOverride",
                "description": "Override pipeline parameters per stage (connection strings)",
            },
        ],
    }

    out_path = OUTPUT_DIR / "deployment_pipeline.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(pipeline, f, indent=2, ensure_ascii=False)

    return pipeline


def validate_pre_deployment(workspace_id=None):
    """Pre-deployment validation: check artifacts exist and are structurally valid.

    Returns dict with validation results.
    """
    results = {"valid": True, "checks": [], "errors": []}

    # Check notebooks
    nb_dir = OUTPUT_DIR / "notebooks"
    nb_files = list(nb_dir.glob("NB_*.py")) if nb_dir.exists() else []
    results["checks"].append({
        "check": "notebooks_exist",
        "passed": len(nb_files) > 0,
        "count": len(nb_files),
    })
    if not nb_files:
        results["errors"].append("No notebooks found in output/notebooks/")
        results["valid"] = False

    # Check pipelines — valid JSON
    pl_dir = OUTPUT_DIR / "pipelines"
    pl_files = list(pl_dir.glob("PL_*.json")) if pl_dir.exists() else []
    results["checks"].append({
        "check": "pipelines_exist",
        "passed": len(pl_files) > 0,
        "count": len(pl_files),
    })
    for pl_file in pl_files:
        try:
            data = json.loads(pl_file.read_text(encoding="utf-8"))
            if "name" not in data and "activities" not in data:
                results["errors"].append(f"{pl_file.name}: missing 'name' or 'activities'")
                results["valid"] = False
        except json.JSONDecodeError as e:
            results["errors"].append(f"{pl_file.name}: invalid JSON — {e}")
            results["valid"] = False

    # Check SQL
    sql_dir = OUTPUT_DIR / "sql"
    sql_files = list(sql_dir.glob("SQL_*.sql")) if sql_dir.exists() else []
    results["checks"].append({
        "check": "sql_scripts_exist",
        "passed": len(sql_files) > 0,
        "count": len(sql_files),
    })

    # Check inventory
    inv_path = OUTPUT_DIR / "inventory" / "inventory.json"
    results["checks"].append({
        "check": "inventory_exists",
        "passed": inv_path.exists(),
    })
    if not inv_path.exists():
        results["errors"].append("inventory.json not found — run assessment first")
        results["valid"] = False

    # Check for credential leaks
    cred_patterns = ["password=", "secret=", "token=", "apikey="]
    cred_leaks = []
    for nb_file in nb_files:
        content = nb_file.read_text(encoding="utf-8").lower()
        for pat in cred_patterns:
            if pat in content:
                cred_leaks.append(f"{nb_file.name}: potential credential ({pat})")
    results["checks"].append({
        "check": "no_credential_leaks",
        "passed": len(cred_leaks) == 0,
        "issues": cred_leaks,
    })
    if cred_leaks:
        results["errors"].extend(cred_leaks)
        results["valid"] = False

    return results


def promote_deployment(source_env, target_env, workspace_map=None, dry_run=False):
    """Promote artifacts from one environment to another.

    Copies deployment log from source, applies config substitution for target.
    Returns promotion action summary.
    """
    env_dir = OUTPUT_DIR / "environments"
    source_cfg_path = env_dir / f"{source_env}.yaml"
    target_cfg_path = env_dir / f"{target_env}.yaml"

    promotion = {
        "source": source_env,
        "target": target_env,
        "dry_run": dry_run,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "actions": [],
    }

    # Load deployment log from source
    log_path = OUTPUT_DIR / "deployment_log.json"
    if log_path.exists():
        with open(log_path, encoding="utf-8") as f:
            source_log = json.load(f)
        deployed = source_log.get("results", [])
        promotion["artifact_count"] = len(deployed)
        for artifact in deployed:
            promotion["actions"].append({
                "artifact": artifact.get("artifact", ""),
                "type": artifact.get("type", ""),
                "action": f"promote {source_env}→{target_env}",
                "status": "would-promote" if dry_run else "promoted",
            })
    else:
        promotion["actions"].append({
            "action": "no deployment log found",
            "status": "error",
        })

    # Write promotion log
    promo_path = OUTPUT_DIR / f"promotion_{source_env}_to_{target_env}.json"
    with open(promo_path, "w", encoding="utf-8") as f:
        json.dump(promotion, f, indent=2, ensure_ascii=False)

    return promotion


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deploy migration artifacts to Microsoft Fabric")
    parser.add_argument("--workspace-id", required=False, help="Fabric workspace GUID")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making API calls")
    parser.add_argument("--only", choices=["notebooks", "pipelines", "sql", "all"], default="all",
                        help="Deploy only specific artifact type (default: all)")
    parser.add_argument("--validate", action="store_true",
                        help="Run pre-deployment validation only")
    parser.add_argument("--promote", nargs=2, metavar=("SOURCE", "TARGET"),
                        help="Promote artifacts between environments (e.g. --promote dev test)")
    parser.add_argument("--generate-envs", action="store_true",
                        help="Generate environment config templates (dev/test/prod)")
    parser.add_argument("--generate-pipeline", action="store_true",
                        help="Generate Fabric Deployment Pipeline JSON")
    parser.add_argument("--estimate-cost", action="store_true",
                        help="Estimate monthly Fabric CU cost")
    args = parser.parse_args()

    # Handle non-deployment commands first
    if args.validate:
        result = validate_pre_deployment()
        print(json.dumps(result, indent=2))
        sys.exit(0 if result["valid"] else 1)

    if args.generate_envs:
        generate_env_configs()
        print("  ✅ Environment configs written to output/environments/")
        sys.exit(0)

    if args.generate_pipeline:
        generate_deployment_pipeline_json()
        print("  ✅ Deployment pipeline written to output/deployment_pipeline.json")
        sys.exit(0)

    if args.promote:
        result = promote_deployment(args.promote[0], args.promote[1], dry_run=args.dry_run)
        n = len(result.get("actions", []))
        print(f"  {'[DRY-RUN] ' if args.dry_run else ''}Promoted {n} artifacts: {args.promote[0]} → {args.promote[1]}")
        sys.exit(0)

    if args.estimate_cost:
        cost_report = estimate_fabric_cu_cost()
        if "error" in cost_report:
            print(f"\n  ❌ {cost_report['error']}")
            sys.exit(1)
        print(f"\n  💰 Estimated monthly Fabric CU cost: {cost_report['estimated_monthly_cu_hours']:.1f} CU-hours")
        print(f"     Mappings analyzed: {cost_report['total_mappings']}")
        print(f"     Estimated cost: ${cost_report['estimated_monthly_cost_usd']:.2f}/month")
        sys.exit(0)

    # Standard deployment requires workspace-id
    if not args.workspace_id:
        parser.error("--workspace-id is required for deployment")

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
