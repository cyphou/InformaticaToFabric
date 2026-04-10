"""
Migration Orchestrator — End-to-End
Runs all migration phases in sequence:
  Phase 0: Assessment  (run_assessment.py)
  Phase 1: SQL         (run_sql_migration.py)
  Phase 2: Notebooks   (run_notebook_migration.py)
  Phase 3: Pipelines   (run_pipeline_migration.py)
  Phase 4: Validation  (run_validation.py)

Outputs:
  output/migration_summary.md — overall migration summary
  output/.checkpoint.json     — incremental migration state

Usage:
    python run_migration.py                    # Run all phases
    python run_migration.py --skip 0           # Skip assessment
    python run_migration.py --only 1 2         # Only SQL + Notebooks
    python run_migration.py --verbose          # Debug-level logging
    python run_migration.py --dry-run          # Preview without executing
    python run_migration.py --config my.yaml   # Custom config file
    python run_migration.py --resume           # Resume from last checkpoint
    python run_migration.py --reset            # Clear checkpoint and start fresh
"""

import argparse
import concurrent.futures
import importlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent

PHASES = [
    {"id": 0, "name": "Assessment",  "module": "run_assessment"},
    {"id": 1, "name": "SQL Migration", "module": "run_sql_migration"},
    {"id": 2, "name": "Notebook Migration", "module": "run_notebook_migration"},
    {"id": 3, "name": "DBT Migration", "module": "run_dbt_migration"},
    {"id": 4, "name": "Pipeline Migration", "module": "run_pipeline_migration"},
    {"id": 5, "name": "AutoSys Migration", "module": "run_autosys_migration"},
    {"id": 6, "name": "Schema Generation", "module": "run_schema_generator"},
    {"id": 7, "name": "Functions Migration", "module": "run_functions_migration"},
    {"id": 8, "name": "Validation", "module": "run_validation"},
]

# Credential patterns to sanitize in audit logs
_CREDENTIAL_PATTERNS = [
    (re.compile(r'(password\s*[=:]\s*)\S+', re.IGNORECASE), r'\1***REDACTED***'),
    (re.compile(r'(secret\s*[=:]\s*)\S+', re.IGNORECASE), r'\1***REDACTED***'),
    (re.compile(r'(token\s*[=:]\s*)\S+', re.IGNORECASE), r'\1***REDACTED***'),
    (re.compile(r'(jdbc:[^\s]+@)([^\s/]+)', re.IGNORECASE), r'\1***REDACTED***'),
    (re.compile(r'(AccountKey\s*=\s*)\S+', re.IGNORECASE), r'\1***REDACTED***'),
]


def sanitize_output(text):
    """Remove credentials and secrets from text using _CREDENTIAL_PATTERNS."""
    result = str(text)
    for pattern, replacement in _CREDENTIAL_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


def _write_audit_log(results, config):
    """Write a structured JSON audit log to output/audit_log.json."""
    audit = {
        "migration_run": datetime.now(timezone.utc).isoformat(),
        "config_file": config.get("_config_file", "migration.yaml"),
        "phases": [],
    }
    for r in results:
        entry = {
            "phase_id": r["id"],
            "phase_name": r["name"],
            "status": r["status"],
            "duration_seconds": round(r["duration"], 2) if r["duration"] else 0,
            "error": sanitize_output(r["error"]) if r.get("error") else None,
        }
        audit["phases"].append(entry)
    ok = sum(1 for r in results if r["status"] == "ok")
    audit["summary"] = {
        "total_phases": len(results),
        "succeeded": ok,
        "failed": sum(1 for r in results if r["status"] == "error"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
        "total_duration_seconds": round(sum(r["duration"] for r in results if r["duration"]), 2),
    }
    audit_path = WORKSPACE / "output" / "audit_log.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2, ensure_ascii=False)
    return audit_path


def _parse_args():
    """Parse CLI arguments with argparse."""
    parser = argparse.ArgumentParser(
        description="Informatica → Fabric Migration Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--skip", type=int, nargs="+", default=[], metavar="N",
                        help="Phase IDs to skip (e.g., --skip 0 2)")
    parser.add_argument("--only", type=int, nargs="+", default=None, metavar="N",
                        help="Run only these phases (e.g., --only 1 2)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable debug-level logging")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview phases without executing them")
    parser.add_argument("--config", type=str, default=None,
                        help="Path to migration.yaml config file")
    parser.add_argument("--log-format", choices=["text", "json"], default=None,
                        help="Log format: text (default) or json")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint (skip completed phases)")
    parser.add_argument("--reset", action="store_true",
                        help="Clear checkpoint and start fresh")
    # Sprint 35: Multi-tenant & enterprise flags
    parser.add_argument("--batch", type=str, nargs="+", default=None, metavar="DIR",
                        help="Batch mode: run migration for multiple input directories")
    parser.add_argument("--manifest", action="store_true",
                        help="Generate deployment manifest after migration")
    parser.add_argument("--tenant", type=str, default=None, metavar="ID",
                        help="Tenant ID for Key Vault secret substitution")
    parser.add_argument("--parallel-waves", type=int, default=None, metavar="N",
                        help="Max parallel wave executions (default: sequential)")
    # Sprint 37: Performance flags
    parser.add_argument("--profile", action="store_true",
                        help="Enable per-phase memory and timing profiling")
    # Target platform
    parser.add_argument("--target", choices=["fabric", "databricks", "dbt", "pyspark", "auto", "all"], default=None,
                        help="Target platform: fabric | databricks | dbt | pyspark | auto | all (default: fabric)")
    # Sprint 45: Cross-platform comparison
    parser.add_argument("--compare", action="store_true",
                        help="Generate cross-platform comparison report after migration")
    parser.add_argument("--advisor", action="store_true",
                        help="Generate migration target advisor report")
    # AutoSys JIL support
    parser.add_argument("--autosys-dir", type=str, default=None, metavar="DIR",
                        help="Path to directory containing AutoSys JIL files (default: input/autosys/)")
    # DD1-DD3: Datadog integration
    parser.add_argument("--datadog", action="store_true",
                        help="Enable Datadog integration (logs, metrics, tracing)")
    # DD4-DD6: Agentic alerting
    parser.add_argument("--agent", choices=["monitor", "auto_fix", "full_auto"], default=None,
                        help="Enable agentic alerting (monitor | auto_fix | full_auto)")
    # DD7-DD9: Monitoring platform
    parser.add_argument("--platform-status", action="store_true",
                        help="Generate platform status page after migration")
    parser.add_argument("--platform-report", action="store_true",
                        help="Generate platform JSON report after migration")
    # DD11: Migration review
    parser.add_argument("--review", action="store_true",
                        help="Run migration review (duplicates, anti-patterns, rework)")
    parsed = parser.parse_args()
    return parsed


def _load_config(config_path=None):
    """Load migration.yaml config. Returns dict (empty if not found)."""
    if config_path:
        p = Path(config_path)
    else:
        p = WORKSPACE / "migration.yaml"
    if not p.exists():
        return {}
    try:
        # Use PyYAML if available, otherwise basic key-value parsing
        import yaml
        with open(p, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg or {}
    except ImportError:
        # Lightweight fallback — just confirm file exists
        return {"_config_file": str(p), "_note": "Install PyYAML for full config support"}


def _setup_logging(verbose=False, log_format=None, config=None):
    """Configure structured logging."""
    cfg_logging = (config or {}).get("logging", {})
    level_str = "DEBUG" if verbose else cfg_logging.get("level", "INFO")
    level = getattr(logging, level_str.upper(), logging.INFO)
    fmt = log_format or cfg_logging.get("format", "text")
    log_file = cfg_logging.get("file", "")

    logger = logging.getLogger("migration")
    logger.setLevel(level)
    logger.handlers.clear()

    if fmt == "json":
        class JsonFormatter(logging.Formatter):
            def format(self, record):
                entry = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": record.levelname,
                    "msg": record.getMessage(),
                }
                if hasattr(record, "phase"):
                    entry["phase"] = record.phase
                if hasattr(record, "duration"):
                    entry["duration"] = record.duration
                return json.dumps(entry)
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler (if configured)
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # DD1: Datadog logging handler (if enabled)
    try:
        from datadog_integration import setup_datadog_logging
        dd_handler = setup_datadog_logging(logger, config or {})
        if dd_handler:
            logger.debug("DatadogHandler attached")
    except ImportError:
        pass

    return logger


CHECKPOINT_PATH = WORKSPACE / "output" / ".checkpoint.json"


def _load_checkpoint():
    """Load checkpoint state from disk. Returns dict with completed phase IDs."""
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {"completed_phases": [], "results": []}
    return {"completed_phases": [], "results": []}


def _save_checkpoint(checkpoint):
    """Persist checkpoint state to disk."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    checkpoint["updated"] = datetime.now(timezone.utc).isoformat()
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)


def _clear_checkpoint():
    """Remove checkpoint file."""
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()


def run_phase(phase):
    """Import and run a phase's main() function."""
    # Isolate sys.argv so child modules don't see orchestrator flags
    saved_argv = sys.argv
    sys.argv = [phase["module"] + ".py"]
    try:
        mod = importlib.import_module(phase["module"])
        importlib.reload(mod)  # Ensure fresh state if re-imported
        mod.main()
    finally:
        sys.argv = saved_argv


# ─────────────────────────────────────────────
#  Sprint 35: Enterprise helpers
# ─────────────────────────────────────────────

def substitute_keyvault_refs(config, tenant_id):
    """Replace {{KV:secret-name}} placeholders with platform-specific secret calls.

    For Fabric: notebookutils.credentials.getSecret(vault, secret)
    For Databricks: dbutils.secrets.get(scope=scope, key=secret)
    """
    kv_pattern = re.compile(r"\{\{KV:([^}]+)\}\}")
    target = config.get("target", os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric"))

    def _replace(value):
        if isinstance(value, str):
            if target == "databricks":
                scope = config.get("databricks", {}).get("secret_scope", "migration-secrets")
                return kv_pattern.sub(
                    lambda m: f'dbutils.secrets.get(scope="{scope}", key="{m.group(1)}")',
                    value,
                )
            return kv_pattern.sub(
                lambda m: f'notebookutils.credentials.getSecret("{tenant_id}", "{m.group(1)}")',
                value,
            )
        if isinstance(value, dict):
            return {k: _replace(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_replace(v) for v in value]
        return value

    return _replace(config)


def generate_manifest(results, config):
    """Generate output/manifest.json — deployment manifest for Fabric CI/CD."""
    manifest = {
        "schema_version": "1.0",
        "generated": datetime.now(timezone.utc).isoformat(),
        "tenant_id": config.get("tenant_id"),
        "workspace_id": config.get("fabric", {}).get("workspace_id"),
        "artifacts": [],
        "deployment_order": [],
    }

    output_root = WORKSPACE / "output"
    artifact_dirs = {
        "notebook": output_root / "notebooks",
        "pipeline": output_root / "pipelines",
        "sql": output_root / "sql",
        "schema": output_root / "schema",
        "validation": output_root / "validation",
    }

    order_idx = 1
    for artifact_type, directory in artifact_dirs.items():
        if directory.exists():
            for f in sorted(directory.iterdir()):
                if f.is_file():
                    entry = {
                        "name": f.stem,
                        "type": artifact_type,
                        "path": str(f.relative_to(WORKSPACE)),
                        "size_bytes": f.stat().st_size,
                        "deploy_order": order_idx,
                    }
                    manifest["artifacts"].append(entry)
                    manifest["deployment_order"].append(f.stem)
                    order_idx += 1

    manifest["total_artifacts"] = len(manifest["artifacts"])

    manifest_path = output_root / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    return manifest_path


def run_batch(batch_dirs, args, config):
    """Run migration for multiple input directories (multi-tenant batch)."""
    batch_results = []
    for input_dir in batch_dirs:
        input_path = Path(input_dir)
        if not input_path.exists():
            batch_results.append({"input": input_dir, "status": "error", "error": "Directory not found"})
            continue
        # Override INPUT_DIR for child modules
        config["_batch_input_dir"] = str(input_path)
        os.environ["INFORMATICA_INPUT_DIR"] = str(input_path)
        # Redirect child module input directories
        for mod_name in ["run_assessment", "run_sql_migration", "run_notebook_migration",
                         "run_pipeline_migration", "run_validation", "run_schema_generator"]:
            mod = sys.modules.get(mod_name)
            if mod and hasattr(mod, "INPUT_DIR"):
                mod.INPUT_DIR = input_path
        batch_results.append({"input": input_dir, "status": "ok"})
    return batch_results


def run_parallel_waves(phases, max_workers, args, config, log):
    """Execute independent migration phases in parallel using ThreadPoolExecutor."""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_phase = {}
        for phase in phases:
            future = executor.submit(run_phase, phase)
            future_to_phase[future] = phase

        for future in concurrent.futures.as_completed(future_to_phase):
            phase = future_to_phase[future]
            try:
                future.result()
                results.append({
                    "id": phase["id"], "name": phase["name"],
                    "status": "ok", "duration": 0, "error": None,
                })
                log.info(f"Parallel phase {phase['id']} completed: {phase['name']}")
            except Exception as exc:
                results.append({
                    "id": phase["id"], "name": phase["name"],
                    "status": "error", "duration": 0, "error": str(exc)[:100],
                })
                log.error(f"Parallel phase {phase['id']} failed: {exc}")
    return results


# ─────────────────────────────────────────────
#  Sprint 37: Profiling helpers
# ─────────────────────────────────────────────

def _get_memory_mb():
    """Return current process memory usage in MB (best-effort)."""
    try:
        import resource  # Unix only
        import platform as _platform
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports ru_maxrss in bytes; Linux reports in KB
        if _platform.system() == "Darwin":
            return round(usage / (1024 * 1024), 1)
        return round(usage / 1024, 1)  # KB → MB on Linux
    except ImportError:
        pass
    try:
        import psutil
        proc = psutil.Process()
        return round(proc.memory_info().rss / (1024 * 1024), 1)
    except ImportError:
        return 0.0


def generate_summary(results, target="fabric"):
    """Generate output/migration_summary.md."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    target_label = "Databricks (Unity Catalog)" if target == "databricks" else "Fabric"
    lines = [
        f"# Migration Summary — Target: {target_label}",
        "",
        f"**Generated:** {ts}",
        f"**Target Platform:** {target_label}",
        "",
        "## Phase Results",
        "",
        "| Phase | Name | Status | Duration |",
        "|-------|------|--------|----------|",
    ]
    for r in results:
        status = "✅ OK" if r["status"] == "ok" else ("⏭️ Skipped" if r["status"] == "skipped" else f"❌ {r['error']}")
        duration = f"{r['duration']:.1f}s" if r["duration"] else "—"
        lines.append(f"| {r['id']} | {r['name']} | {status} | {duration} |")

    ok_count = sum(1 for r in results if r["status"] == "ok")
    total_time = sum(r["duration"] for r in results if r["duration"])
    lines.extend([
        "",
        f"**Phases completed:** {ok_count}/{len(results)}",
        f"**Total duration:** {total_time:.1f}s",
        "",
        "## Output Directories",
        "",
        "| Directory | Contents |",
        "|-----------|----------|",
        "| `output/inventory/` | Assessment inventory, complexity report, DAG, HTML report |",
        "| `output/sql/` | Converted SQL files (Oracle/SQL Server → Spark SQL) |",
        "| `output/notebooks/` | PySpark notebooks (one per mapping) |",
        "| `output/pipelines/` | Pipeline definitions (one per workflow) |",
        "| `output/schema/` | DDL + workspace setup notebook |",
        "| `output/validation/` | Validation notebooks + test matrix |",
        "| `output/audit_log.json` | Structured audit log (JSON) |",
        "",
        "## Next Steps",
        "",
        "1. Review generated artifacts in `output/`",
        "2. Fill in TODO placeholders in notebooks and SQL files",
        "3. Configure JDBC connections in validation notebooks",
    ])
    if target == "databricks":
        lines.extend([
            "4. Deploy notebooks to Databricks workspace",
            "5. Import workflows as Databricks Jobs",
            "6. Run validation notebooks against live data",
        ])
    else:
        lines.extend([
            "4. Deploy to Fabric via Git integration or REST API",
            "5. Run validation notebooks against live data",
        ])
    lines.append("")

    summary_path = WORKSPACE / "output" / "migration_summary.md"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return summary_path


def main():
    # Ensure UTF-8 output on Windows (box-drawing chars, emoji)
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")

    args = _parse_args()
    config = _load_config(args.config)
    log = _setup_logging(verbose=args.verbose, log_format=args.log_format, config=config)

    # DD1-DD3: Datadog integration
    dd_config = None
    dd_tracer_instance = None
    if args.datadog:
        config.setdefault("datadog", {})["enabled"] = True
    try:
        from datadog_integration import load_datadog_config, init_tracer as dd_init_tracer
        dd_config = load_datadog_config(config)
        if dd_config.get("enabled"):
            dd_tracer_instance = dd_init_tracer(dd_config)
    except ImportError:
        dd_config = None

    # DD4-DD6: Agentic alerting
    migration_agent = None
    if args.agent:
        config.setdefault("agent", {})["enabled"] = True
        config["agent"]["mode"] = args.agent
    try:
        from agentic_alerting import MigrationAgent
        agent_cfg = (config or {}).get("agent", {})
        if agent_cfg.get("enabled"):
            migration_agent = MigrationAgent(config)
            log.info(f"Migration agent enabled (mode: {agent_cfg.get('mode', 'monitor')})")
    except ImportError:
        migration_agent = None

    # DD7-DD9: Monitoring platform
    platform_state = None
    alerting_orch = None
    slo_tracker = None
    try:
        from monitoring_platform import PlatformState, AlertingOrchestrator, SLOTracker
        if args.platform_status or args.platform_report or (config or {}).get("alerting_platform"):
            platform_state = PlatformState()
            alerting_orch = AlertingOrchestrator(config)
            slo_tracker = SLOTracker(config)
            platform_run = platform_state.start_run(target=config.get("target", "fabric"))
    except ImportError:
        pass

    # Resolve target platform: CLI flag > config file > default
    target = args.target or config.get("target", "fabric")
    # Sprint 45: --target all runs dual-target (databricks + dbt)
    if target == "all":
        os.environ["INFORMATICA_MIGRATION_TARGET"] = "databricks"
        os.environ["INFORMATICA_DBT_MODE"] = ""
        config["target"] = "databricks"
        config["dual_target"] = True
        catalog = config.get("databricks", {}).get("catalog", "main")
        os.environ["INFORMATICA_DATABRICKS_CATALOG"] = catalog
    # Normalize: dbt/pyspark/auto are sub-modes of databricks
    elif target in ("dbt", "pyspark", "auto"):
        os.environ["INFORMATICA_MIGRATION_TARGET"] = "databricks"
        os.environ["INFORMATICA_DBT_MODE"] = target  # dbt | pyspark | auto
        config["target"] = "databricks"
        config["dbt_mode"] = target
    else:
        os.environ["INFORMATICA_MIGRATION_TARGET"] = target
        os.environ["INFORMATICA_DBT_MODE"] = ""
        config["target"] = target
    if target in ("databricks", "dbt", "pyspark", "auto", "all"):
        catalog = config.get("databricks", {}).get("catalog", "main")
        os.environ["INFORMATICA_DATABRICKS_CATALOG"] = catalog

    # Handle --reset
    if args.reset:
        _clear_checkpoint()
        print("  Checkpoint cleared.")

    # Handle --resume
    checkpoint = _load_checkpoint() if args.resume else {"completed_phases": [], "results": []}
    completed_phases = set(checkpoint.get("completed_phases", []))

    skip = set(args.skip)
    only = set(args.only) if args.only is not None else None

    print()
    print("╔" + "═" * 58 + "╗")
    dbt_mode = config.get("dbt_mode", "")
    if target == "all":
        target_label = "Databricks + DBT (Dual-Target)"
    elif dbt_mode:
        target_label = f"Databricks — {dbt_mode.upper()} mode"
    elif target in ("databricks", "dbt", "pyspark", "auto"):
        target_label = "Databricks (Unity Catalog)"
    else:
        target_label = "Fabric"
    print("║" + f"  Informatica → {target_label} Migration  ".center(58) + "║")
    print("║" + "  End-to-End Orchestrator          ".center(58) + "║")
    print("╚" + "═" * 58 + "╝")
    if args.dry_run:
        print("  *** DRY-RUN MODE — no phases will execute ***")
    if args.verbose:
        print("  *** VERBOSE logging enabled ***")
    if args.resume and completed_phases:
        print(f"  *** RESUMING — skipping phases: {sorted(completed_phases)} ***")
    if args.profile:
        print("  *** PROFILING — per-phase timing & memory enabled ***")
    if args.tenant:
        print(f"  *** TENANT: {args.tenant} (Key Vault substitution enabled) ***")
    if args.batch:
        print(f"  *** BATCH MODE — {len(args.batch)} input directories ***")
    if args.autosys_dir:
        print(f"  *** AutoSys JIL directory: {args.autosys_dir} ***")
        os.environ["INFORMATICA_AUTOSYS_DIR"] = args.autosys_dir
    print(f"  Target platform: {target_label}")
    if config and config.get("fabric", {}).get("workspace_id") and target == "fabric":
        print(f"  Config workspace: {config['fabric']['workspace_id']}")
    if target == "databricks":
        cat = config.get("databricks", {}).get("catalog", "main")
        print(f"  Unity Catalog: {cat}")
    print()

    # Sprint 35: Key Vault template substitution
    if args.tenant:
        config = substitute_keyvault_refs(config, args.tenant)
        config["tenant_id"] = args.tenant
        log.info(f"Key Vault substitution applied for tenant: {args.tenant}")

    # Sprint 35: Batch mode
    if args.batch:
        batch_results = run_batch(args.batch, args, config)
        for br in batch_results:
            print(f"  Batch: {br['input']} — {br['status']}")
        log.info(f"Batch mode: {len(batch_results)} directories processed")

    log.info("Migration started")
    if config.get("_config_file"):
        log.debug(f"Config loaded from {config['_config_file']}")

    results = []

    for phase in PHASES:
        pid = phase["id"]
        pname = phase["name"]

        # Determine if this phase should run
        if only is not None and pid not in only:
            print(f"  ⏭️  Phase {pid}: {pname} — skipped (--only)")
            log.debug(f"Phase {pid} skipped (--only filter)")
            results.append({"id": pid, "name": pname, "status": "skipped", "duration": 0, "error": None})
            continue
        if pid in skip:
            print(f"  ⏭️  Phase {pid}: {pname} — skipped (--skip)")
            log.debug(f"Phase {pid} skipped (--skip)")
            results.append({"id": pid, "name": pname, "status": "skipped", "duration": 0, "error": None})
            continue

        if pid in completed_phases:
            print(f"  ⏭️  Phase {pid}: {pname} — already completed (checkpoint)")
            log.debug(f"Phase {pid} skipped (checkpoint)")
            results.append({"id": pid, "name": pname, "status": "skipped", "duration": 0, "error": None})
            continue

        if args.dry_run:
            print(f"  📋 Phase {pid}: {pname} — would execute ({phase['module']})")
            log.info(f"Phase {pid} dry-run: {pname}")
            results.append({"id": pid, "name": pname, "status": "dry-run", "duration": 0, "error": None})
            continue

        print(f"  ▶  Phase {pid}: {pname}")
        print("  " + "─" * 56)

        log.info(f"Phase {pid} starting: {pname}")
        mem_before = _get_memory_mb() if args.profile else 0
        if migration_agent:
            migration_agent.on_phase_start(pname, phase_id=pid)
        t0 = time.time()
        # DD3: Trace span around phase execution
        span = None
        try:
            from datadog_integration import trace_phase as dd_trace_phase, tag_span_error
            if dd_config and dd_config.get("enabled"):
                span = dd_trace_phase(pname, phase_id=pid)
                if hasattr(span, '__enter__'):
                    span.__enter__()
        except ImportError:
            pass
        try:
            run_phase(phase)
            elapsed = time.time() - t0
            mem_after = _get_memory_mb() if args.profile else 0
            phase_result = {"id": pid, "name": pname, "status": "ok", "duration": elapsed, "error": None}
            if args.profile:
                phase_result["memory_before_mb"] = mem_before
                phase_result["memory_after_mb"] = mem_after
                phase_result["memory_delta_mb"] = round(mem_after - mem_before, 1)
            results.append(phase_result)
            log.info(f"Phase {pid} completed in {elapsed:.1f}s")
            profile_msg = f" | mem: {mem_before}→{mem_after}MB" if args.profile else ""
            print(f"  ✅ Phase {pid} completed in {elapsed:.1f}s{profile_msg}")
            # DD2: Emit phase metrics
            if dd_config and dd_config.get("enabled"):
                try:
                    from datadog_integration import emit_phase_metrics
                    emit_phase_metrics(pid, pname, "ok", elapsed, config=dd_config)
                except ImportError:
                    pass
            # DD4: Agent notification
            if migration_agent:
                migration_agent.on_phase_complete(pname, elapsed, phase_id=pid)
            # DD7: Platform state
            if platform_state and platform_run:
                platform_run.record_phase(pname, "ok", duration=elapsed)
            # Save checkpoint
            completed_phases.add(pid)
            checkpoint["completed_phases"] = sorted(completed_phases)
            checkpoint["results"] = results[:]
            _save_checkpoint(checkpoint)
        except SystemExit as se:
            elapsed = time.time() - t0
            if se.code == 0:
                results.append({"id": pid, "name": pname, "status": "ok", "duration": elapsed, "error": None})
                log.info(f"Phase {pid} completed in {elapsed:.1f}s")
                print(f"  ✅ Phase {pid} completed in {elapsed:.1f}s")
                # Save checkpoint
                completed_phases.add(pid)
                checkpoint["completed_phases"] = sorted(completed_phases)
                checkpoint["results"] = results[:]
                _save_checkpoint(checkpoint)
            else:
                results.append({"id": pid, "name": pname, "status": "error", "duration": elapsed, "error": f"exit code {se.code}"})
                log.error(f"Phase {pid} failed: exit code {se.code}")
                print(f"  ❌ Phase {pid} failed (exit code {se.code})")
                print("     Continuing to next phase...")
        except Exception as exc:
            elapsed = time.time() - t0
            error_msg = str(exc)[:100]
            results.append({"id": pid, "name": pname, "status": "error", "duration": elapsed, "error": error_msg})
            log.error(f"Phase {pid} failed: {error_msg}")
            print(f"  ❌ Phase {pid} failed: {error_msg}")
            print("     Continuing to next phase...")
            # DD3: Tag span with error
            if span and hasattr(span, 'set_tag'):
                try:
                    from datadog_integration import tag_span_error
                    tag_span_error(span, exc)
                except ImportError:
                    pass
            # DD2: Emit error metrics
            if dd_config and dd_config.get("enabled"):
                try:
                    from datadog_integration import emit_phase_metrics
                    emit_phase_metrics(pid, pname, "error", elapsed, config=dd_config)
                except ImportError:
                    pass
            # DD4: Agent error notification
            if migration_agent:
                decision = migration_agent.on_phase_error(pname, exc, phase_id=pid)
                if decision:
                    log.info(f"Agent decision for {pname}: {decision.get('action', 'n/a')}")
            # DD7: Platform state
            if platform_state and platform_run:
                platform_run.record_phase(pname, "error", duration=elapsed, errors=[error_msg])
            # DD8: Create alert
            if alerting_orch:
                alerting_orch.create_alert(
                    f"Phase {pid} ({pname}) failed: {error_msg[:50]}",
                    "high" if "OOM" not in error_msg else "critical",
                    details=error_msg)
        finally:
            # DD3: Finish span
            if span and hasattr(span, '__exit__'):
                span.__exit__(None, None, None)
        print()

    # Sprint 45: Dual-target — run DBT phase too if --target all
    if config.get("dual_target"):
        print("  ▶  Dual-target: re-running notebook/pipeline phases with DBT mode")
        os.environ["INFORMATICA_DBT_MODE"] = "dbt"
        for phase in PHASES:
            if phase["module"] in ("run_notebook_migration", "run_dbt_migration"):
                try:
                    run_phase(phase)
                    print(f"  ✅ Dual-target {phase['name']} (DBT) complete")
                except (SystemExit, Exception) as exc:
                    print(f"  ⚠️  Dual-target {phase['name']} (DBT): {exc}")
        os.environ["INFORMATICA_DBT_MODE"] = ""
        print()

    # Sprint 45: Comparison report & advisor
    if getattr(args, 'compare', False) or getattr(args, 'advisor', False) or config.get("dual_target"):
        try:
            import run_target_comparison as rtc
            inv_path = WORKSPACE / "output" / "inventory" / "inventory.json"
            if inv_path.exists():
                with open(inv_path, encoding="utf-8") as f:
                    inv = json.load(f)
                if getattr(args, 'advisor', False) or config.get("dual_target"):
                    adv = rtc.generate_advisor_report(inv)
                    print(f"  📊 Target advisor: {adv['summary']['dbt_recommended']} DBT / "
                          f"{adv['summary']['pyspark_recommended']} PySpark")
                if getattr(args, 'compare', False) or config.get("dual_target"):
                    comp = rtc.generate_comparison_report(inv)
                    print(f"  📊 Comparison report: {comp['report_path']}")
                manifest = rtc.generate_unified_manifest(inv)
                print(f"  📦 Unified manifest: {manifest['total_artifacts']} artifacts")
        except Exception as exc:
            log.warning(f"Sprint 45 reports: {exc}")

    # Generate audit log and summary
    audit_path = _write_audit_log(results, config)
    log.info(f"Audit log written to {audit_path}")

    # DD2: Emit final artifact metrics
    if dd_config and dd_config.get("enabled"):
        try:
            from datadog_integration import emit_artifact_metrics, send_datadog_event
            # Count output artifacts
            artifact_counts = {}
            for subdir in ("notebooks", "pipelines", "sql", "dbt", "schema", "validation"):
                d = WORKSPACE / "output" / subdir
                if d.is_dir():
                    artifact_counts[subdir] = len([f for f in d.iterdir() if f.is_file()])
            emit_artifact_metrics(artifact_counts, config=dd_config)
            # Send completion event
            ok_count_dd = sum(1 for r in results if r["status"] == "ok")
            total_dd = len([r for r in results if r["status"] not in ("skipped", "dry-run")])
            send_datadog_event(
                f"Migration Complete: {ok_count_dd}/{total_dd} phases",
                f"Target: {target}\nArtifacts: {sum(artifact_counts.values())}",
                alert_type="success" if ok_count_dd == total_dd else "warning",
                config=dd_config)
        except ImportError:
            pass

    # DD11: Migration review
    if getattr(args, 'review', False):
        try:
            from migration_review import run_migration_review
            review_results = run_migration_review(output_dir=str(WORKSPACE / "output"), config=config)
            queue_summary = review_results.get("queue", {})
            print(f"  📝 Review: {queue_summary.get('total', 0)} items "
                  f"({review_results.get('rework', {}).get('total', 0)} rework, "
                  f"{len(review_results.get('duplicates', []))} duplicates)")
        except Exception as exc:
            log.warning(f"Migration review: {exc}")

    # DD7-DD9: Platform reporting
    if platform_state:
        platform_state.end_run("completed" if all(r["status"] in ("ok", "skipped", "dry-run") for r in results) else "failed")
        if slo_tracker and platform_run:
            slo_check = slo_tracker.check(platform_run)
            if not slo_check.get("overall_met"):
                log.warning("SLO targets not met — see platform report")
        if getattr(args, 'platform_status', False):
            try:
                from monitoring_platform import generate_status_page
                status_path = generate_status_page(platform_state, alerting_orch, slo_tracker)
                print(f"  🌐 Platform status: {Path(status_path).name}")
            except Exception as exc:
                log.warning(f"Status page: {exc}")
        if getattr(args, 'platform_report', False):
            try:
                from monitoring_platform import generate_platform_report
                report = generate_platform_report(platform_state, alerting_orch, slo_tracker)
                print(f"  📊 Platform report: health={report.get('health', {}).get('overall', 0)}/100")
            except Exception as exc:
                log.warning(f"Platform report: {exc}")

    # DD4-DD6: Agent report
    if migration_agent:
        agent_report = migration_agent.get_report()
        if agent_report.get("decisions", 0) > 0:
            log.info(f"Agent: {agent_report['decisions']} decisions made")
        migration_agent.close()

    summary_path = generate_summary(results, target=target)

    # Sprint 35: Deployment manifest
    if args.manifest:
        manifest_path = generate_manifest(results, config)
        log.info(f"Manifest written to {manifest_path}")
        print(f"  📦 Deployment manifest: {manifest_path.name}")

    ok_count = sum(1 for r in results if r["status"] == "ok")
    total = len([r for r in results if r["status"] not in ("skipped", "dry-run")])

    log.info(f"Migration finished: {ok_count}/{total if total else len(results)} phases succeeded")

    print("╔" + "═" * 58 + "╗")
    if args.dry_run:
        dry_count = sum(1 for r in results if r["status"] == "dry-run")
        print("║" + f"  Dry-run: {dry_count} phases would execute".center(58) + "║")
    else:
        print("║" + f"  Migration complete: {ok_count}/{total} phases succeeded".center(58) + "║")
    print("║" + f"  Summary: {summary_path.name}".center(58) + "║")
    print("╚" + "═" * 58 + "╝")


if __name__ == "__main__":
    main()
