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
import importlib
import json
import logging
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
    {"id": 3, "name": "Pipeline Migration", "module": "run_pipeline_migration"},
    {"id": 4, "name": "Schema Generation", "module": "run_schema_generator"},
    {"id": 5, "name": "Validation", "module": "run_validation"},
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


def generate_summary(results):
    """Generate output/migration_summary.md."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Migration Summary",
        "",
        f"**Generated:** {ts}",
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
        "| `output/pipelines/` | Fabric Pipeline JSON (one per workflow) |",
        "| `output/schema/` | Delta Lake DDL + workspace setup notebook |",
        "| `output/validation/` | Validation notebooks + test matrix |",
        "| `output/audit_log.json` | Structured audit log (JSON) |",
        "",
        "## Next Steps",
        "",
        "1. Review generated artifacts in `output/`",
        "2. Fill in TODO placeholders in notebooks and SQL files",
        "3. Configure JDBC connections in validation notebooks",
        "4. Deploy to Fabric via Git integration or REST API",
        "5. Run validation notebooks against live data",
        ""
    ])

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
    print("║" + "  Informatica → Fabric Migration  ".center(58) + "║")
    print("║" + "  End-to-End Orchestrator          ".center(58) + "║")
    print("╚" + "═" * 58 + "╝")
    if args.dry_run:
        print("  *** DRY-RUN MODE — no phases will execute ***")
    if args.verbose:
        print("  *** VERBOSE logging enabled ***")
    if args.resume and completed_phases:
        print(f"  *** RESUMING — skipping phases: {sorted(completed_phases)} ***")
    if config and config.get("fabric", {}).get("workspace_id"):
        print(f"  Config workspace: {config['fabric']['workspace_id']}")
    print()

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
        t0 = time.time()
        try:
            run_phase(phase)
            elapsed = time.time() - t0
            results.append({"id": pid, "name": pname, "status": "ok", "duration": elapsed, "error": None})
            log.info(f"Phase {pid} completed in {elapsed:.1f}s")
            print(f"  ✅ Phase {pid} completed in {elapsed:.1f}s")
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
        print()

    # Generate audit log and summary
    audit_path = _write_audit_log(results, config)
    log.info(f"Audit log written to {audit_path}")
    summary_path = generate_summary(results)

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
