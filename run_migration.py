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

Usage:
    python run_migration.py              # Run all phases
    python run_migration.py --skip 0     # Skip assessment
    python run_migration.py --only 1 2   # Only SQL + Notebooks
"""

import importlib
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
    {"id": 4, "name": "Validation", "module": "run_validation"},
]


def _parse_args():
    """Parse --skip and --only flags."""
    skip = set()
    only = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--skip":
            i += 1
            while i < len(args) and not args[i].startswith("--"):
                skip.add(int(args[i]))
                i += 1
        elif args[i] == "--only":
            only = set()
            i += 1
            while i < len(args) and not args[i].startswith("--"):
                only.add(int(args[i]))
                i += 1
        else:
            i += 1
    return skip, only


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
        "| `output/validation/` | Validation notebooks + test matrix |",
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
    skip, only = _parse_args()

    print()
    print("╔" + "═" * 58 + "╗")
    print("║" + "  Informatica → Fabric Migration  ".center(58) + "║")
    print("║" + "  End-to-End Orchestrator          ".center(58) + "║")
    print("╚" + "═" * 58 + "╝")
    print()

    results = []

    for phase in PHASES:
        pid = phase["id"]
        pname = phase["name"]

        # Determine if this phase should run
        if only is not None and pid not in only:
            print(f"  ⏭️  Phase {pid}: {pname} — skipped (--only)")
            results.append({"id": pid, "name": pname, "status": "skipped", "duration": 0, "error": None})
            continue
        if pid in skip:
            print(f"  ⏭️  Phase {pid}: {pname} — skipped (--skip)")
            results.append({"id": pid, "name": pname, "status": "skipped", "duration": 0, "error": None})
            continue

        print(f"  ▶  Phase {pid}: {pname}")
        print("  " + "─" * 56)

        t0 = time.time()
        try:
            run_phase(phase)
            elapsed = time.time() - t0
            results.append({"id": pid, "name": pname, "status": "ok", "duration": elapsed, "error": None})
            print(f"  ✅ Phase {pid} completed in {elapsed:.1f}s")
        except SystemExit as se:
            elapsed = time.time() - t0
            if se.code == 0:
                results.append({"id": pid, "name": pname, "status": "ok", "duration": elapsed, "error": None})
                print(f"  ✅ Phase {pid} completed in {elapsed:.1f}s")
            else:
                results.append({"id": pid, "name": pname, "status": "error", "duration": elapsed, "error": f"exit code {se.code}"})
                print(f"  ❌ Phase {pid} failed (exit code {se.code})")
                print(f"     Continuing to next phase...")
        except Exception as exc:
            elapsed = time.time() - t0
            error_msg = str(exc)[:100]
            results.append({"id": pid, "name": pname, "status": "error", "duration": elapsed, "error": error_msg})
            print(f"  ❌ Phase {pid} failed: {error_msg}")
            print(f"     Continuing to next phase...")
        print()

    # Generate summary
    summary_path = generate_summary(results)

    ok_count = sum(1 for r in results if r["status"] == "ok")
    total = len([r for r in results if r["status"] != "skipped"])

    print("╔" + "═" * 58 + "╗")
    print("║" + f"  Migration complete: {ok_count}/{total} phases succeeded".center(58) + "║")
    print("║" + f"  Summary: {summary_path.name}".center(58) + "║")
    print("╚" + "═" * 58 + "╝")


if __name__ == "__main__":
    main()
