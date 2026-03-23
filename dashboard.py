"""
Migration Dashboard — Interactive Status Overview
Generates a self-contained HTML dashboard aggregating all migration outputs:
  - Migration summary (phases, timing)
  - Inventory overview (mappings, complexity, connections)
  - Artifact status (notebooks, pipelines, SQL, validation)
  - Checkpoint / incremental progress
  - Test matrix summary

No external dependencies — pure Python stdlib.

Usage:
    python dashboard.py                # Generate output/dashboard.html
    python dashboard.py --open         # Generate and open in browser
    python dashboard.py --json         # Output JSON status instead of HTML
"""

import json
import sys
import webbrowser
from datetime import datetime, timezone
from html import escape
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output"


def _collect_status():
    """Collect migration status from all output artifacts."""
    status = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "phases": [],
        "inventory": None,
        "artifacts": {"notebooks": [], "pipelines": [], "sql": [], "validation": []},
        "checkpoint": None,
        "test_matrix": None,
        "deployment": None,
    }

    # Phase results from migration_summary.md
    summary_path = OUTPUT_DIR / "migration_summary.md"
    if summary_path.exists():
        content = summary_path.read_text(encoding="utf-8", errors="replace")
        for line in content.splitlines():
            if line.startswith("| ") and "|" in line[2:]:
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 5 and parts[1].isdigit():
                    status["phases"].append({
                        "id": int(parts[1]),
                        "name": parts[2],
                        "status": parts[3],
                        "duration": parts[4],
                    })

    # Inventory
    inv_path = OUTPUT_DIR / "inventory" / "inventory.json"
    if inv_path.exists():
        try:
            inv = json.loads(inv_path.read_text(encoding="utf-8"))
            mappings = inv.get("mappings", [])
            status["inventory"] = {
                "total_mappings": len(mappings),
                "complexity": {},
                "total_workflows": len(inv.get("workflows", [])),
                "total_connections": len(inv.get("connections", [])),
                "total_sql_files": len(inv.get("sql_files", [])),
            }
            for m in mappings:
                c = m.get("complexity", "Unknown")
                status["inventory"]["complexity"][c] = status["inventory"]["complexity"].get(c, 0) + 1
        except (json.JSONDecodeError, KeyError):
            pass

    # Artifacts
    nb_dir = OUTPUT_DIR / "notebooks"
    if nb_dir.exists():
        for f in sorted(nb_dir.glob("NB_*.py")):
            status["artifacts"]["notebooks"].append(f.name)

    pl_dir = OUTPUT_DIR / "pipelines"
    if pl_dir.exists():
        for f in sorted(pl_dir.glob("PL_*.json")):
            status["artifacts"]["pipelines"].append(f.name)

    sql_dir = OUTPUT_DIR / "sql"
    if sql_dir.exists():
        for f in sorted(sql_dir.glob("SQL_*.sql")):
            status["artifacts"]["sql"].append(f.name)

    val_dir = OUTPUT_DIR / "validation"
    if val_dir.exists():
        for f in sorted(val_dir.glob("VAL_*.py")):
            status["artifacts"]["validation"].append(f.name)

    # Checkpoint
    cp_path = OUTPUT_DIR / ".checkpoint.json"
    if cp_path.exists():
        try:
            status["checkpoint"] = json.loads(cp_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    # Test matrix
    tm_path = OUTPUT_DIR / "validation" / "test_matrix.md"
    if tm_path.exists():
        status["test_matrix"] = tm_path.read_text(encoding="utf-8", errors="replace")

    # Deployment log
    dl_path = OUTPUT_DIR / "deployment_log.json"
    if dl_path.exists():
        try:
            status["deployment"] = json.loads(dl_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    return status


def _generate_html(status):
    """Generate self-contained HTML dashboard."""
    inv = status.get("inventory") or {}
    artifacts = status.get("artifacts", {})
    phases = status.get("phases", [])
    checkpoint = status.get("checkpoint")
    deployment = status.get("deployment")

    # Counts
    nb_count = len(artifacts.get("notebooks", []))
    pl_count = len(artifacts.get("pipelines", []))
    sql_count = len(artifacts.get("sql", []))
    val_count = len(artifacts.get("validation", []))
    total_artifacts = nb_count + pl_count + sql_count + val_count

    # Complexity
    complexity = inv.get("complexity", {})
    simple = complexity.get("Simple", 0)
    medium = complexity.get("Medium", 0)
    cplx = complexity.get("Complex", 0)
    custom = complexity.get("Custom", 0)

    # Phase rows
    phase_rows = ""
    for p in phases:
        st = p.get("status", "")
        if "OK" in st or "✅" in st:
            badge = '<span class="badge ok">OK</span>'
        elif "Skipped" in st or "⏭" in st:
            badge = '<span class="badge skip">Skipped</span>'
        else:
            badge = f'<span class="badge err">{escape(st)}</span>'
        phase_rows += f"<tr><td>{p['id']}</td><td>{escape(p['name'])}</td><td>{badge}</td><td>{escape(p.get('duration', '—'))}</td></tr>\n"

    # Artifact list helper
    def _artifact_list(items, cls="nb"):
        if not items:
            return "<em>None generated yet</em>"
        return "".join(f'<span class="artifact {cls}">{escape(name)}</span>' for name in items)

    # Checkpoint info
    cp_html = ""
    if checkpoint:
        cp_phases = checkpoint.get("completed_phases", [])
        cp_updated = checkpoint.get("updated", "N/A")
        cp_html = f"""
        <div class="card">
            <h3>🔄 Checkpoint</h3>
            <p>Completed phases: <strong>{cp_phases}</strong></p>
            <p>Last updated: <code>{escape(str(cp_updated))}</code></p>
            <p class="hint">Use <code>--resume</code> to continue from here</p>
        </div>"""

    # Deployment info
    deploy_html = ""
    if deployment:
        results = deployment.get("results", [])
        created = sum(1 for r in results if r.get("status") == "created")
        dry = sum(1 for r in results if r.get("status") == "dry-run")
        errors = sum(1 for r in results if r.get("status") == "error")
        mode = "DRY-RUN" if deployment.get("dry_run") else "LIVE"
        deploy_html = f"""
        <div class="card">
            <h3>🚀 Deployment</h3>
            <p>Mode: <strong>{mode}</strong> | Workspace: <code>{escape(str(deployment.get('workspace_id', 'N/A')))}</code></p>
            <p>Created: {created} | Dry-run: {dry} | Errors: {errors}</p>
            <p>Timestamp: <code>{escape(str(deployment.get('timestamp', 'N/A')))}</code></p>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Migration Dashboard — Informatica to Fabric</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f2f5; color: #1a1a2e; }}
  .header {{ background: linear-gradient(135deg, #0078D4 0%, #005A9E 100%); color: white; padding: 2rem; text-align: center; }}
  .header h1 {{ font-size: 1.8rem; margin-bottom: 0.5rem; }}
  .header p {{ opacity: 0.85; font-size: 0.9rem; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; padding: 1.5rem; max-width: 1200px; margin: 0 auto; }}
  .kpi {{ background: white; border-radius: 12px; padding: 1.5rem; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
  .kpi .number {{ font-size: 2.5rem; font-weight: 700; color: #0078D4; }}
  .kpi .label {{ font-size: 0.85rem; color: #6c757d; margin-top: 0.3rem; }}
  .content {{ max-width: 1200px; margin: 0 auto; padding: 0 1.5rem 2rem; }}
  .card {{ background: white; border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
  .card h3 {{ margin-bottom: 1rem; color: #1a1a2e; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 0.6rem 0.8rem; text-align: left; border-bottom: 1px solid #eee; }}
  th {{ background: #f8f9fa; font-weight: 600; font-size: 0.85rem; color: #6c757d; text-transform: uppercase; }}
  .badge {{ display: inline-block; padding: 0.2rem 0.6rem; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }}
  .badge.ok {{ background: #d4edda; color: #155724; }}
  .badge.skip {{ background: #fff3cd; color: #856404; }}
  .badge.err {{ background: #f8d7da; color: #721c24; }}
  .artifact {{ display: inline-block; padding: 0.25rem 0.5rem; margin: 0.15rem; border-radius: 6px; font-size: 0.8rem; font-family: monospace; }}
  .artifact.nb {{ background: #e8f5e9; color: #2e7d32; }}
  .artifact.pl {{ background: #e3f2fd; color: #1565c0; }}
  .artifact.sql {{ background: #f3e5f5; color: #7b1fa2; }}
  .artifact.val {{ background: #fce4ec; color: #c62828; }}
  .bar {{ display: flex; height: 28px; border-radius: 8px; overflow: hidden; margin: 0.5rem 0; }}
  .bar-seg {{ display: flex; align-items: center; justify-content: center; font-size: 0.7rem; font-weight: 600; color: white; }}
  .hint {{ color: #6c757d; font-size: 0.85rem; margin-top: 0.5rem; }}
  code {{ background: #f1f3f5; padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.85rem; }}
  .footer {{ text-align: center; padding: 1.5rem; color: #adb5bd; font-size: 0.8rem; }}
</style>
</head>
<body>
<div class="header">
  <h1>Informatica → Fabric Migration</h1>
  <p>Dashboard generated {escape(status.get('generated', 'N/A')[:19])}</p>
</div>

<div class="grid">
  <div class="kpi"><div class="number">{total_artifacts}</div><div class="label">Total Artifacts</div></div>
  <div class="kpi"><div class="number">{nb_count}</div><div class="label">Notebooks</div></div>
  <div class="kpi"><div class="number">{pl_count}</div><div class="label">Pipelines</div></div>
  <div class="kpi"><div class="number">{sql_count}</div><div class="label">SQL Files</div></div>
  <div class="kpi"><div class="number">{val_count}</div><div class="label">Validation</div></div>
</div>

<div class="content">

  <div class="card">
    <h3>📊 Complexity Distribution</h3>
    <div class="bar">
      {"".join(f'<div class="bar-seg" style="flex:{v};background:{c}">{escape(k)} ({v})</div>' for k, v, c in [("Simple", simple, "#27AE60"), ("Medium", medium, "#F39C12"), ("Complex", cplx, "#E74C3C"), ("Custom", custom, "#8E44AD")] if v > 0)}
    </div>
    <p class="hint">{inv.get('total_mappings', 0)} mappings, {inv.get('total_workflows', 0)} workflows, {inv.get('total_connections', 0)} connections</p>
  </div>

  <div class="card">
    <h3>⚡ Phase Results</h3>
    {"<table><tr><th>#</th><th>Phase</th><th>Status</th><th>Duration</th></tr>" + phase_rows + "</table>" if phase_rows else "<p><em>No migration run yet. Use <code>python run_migration.py</code></em></p>"}
  </div>

  <div class="card">
    <h3>📓 Notebooks</h3>
    {_artifact_list(artifacts.get("notebooks", []), "nb")}
  </div>

  <div class="card">
    <h3>⚡ Pipelines</h3>
    {_artifact_list(artifacts.get("pipelines", []), "pl")}
  </div>

  <div class="card">
    <h3>🗄️ SQL Files</h3>
    {_artifact_list(artifacts.get("sql", []), "sql")}
  </div>

  <div class="card">
    <h3>✅ Validation</h3>
    {_artifact_list(artifacts.get("validation", []), "val")}
  </div>

  {cp_html}
  {deploy_html}

</div>

<div class="footer">
  Informatica to Fabric Migration — Dashboard v0.12.0
</div>
</body>
</html>"""
    return html


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Migration Dashboard Generator")
    parser.add_argument("--open", action="store_true", help="Open dashboard in browser")
    parser.add_argument("--json", action="store_true", help="Output JSON status")
    args = parser.parse_args()

    status = _collect_status()

    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")

    if args.json:
        print(json.dumps(status, indent=2, ensure_ascii=False))
        return

    html = _generate_html(status)
    out_path = OUTPUT_DIR / "dashboard.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Dashboard generated: {out_path}")

    if args.open:
        webbrowser.open(str(out_path))


if __name__ == "__main__":
    main()
