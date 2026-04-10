"""
Phase 14 — Migration Diff & Side-by-Side Review (Sprint 91)

Generates side-by-side comparison views of source Informatica logic vs. generated
target code for manual review and approval.

Architecture:
  1. Source Logic Extractor — readable form of Informatica mapping
  2. Target Code Formatter — syntax-highlighted generated code
  3. Side-by-Side HTML — 2-panel aligned comparison
  4. Annotations — ✅ (auto), ⚠️ (heuristic), 🔴 (TODO/manual)
  5. Batch Review Report — consolidated filterable report

Usage:
    python diff_generator.py
    python diff_generator.py --mapping M_LOAD_CUSTOMERS
    python diff_generator.py --filter todo                # only TODOs
"""

import json
import html
import re
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "review"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"


# ─────────────────────────────────────────────
#  Source Logic Extractor
# ─────────────────────────────────────────────

def extract_source_logic(mapping):
    """Extract human-readable representation of Informatica mapping logic.

    Produces a structured description of the transform chain.
    """
    name = mapping.get("name", "Unknown")
    sources = mapping.get("sources", [])
    targets = mapping.get("targets", [])
    transforms = mapping.get("transformations", [])
    sql_override = mapping.get("sql_override", "")
    complexity = mapping.get("complexity", "Simple")

    sections = []

    # Header
    sections.append({
        "type": "header",
        "label": f"Mapping: {name}",
        "content": f"Complexity: {complexity}",
    })

    # Sources
    sections.append({
        "type": "sources",
        "label": "Source Tables",
        "content": "\n".join(f"  → {s}" for s in sources) if sources else "  (no sources)",
    })

    # Transformation chain
    if transforms:
        chain_lines = []
        for i, t in enumerate(transforms):
            t_type = t.get("type", "Unknown")
            t_name = t.get("name", f"Transform_{i}")
            t_desc = t.get("description", "")
            chain_lines.append(f"  [{i+1}] {t_type}: {t_name}")
            if t_desc:
                chain_lines.append(f"       {t_desc}")
            if t.get("expression"):
                chain_lines.append(f"       Expression: {t['expression'][:120]}")
        sections.append({
            "type": "transforms",
            "label": "Transformation Chain",
            "content": "\n".join(chain_lines),
        })
    else:
        sections.append({
            "type": "transforms",
            "label": "Transformation Chain",
            "content": "  (no transformations recorded)",
        })

    # SQL Override
    if sql_override:
        sections.append({
            "type": "sql_override",
            "label": "SQL Override (Source Dialect)",
            "content": sql_override[:500],
        })

    # Targets
    sections.append({
        "type": "targets",
        "label": "Target Tables",
        "content": "\n".join(f"  → {t}" for t in targets) if targets else "  (no targets)",
    })

    return {"name": name, "sections": sections, "complexity": complexity}


# ─────────────────────────────────────────────
#  Target Code Extractor
# ─────────────────────────────────────────────

def extract_target_code(mapping_name):
    """Extract generated target code (notebook, SQL, pipeline) for a mapping.

    Returns structured sections with code content and status annotations.
    """
    sections = []

    # Notebook
    nb_path = WORKSPACE / "output" / "notebooks" / f"NB_{mapping_name}.py"
    if nb_path.exists():
        code = nb_path.read_text(encoding="utf-8")
        status = _classify_code(code)
        sections.append({
            "type": "notebook",
            "label": f"Generated Notebook (NB_{mapping_name}.py)",
            "content": code[:2000],
            "status": status,
            "file": nb_path.name,
        })

    # SQL
    sql_path = WORKSPACE / "output" / "sql" / f"SQL_{mapping_name}.sql"
    if not sql_path.exists():
        sql_path = WORKSPACE / "output" / "sql" / f"SQL_OVERRIDES_{mapping_name}.sql"
    if sql_path.exists():
        code = sql_path.read_text(encoding="utf-8")
        status = _classify_code(code)
        sections.append({
            "type": "sql",
            "label": f"Converted SQL ({sql_path.name})",
            "content": code[:2000],
            "status": status,
            "file": sql_path.name,
        })

    # Pipeline
    pl_path = WORKSPACE / "output" / "pipelines" / f"PL_{mapping_name}.json"
    if pl_path.exists():
        code = pl_path.read_text(encoding="utf-8")
        sections.append({
            "type": "pipeline",
            "label": f"Pipeline JSON (PL_{mapping_name}.json)",
            "content": code[:2000],
            "status": "auto",
            "file": pl_path.name,
        })

    # DBT model
    dbt_path = WORKSPACE / "output" / "dbt" / "models" / f"{mapping_name.lower()}.sql"
    if dbt_path.exists():
        code = dbt_path.read_text(encoding="utf-8")
        status = _classify_code(code)
        sections.append({
            "type": "dbt",
            "label": f"DBT Model ({mapping_name.lower()}.sql)",
            "content": code[:2000],
            "status": status,
            "file": dbt_path.name,
        })

    if not sections:
        sections.append({
            "type": "none",
            "label": "No Generated Artifacts",
            "content": f"No output files found for mapping {mapping_name}.",
            "status": "missing",
        })

    return sections


def _classify_code(code):
    """Classify generated code status: auto, heuristic, todo."""
    if re.search(r'--\s*TODO:', code, re.IGNORECASE):
        return "todo"
    if re.search(r'--\s*(REVIEW|HEURISTIC|AI-converted):', code, re.IGNORECASE):
        return "heuristic"
    return "auto"


# ─────────────────────────────────────────────
#  Side-by-Side HTML Generator
# ─────────────────────────────────────────────

STATUS_ICONS = {
    "auto": "✅",
    "heuristic": "⚠️",
    "todo": "🔴",
    "missing": "❓",
}

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Migration Review — {title}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
  .header {{ background: #0078D4; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
  .header h1 {{ margin: 0; }}
  .header .meta {{ opacity: 0.8; margin-top: 8px; }}
  .comparison {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }}
  .panel {{ background: white; border-radius: 8px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: auto; }}
  .panel h3 {{ margin-top: 0; color: #333; border-bottom: 2px solid #0078D4; padding-bottom: 8px; }}
  .panel.source h3 {{ border-color: #E67E22; }}
  .panel.target h3 {{ border-color: #27AE60; }}
  pre {{ background: #f8f8f8; padding: 12px; border-radius: 4px; font-size: 13px; overflow-x: auto; white-space: pre-wrap; word-break: break-word; }}
  .status {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: bold; }}
  .status-auto {{ background: #d4edda; color: #155724; }}
  .status-heuristic {{ background: #fff3cd; color: #856404; }}
  .status-todo {{ background: #f8d7da; color: #721c24; }}
  .status-missing {{ background: #e2e3e5; color: #383d41; }}
  .summary {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .stats {{ display: flex; gap: 20px; flex-wrap: wrap; }}
  .stat {{ text-align: center; padding: 12px 20px; }}
  .stat-value {{ font-size: 28px; font-weight: bold; color: #0078D4; }}
  .stat-label {{ font-size: 12px; color: #666; }}
  .filter-bar {{ margin-bottom: 16px; }}
  .filter-bar button {{ padding: 6px 16px; border: 1px solid #ccc; background: white; border-radius: 4px; cursor: pointer; margin-right: 4px; }}
  .filter-bar button.active {{ background: #0078D4; color: white; border-color: #0078D4; }}
</style>
</head>
<body>
{content}
</body>
</html>"""


def generate_diff_html(mapping, source_logic, target_sections):
    """Generate side-by-side HTML comparison for a single mapping."""
    name = mapping.get("name", "Unknown")

    # Determine overall status
    statuses = [s.get("status", "auto") for s in target_sections]
    overall = "todo" if "todo" in statuses else ("heuristic" if "heuristic" in statuses else "auto")

    content_parts = []

    # Header
    content_parts.append(
        f'<div class="header">'
        f'<h1>{STATUS_ICONS.get(overall, "")} {html.escape(name)}</h1>'
        f'<div class="meta">Complexity: {source_logic.get("complexity", "?")} | '
        f'Status: {overall} | Generated: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
        f'</div></div>'
    )

    # Side-by-side panels
    # Source panel
    source_html = '<div class="panel source"><h3>📂 Source (Informatica)</h3>'
    for section in source_logic.get("sections", []):
        source_html += (f'<h4>{html.escape(section["label"])}</h4>'
                        f'<pre>{html.escape(section["content"])}</pre>')
    source_html += '</div>'

    # Target panel
    target_html = '<div class="panel target"><h3>🎯 Target (Generated)</h3>'
    for section in target_sections:
        status = section.get("status", "auto")
        icon = STATUS_ICONS.get(status, "")
        badge = f'<span class="status status-{status}">{icon} {status}</span>'
        target_html += (f'<h4>{html.escape(section["label"])} {badge}</h4>'
                        f'<pre>{html.escape(section["content"])}</pre>')
    target_html += '</div>'

    content_parts.append(f'<div class="comparison">{source_html}{target_html}</div>')

    return HTML_TEMPLATE.format(title=html.escape(name), content="\n".join(content_parts))


# ─────────────────────────────────────────────
#  Batch Review Report
# ─────────────────────────────────────────────

def generate_batch_report(inventory_path=None, filter_status=None):
    """Generate consolidated review report across all mappings.

    Args:
        inventory_path: Path to inventory.json
        filter_status: Filter by status ('auto', 'heuristic', 'todo', or None for all)

    Returns dict with report summary and per-mapping details.
    """
    inv_path = Path(inventory_path or INVENTORY_PATH)
    if not inv_path.exists():
        return {"mappings": [], "summary": {"total": 0, "auto": 0, "heuristic": 0, "todo": 0, "missing": 0}}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    results = []
    status_counts = {"auto": 0, "heuristic": 0, "todo": 0, "missing": 0}

    for mapping in inventory.get("mappings", []):
        name = mapping["name"]
        source_logic = extract_source_logic(mapping)
        target_sections = extract_target_code(name)

        # Determine status
        statuses = [s.get("status", "auto") for s in target_sections]
        overall = "todo" if "todo" in statuses else (
            "heuristic" if "heuristic" in statuses else (
                "missing" if "missing" in statuses else "auto"
            )
        )
        status_counts[overall] = status_counts.get(overall, 0) + 1

        if filter_status and overall != filter_status:
            continue

        results.append({
            "name": name,
            "complexity": mapping.get("complexity", "Simple"),
            "status": overall,
            "source_sections": len(source_logic.get("sections", [])),
            "target_sections": len(target_sections),
            "artifact_files": [s.get("file", "") for s in target_sections if s.get("file")],
        })

    return {
        "mappings": results,
        "summary": {
            "total": len(inventory.get("mappings", [])),
            **status_counts,
        },
        "filter": filter_status,
        "generated": datetime.now(timezone.utc).isoformat(),
    }


def generate_batch_html(inventory_path=None, filter_status=None):
    """Generate HTML batch review report."""
    report = generate_batch_report(inventory_path, filter_status)
    s = report["summary"]

    # Build stats bar
    stats_html = (
        f'<div class="stats">'
        f'<div class="stat"><div class="stat-value">{s["total"]}</div><div class="stat-label">Total Mappings</div></div>'
        f'<div class="stat"><div class="stat-value" style="color:#27AE60">{s["auto"]}</div><div class="stat-label">✅ Auto-Converted</div></div>'
        f'<div class="stat"><div class="stat-value" style="color:#F39C12">{s["heuristic"]}</div><div class="stat-label">⚠️ Heuristic</div></div>'
        f'<div class="stat"><div class="stat-value" style="color:#E74C3C">{s["todo"]}</div><div class="stat-label">🔴 Manual/TODO</div></div>'
        f'<div class="stat"><div class="stat-value" style="color:#95a5a6">{s["missing"]}</div><div class="stat-label">❓ Missing</div></div>'
        f'</div>'
    )

    # Build table rows
    rows = []
    for m in report["mappings"]:
        icon = STATUS_ICONS.get(m["status"], "")
        badge_class = f'status-{m["status"]}'
        rows.append(
            f'<tr>'
            f'<td><strong>{html.escape(m["name"])}</strong></td>'
            f'<td>{m["complexity"]}</td>'
            f'<td><span class="status {badge_class}">{icon} {m["status"]}</span></td>'
            f'<td>{", ".join(m["artifact_files"])}</td>'
            f'</tr>'
        )

    table_html = (
        '<table style="width:100%;border-collapse:collapse;background:white;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)">'
        '<thead><tr style="background:#0078D4;color:white">'
        '<th style="padding:10px;text-align:left">Mapping</th>'
        '<th style="padding:10px;text-align:left">Complexity</th>'
        '<th style="padding:10px;text-align:left">Status</th>'
        '<th style="padding:10px;text-align:left">Artifacts</th>'
        '</tr></thead><tbody>'
        + "\n".join(rows)
        + '</tbody></table>'
    )

    content = (
        f'<div class="header"><h1>📋 Migration Batch Review Report</h1>'
        f'<div class="meta">Generated: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
        f'{" | Filter: " + filter_status if filter_status else ""}</div></div>'
        f'<div class="summary">{stats_html}</div>'
        f'{table_html}'
    )

    return HTML_TEMPLATE.format(title="Batch Review Report", content=content)


# ─────────────────────────────────────────────
#  Lineage Graph JSON API (Sprint 90)
# ─────────────────────────────────────────────

def generate_lineage_json(inventory_path=None):
    """Generate lineage graph as nodes + edges JSON for browser visualization.

    Returns {nodes: [{id, label, type, tier}], edges: [{source, target, label}]}
    """
    inv_path = Path(inventory_path or INVENTORY_PATH)
    if not inv_path.exists():
        return {"nodes": [], "edges": []}

    with open(inv_path, encoding="utf-8") as f:
        inventory = json.load(f)

    nodes = {}
    edges = []

    for mapping in inventory.get("mappings", []):
        name = mapping["name"]
        # Mapping node
        nodes[name] = {
            "id": name,
            "label": name,
            "type": "mapping",
            "tier": _infer_tier(name),
            "complexity": mapping.get("complexity", "Simple"),
        }

        # Source nodes and edges
        for src in mapping.get("sources", []):
            if src not in nodes:
                nodes[src] = {"id": src, "label": src, "type": "table",
                              "tier": _infer_tier(src)}
            edges.append({"source": src, "target": name, "label": "reads"})

        # Target nodes and edges
        for tgt in mapping.get("targets", []):
            if tgt not in nodes:
                nodes[tgt] = {"id": tgt, "label": tgt, "type": "table",
                              "tier": _infer_tier(tgt)}
            edges.append({"source": name, "target": tgt, "label": "writes"})

    return {"nodes": list(nodes.values()), "edges": edges}


def generate_impact_analysis(entity, inventory_path=None, direction="downstream"):
    """Trace upstream or downstream impact from a given entity.

    Returns list of impacted nodes with distance from origin.
    """
    lineage = generate_lineage_json(inventory_path)
    edges = lineage["edges"]
    node_ids = {n["id"] for n in lineage["nodes"]}

    if entity not in node_ids:
        return {"entity": entity, "direction": direction, "impacted": [], "found": False}

    # Build adjacency
    adjacency = {}
    for e in edges:
        src, tgt = e["source"], e["target"]
        if direction == "downstream":
            adjacency.setdefault(src, []).append(tgt)
        else:
            adjacency.setdefault(tgt, []).append(src)

    # BFS
    visited = {}
    queue = [(entity, 0)]
    visited[entity] = 0
    while queue:
        current, depth = queue.pop(0)
        for neighbor in adjacency.get(current, []):
            if neighbor not in visited:
                visited[neighbor] = depth + 1
                queue.append((neighbor, depth + 1))

    impacted = [{"id": nid, "distance": dist}
                for nid, dist in sorted(visited.items(), key=lambda x: x[1])
                if nid != entity]

    return {"entity": entity, "direction": direction, "impacted": impacted, "found": True}


def _infer_tier(name):
    """Infer medallion tier from naming convention."""
    upper = name.upper()
    if any(p in upper for p in ("BRONZE", "RAW", "STG", "STAGING", "SRC")):
        return "bronze"
    if any(p in upper for p in ("SILVER", "CLEANED", "CURATED")):
        return "silver"
    if any(p in upper for p in ("GOLD", "AGG", "SUMMARY", "RPT", "REPORT", "DIM", "FACT")):
        return "gold"
    return "unknown"


def generate_lineage_html(inventory_path=None):
    """Generate interactive lineage HTML (Cytoscape.js-based)."""
    lineage = generate_lineage_json(inventory_path)

    cytoscape_elements = []
    for node in lineage["nodes"]:
        cytoscape_elements.append({"data": {"id": node["id"], "label": node["label"],
                                            "type": node["type"], "tier": node.get("tier", "unknown")}})
    for edge in lineage["edges"]:
        cytoscape_elements.append({"data": {"source": edge["source"], "target": edge["target"],
                                            "label": edge.get("label", "")}})

    lineage_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Lineage Explorer</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.28.1/cytoscape.min.js"></script>
<style>
  body {{ margin: 0; font-family: -apple-system, sans-serif; }}
  #cy {{ width: 100%; height: 100vh; }}
  .toolbar {{ position: fixed; top: 10px; left: 10px; z-index: 10; background: white;
              padding: 10px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.15); }}
  .toolbar button {{ padding: 4px 12px; margin: 2px; border: 1px solid #ccc; border-radius: 4px; cursor: pointer; background: white; }}
  .toolbar button:hover {{ background: #0078D4; color: white; }}
</style>
</head>
<body>
<div class="toolbar">
  <strong>🔍 Lineage Explorer</strong><br/>
  <button onclick="cy.fit()">Fit</button>
  <button onclick="filterTier('bronze')">Bronze</button>
  <button onclick="filterTier('silver')">Silver</button>
  <button onclick="filterTier('gold')">Gold</button>
  <button onclick="filterTier(null)">All</button>
</div>
<div id="cy"></div>
<script>
const elements = {json.dumps(cytoscape_elements)};
const tierColors = {{bronze: '#CD7F32', silver: '#C0C0C0', gold: '#FFD700', unknown: '#95a5a6'}};
const cy = cytoscape({{
  container: document.getElementById('cy'),
  elements: elements,
  style: [
    {{selector: 'node[type="table"]', style: {{'shape': 'round-rectangle', 'label': 'data(label)',
      'background-color': function(ele) {{ return tierColors[ele.data('tier')] || '#95a5a6'; }},
      'width': 120, 'height': 40, 'font-size': 10, 'text-valign': 'center'}}}},
    {{selector: 'node[type="mapping"]', style: {{'shape': 'ellipse', 'label': 'data(label)',
      'background-color': '#0078D4', 'color': 'white', 'width': 100, 'height': 40,
      'font-size': 9, 'text-valign': 'center'}}}},
    {{selector: 'edge', style: {{'width': 2, 'line-color': '#ccc', 'target-arrow-shape': 'triangle',
      'target-arrow-color': '#999', 'curve-style': 'bezier', 'label': 'data(label)', 'font-size': 8}}}},
  ],
  layout: {{name: 'breadthfirst', directed: true, padding: 40}},
}});
function filterTier(tier) {{
  cy.elements().show();
  if (tier) cy.nodes().filter(n => n.data('tier') !== tier && n.data('type') === 'table').hide();
}}
</script>
</body>
</html>"""
    return lineage_html


# ─────────────────────────────────────────────
#  Orchestrator
# ─────────────────────────────────────────────

def generate_review(inventory_path=None, mapping_name=None, filter_status=None):
    """Generate all review artifacts.

    Returns dict with paths to generated files.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generated = []

    inv_path = Path(inventory_path or INVENTORY_PATH)

    # Batch report
    batch_html = generate_batch_html(str(inv_path), filter_status)
    batch_path = OUTPUT_DIR / "batch_review.html"
    batch_path.write_text(batch_html, encoding="utf-8")
    generated.append(str(batch_path))

    # Batch report JSON
    batch_report = generate_batch_report(str(inv_path), filter_status)
    batch_json_path = OUTPUT_DIR / "batch_review.json"
    with open(batch_json_path, "w", encoding="utf-8") as f:
        json.dump(batch_report, f, indent=2)
    generated.append(str(batch_json_path))

    # Lineage JSON
    lineage = generate_lineage_json(str(inv_path))
    lineage_path = OUTPUT_DIR / "lineage_graph.json"
    with open(lineage_path, "w", encoding="utf-8") as f:
        json.dump(lineage, f, indent=2)
    generated.append(str(lineage_path))

    # Lineage HTML
    lineage_html = generate_lineage_html(str(inv_path))
    lineage_html_path = OUTPUT_DIR / "lineage_explorer.html"
    lineage_html_path.write_text(lineage_html, encoding="utf-8")
    generated.append(str(lineage_html_path))

    # Per-mapping diffs (if specific mapping requested or all)
    if inv_path.exists():
        with open(inv_path, encoding="utf-8") as f:
            inventory = json.load(f)
        for mapping in inventory.get("mappings", []):
            name = mapping["name"]
            if mapping_name and name != mapping_name:
                continue
            source_logic = extract_source_logic(mapping)
            target_sections = extract_target_code(name)
            diff_html = generate_diff_html(mapping, source_logic, target_sections)
            diff_path = OUTPUT_DIR / f"diff_{name}.html"
            diff_path.write_text(diff_html, encoding="utf-8")
            generated.append(str(diff_path))

    return {
        "generated": generated,
        "output_dir": str(OUTPUT_DIR),
        "mapping_filter": mapping_name,
        "status_filter": filter_status,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Migration Diff & Review (Phase 14)")
    parser.add_argument("--inventory", default=None, help="Path to inventory.json")
    parser.add_argument("--mapping", default=None, help="Specific mapping to review")
    parser.add_argument("--filter", choices=["auto", "heuristic", "todo"], default=None,
                        help="Filter by status")
    args = parser.parse_args()

    result = generate_review(args.inventory, args.mapping, args.filter)
    print(f"✅ Review artifacts generated: {len(result['generated'])} files")
    print(f"   Output: {result['output_dir']}")


if __name__ == "__main__":
    main()
