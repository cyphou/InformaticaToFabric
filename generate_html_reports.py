"""
HTML Report Generator for Informatica-to-Fabric Migration
Produces two self-contained HTML reports (no external dependencies):
  1. assessment_report.html  — Inventory, complexity, SQL constructs, connections
  2. migration_report.html   — Migration readiness, artifact status, action items

Usage:
    python generate_html_reports.py                    # reads output/inventory/inventory.json
    python generate_html_reports.py path/to/inventory.json
"""

import json
import math
import sys
from datetime import datetime, timezone
from html import escape
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "inventory"
DEFAULT_INVENTORY = OUTPUT_DIR / "inventory.json"


# ─────────────────────────────────────────────
#  Shared HTML helpers
# ─────────────────────────────────────────────

COLORS = {
    "Simple": "#27AE60",
    "Medium": "#F39C12",
    "Complex": "#E74C3C",
    "Custom": "#8E44AD",
    "Oracle": "#E74C3C",
    "sqlserver": "#0078D4",
    "Database": "#0078D4",
    "FTP": "#2ECC71",
    "unknown": "#95A5A6",
    "primary": "#0078D4",
    "bg": "#F8F9FA",
    "border": "#DEE2E6",
    "text": "#212529",
    "muted": "#6C757D",
}


def _svg_donut(slices, size=180, hole=0.55):
    """Return an SVG donut chart. slices = [(label, value, color), ...]"""
    total = sum(v for _, v, _ in slices)
    if total == 0:
        return ""
    r = size / 2 - 2
    cx = cy = size / 2
    parts = []
    cum = 0
    for label, value, color in slices:
        if value == 0:
            continue
        frac = value / total
        start_angle = cum * 2 * math.pi - math.pi / 2
        cum += frac
        end_angle = cum * 2 * math.pi - math.pi / 2
        large = 1 if frac > 0.5 else 0
        x1 = cx + r * math.cos(start_angle)
        y1 = cy + r * math.sin(start_angle)
        x2 = cx + r * math.cos(end_angle)
        y2 = cy + r * math.sin(end_angle)
        ir = r * hole
        x3 = cx + ir * math.cos(end_angle)
        y3 = cy + ir * math.sin(end_angle)
        x4 = cx + ir * math.cos(start_angle)
        y4 = cy + ir * math.sin(start_angle)
        d = (
            f"M {x1:.2f},{y1:.2f} A {r},{r} 0 {large},1 {x2:.2f},{y2:.2f} "
            f"L {x3:.2f},{y3:.2f} A {ir},{ir} 0 {large},0 {x4:.2f},{y4:.2f} Z"
        )
        parts.append(f'<path d="{d}" fill="{color}"><title>{escape(label)}: {value}</title></path>')

    legend = []
    for label, value, color in slices:
        pct = value / total * 100 if total else 0
        legend.append(
            f'<span style="display:inline-flex;align-items:center;margin-right:14px;">'
            f'<span style="width:12px;height:12px;border-radius:2px;background:{color};display:inline-block;margin-right:5px;"></span>'
            f'{escape(label)}: {value} ({pct:.0f}%)</span>'
        )
    return (
        f'<div style="text-align:center;">'
        f'<svg width="{size}" height="{size}" viewBox="0 0 {size} {size}">{"".join(parts)}'
        f'<text x="{cx}" y="{cy + 6}" text-anchor="middle" font-size="22" font-weight="700" fill="{COLORS["text"]}">{total}</text>'
        f'</svg>'
        f'<div style="margin-top:8px;font-size:13px;color:{COLORS["muted"]};">{"".join(legend)}</div>'
        f'</div>'
    )


def _svg_bar(items, max_width=320):
    """Horizontal bar chart. items = [(label, value, color), ...]"""
    if not items:
        return ""
    peak = max(v for _, v, _ in items)
    if peak == 0:
        return ""
    rows = []
    for label, value, color in items:
        w = int(value / peak * max_width) if peak else 0
        rows.append(
            f'<div style="display:flex;align-items:center;margin-bottom:6px;">'
            f'<span style="width:140px;font-size:13px;text-align:right;padding-right:10px;color:{COLORS["text"]};">{escape(str(label))}</span>'
            f'<div style="background:{COLORS["bg"]};border:1px solid {COLORS["border"]};border-radius:4px;width:{max_width + 4}px;height:22px;">'
            f'<div style="background:{color};width:{w}px;height:100%;border-radius:3px;"></div></div>'
            f'<span style="padding-left:8px;font-size:13px;font-weight:600;color:{COLORS["text"]};">{value}</span>'
            f'</div>'
        )
    return "".join(rows)


def _card(title, body, icon=""):
    """Wrap content in a styled card."""
    return (
        f'<div class="card">'
        f'<h3>{icon} {escape(title)}</h3>'
        f'{body}'
        f'</div>'
    )


def _table(headers, rows, highlight_col=None):
    """Return an HTML table."""
    ths = "".join(f"<th>{escape(str(h))}</th>" for h in headers)
    trs = []
    for row in rows:
        tds = []
        for i, cell in enumerate(row):
            style = ""
            if highlight_col is not None and i == highlight_col:
                color = COLORS.get(str(cell), COLORS["muted"])
                style = f' style="color:{color};font-weight:600;"'
            tds.append(f"<td{style}>{escape(str(cell))}</td>")
        trs.append(f"<tr>{''.join(tds)}</tr>")
    return f'<table><thead><tr>{ths}</tr></thead><tbody>{"".join(trs)}</tbody></table>'


def _badge(text, color):
    return (
        f'<span style="display:inline-block;padding:3px 10px;border-radius:12px;'
        f'background:{color};color:#fff;font-size:12px;font-weight:600;">'
        f'{escape(text)}</span>'
    )


def _svg_lineage_flow(mapping_name, field_lineage):
    """Render a mapping's field-level lineage as an inline SVG flow diagram.

    Groups by unique instance chain and draws source → transformations → target.
    Returns an HTML string with an embedded SVG.
    """
    if not field_lineage:
        return '<p style="color:#6C757D;font-size:13px;">No field-level lineage extracted.</p>'

    # Collect unique instances in order and their types
    instance_order = []
    instance_labels = {}
    seen = set()
    for entry in field_lineage:
        src = entry["source_instance"]
        if src not in seen:
            instance_order.append(src)
            seen.add(src)
            instance_labels[src] = "SQ"
        for tx in entry.get("transformations", []):
            inst = tx["instance"]
            if inst not in seen:
                instance_order.append(inst)
                seen.add(inst)
                instance_labels[inst] = tx.get("type", "?")[:12]
        tgt = entry["target_instance"]
        if tgt not in seen:
            instance_order.append(tgt)
            seen.add(tgt)
            instance_labels[tgt] = "TGT"

    # Unique edges
    edges = set()
    for entry in field_lineage:
        chain = [entry["source_instance"]]
        for tx in entry.get("transformations", []):
            chain.append(tx["instance"])
        chain.append(entry["target_instance"])
        for i in range(len(chain) - 1):
            edges.add((chain[i], chain[i + 1]))

    n = len(instance_order)
    if n == 0:
        return ""

    # Layout: boxes in a single horizontal row
    box_w, box_h = 130, 50
    gap_x = 50
    padding = 20
    total_w = padding * 2 + n * box_w + (n - 1) * gap_x
    total_h = padding * 2 + box_h + 30  # extra space for labels below

    # Build position map
    positions = {}
    for i, inst in enumerate(instance_order):
        x = padding + i * (box_w + gap_x)
        y = padding
        positions[inst] = (x, y)

    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {total_w} {total_h}" '
             f'style="max-width:100%;height:auto;font-family:Segoe UI,sans-serif;">']

    # Draw edges (arrows)
    idx_map = {inst: i for i, inst in enumerate(instance_order)}
    for src, dst in sorted(edges):
        sx = positions[src][0] + box_w
        sy = positions[src][1] + box_h // 2
        dx = positions[dst][0]
        dy = positions[dst][1] + box_h // 2
        parts.append(f'<line x1="{sx}" y1="{sy}" x2="{dx - 6}" y2="{dy}" '
                     f'stroke="#0078D4" stroke-width="2" marker-end="url(#arrow)"/>')

    # Arrow marker definition
    parts.insert(1, '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
                    'markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
                    '<path d="M 0 0 L 10 5 L 0 10 z" fill="#0078D4"/></marker></defs>')

    # Draw boxes
    for inst in instance_order:
        x, y = positions[inst]
        lbl = instance_labels.get(inst, "?")
        if lbl == "SQ":
            fill, stroke = "#E8F4FD", "#0078D4"
        elif lbl == "TGT":
            fill, stroke = "#E8F8E8", "#27AE60"
        else:
            fill, stroke = "#FFF8E1", "#F39C12"

        parts.append(f'<rect x="{x}" y="{y}" width="{box_w}" height="{box_h}" '
                     f'rx="6" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        # Instance name (truncated)
        display_name = inst if len(inst) <= 16 else inst[:14] + ".."
        parts.append(f'<text x="{x + box_w // 2}" y="{y + 20}" text-anchor="middle" '
                     f'font-size="11" font-weight="600" fill="#212529">{escape(display_name)}</text>')
        # Type label
        parts.append(f'<text x="{x + box_w // 2}" y="{y + 36}" text-anchor="middle" '
                     f'font-size="10" fill="#6C757D">{escape(lbl)}</text>')

    parts.append('</svg>')
    return "".join(parts)


def _build_cross_mapping_lineage(mappings):
    """Build a cross-mapping lineage summary: source tables → mappings → target tables.

    Returns a list of dicts: [{source, mappings: [name, ...], targets: [name, ...]}]
    suitable for rendering as a table.
    """
    # source_table -> set of mappings, target_table -> set of mappings
    source_to_mappings = {}
    target_to_mappings = {}
    mapping_sources = {}  # mapping_name -> set of source tables
    mapping_targets = {}  # mapping_name -> set of target tables

    for m in mappings:
        name = m["name"]
        srcs = set(m.get("sources", []))
        tgts = set(m.get("targets", []))
        mapping_sources[name] = srcs
        mapping_targets[name] = tgts
        for s in srcs:
            source_to_mappings.setdefault(s, set()).add(name)
        for t in tgts:
            target_to_mappings.setdefault(t, set()).add(name)

    # Build rows: one per unique source table
    rows = []
    for src in sorted(source_to_mappings.keys()):
        m_names = sorted(source_to_mappings[src])
        # All targets reachable from these mappings
        all_targets = set()
        for mn in m_names:
            all_targets.update(mapping_targets.get(mn, set()))
        rows.append({
            "source": src,
            "mappings": m_names,
            "targets": sorted(all_targets),
        })
    return rows


CSS = """
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
    background: #F0F2F5; color: #212529; line-height: 1.6;
  }
  .header {
    background: linear-gradient(135deg, #0078D4 0%, #005A9E 100%);
    color: #fff; padding: 32px 40px; margin-bottom: 28px;
  }
  .header h1 { font-size: 26px; margin-bottom: 6px; }
  .header .subtitle { font-size: 14px; opacity: 0.85; }
  .container { max-width: 1140px; margin: 0 auto; padding: 0 20px 40px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; margin-bottom: 24px; }
  .kpi { background: #fff; border-radius: 10px; padding: 22px 26px;
         box-shadow: 0 1px 3px rgba(0,0,0,0.08); text-align: center; }
  .kpi .number { font-size: 36px; font-weight: 700; color: #0078D4; }
  .kpi .label  { font-size: 13px; color: #6C757D; margin-top: 2px; }
  .card {
    background: #fff; border-radius: 10px; padding: 24px 28px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 24px;
  }
  .card h3 { font-size: 17px; margin-bottom: 16px; color: #212529; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { background: #F8F9FA; padding: 10px 12px; text-align: left;
       border-bottom: 2px solid #DEE2E6; font-weight: 600; color: #495057; }
  td { padding: 9px 12px; border-bottom: 1px solid #EDEFF2; }
  tr:hover td { background: #F8F9FA; }
  .section-title { font-size: 20px; font-weight: 700; margin: 32px 0 16px;
                   padding-bottom: 8px; border-bottom: 2px solid #0078D4; color: #0078D4; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 4px;
         font-size: 11px; font-weight: 600; margin: 1px 2px; }
  .footer { text-align: center; padding: 20px; font-size: 12px; color: #6C757D; }
  .lineage-flow { overflow-x: auto; margin: 12px 0; padding: 8px;
                  background: #FAFBFC; border-radius: 8px; border: 1px solid #EDEFF2; }
  .lineage-mapping-title { font-size: 14px; font-weight: 600; color: #0078D4;
                           margin: 16px 0 4px; cursor: pointer; }
  .lineage-mapping-title:hover { text-decoration: underline; }
  .lineage-stats { font-size: 12px; color: #6C757D; margin-bottom: 8px; }
  .lineage-toggle { display: none; }
  .lineage-toggle:checked + .lineage-detail { display: block; }
  .lineage-detail { display: none; }
  @media print {
    body { background: #fff; }
    .header { background: #0078D4 !important; -webkit-print-color-adjust: exact; }
    .card, .kpi { box-shadow: none; border: 1px solid #DEE2E6; }
  }
</style>
"""


# ─────────────────────────────────────────────
#  Assessment Report
# ─────────────────────────────────────────────

def generate_assessment_report(inv, out_path):
    """Generate assessment_report.html from inventory data."""
    summary = inv.get("summary", {})
    mappings = inv.get("mappings", [])
    workflows = inv.get("workflows", [])
    sql_files = inv.get("sql_files", [])
    connections = inv.get("connections", [])
    param_files = inv.get("parameter_files", [])
    mapplets = inv.get("mapplets", {})
    cb = summary.get("complexity_breakdown", {})
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ----- KPI row -----
    kpis = [
        (summary.get("total_mappings", 0), "Mappings"),
        (summary.get("total_workflows", 0), "Workflows"),
        (summary.get("total_sessions", 0), "Sessions"),
        (summary.get("total_sql_files", 0), "SQL Files"),
        (len(param_files), "Parameter Files"),
        (len(mapplets), "Mapplets"),
        (len(connections), "Connections"),
    ]
    kpi_html = "".join(
        f'<div class="kpi"><div class="number">{v}</div><div class="label">{l}</div></div>'
        for v, l in kpis
    )

    # ----- Complexity donut -----
    slices = [(level, cb.get(level, 0), COLORS.get(level, "#999")) for level in ["Simple", "Medium", "Complex", "Custom"]]
    donut_html = _svg_donut(slices)
    complexity_card = _card("Complexity Distribution", donut_html, "📊")

    # ----- Transformation frequency bar chart -----
    tx_freq = {}
    for m in mappings:
        for t in m.get("transformations", []):
            tx_freq[t] = tx_freq.get(t, 0) + 1
    tx_sorted = sorted(tx_freq.items(), key=lambda x: -x[1])
    tx_bar = _svg_bar([(t, c, COLORS["primary"]) for t, c in tx_sorted[:15]])
    tx_card = _card("Transformation Types (Top 15)", tx_bar, "🔄")

    # ----- Mapping table -----
    mapping_rows = []
    for m in mappings:
        txs = ", ".join(m.get("transformations", []))
        srcs = ", ".join(m.get("sources", []))
        tgts = ", ".join(m.get("targets", []))
        sql_flag = "Yes" if m.get("has_sql_override") else ""
        mplt_flag = "Yes" if m.get("has_mapplet") else ""
        mapping_rows.append([m["name"], m.get("complexity", "?"), srcs, tgts, txs, sql_flag, mplt_flag])
    mapping_table = _table(
        ["Mapping", "Complexity", "Sources", "Targets", "Transformations", "SQL Override", "Mapplet"],
        mapping_rows, highlight_col=1
    )
    mapping_card = _card(f"Mappings ({len(mappings)})", mapping_table, "📓")

    # ----- Workflow table -----
    wf_rows = []
    for wf in workflows:
        sessions = ", ".join(wf.get("sessions", []))
        deps_count = sum(len(v) for v in wf.get("dependencies", {}).values())
        decision = "Yes" if wf.get("has_decision") else ""
        schedule = wf.get("schedule", "")
        wf_rows.append([wf["name"], len(wf.get("sessions", [])), deps_count, decision, schedule, sessions])
    wf_table = _table(["Workflow", "Sessions", "Dependencies", "Decision", "Schedule", "Session List"], wf_rows)
    wf_card = _card(f"Workflows ({len(workflows)})", wf_table, "⚡")

    # ----- SQL files table -----
    sql_rows = []
    for sf in sql_files:
        ora = [c["construct"] for c in sf.get("oracle_constructs", [])]
        mssql = [c["construct"] for c in sf.get("sqlserver_constructs", [])]
        total_constructs = sum(c["occurrences"] for c in sf.get("oracle_constructs", [])) + \
                           sum(c["occurrences"] for c in sf.get("sqlserver_constructs", []))
        sql_rows.append([
            sf.get("file", "?"),
            sf.get("db_type", "?").upper(),
            sf.get("total_lines", 0),
            total_constructs,
            ", ".join(ora[:8]) + ("..." if len(ora) > 8 else ""),
            ", ".join(mssql[:8]) + ("..." if len(mssql) > 8 else ""),
        ])
    sql_table = _table(["File", "DB Type", "Lines", "Constructs", "Oracle Patterns", "SQL Server Patterns"], sql_rows)
    sql_card = _card(f"SQL Files ({len(sql_files)})", sql_table, "🗄️")

    # ----- SQL construct frequency -----
    construct_freq = {}
    for sf in sql_files:
        for c in sf.get("oracle_constructs", []):
            key = f"Oracle: {c['construct']}"
            construct_freq[key] = construct_freq.get(key, 0) + c["occurrences"]
        for c in sf.get("sqlserver_constructs", []):
            key = f"MSSQL: {c['construct']}"
            construct_freq[key] = construct_freq.get(key, 0) + c["occurrences"]
    construct_sorted = sorted(construct_freq.items(), key=lambda x: -x[1])[:20]
    construct_bar = _svg_bar(
        [(k, v, COLORS["Oracle"] if k.startswith("Oracle") else COLORS["sqlserver"]) for k, v in construct_sorted],
        max_width=300
    )
    construct_card = _card("SQL Construct Frequency (Top 20)", construct_bar, "📈")

    # ----- Connections table -----
    conn_rows = []
    for c in connections:
        conn_rows.append([
            c.get("name", "?"),
            c.get("type", "?"),
            c.get("subtype", c.get("schema", "")),
            c.get("connect_string", c.get("host", "")),
        ])
    conn_table = _table(["Name", "Type", "Subtype / Schema", "Connection String / Host"], conn_rows)
    conn_card = _card(f"Connections ({len(connections)})", conn_table, "🔌")

    # ----- Parameter files -----
    param_html = ""
    if param_files:
        param_rows = []
        for pf in param_files:
            sections = list(pf.get("sections", {}).keys())
            param_rows.append([pf["file"], pf.get("total_params", 0), ", ".join(sections)])
        param_html = _card(
            f"Parameter Files ({len(param_files)})",
            _table(["File", "Total Params", "Sections"], param_rows),
            "📋"
        )

    # ----- Mapplets -----
    mapplet_html = ""
    if mapplets:
        mplt_rows = [[name, ", ".join(txs), len(txs)] for name, txs in mapplets.items()]
        mapplet_html = _card(
            f"Mapplets ({len(mapplets)})",
            _table(["Mapplet", "Transformations", "Count"], mplt_rows),
            "🧩"
        )

    # ----- Cross-mapping lineage summary -----
    cross_lineage = _build_cross_mapping_lineage(mappings)
    cross_rows = []
    for cl in cross_lineage:
        cross_rows.append([
            cl["source"],
            ", ".join(cl["mappings"]),
            ", ".join(cl["targets"]),
            len(cl["mappings"]),
        ])
    cross_table = _table(["Source Table", "Used By Mappings", "Feeds Targets", "# Mappings"], cross_rows)
    cross_card = _card(f"Source → Mapping → Target Lineage ({len(cross_lineage)} sources)", cross_table, "🔗")

    # ----- Per-mapping lineage flow diagrams -----
    lineage_items = []
    mappings_with_lineage = 0
    total_field_paths = 0
    for m in mappings:
        fl = m.get("field_lineage", [])
        total_field_paths += len(fl)
        if fl:
            mappings_with_lineage += 1
        flow_svg = _svg_lineage_flow(m["name"], fl)
        field_count = len(fl)
        toggle_id = f"lin_{escape(m['name']).replace(' ', '_')}"
        lineage_items.append(
            f'<div>'
            f'<label class="lineage-mapping-title" for="{toggle_id}">'
            f'▸ {escape(m["name"])} '
            f'{_badge(m.get("complexity", "?"), COLORS.get(m.get("complexity", ""), COLORS["muted"]))}'
            f'</label>'
            f'<span class="lineage-stats"> — {field_count} field paths, '
            f'score {m.get("conversion_score", 0)}/100</span>'
            f'<input type="checkbox" class="lineage-toggle" id="{toggle_id}"/>'
            f'<div class="lineage-detail">'
            f'<div class="lineage-flow">{flow_svg}</div>'
            f'</div></div>'
        )
    lineage_body = "".join(lineage_items)
    lineage_kpi = (
        f'<div style="display:flex;gap:24px;margin-bottom:16px;">'
        f'<div class="kpi" style="flex:1;"><div class="number">{mappings_with_lineage}</div>'
        f'<div class="label">Mappings with Lineage</div></div>'
        f'<div class="kpi" style="flex:1;"><div class="number">{total_field_paths}</div>'
        f'<div class="label">Total Field Paths</div></div>'
        f'<div class="kpi" style="flex:1;"><div class="number">{len(cross_lineage)}</div>'
        f'<div class="label">Source Tables</div></div>'
        f'</div>'
    )
    lineage_card = _card(
        f"Per-Mapping Lineage ({mappings_with_lineage}/{len(mappings)} have field-level lineage)",
        lineage_kpi + '<p style="font-size:12px;color:#6C757D;margin-bottom:12px;">'
        'Click a mapping name to expand its data flow diagram.</p>' + lineage_body,
        "🔍"
    )

    # ----- Assemble -----
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Assessment Report — Informatica to Fabric</title>
{CSS}
</head>
<body>
<div class="header">
  <h1>🔍 Assessment Report</h1>
  <div class="subtitle">Informatica to Microsoft Fabric Migration &mdash; Generated {escape(now)}</div>
</div>
<div class="container">
  <div class="grid">{kpi_html}</div>
  <div class="grid" style="grid-template-columns: 1fr 1fr;">
    {complexity_card}
    {tx_card}
  </div>
  <h2 class="section-title">Mappings</h2>
  {mapping_card}
  <h2 class="section-title">Workflows</h2>
  {wf_card}
  <h2 class="section-title">SQL Analysis</h2>
  <div class="grid" style="grid-template-columns: 1fr 1fr;">
    {sql_card}
    {construct_card}
  </div>
  <h2 class="section-title">Infrastructure</h2>
  {conn_card}
  {param_html}
  {mapplet_html}
  <h2 class="section-title">Data Lineage</h2>
  {cross_card}
  {lineage_card}
</div>
<div class="footer">
  Informatica-to-Fabric Assessment Report &bull; {escape(now)} &bull; Source: inventory.json
</div>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    return out_path


# ─────────────────────────────────────────────
#  Migration Report
# ─────────────────────────────────────────────

def _artifact_status(output_dir, prefix, ext):
    """Scan output folders for generated artifacts and return list of (name, exists)."""
    folder = output_dir / prefix
    if not folder.exists():
        return []
    files = sorted(folder.glob(f"*{ext}"))
    return [f.stem for f in files if f.name != ".gitkeep"]


def generate_migration_report(inv, out_path):
    """Generate migration_report.html showing migration readiness and artifact status."""
    summary = inv.get("summary", {})
    mappings = inv.get("mappings", [])
    workflows = inv.get("workflows", [])
    sql_files = inv.get("sql_files", [])
    cb = summary.get("complexity_breakdown", {})
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    output_root = WORKSPACE / "output"

    # Discover generated artifacts
    notebooks = _artifact_status(output_root, "notebooks", ".py")
    pipelines = _artifact_status(output_root, "pipelines", ".json")
    sql_converted = _artifact_status(output_root, "sql", ".sql")
    validations = _artifact_status(output_root, "validation", ".py")

    # Build mapping migration status
    mapping_status = []
    for m in mappings:
        name = m["name"]
        nb_name = f"NB_{name}"
        has_nb = nb_name in notebooks
        # Check if any validation covers this mapping's targets
        has_val = any(t.upper() in [v.replace("VAL_", "").upper() for v in validations] for t in m.get("targets", []))
        status = "Ready" if has_nb else "Pending"
        mapping_status.append({
            "name": name,
            "complexity": m.get("complexity", "?"),
            "has_notebook": has_nb,
            "has_validation": has_val,
            "status": status,
        })

    # Build workflow migration status
    wf_status = []
    for wf in workflows:
        name = wf["name"]
        pl_name = f"PL_{name}"
        has_pl = pl_name in pipelines
        status = "Ready" if has_pl else "Pending"
        wf_status.append({
            "name": name,
            "sessions": len(wf.get("sessions", [])),
            "has_pipeline": has_pl,
            "status": status,
        })

    # Build SQL migration status
    sql_status = []
    for sf in sql_files:
        fname = sf.get("file", "")
        stem = Path(fname).stem
        converted_name = f"SQL_{stem}"
        has_converted = converted_name in sql_converted
        sql_status.append({
            "file": fname,
            "db_type": sf.get("db_type", "?").upper(),
            "has_converted": has_converted,
            "status": "Converted" if has_converted else "Pending",
        })

    # ----- KPIs -----
    total_mappings = len(mappings)
    notebooks_done = sum(1 for ms in mapping_status if ms["has_notebook"])
    pipelines_done = sum(1 for ws in wf_status if ws["has_pipeline"])
    sql_done = sum(1 for ss in sql_status if ss["has_converted"])
    val_done = len(validations)

    overall_items = total_mappings + len(workflows) + len(sql_files)
    overall_done = notebooks_done + pipelines_done + sql_done
    overall_pct = int(overall_done / overall_items * 100) if overall_items else 0

    kpis = [
        (f"{overall_pct}%", "Overall Progress"),
        (f"{notebooks_done}/{total_mappings}", "Notebooks"),
        (f"{pipelines_done}/{len(workflows)}", "Pipelines"),
        (f"{sql_done}/{len(sql_files)}", "SQL Converted"),
        (str(val_done), "Validation Scripts"),
    ]
    kpi_html = "".join(
        f'<div class="kpi"><div class="number">{v}</div><div class="label">{l}</div></div>'
        for v, l in kpis
    )

    # ----- Progress donut -----
    done_count = overall_done
    pending_count = overall_items - overall_done
    slices = [("Completed", done_count, "#27AE60"), ("Pending", pending_count, "#DEE2E6")]
    donut = _svg_donut(slices)
    progress_card = _card("Migration Progress", donut, "📊")

    # ----- Effort estimation donut -----
    effort_slices = [
        (level, cb.get(level, 0), COLORS.get(level, "#999"))
        for level in ["Simple", "Medium", "Complex", "Custom"]
    ]
    effort_donut = _svg_donut(effort_slices)
    effort_card = _card("Estimated Effort by Complexity", effort_donut, "⚙️")

    # ----- Mapping status table -----
    ms_rows = []
    for ms in mapping_status:
        nb_badge = _badge("✓ Generated", "#27AE60") if ms["has_notebook"] else _badge("Pending", "#F39C12")
        val_badge = _badge("✓ Exists", "#27AE60") if ms["has_validation"] else _badge("—", "#95A5A6")
        cx_badge = _badge(ms["complexity"], COLORS.get(ms["complexity"], "#999"))
        ms_rows.append(f"<tr><td>{escape(ms['name'])}</td><td>{cx_badge}</td><td>{nb_badge}</td><td>{val_badge}</td></tr>")
    ms_table = (
        '<table><thead><tr><th>Mapping</th><th>Complexity</th><th>Notebook</th><th>Validation</th></tr></thead>'
        f'<tbody>{"".join(ms_rows)}</tbody></table>'
    )
    ms_card = _card(f"Mapping Migration Status ({notebooks_done}/{total_mappings})", ms_table, "📓")

    # ----- Workflow status table -----
    ws_rows = []
    for ws in wf_status:
        pl_badge = _badge("✓ Generated", "#27AE60") if ws["has_pipeline"] else _badge("Pending", "#F39C12")
        ws_rows.append(f"<tr><td>{escape(ws['name'])}</td><td>{ws['sessions']}</td><td>{pl_badge}</td></tr>")
    ws_table = (
        '<table><thead><tr><th>Workflow</th><th>Sessions</th><th>Pipeline</th></tr></thead>'
        f'<tbody>{"".join(ws_rows)}</tbody></table>'
    )
    ws_card = _card(f"Workflow Migration Status ({pipelines_done}/{len(workflows)})", ws_table, "⚡")

    # ----- SQL status table -----
    ss_rows = []
    for ss in sql_status:
        s_badge = _badge("✓ Converted", "#27AE60") if ss["has_converted"] else _badge("Pending", "#F39C12")
        ss_rows.append(f"<tr><td>{escape(ss['file'])}</td><td>{escape(ss['db_type'])}</td><td>{s_badge}</td></tr>")
    ss_table = (
        '<table><thead><tr><th>SQL File</th><th>DB Type</th><th>Status</th></tr></thead>'
        f'<tbody>{"".join(ss_rows)}</tbody></table>'
    )
    ss_card = _card(f"SQL Migration Status ({sql_done}/{len(sql_files)})", ss_table, "🗄️")

    # ----- Validation list -----
    val_rows = "".join(
        f"<tr><td>{escape(v)}</td><td>{_badge('✓ Ready', '#27AE60')}</td></tr>"
        for v in validations
    )
    val_table = (
        '<table><thead><tr><th>Validation Script</th><th>Status</th></tr></thead>'
        f'<tbody>{val_rows}</tbody></table>'
    )
    val_card = _card(f"Validation Scripts ({val_done})", val_table, "✅")

    # ----- Action items -----
    actions = []
    for ms in mapping_status:
        if not ms["has_notebook"]:
            actions.append(("High", f"Generate notebook for mapping <b>{escape(ms['name'])}</b>", "Notebook"))
    for ws in wf_status:
        if not ws["has_pipeline"]:
            actions.append(("High", f"Generate pipeline for workflow <b>{escape(ws['name'])}</b>", "Pipeline"))
    for ss in sql_status:
        if not ss["has_converted"]:
            actions.append(("Medium", f"Convert SQL file <b>{escape(ss['file'])}</b> ({ss['db_type']})", "SQL"))
    for ms in mapping_status:
        if ms["has_notebook"] and not ms["has_validation"]:
            actions.append(("Low", f"Generate validation for mapping <b>{escape(ms['name'])}</b>", "Validation"))

    if actions:
        action_rows = "".join(
            f'<tr><td>{_badge(p, "#E74C3C" if p == "High" else "#F39C12" if p == "Medium" else "#27AE60")}</td>'
            f'<td>{desc}</td><td>{escape(cat)}</td></tr>'
            for p, desc, cat in actions
        )
        action_table = (
            '<table><thead><tr><th>Priority</th><th>Action</th><th>Category</th></tr></thead>'
            f'<tbody>{action_rows}</tbody></table>'
        )
        action_card = _card(f"Action Items ({len(actions)})", action_table, "🚀")
    else:
        action_card = _card("Action Items", '<p style="color:#27AE60;font-weight:600;">✅ All artifacts generated — ready for Fabric deployment!</p>', "🚀")

    # ----- Assemble -----
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Migration Report — Informatica to Fabric</title>
{CSS}
</head>
<body>
<div class="header" style="background:linear-gradient(135deg, #27AE60 0%, #1E8449 100%);">
  <h1>📋 Migration Report</h1>
  <div class="subtitle">Informatica to Microsoft Fabric &mdash; Generated {escape(now)}</div>
</div>
<div class="container">
  <div class="grid">{kpi_html}</div>
  <div class="grid" style="grid-template-columns: 1fr 1fr;">
    {progress_card}
    {effort_card}
  </div>
  {action_card}
  <h2 class="section-title">Mapping Status</h2>
  {ms_card}
  <h2 class="section-title">Workflow Status</h2>
  {ws_card}
  <h2 class="section-title">SQL Conversion Status</h2>
  {ss_card}
  <h2 class="section-title">Validation</h2>
  {val_card}
</div>
<div class="footer">
  Informatica-to-Fabric Migration Report &bull; {escape(now)} &bull; Source: inventory.json + output/
</div>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    return out_path


# ─────────────────────────────────────────────
#  Lineage Report
# ─────────────────────────────────────────────

def generate_lineage_report(inv, out_path):
    """Generate a standalone lineage_report.html with cross-mapping and per-mapping lineage."""
    mappings = inv.get("mappings", [])
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Cross-mapping lineage
    cross_lineage = _build_cross_mapping_lineage(mappings)
    cross_rows = []
    for cl in cross_lineage:
        cross_rows.append([
            cl["source"],
            ", ".join(cl["mappings"]),
            ", ".join(cl["targets"]),
            len(cl["mappings"]),
        ])
    cross_table = _table(["Source Table", "Used By Mappings", "Feeds Targets", "# Mappings"], cross_rows)
    cross_card = _card(f"Source → Target Lineage ({len(cross_lineage)} source tables)", cross_table, "🔗")

    # Target-centric view
    target_to_mappings = {}
    for m in mappings:
        for t in m.get("targets", []):
            target_to_mappings.setdefault(t, set()).add(m["name"])
    target_rows = [[t, ", ".join(sorted(ms)), len(ms)] for t, ms in sorted(target_to_mappings.items())]
    target_table = _table(["Target Table", "Fed By Mappings", "# Mappings"], target_rows)
    target_card = _card(f"Target Tables ({len(target_rows)})", target_table, "🎯")

    # Per-mapping detail
    lineage_items = []
    mappings_with_lineage = 0
    total_field_paths = 0
    for m in mappings:
        fl = m.get("field_lineage", [])
        total_field_paths += len(fl)
        if fl:
            mappings_with_lineage += 1
        flow_svg = _svg_lineage_flow(m["name"], fl)
        field_count = len(fl)
        toggle_id = f"lr_{escape(m['name']).replace(' ', '_')}"

        # Field-level detail table (collapsed)
        field_rows = []
        for entry in fl[:50]:  # Cap at 50 to keep report size reasonable
            tx_chain = " → ".join(tx["type"][:10] for tx in entry.get("transformations", []))
            field_rows.append([
                entry.get("source_field", ""),
                entry.get("source_instance", ""),
                tx_chain or "-",
                entry.get("target_field", ""),
                entry.get("target_instance", ""),
            ])
        field_table = ""
        if field_rows:
            field_table = _table(
                ["Source Field", "Source Instance", "Transformations", "Target Field", "Target Instance"],
                field_rows
            )
            if len(fl) > 50:
                field_table += f'<p style="color:#6C757D;font-size:12px;">Showing 50 of {len(fl)} field paths.</p>'

        lineage_items.append(
            f'<div style="margin-bottom:8px;">'
            f'<label class="lineage-mapping-title" for="{toggle_id}">'
            f'▸ {escape(m["name"])} '
            f'{_badge(m.get("complexity", "?"), COLORS.get(m.get("complexity", ""), COLORS["muted"]))}'
            f'</label>'
            f'<span class="lineage-stats"> — {field_count} field paths, '
            f'score {m.get("conversion_score", 0)}/100, '
            f'{len(m.get("sources", []))} sources → {len(m.get("targets", []))} targets</span>'
            f'<input type="checkbox" class="lineage-toggle" id="{toggle_id}"/>'
            f'<div class="lineage-detail">'
            f'<div class="lineage-flow">{flow_svg}</div>'
            f'{field_table}'
            f'</div></div>'
        )
    lineage_body = "".join(lineage_items)

    # KPIs
    total_sources = len(cross_lineage)
    total_targets = len(target_to_mappings)
    kpi_html = "".join(f'<div class="kpi"><div class="number">{v}</div><div class="label">{l}</div></div>' for v, l in [
        (len(mappings), "Total Mappings"),
        (mappings_with_lineage, "With Field Lineage"),
        (total_field_paths, "Field Paths Traced"),
        (total_sources, "Source Tables"),
        (total_targets, "Target Tables"),
    ])

    detail_card = _card(
        f"Per-Mapping Lineage Detail ({mappings_with_lineage}/{len(mappings)})",
        '<p style="font-size:12px;color:#6C757D;margin-bottom:12px;">'
        'Click a mapping name to expand its flow diagram and field-level detail.</p>' + lineage_body,
        "🔍"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lineage Report — Informatica to Fabric</title>
{CSS}
</head>
<body>
<div class="header" style="background:linear-gradient(135deg, #8E44AD 0%, #6C3483 100%);">
  <h1>🔗 Data Lineage Report</h1>
  <div class="subtitle">Source → Transformation → Target &mdash; Generated {escape(now)}</div>
</div>
<div class="container">
  <div class="grid">{kpi_html}</div>
  <h2 class="section-title">Cross-Mapping Lineage (Source → Target)</h2>
  {cross_card}
  {target_card}
  <h2 class="section-title">Per-Mapping Lineage Detail</h2>
  {detail_card}
</div>
<div class="footer">
  Informatica-to-Fabric Lineage Report &bull; {escape(now)} &bull; Source: inventory.json
</div>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    return out_path


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────

def main():
    inv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_INVENTORY
    if not inv_path.exists():
        print(f"ERROR: Inventory file not found: {inv_path}")
        print("Run run_assessment.py first to generate inventory.json")
        sys.exit(1)

    with open(inv_path, encoding="utf-8") as f:
        inv = json.load(f)

    print("=" * 50)
    print("  HTML Report Generator")
    print("=" * 50)

    # Assessment report
    assessment_path = OUTPUT_DIR / "assessment_report.html"
    generate_assessment_report(inv, assessment_path)
    print(f"  ✅ Assessment report: {assessment_path}")

    # Migration report
    migration_path = OUTPUT_DIR / "migration_report.html"
    generate_migration_report(inv, migration_path)
    print(f"  ✅ Migration report:  {migration_path}")

    # Lineage report
    lineage_path = OUTPUT_DIR / "lineage_report.html"
    generate_lineage_report(inv, lineage_path)
    print(f"  ✅ Lineage report:    {lineage_path}")

    print()
    print("  Open in browser to view reports.")
    print("=" * 50)


if __name__ == "__main__":
    main()
