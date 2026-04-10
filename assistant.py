"""
Phase 13 — Chat-Based Migration Assistant (Sprint 88)

Conversational assistant for migration Q&A, interactive TODO review,
and context-aware guidance.

Architecture:
  1. Context Builder — aggregates inventory, lineage, TODOs, conversion history
  2. Query Handlers — answers "Why was X flagged?", "Show lineage for Y", etc.
  3. Interactive Review — presents TODO items for accept/modify/skip decisions

Usage:
    python assistant.py                                # interactive mode
    python assistant.py --query "Why was M_LOAD_CUSTOMERS flagged?"
    python assistant.py --review                       # review TODO items
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"


# ─────────────────────────────────────────────
#  Context Builder
# ─────────────────────────────────────────────

def build_context(inventory_path=None):
    """Build comprehensive context from migration artifacts.

    Aggregates:
    - Inventory (mappings, workflows, complexity)
    - Lineage (dependency DAG, table relationships)
    - TODO items from converted SQL
    - Conversion history and pattern store
    """
    inv_path = Path(inventory_path or INVENTORY_PATH)
    context = {
        "inventory": {},
        "mappings": [],
        "workflows": [],
        "todos": [],
        "artifacts": {"notebooks": [], "pipelines": [], "sql": [], "dbt": []},
        "stats": {},
        "lineage": {},
    }

    # Load inventory
    if inv_path.exists():
        with open(inv_path, encoding="utf-8") as f:
            inv = json.load(f)
        context["inventory"] = inv
        context["mappings"] = inv.get("mappings", [])
        context["workflows"] = inv.get("workflows", [])
        context["lineage"] = inv.get("dependency_dag", {})

        # Compute stats
        mappings = inv.get("mappings", [])
        complexities = [m.get("complexity", "Simple") for m in mappings]
        context["stats"] = {
            "total_mappings": len(mappings),
            "total_workflows": len(inv.get("workflows", [])),
            "complexity_distribution": {
                c: complexities.count(c) for c in set(complexities)
            },
        }

    # Scan for TODO markers in SQL output
    sql_dir = WORKSPACE / "output" / "sql"
    if sql_dir.exists():
        for f in sorted(sql_dir.glob("*.sql")):
            content = f.read_text(encoding="utf-8")
            for i, line in enumerate(content.split("\n"), 1):
                if re.search(r'--\s*TODO:', line, re.IGNORECASE):
                    context["todos"].append({
                        "file": f.name, "line": i, "text": line.strip(),
                    })

    # Scan artifacts
    for key, subdir in [("notebooks", "notebooks"), ("pipelines", "pipelines"),
                         ("sql", "sql"), ("dbt", "dbt")]:
        art_dir = WORKSPACE / "output" / subdir
        if art_dir.exists():
            context["artifacts"][key] = sorted([
                f.name for f in art_dir.iterdir() if f.is_file()
            ])

    return context


# ─────────────────────────────────────────────
#  Query Handlers
# ─────────────────────────────────────────────

QUERY_PATTERNS = [
    (r"(?:why|how)\s+(?:was|is)\s+(\S+)\s+flagged", "explain_flagged"),
    (r"(?:what|explain)\s+(?:does|is)\s+(?:this\s+)?(?:sql|mapping)\s+(\S+)", "explain_mapping"),
    (r"(?:show|get|display)\s+lineage\s+(?:for|of)\s+(\S+)", "show_lineage"),
    (r"(?:how|what)\s+(?:should|to)\s+migrate\s+(\S+)", "migration_advice"),
    (r"(?:list|show)\s+todos?", "list_todos"),
    (r"(?:summary|status|overview|progress)", "show_summary"),
    (r"(?:list|show)\s+(?:complex|hard|difficult)", "list_complex"),
]


def handle_query(query, context):
    """Route a natural language query to the appropriate handler.

    Returns a structured response with answer text and metadata.
    """
    query_lower = query.lower().strip()

    for pattern, handler_name in QUERY_PATTERNS:
        match = re.search(pattern, query_lower)
        if match:
            handler = HANDLERS.get(handler_name)
            if handler:
                entity = match.group(1) if match.lastindex else None
                return handler(entity, context)

    # Default: try to find relevant information
    return _default_handler(query, context)


def _explain_flagged(entity, context):
    """Explain why a mapping/table was flagged as TODO or complex."""
    mapping = _find_mapping(entity, context)
    if not mapping:
        return {"answer": f"No mapping found matching '{entity}'.", "found": False}

    reasons = []
    if mapping.get("complexity") == "Complex":
        reasons.append("High complexity classification")
    if mapping.get("has_custom_sql"):
        reasons.append("Contains custom SQL overrides")
    if mapping.get("unsupported_transforms"):
        reasons.append(f"Unsupported transforms: {', '.join(mapping['unsupported_transforms'])}")

    # Check for TODOs in this mapping's SQL
    mapping_todos = [t for t in context["todos"]
                     if entity.upper() in t["file"].upper()]
    if mapping_todos:
        reasons.append(f"{len(mapping_todos)} TODO markers in converted SQL")

    answer = f"**{mapping['name']}** was flagged because:\n" + "\n".join(
        f"  - {r}" for r in reasons
    ) if reasons else f"**{mapping.get('name', entity)}** has no specific flags."

    return {"answer": answer, "found": True, "mapping": mapping["name"],
            "reasons": reasons}


def _explain_mapping(entity, context):
    """Explain what a mapping does."""
    mapping = _find_mapping(entity, context)
    if not mapping:
        return {"answer": f"No mapping found matching '{entity}'.", "found": False}

    sources = mapping.get("sources", [])
    targets = mapping.get("targets", [])
    transforms = mapping.get("transformations", [])
    complexity = mapping.get("complexity", "Unknown")

    answer = (f"**{mapping['name']}** ({complexity} complexity)\n"
              f"  Sources: {', '.join(sources) if sources else 'N/A'}\n"
              f"  Targets: {', '.join(targets) if targets else 'N/A'}\n"
              f"  Transforms: {len(transforms)} steps")

    return {"answer": answer, "found": True, "mapping": mapping}


def _show_lineage(entity, context):
    """Show lineage (upstream/downstream) for a table or mapping."""
    dag = context.get("lineage", {})
    entity_upper = entity.upper() if entity else ""

    # Search in DAG
    node = dag.get(entity_upper) or dag.get(entity) or {}
    if not node:
        # Search partial match
        for key in dag:
            if entity_upper in key.upper():
                node = dag[key]
                entity = key
                break

    if not node:
        return {"answer": f"No lineage data found for '{entity}'.", "found": False}

    upstream = node.get("upstream", [])
    downstream = node.get("downstream", [])

    answer = (f"**Lineage for {entity}:**\n"
              f"  Upstream: {', '.join(upstream) if upstream else 'None (source)'}\n"
              f"  Downstream: {', '.join(downstream) if downstream else 'None (leaf)'}")

    return {"answer": answer, "found": True,
            "upstream": upstream, "downstream": downstream}


def _migration_advice(entity, context):
    """Provide migration advice for a specific mapping."""
    mapping = _find_mapping(entity, context)
    if not mapping:
        return {"answer": f"No mapping found matching '{entity}'.", "found": False}

    complexity = mapping.get("complexity", "Simple")
    advice = []

    if complexity == "Simple":
        advice.append("This mapping should convert automatically via regex engine.")
    elif complexity == "Medium":
        advice.append("This mapping may need manual review of some transformations.")
    else:
        advice.append("This mapping requires careful manual review — consider AI-assisted conversion.")

    if mapping.get("has_custom_sql"):
        advice.append("Custom SQL detected — run `--ai-assist` for LLM-powered conversion.")

    if mapping.get("cdc"):
        advice.append("CDC pattern detected — will generate MERGE INTO with change tracking.")

    answer = f"**Migration advice for {mapping['name']}:**\n" + "\n".join(
        f"  - {a}" for a in advice
    )

    return {"answer": answer, "found": True, "advice": advice}


def _list_todos(entity, context):
    """List all TODO items."""
    todos = context.get("todos", [])
    if not todos:
        return {"answer": "No TODO items found. All conversions are complete!", "found": True,
                "count": 0}

    lines = [f"**{len(todos)} TODO items:**"]
    for t in todos[:20]:
        lines.append(f"  - `{t['file']}` line {t['line']}: {t['text'][:80]}")
    if len(todos) > 20:
        lines.append(f"  ... and {len(todos) - 20} more")

    return {"answer": "\n".join(lines), "found": True, "count": len(todos)}


def _show_summary(entity, context):
    """Show migration summary/progress."""
    stats = context.get("stats", {})
    arts = context.get("artifacts", {})
    todos = context.get("todos", [])

    answer = (f"**Migration Summary:**\n"
              f"  Mappings: {stats.get('total_mappings', 0)}\n"
              f"  Workflows: {stats.get('total_workflows', 0)}\n"
              f"  Complexity: {stats.get('complexity_distribution', {})}\n"
              f"  Notebooks: {len(arts.get('notebooks', []))}\n"
              f"  Pipelines: {len(arts.get('pipelines', []))}\n"
              f"  SQL files: {len(arts.get('sql', []))}\n"
              f"  DBT models: {len(arts.get('dbt', []))}\n"
              f"  TODOs remaining: {len(todos)}")

    return {"answer": answer, "found": True, "stats": stats}


def _list_complex(entity, context):
    """List complex mappings that need attention."""
    complex_mappings = [m for m in context.get("mappings", [])
                        if m.get("complexity") in ("Complex", "Custom")]
    if not complex_mappings:
        return {"answer": "No complex mappings found.", "found": True, "count": 0}

    lines = [f"**{len(complex_mappings)} complex mappings:**"]
    for m in complex_mappings[:20]:
        lines.append(f"  - **{m['name']}** ({m.get('complexity')}) — "
                     f"sources: {', '.join(m.get('sources', []))}")

    return {"answer": "\n".join(lines), "found": True, "count": len(complex_mappings)}


def _default_handler(query, context):
    """Default handler — tries to find any relevant information."""
    # Search mappings by name
    for m in context.get("mappings", []):
        if any(word.upper() in m["name"].upper() for word in query.split() if len(word) > 3):
            return _explain_mapping(m["name"], context)

    return {
        "answer": ("I can help with:\n"
                   "  - 'Why was M_MAPPING_NAME flagged?'\n"
                   "  - 'Show lineage for TABLE_NAME'\n"
                   "  - 'How should I migrate M_MAPPING_NAME?'\n"
                   "  - 'List todos'\n"
                   "  - 'Summary'"),
        "found": False,
    }


# Handler registry
HANDLERS = {
    "explain_flagged": _explain_flagged,
    "explain_mapping": _explain_mapping,
    "show_lineage": _show_lineage,
    "migration_advice": _migration_advice,
    "list_todos": _list_todos,
    "show_summary": _show_summary,
    "list_complex": _list_complex,
}


def _find_mapping(name, context):
    """Find a mapping by exact or partial name match."""
    if not name:
        return None
    name_upper = name.upper()
    for m in context.get("mappings", []):
        if m["name"].upper() == name_upper or name_upper in m["name"].upper():
            return m
    return None


# ─────────────────────────────────────────────
#  Interactive Review Mode
# ─────────────────────────────────────────────

def interactive_review(context, pattern_store=None):
    """Present TODO items one-by-one for user review.

    Returns list of review decisions.
    """
    from ai_converter import PatternStore, generate_suggestions

    store = pattern_store or PatternStore()
    todos = context.get("todos", [])
    decisions = []

    for i, todo in enumerate(todos):
        suggestions = generate_suggestions(todo["text"], store)
        decision = {
            "todo": todo,
            "suggestions": suggestions,
            "action": "pending",
        }
        decisions.append(decision)

    return decisions


def apply_review_decisions(decisions, pattern_store=None):
    """Apply accepted review decisions to the SQL files.

    Returns summary of applied changes.
    """
    from ai_converter import PatternStore

    store = pattern_store or PatternStore()
    applied = 0
    skipped = 0

    for decision in decisions:
        if decision.get("action") == "accept" and decision.get("selected_suggestion"):
            suggestion = decision["selected_suggestion"]
            todo = decision["todo"]
            # Apply fix
            sql_path = WORKSPACE / "output" / "sql" / todo["file"]
            if sql_path.exists():
                lines = sql_path.read_text(encoding="utf-8").split("\n")
                if 0 < todo["line"] <= len(lines):
                    lines[todo["line"] - 1] = suggestion["converted_sql"] + "  -- Reviewed & accepted"
                    sql_path.write_text("\n".join(lines), encoding="utf-8")
                    store.add_pair(todo["text"], suggestion["converted_sql"],
                                   suggestion["confidence"])
                    applied += 1
                    continue
        skipped += 1

    return {"applied": applied, "skipped": skipped, "total": len(decisions)}


# ─────────────────────────────────────────────
#  Main entry point
# ─────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Migration Assistant (Phase 13)")
    parser.add_argument("--query", help="Ask a question about the migration")
    parser.add_argument("--review", action="store_true", help="Interactive TODO review mode")
    parser.add_argument("--inventory", default=None, help="Path to inventory.json")
    args = parser.parse_args()

    context = build_context(args.inventory)

    if args.query:
        result = handle_query(args.query, context)
        print(result["answer"])
    elif args.review:
        decisions = interactive_review(context)
        print(f"📋 {len(decisions)} TODO items ready for review")
        for d in decisions[:5]:
            print(f"  - {d['todo']['file']}:{d['todo']['line']} "
                  f"({len(d['suggestions'])} suggestions)")
    else:
        result = handle_query("summary", context)
        print(result["answer"])


if __name__ == "__main__":
    main()
