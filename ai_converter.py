"""
Phase 13 — AI-Assisted SQL Conversion (Sprints 86–87)

Provides LLM-powered SQL conversion for complex patterns that defeat regex,
intelligent gap resolution via pattern learning, and confidence scoring.

Architecture:
  1. LLM Conversion Client — Azure OpenAI / GPT-4 fallback for TODO SQL markers
  2. Confidence Scoring — Syntax check, semantic match, score 0-100
  3. Pattern Learning — Learn from successful conversions, suggest fixes for new gaps
  4. Gap Severity Ranking — Prioritize TODOs by business impact
  5. Cost Guardrails — Token budget, caching, batch limits

Usage:
    python ai_converter.py                                  # process all TODOs
    python ai_converter.py --inventory path/to/inventory.json
    python ai_converter.py --dry-run                        # preview without LLM calls
"""

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent
OUTPUT_DIR = WORKSPACE / "output" / "sql"
INVENTORY_PATH = WORKSPACE / "output" / "inventory" / "inventory.json"
PATTERN_STORE_PATH = WORKSPACE / "output" / "ai" / "pattern_store.json"
CONVERSION_CACHE_PATH = WORKSPACE / "output" / "ai" / "conversion_cache.json"


# ─────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────

DEFAULT_CONFIG = {
    "api_endpoint": os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
    "api_key": os.environ.get("AZURE_OPENAI_KEY", ""),
    "deployment_name": os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4"),
    "api_version": "2024-02-15-preview",
    "max_tokens_per_request": 2000,
    "max_tokens_budget": 100000,
    "temperature": 0.1,
    "confidence_threshold": 80,
    "max_retries": 3,
    "retry_delay_seconds": 2,
    "target_dialect": os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric"),
}


# ─────────────────────────────────────────────
#  Prompt Engineering
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert SQL migration engineer. Convert the given source SQL
(Oracle/SQL Server/Teradata/DB2) to the specified target dialect.

Rules:
- Output ONLY valid SQL — no explanations, no markdown fences
- Preserve exact column aliases and table references
- Use target-native functions (no Oracle-specific syntax in output)
- If unsure about a conversion, add a `-- REVIEW:` comment on that line
- Never invent table or column names not present in the source"""

CONVERSION_PROMPT_TEMPLATE = """Convert this {source_dialect} SQL to {target_dialect}:

Source SQL:
```
{source_sql}
```

Context:
- Source tables: {source_tables}
- Target tables: {target_tables}
- Known functions needing conversion: {known_functions}

Output only the converted SQL."""


def build_conversion_prompt(source_sql, source_dialect="Oracle",
                            target_dialect="Spark SQL",
                            source_tables=None, target_tables=None):
    """Build a structured prompt for LLM SQL conversion."""
    # Extract function names from source SQL for context
    func_pattern = r'\b([A-Z_]+)\s*\('
    known_functions = list(set(re.findall(func_pattern, source_sql.upper())))

    return CONVERSION_PROMPT_TEMPLATE.format(
        source_dialect=source_dialect,
        target_dialect=target_dialect,
        source_sql=source_sql.strip(),
        source_tables=", ".join(source_tables or ["(unknown)"]),
        target_tables=", ".join(target_tables or ["(unknown)"]),
        known_functions=", ".join(known_functions[:20]) or "(none)",
    )


# ─────────────────────────────────────────────
#  LLM Client (Azure OpenAI)
# ─────────────────────────────────────────────

class LLMClient:
    """Azure OpenAI client with retry, caching, and cost guardrails."""

    def __init__(self, config=None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.total_tokens_used = 0
        self._cache = self._load_cache()

    def _load_cache(self):
        """Load conversion cache from disk."""
        if CONVERSION_CACHE_PATH.exists():
            with open(CONVERSION_CACHE_PATH, encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        """Save conversion cache to disk."""
        CONVERSION_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CONVERSION_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, indent=2)

    def _cache_key(self, prompt):
        """Generate a cache key from prompt content."""
        return hashlib.sha256(prompt.encode()).hexdigest()[:16]

    def _check_budget(self, estimated_tokens):
        """Check if token budget allows this request."""
        budget = self.config["max_tokens_budget"]
        if self.total_tokens_used + estimated_tokens > budget:
            return False, {
                "error": "token_budget_exceeded",
                "used": self.total_tokens_used,
                "budget": budget,
                "requested": estimated_tokens,
            }
        return True, None

    def convert_sql(self, source_sql, source_dialect="Oracle",
                    target_dialect="Spark SQL", context=None):
        """Convert SQL using LLM with caching and cost guardrails.

        Returns dict with: converted_sql, confidence, tokens_used, cached, model
        """
        prompt = build_conversion_prompt(
            source_sql, source_dialect, target_dialect,
            source_tables=(context or {}).get("source_tables"),
            target_tables=(context or {}).get("target_tables"),
        )

        # Check cache
        key = self._cache_key(prompt)
        if key in self._cache:
            cached = self._cache[key]
            cached["cached"] = True
            return cached

        # Estimate tokens (~4 chars per token)
        estimated_tokens = len(prompt) // 4 + self.config["max_tokens_per_request"]
        ok, err = self._check_budget(estimated_tokens)
        if not ok:
            return {"converted_sql": None, "confidence": 0, "error": err,
                    "tokens_used": 0, "cached": False, "model": None}

        # Call LLM (or return mock for testing without API key)
        result = self._call_llm(prompt)
        result["cached"] = False

        # Cache successful results
        if result.get("converted_sql"):
            self._cache[key] = {
                "converted_sql": result["converted_sql"],
                "confidence": result["confidence"],
                "tokens_used": result["tokens_used"],
                "model": result.get("model"),
            }
            self._save_cache()

        return result

    def _call_llm(self, prompt):
        """Call Azure OpenAI API with retries. Falls back to local heuristic if no API key."""
        endpoint = self.config["api_endpoint"]
        api_key = self.config["api_key"]

        if not endpoint or not api_key:
            # No API configured — use local heuristic fallback
            return self._local_fallback(prompt)

        # Azure OpenAI REST API call
        import urllib.request
        import urllib.error

        url = (f"{endpoint}/openai/deployments/{self.config['deployment_name']}"
               f"/chat/completions?api-version={self.config['api_version']}")

        body = json.dumps({
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": self.config["max_tokens_per_request"],
            "temperature": self.config["temperature"],
        }).encode()

        headers = {
            "Content-Type": "application/json",
            "api-key": api_key,
        }

        for attempt in range(self.config["max_retries"]):
            try:
                req = urllib.request.Request(url, data=body, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())

                content = data["choices"][0]["message"]["content"].strip()
                tokens = data.get("usage", {}).get("total_tokens", 0)
                self.total_tokens_used += tokens

                # Strip markdown fences if present
                content = re.sub(r'^```(?:sql)?\n?', '', content)
                content = re.sub(r'\n?```$', '', content)

                confidence = score_confidence(content)
                return {
                    "converted_sql": content,
                    "confidence": confidence,
                    "tokens_used": tokens,
                    "model": self.config["deployment_name"],
                }

            except (urllib.error.URLError, urllib.error.HTTPError, KeyError) as e:
                if attempt == self.config["max_retries"] - 1:
                    return {
                        "converted_sql": None,
                        "confidence": 0,
                        "tokens_used": 0,
                        "model": self.config["deployment_name"],
                        "error": str(e),
                    }
                time.sleep(self.config["retry_delay_seconds"])

        return {"converted_sql": None, "confidence": 0, "tokens_used": 0,
                "model": None, "error": "max_retries_exceeded"}

    def _local_fallback(self, prompt):
        """Local heuristic fallback when no LLM API is configured."""
        # Extract source SQL from prompt
        match = re.search(r'Source SQL:\n```\n(.+?)\n```', prompt, re.DOTALL)
        source = match.group(1) if match else ""

        # Apply basic regex conversions
        converted = _apply_basic_conversions(source)
        confidence = score_confidence(converted, source)

        return {
            "converted_sql": converted,
            "confidence": confidence,
            "tokens_used": 0,
            "model": "local_heuristic",
        }

    @property
    def budget_remaining(self):
        return self.config["max_tokens_budget"] - self.total_tokens_used


def _apply_basic_conversions(sql):
    """Apply basic Oracle→Spark SQL conversions as local fallback."""
    rules = [
        (r"\bNVL\b\s*\(", "COALESCE("),
        (r"\bSYSDATE\b", "current_timestamp()"),
        (r"\bSYSTIMESTAMP\b", "current_timestamp()"),
        (r"\bTO_NUMBER\s*\(([^)]+)\)", r"CAST(\1 AS DECIMAL)"),
        (r"\bSUBSTR\s*\(", "SUBSTRING("),
        (r"\bFROM\s+DUAL\b", ""),
        (r"\bVARCHAR2\b", "STRING"),
        (r"\bNUMBER\b", "DECIMAL"),
    ]
    result = sql
    for pattern, replacement in rules:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return result


# ─────────────────────────────────────────────
#  Confidence Scoring
# ─────────────────────────────────────────────

def score_confidence(converted_sql, source_sql=None):
    """Score conversion quality 0-100 based on multiple signals.

    Signals:
    - Syntax validity (balanced parens, no dangling keywords)
    - No leftover source-dialect functions
    - Preserved table/column references
    - No TODO/FIXME markers
    """
    if not converted_sql or not converted_sql.strip():
        return 0

    score = 100

    # Check balanced parentheses
    if converted_sql.count("(") != converted_sql.count(")"):
        score -= 25

    # Check for leftover Oracle functions
    oracle_leftovers = re.findall(
        r'\b(NVL|SYSDATE|SYSTIMESTAMP|DECODE|ROWNUM|ROWID|TO_NUMBER|TO_DATE|TO_CHAR'
        r'|NVL2|LISTAGG|WM_CONCAT|CONNECT\s+BY)\b',
        converted_sql, re.IGNORECASE
    )
    score -= len(oracle_leftovers) * 10

    # Check for TODO / FIXME / REVIEW markers
    todo_count = len(re.findall(r'--\s*(TODO|FIXME|REVIEW):', converted_sql, re.IGNORECASE))
    score -= todo_count * 5

    # Check for empty output
    if len(converted_sql.strip()) < 10:
        score -= 30

    # Bonus: check table references preserved
    if source_sql:
        source_tables = set(re.findall(r'\bFROM\s+(\w+)', source_sql, re.IGNORECASE))
        preserved = sum(1 for t in source_tables if t.lower() in converted_sql.lower())
        if source_tables:
            preservation_ratio = preserved / len(source_tables)
            if preservation_ratio < 0.5:
                score -= 20

    return max(0, min(100, score))


# ─────────────────────────────────────────────
#  Pattern Learning Engine (Sprint 87)
# ─────────────────────────────────────────────

class PatternStore:
    """Stores successful conversion patterns for reuse and learning."""

    def __init__(self, store_path=None):
        self.store_path = Path(store_path or PATTERN_STORE_PATH)
        self.patterns = self._load()

    def _load(self):
        if self.store_path.exists():
            with open(self.store_path, encoding="utf-8") as f:
                return json.load(f)
        return {"pairs": [], "rules": [], "feedback": []}

    def save(self):
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.store_path, "w", encoding="utf-8") as f:
            json.dump(self.patterns, f, indent=2)

    def add_pair(self, source_sql, target_sql, confidence, source_dialect="Oracle",
                 target_dialect="Spark SQL"):
        """Record a successful conversion pair for future pattern matching."""
        self.patterns["pairs"].append({
            "source": source_sql.strip(),
            "target": target_sql.strip(),
            "source_dialect": source_dialect,
            "target_dialect": target_dialect,
            "confidence": confidence,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.save()

    def add_feedback(self, source_sql, suggestion, accepted, user_correction=None):
        """Store user feedback on a suggestion (accept/reject/modify)."""
        entry = {
            "source": source_sql.strip(),
            "suggestion": suggestion.strip(),
            "accepted": accepted,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if user_correction:
            entry["correction"] = user_correction.strip()
            # Also add the corrected version as a new pair
            self.add_pair(source_sql, user_correction, confidence=95)
        self.patterns["feedback"].append(entry)
        self.save()

    def find_similar(self, source_sql, top_k=3):
        """Find the most similar previously-converted patterns.

        Uses token overlap (Jaccard similarity) for fast matching.
        """
        source_tokens = _tokenize(source_sql)
        scored = []
        for pair in self.patterns.get("pairs", []):
            pair_tokens = _tokenize(pair["source"])
            similarity = _jaccard(source_tokens, pair_tokens)
            scored.append({**pair, "similarity": similarity})

        scored.sort(key=lambda x: x["similarity"], reverse=True)
        return scored[:top_k]

    def extract_rules(self):
        """Extract reusable transformation rules from successful pairs."""
        rules = []
        for pair in self.patterns.get("pairs", []):
            # Extract function-level transformations
            src_funcs = set(re.findall(r'\b([A-Z_]+)\s*\(', pair["source"].upper()))
            tgt_funcs = set(re.findall(r'\b([A-Z_]+)\s*\(', pair["target"].upper()))
            new_funcs = tgt_funcs - src_funcs
            removed_funcs = src_funcs - tgt_funcs
            if new_funcs or removed_funcs:
                rules.append({
                    "type": "function_replacement",
                    "removed": sorted(removed_funcs),
                    "added": sorted(new_funcs),
                    "confidence": pair.get("confidence", 0),
                })
        self.patterns["rules"] = rules
        self.save()
        return rules

    @property
    def pair_count(self):
        return len(self.patterns.get("pairs", []))

    @property
    def feedback_count(self):
        return len(self.patterns.get("feedback", []))


def _tokenize(sql):
    """Tokenize SQL for similarity comparison."""
    return set(re.findall(r'\w+', sql.lower()))


def _jaccard(set_a, set_b):
    """Jaccard similarity between two token sets."""
    if not set_a and not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0.0


# ─────────────────────────────────────────────
#  Gap Severity Ranking
# ─────────────────────────────────────────────

def rank_gaps(todo_items, inventory=None):
    """Rank TODO items by business impact for prioritized review.

    Factors:
    - Data volume (row count estimate from source tables)
    - Downstream dependency count
    - Execution frequency (daily > weekly > monthly)
    - Complexity (Complex > Medium > Simple)
    """
    inv = inventory or {}
    mappings_by_name = {m["name"]: m for m in inv.get("mappings", [])}
    dag = inv.get("dependency_dag", {})

    ranked = []
    for item in todo_items:
        severity = 50  # base score

        mapping_name = item.get("mapping", "")
        mapping_meta = mappings_by_name.get(mapping_name, {})

        # Complexity bonus
        complexity = mapping_meta.get("complexity", "Simple")
        severity += {"Complex": 30, "Medium": 15, "Simple": 0}.get(complexity, 0)

        # Downstream dependency count
        downstream = len(dag.get(mapping_name, {}).get("downstream", []))
        severity += min(downstream * 5, 25)

        # Frequency bonus
        freq = mapping_meta.get("schedule", {}).get("frequency", "")
        severity += {"daily": 20, "hourly": 25, "weekly": 10, "monthly": 5}.get(freq, 0)

        ranked.append({**item, "severity": min(100, severity), "complexity": complexity})

    ranked.sort(key=lambda x: x["severity"], reverse=True)
    return ranked


# ─────────────────────────────────────────────
#  Auto-Fix Suggestions
# ─────────────────────────────────────────────

def generate_suggestions(source_sql, pattern_store, llm_client=None, top_k=3):
    """Generate candidate fix suggestions for a TODO SQL block.

    Strategy:
    1. Check pattern store for similar prior conversions
    2. Apply local heuristic
    3. Call LLM if available

    Returns list of suggestions with confidence scores.
    """
    suggestions = []

    # 1. Pattern-based suggestions
    similar = pattern_store.find_similar(source_sql, top_k=top_k)
    for match in similar:
        if match["similarity"] > 0.3:
            suggestions.append({
                "method": "pattern_match",
                "converted_sql": match["target"],
                "confidence": int(match["similarity"] * match.get("confidence", 80)),
                "similar_source": match["source"][:100],
                "similarity": round(match["similarity"], 3),
            })

    # 2. Local heuristic
    local_converted = _apply_basic_conversions(source_sql)
    local_confidence = score_confidence(local_converted, source_sql)
    suggestions.append({
        "method": "local_heuristic",
        "converted_sql": local_converted,
        "confidence": local_confidence,
    })

    # 3. LLM conversion (if client provided and budget allows)
    if llm_client and llm_client.budget_remaining > 0:
        result = llm_client.convert_sql(source_sql)
        if result.get("converted_sql"):
            suggestions.append({
                "method": "llm",
                "converted_sql": result["converted_sql"],
                "confidence": result["confidence"],
                "model": result.get("model"),
                "tokens_used": result.get("tokens_used", 0),
            })

    # Sort by confidence descending
    suggestions.sort(key=lambda x: x["confidence"], reverse=True)
    return suggestions[:top_k]


# ─────────────────────────────────────────────
#  TODO Backfill — process output/sql/ files
# ─────────────────────────────────────────────

def extract_todos(sql_dir=None):
    """Extract all TODO markers from converted SQL files."""
    sql_path = Path(sql_dir or OUTPUT_DIR)
    todos = []

    if not sql_path.exists():
        return todos

    for f in sorted(sql_path.glob("*.sql")):
        content = f.read_text(encoding="utf-8")
        for i, line in enumerate(content.split("\n"), 1):
            match = re.search(r'--\s*TODO:\s*(.+)', line, re.IGNORECASE)
            if match:
                todos.append({
                    "file": f.name,
                    "line": i,
                    "todo_text": match.group(1).strip(),
                    "full_line": line.strip(),
                    "mapping": f.stem.replace("SQL_", "").replace("SQL_OVERRIDES_", ""),
                })

    return todos


def backfill_todos(sql_dir=None, llm_client=None, pattern_store=None, dry_run=False):
    """Re-process all TODO markers through AI conversion.

    Returns summary of processed items with suggestions.
    """
    todos = extract_todos(sql_dir)
    store = pattern_store or PatternStore()
    results = []

    for todo in todos:
        suggestions = generate_suggestions(
            todo["full_line"], store, llm_client=llm_client
        )
        best = suggestions[0] if suggestions else None

        entry = {
            **todo,
            "suggestions": suggestions,
            "best_confidence": best["confidence"] if best else 0,
            "best_method": best["method"] if best else None,
            "applied": False,
        }

        # Auto-apply if high confidence and not dry run
        if best and best["confidence"] >= DEFAULT_CONFIG["confidence_threshold"] and not dry_run:
            entry["applied"] = True
            # Apply to file
            _apply_fix(todo["file"], todo["line"], best["converted_sql"], sql_dir)
            # Record in pattern store
            store.add_pair(todo["full_line"], best["converted_sql"], best["confidence"])

        results.append(entry)

    return {
        "total_todos": len(todos),
        "processed": len(results),
        "auto_applied": sum(1 for r in results if r["applied"]),
        "needs_review": sum(1 for r in results if not r["applied"]),
        "results": results,
    }


def _apply_fix(filename, line_num, fixed_sql, sql_dir=None):
    """Apply a fix to a specific line in a SQL file."""
    sql_path = Path(sql_dir or OUTPUT_DIR) / filename
    if not sql_path.exists():
        return False
    lines = sql_path.read_text(encoding="utf-8").split("\n")
    if 0 < line_num <= len(lines):
        lines[line_num - 1] = fixed_sql + "  -- AI-converted"
        sql_path.write_text("\n".join(lines), encoding="utf-8")
        return True
    return False


# ─────────────────────────────────────────────
#  Main entry point
# ─────────────────────────────────────────────

def run_ai_conversion(inventory_path=None, sql_dir=None, dry_run=False, config=None):
    """Run AI-assisted SQL conversion on all TODO markers.

    Returns summary report.
    """
    print("=" * 60)
    print("  AI-Assisted SQL Conversion (Phase 13)")
    print("=" * 60)

    # Load inventory for context
    inv_path = Path(inventory_path or INVENTORY_PATH)
    inventory = {}
    if inv_path.exists():
        with open(inv_path, encoding="utf-8") as f:
            inventory = json.load(f)

    # Initialize components
    llm = LLMClient(config)
    store = PatternStore()

    # Extract and rank TODOs
    todos = extract_todos(sql_dir)
    ranked = rank_gaps(todos, inventory)

    print(f"\n📋 Found {len(todos)} TODO markers")
    print(f"   Pattern store: {store.pair_count} known pairs")
    print(f"   LLM budget remaining: {llm.budget_remaining:,} tokens")

    # Process
    result = backfill_todos(sql_dir, llm_client=llm, pattern_store=store, dry_run=dry_run)

    print(f"\n✅ Processed: {result['processed']}")
    print(f"   Auto-applied: {result['auto_applied']}")
    print(f"   Needs review: {result['needs_review']}")

    # Save report
    report_path = WORKSPACE / "output" / "ai" / "conversion_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {k: v for k, v in result.items() if k != "results"},
            "ranked_gaps": ranked[:20],
            "pattern_store_size": store.pair_count,
            "tokens_used": llm.total_tokens_used,
            "dry_run": dry_run,
        }, f, indent=2)

    print(f"\n📄 Report: {report_path}")
    return result


def main():
    import argparse
    parser = argparse.ArgumentParser(description="AI-Assisted SQL Conversion")
    parser.add_argument("--inventory", default=None, help="Path to inventory.json")
    parser.add_argument("--sql-dir", default=None, help="Path to SQL output directory")
    parser.add_argument("--dry-run", action="store_true", help="Preview without applying fixes")
    args = parser.parse_args()
    run_ai_conversion(args.inventory, args.sql_dir, args.dry_run)


if __name__ == "__main__":
    main()
