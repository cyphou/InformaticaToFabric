"""
Tests for Phase 13 — AI-Assisted SQL Conversion (Sprints 86–88)
Tests LLM client, confidence scoring, pattern store, gap ranking,
suggestions, TODO extraction, and the migration assistant.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path


# ─────────────────────────────────────────────
#  Sprint 86 — LLM-Powered SQL Conversion
# ─────────────────────────────────────────────

class TestConfidenceScoring(unittest.TestCase):
    """Tests for score_confidence()."""

    def test_import(self):
        import ai_converter
        self.assertTrue(hasattr(ai_converter, "score_confidence"))

    def test_empty_sql(self):
        from ai_converter import score_confidence
        self.assertEqual(score_confidence(""), 0)
        self.assertEqual(score_confidence(None), 0)

    def test_perfect_sql(self):
        from ai_converter import score_confidence
        sql = "SELECT col_a, col_b FROM my_table WHERE col_a > 10"
        score = score_confidence(sql)
        self.assertGreaterEqual(score, 80)

    def test_unbalanced_parens(self):
        from ai_converter import score_confidence
        sql = "SELECT COALESCE(col_a, 'default' FROM table"
        score = score_confidence(sql)
        self.assertLess(score, 80)

    def test_leftover_oracle_functions(self):
        from ai_converter import score_confidence
        sql = "SELECT NVL(col, 0), SYSDATE FROM DUAL"
        score = score_confidence(sql)
        self.assertLess(score, 85)

    def test_todo_markers_reduce_score(self):
        from ai_converter import score_confidence
        sql = "SELECT col FROM t  -- TODO: Convert outer join"
        score = score_confidence(sql)
        score_clean = score_confidence("SELECT col FROM t")
        self.assertLess(score, score_clean)

    def test_table_preservation(self):
        from ai_converter import score_confidence
        source = "SELECT * FROM orders WHERE status = 'A'"
        good_target = "SELECT * FROM orders WHERE status = 'A'"
        bad_target = "SELECT * FROM wrong_table WHERE status = 'A'"
        self.assertGreater(
            score_confidence(good_target, source),
            score_confidence(bad_target, source),
        )

    def test_score_capped_at_100(self):
        from ai_converter import score_confidence
        score = score_confidence("SELECT 1")
        self.assertLessEqual(score, 100)

    def test_score_minimum_zero(self):
        from ai_converter import score_confidence
        sql = "NVL(NVL(NVL(NVL(NVL(NVL(SYSDATE, DECODE(x, 1, 2))"
        score = score_confidence(sql)
        self.assertGreaterEqual(score, 0)


class TestPromptBuilding(unittest.TestCase):
    """Tests for prompt engineering."""

    def test_prompt_includes_source_sql(self):
        from ai_converter import build_conversion_prompt
        prompt = build_conversion_prompt("SELECT NVL(col, 0) FROM orders")
        self.assertIn("NVL", prompt)
        self.assertIn("orders", prompt)

    def test_prompt_includes_dialect(self):
        from ai_converter import build_conversion_prompt
        prompt = build_conversion_prompt("SELECT 1", source_dialect="SQL Server",
                                         target_dialect="Spark SQL")
        self.assertIn("SQL Server", prompt)
        self.assertIn("Spark SQL", prompt)

    def test_prompt_includes_tables(self):
        from ai_converter import build_conversion_prompt
        prompt = build_conversion_prompt("SELECT 1",
                                         source_tables=["orders", "customers"],
                                         target_tables=["delta_orders"])
        self.assertIn("orders", prompt)
        self.assertIn("delta_orders", prompt)


class TestLLMClient(unittest.TestCase):
    """Tests for LLM client with local fallback."""

    def test_client_creation(self):
        from ai_converter import LLMClient
        client = LLMClient()
        self.assertIsNotNone(client)
        self.assertGreater(client.budget_remaining, 0)

    def test_local_fallback(self):
        from ai_converter import LLMClient
        client = LLMClient({"api_endpoint": "", "api_key": ""})
        result = client.convert_sql("SELECT NVL(col, 0) FROM DUAL")
        self.assertIsNotNone(result["converted_sql"])
        self.assertIn("COALESCE", result["converted_sql"])
        self.assertNotIn("DUAL", result["converted_sql"])
        self.assertEqual(result["model"], "local_heuristic")

    def test_cache_reuse(self):
        from ai_converter import LLMClient
        client = LLMClient({"api_endpoint": "", "api_key": ""})
        r1 = client.convert_sql("SELECT SYSDATE FROM DUAL")
        r2 = client.convert_sql("SELECT SYSDATE FROM DUAL")
        self.assertTrue(r2["cached"])

    def test_budget_enforcement(self):
        from ai_converter import LLMClient
        client = LLMClient({"api_endpoint": "", "api_key": "",
                            "max_tokens_budget": 10})
        # Budget is tiny but _check_budget runs before _call_llm
        result = client.convert_sql("SELECT 1")
        # With a tiny budget, the request may be rejected or fall back locally
        self.assertIn("converted_sql", result)

    def test_budget_remaining(self):
        from ai_converter import LLMClient
        client = LLMClient({"max_tokens_budget": 50000})
        self.assertEqual(client.budget_remaining, 50000)


class TestBasicConversions(unittest.TestCase):
    """Tests for _apply_basic_conversions local fallback."""

    def test_nvl_to_coalesce(self):
        from ai_converter import _apply_basic_conversions
        result = _apply_basic_conversions("SELECT NVL(col, 0) FROM t")
        self.assertIn("COALESCE", result)

    def test_sysdate(self):
        from ai_converter import _apply_basic_conversions
        result = _apply_basic_conversions("SELECT SYSDATE FROM t")
        self.assertIn("current_timestamp()", result)

    def test_varchar2(self):
        from ai_converter import _apply_basic_conversions
        result = _apply_basic_conversions("CAST(x AS VARCHAR2)")
        self.assertIn("STRING", result)


# ─────────────────────────────────────────────
#  Sprint 87 — Pattern Learning & Gap Resolution
# ─────────────────────────────────────────────

class TestPatternStore(unittest.TestCase):
    """Tests for PatternStore."""

    def _temp_store(self):
        f = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        json.dump({"pairs": [], "rules": [], "feedback": []}, f)
        f.close()
        return f.name

    def test_create_empty(self):
        from ai_converter import PatternStore
        store = PatternStore(self._temp_store())
        self.assertEqual(store.pair_count, 0)

    def test_add_pair(self):
        from ai_converter import PatternStore
        path = self._temp_store()
        store = PatternStore(path)
        store.add_pair("SELECT NVL(x,0)", "SELECT COALESCE(x,0)", 90)
        self.assertEqual(store.pair_count, 1)
        os.unlink(path)

    def test_find_similar(self):
        from ai_converter import PatternStore
        path = self._temp_store()
        store = PatternStore(path)
        store.add_pair("SELECT NVL(col, 0) FROM orders", "SELECT COALESCE(col, 0) FROM orders", 90)
        similar = store.find_similar("SELECT NVL(col, 'x') FROM orders")
        self.assertGreater(len(similar), 0)
        self.assertGreater(similar[0]["similarity"], 0.3)
        os.unlink(path)

    def test_add_feedback(self):
        from ai_converter import PatternStore
        path = self._temp_store()
        store = PatternStore(path)
        store.add_feedback("SELECT NVL(x,0)", "SELECT COALESCE(x,0)", accepted=True)
        self.assertEqual(store.feedback_count, 1)
        os.unlink(path)

    def test_feedback_with_correction_adds_pair(self):
        from ai_converter import PatternStore
        path = self._temp_store()
        store = PatternStore(path)
        store.add_feedback("SELECT NVL(x,0)", "SELECT COALESCE(x,0)", False,
                           user_correction="SELECT IFNULL(x,0)")
        self.assertEqual(store.pair_count, 1)
        self.assertEqual(store.feedback_count, 1)
        os.unlink(path)

    def test_extract_rules(self):
        from ai_converter import PatternStore
        path = self._temp_store()
        store = PatternStore(path)
        store.add_pair("SELECT NVL(x, 0)", "SELECT COALESCE(x, 0)", 95)
        rules = store.extract_rules()
        self.assertGreater(len(rules), 0)
        os.unlink(path)

    def test_nonexistent_store(self):
        from ai_converter import PatternStore
        store = PatternStore("/nonexistent/path.json")
        self.assertEqual(store.pair_count, 0)


class TestGapRanking(unittest.TestCase):
    """Tests for rank_gaps()."""

    def test_empty_gaps(self):
        from ai_converter import rank_gaps
        result = rank_gaps([])
        self.assertEqual(len(result), 0)

    def test_ranking_by_complexity(self):
        from ai_converter import rank_gaps
        inventory = {"mappings": [
            {"name": "M_SIMPLE", "complexity": "Simple"},
            {"name": "M_COMPLEX", "complexity": "Complex"},
        ]}
        todos = [
            {"mapping": "M_SIMPLE", "text": "todo1"},
            {"mapping": "M_COMPLEX", "text": "todo2"},
        ]
        ranked = rank_gaps(todos, inventory)
        self.assertEqual(ranked[0]["mapping"], "M_COMPLEX")
        self.assertGreater(ranked[0]["severity"], ranked[1]["severity"])

    def test_ranking_by_downstream(self):
        from ai_converter import rank_gaps
        inventory = {
            "mappings": [
                {"name": "M_A", "complexity": "Simple"},
                {"name": "M_B", "complexity": "Simple"},
            ],
            "dependency_dag": {"M_A": {"downstream": ["X", "Y", "Z"]}, "M_B": {"downstream": []}},
        }
        todos = [{"mapping": "M_A"}, {"mapping": "M_B"}]
        ranked = rank_gaps(todos, inventory)
        self.assertEqual(ranked[0]["mapping"], "M_A")

    def test_severity_capped_at_100(self):
        from ai_converter import rank_gaps
        inventory = {
            "mappings": [{"name": "M_BIG", "complexity": "Complex",
                          "schedule": {"frequency": "hourly"}}],
            "dependency_dag": {"M_BIG": {"downstream": list(range(20))}},
        }
        ranked = rank_gaps([{"mapping": "M_BIG"}], inventory)
        self.assertLessEqual(ranked[0]["severity"], 100)


class TestSuggestions(unittest.TestCase):
    """Tests for generate_suggestions()."""

    def test_generates_local_heuristic(self):
        from ai_converter import generate_suggestions, PatternStore
        store = PatternStore("/nonexistent.json")
        suggestions = generate_suggestions("SELECT NVL(x,0) FROM DUAL", store)
        self.assertGreater(len(suggestions), 0)
        methods = [s["method"] for s in suggestions]
        self.assertIn("local_heuristic", methods)

    def test_pattern_match_included(self):
        from ai_converter import generate_suggestions, PatternStore
        path = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        json.dump({"pairs": [{"source": "SELECT NVL(col, 0) FROM orders",
                               "target": "SELECT COALESCE(col, 0) FROM orders",
                               "source_dialect": "Oracle", "target_dialect": "Spark SQL",
                               "confidence": 95, "timestamp": "2026-01-01"}],
                    "rules": [], "feedback": []}, path)
        path.close()
        store = PatternStore(path.name)
        suggestions = generate_suggestions("SELECT NVL(col, 0) FROM orders", store)
        methods = [s["method"] for s in suggestions]
        self.assertIn("pattern_match", methods)
        os.unlink(path.name)


class TestTodoExtraction(unittest.TestCase):
    """Tests for extract_todos() and backfill_todos()."""

    def test_extract_from_empty_dir(self):
        from ai_converter import extract_todos
        result = extract_todos("/nonexistent/dir")
        self.assertEqual(result, [])

    def test_extract_finds_todos(self):
        from ai_converter import extract_todos
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "SQL_TEST.sql").write_text(
                "SELECT col\n-- TODO: Convert outer join\nFROM t", encoding="utf-8"
            )
            result = extract_todos(d)
            self.assertEqual(len(result), 1)
            self.assertIn("outer join", result[0]["todo_text"].lower())

    def test_backfill_dry_run(self):
        from ai_converter import backfill_todos
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "SQL_X.sql").write_text("-- TODO: Fix this\n", encoding="utf-8")
            result = backfill_todos(d, dry_run=True)
            self.assertEqual(result["auto_applied"], 0)
            self.assertGreaterEqual(result["total_todos"], 1)


# ─────────────────────────────────────────────
#  Sprint 88 — Migration Assistant
# ─────────────────────────────────────────────

class TestAssistantContextBuilder(unittest.TestCase):
    """Tests for assistant.build_context()."""

    def test_build_empty_context(self):
        from assistant import build_context
        ctx = build_context("/nonexistent.json")
        self.assertEqual(ctx["mappings"], [])
        self.assertEqual(ctx["stats"].get("total_mappings", 0), 0)

    def test_build_with_inventory(self):
        from assistant import build_context
        inv = {"mappings": [{"name": "M_A", "sources": ["s"], "targets": ["t"],
                             "complexity": "Simple"}],
               "workflows": [{"name": "WF_1"}]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            ctx = build_context(f.name)
        os.unlink(f.name)
        self.assertEqual(ctx["stats"]["total_mappings"], 1)
        self.assertEqual(ctx["stats"]["total_workflows"], 1)

    def test_context_has_artifact_lists(self):
        from assistant import build_context
        ctx = build_context("/nonexistent.json")
        self.assertIn("notebooks", ctx["artifacts"])
        self.assertIn("pipelines", ctx["artifacts"])


class TestAssistantQueryHandlers(unittest.TestCase):
    """Tests for assistant.handle_query()."""

    def _ctx_with_mapping(self):
        return {
            "mappings": [{"name": "M_LOAD_CUSTOMERS", "sources": ["customers"],
                          "targets": ["delta_customers"], "complexity": "Complex",
                          "has_custom_sql": True, "transformations": [],
                          "unsupported_transforms": ["Custom"]}],
            "workflows": [],
            "todos": [{"file": "SQL_M_LOAD_CUSTOMERS.sql", "line": 5,
                       "text": "-- TODO: Fix outer join"}],
            "artifacts": {"notebooks": [], "pipelines": [], "sql": [], "dbt": []},
            "stats": {"total_mappings": 1, "total_workflows": 0, "complexity_distribution": {"Complex": 1}},
            "lineage": {"M_LOAD_CUSTOMERS": {"upstream": ["customers"], "downstream": ["delta_customers"]}},
            "inventory": {},
        }

    def test_explain_flagged(self):
        from assistant import handle_query
        result = handle_query("Why was M_LOAD_CUSTOMERS flagged?", self._ctx_with_mapping())
        self.assertTrue(result["found"])
        self.assertIn("complexity", result["answer"].lower())

    def test_show_lineage(self):
        from assistant import handle_query
        result = handle_query("Show lineage for M_LOAD_CUSTOMERS", self._ctx_with_mapping())
        self.assertTrue(result["found"])
        self.assertIn("customers", result["answer"])

    def test_migration_advice(self):
        from assistant import handle_query
        result = handle_query("How to migrate M_LOAD_CUSTOMERS", self._ctx_with_mapping())
        self.assertTrue(result["found"])
        self.assertIn("advice", result)

    def test_list_todos(self):
        from assistant import handle_query
        result = handle_query("List todos", self._ctx_with_mapping())
        self.assertTrue(result["found"])
        self.assertEqual(result["count"], 1)

    def test_show_summary(self):
        from assistant import handle_query
        result = handle_query("summary", self._ctx_with_mapping())
        self.assertTrue(result["found"])
        self.assertIn("Mappings: 1", result["answer"])

    def test_list_complex(self):
        from assistant import handle_query
        result = handle_query("Show complex mappings", self._ctx_with_mapping())
        self.assertTrue(result["found"])
        self.assertEqual(result["count"], 1)

    def test_default_handler(self):
        from assistant import handle_query
        result = handle_query("gibberish query xyz", self._ctx_with_mapping())
        self.assertFalse(result["found"])
        self.assertIn("I can help", result["answer"])

    def test_explain_mapping(self):
        from assistant import handle_query
        result = handle_query("What does mapping M_LOAD_CUSTOMERS do?", self._ctx_with_mapping())
        self.assertTrue(result["found"])
        self.assertIn("customers", result["answer"])


class TestInteractiveReview(unittest.TestCase):
    """Tests for assistant interactive review mode."""

    def test_review_empty_todos(self):
        from assistant import interactive_review
        ctx = {"todos": [], "mappings": [], "inventory": {}}
        decisions = interactive_review(ctx)
        self.assertEqual(len(decisions), 0)

    def test_review_with_todos(self):
        from assistant import interactive_review
        ctx = {
            "todos": [{"file": "SQL_TEST.sql", "line": 1, "text": "-- TODO: Fix"}],
            "mappings": [], "inventory": {},
        }
        decisions = interactive_review(ctx)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["action"], "pending")

    def test_apply_decisions_empty(self):
        from assistant import apply_review_decisions
        result = apply_review_decisions([])
        self.assertEqual(result["applied"], 0)
        self.assertEqual(result["total"], 0)


if __name__ == "__main__":
    unittest.main()
