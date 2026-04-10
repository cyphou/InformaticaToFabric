"""
Tests for Phase 14 — Web UI v2, Lineage Explorer, Migration Diff (Sprints 89–91)
Tests diff generation, lineage graph, impact analysis, batch review, and HTML output.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path


# ─────────────────────────────────────────────
#  Sprint 90 — Visual Lineage Explorer
# ─────────────────────────────────────────────

class TestLineageGraph(unittest.TestCase):
    """Tests for lineage graph JSON generation."""

    def test_import(self):
        import diff_generator
        self.assertTrue(hasattr(diff_generator, "generate_lineage_json"))
        self.assertTrue(hasattr(diff_generator, "generate_impact_analysis"))

    def test_empty_inventory(self):
        from diff_generator import generate_lineage_json
        result = generate_lineage_json("/nonexistent.json")
        self.assertEqual(result["nodes"], [])
        self.assertEqual(result["edges"], [])

    def test_lineage_nodes_and_edges(self):
        from diff_generator import generate_lineage_json
        inv = {"mappings": [
            {"name": "M_LOAD", "sources": ["orders"], "targets": ["delta_orders"],
             "complexity": "Simple"},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_lineage_json(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["nodes"]), 3)  # orders, M_LOAD, delta_orders
        self.assertEqual(len(result["edges"]), 2)  # orders→M_LOAD, M_LOAD→delta_orders

    def test_lineage_node_types(self):
        from diff_generator import generate_lineage_json
        inv = {"mappings": [{"name": "M_A", "sources": ["src"], "targets": ["tgt"], "complexity": "Simple"}]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_lineage_json(f.name)
        os.unlink(f.name)
        types = {n["id"]: n["type"] for n in result["nodes"]}
        self.assertEqual(types["M_A"], "mapping")
        self.assertEqual(types["src"], "table")

    def test_tier_inference(self):
        from diff_generator import _infer_tier
        self.assertEqual(_infer_tier("STG_ORDERS"), "bronze")
        self.assertEqual(_infer_tier("SILVER_CUSTOMERS"), "silver")
        self.assertEqual(_infer_tier("GOLD_REVENUE"), "gold")
        self.assertEqual(_infer_tier("RANDOM_TABLE"), "unknown")


class TestImpactAnalysis(unittest.TestCase):
    """Tests for impact analysis."""

    def test_downstream_impact(self):
        from diff_generator import generate_impact_analysis
        inv = {"mappings": [
            {"name": "M_A", "sources": ["src"], "targets": ["mid"], "complexity": "Simple"},
            {"name": "M_B", "sources": ["mid"], "targets": ["tgt"], "complexity": "Simple"},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_impact_analysis("src", f.name, direction="downstream")
        os.unlink(f.name)
        self.assertTrue(result["found"])
        impacted_ids = [i["id"] for i in result["impacted"]]
        self.assertIn("M_A", impacted_ids)
        self.assertIn("mid", impacted_ids)

    def test_upstream_impact(self):
        from diff_generator import generate_impact_analysis
        inv = {"mappings": [
            {"name": "M_A", "sources": ["src"], "targets": ["tgt"], "complexity": "Simple"},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_impact_analysis("tgt", f.name, direction="upstream")
        os.unlink(f.name)
        self.assertTrue(result["found"])
        impacted_ids = [i["id"] for i in result["impacted"]]
        self.assertIn("M_A", impacted_ids)

    def test_nonexistent_entity(self):
        from diff_generator import generate_impact_analysis
        inv = {"mappings": [{"name": "M_A", "sources": ["s"], "targets": ["t"], "complexity": "Simple"}]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_impact_analysis("NONEXISTENT", f.name)
        os.unlink(f.name)
        self.assertFalse(result["found"])


# ─────────────────────────────────────────────
#  Sprint 91 — Diff & Side-by-Side Review
# ─────────────────────────────────────────────

class TestSourceLogicExtractor(unittest.TestCase):
    """Tests for extract_source_logic()."""

    def test_basic_extraction(self):
        from diff_generator import extract_source_logic
        mapping = {"name": "M_TEST", "sources": ["orders"], "targets": ["delta_orders"],
                   "complexity": "Medium", "transformations": [
                       {"type": "Filter", "name": "FIL_ACTIVE", "expression": "status = 'A'"}
                   ]}
        result = extract_source_logic(mapping)
        self.assertEqual(result["name"], "M_TEST")
        self.assertEqual(result["complexity"], "Medium")
        self.assertGreater(len(result["sections"]), 0)

    def test_sql_override_section(self):
        from diff_generator import extract_source_logic
        mapping = {"name": "M_SQL", "sources": [], "targets": [],
                   "sql_override": "SELECT * FROM custom_view", "transformations": []}
        result = extract_source_logic(mapping)
        labels = [s["label"] for s in result["sections"]]
        self.assertIn("SQL Override (Source Dialect)", labels)

    def test_empty_mapping(self):
        from diff_generator import extract_source_logic
        mapping = {"name": "M_EMPTY", "transformations": []}
        result = extract_source_logic(mapping)
        self.assertEqual(result["name"], "M_EMPTY")


class TestCodeClassification(unittest.TestCase):
    """Tests for _classify_code()."""

    def test_auto(self):
        from diff_generator import _classify_code
        self.assertEqual(_classify_code("SELECT col FROM t"), "auto")

    def test_todo(self):
        from diff_generator import _classify_code
        self.assertEqual(_classify_code("SELECT col  -- TODO: Fix join"), "todo")

    def test_heuristic(self):
        from diff_generator import _classify_code
        self.assertEqual(_classify_code("SELECT col  -- REVIEW: Check logic"), "heuristic")
        self.assertEqual(_classify_code("SELECT col  -- AI-converted: fixed"), "heuristic")


class TestDiffHtmlGeneration(unittest.TestCase):
    """Tests for generate_diff_html()."""

    def test_basic_html(self):
        from diff_generator import generate_diff_html
        mapping = {"name": "M_TEST"}
        source = {"name": "M_TEST", "complexity": "Simple",
                  "sections": [{"type": "header", "label": "Mapping", "content": "test"}]}
        targets = [{"type": "sql", "label": "SQL", "content": "SELECT 1", "status": "auto"}]
        html_output = generate_diff_html(mapping, source, targets)
        self.assertIn("M_TEST", html_output)
        self.assertIn("<!DOCTYPE html>", html_output)
        self.assertIn("auto", html_output)

    def test_html_escapes_content(self):
        from diff_generator import generate_diff_html
        mapping = {"name": "M_<SCRIPT>"}
        source = {"name": "M_<SCRIPT>", "complexity": "Simple",
                  "sections": [{"type": "header", "label": "Test", "content": "<script>alert(1)</script>"}]}
        targets = [{"type": "sql", "label": "SQL", "content": "SELECT 1", "status": "auto"}]
        html_output = generate_diff_html(mapping, source, targets)
        self.assertNotIn("<script>alert(1)</script>", html_output)
        self.assertIn("&lt;script&gt;", html_output)

    def test_todo_status_in_html(self):
        from diff_generator import generate_diff_html
        mapping = {"name": "M_BUG"}
        source = {"name": "M_BUG", "complexity": "Complex", "sections": []}
        targets = [{"type": "sql", "label": "SQL", "content": "-- TODO: fix",
                    "status": "todo"}]
        html_output = generate_diff_html(mapping, source, targets)
        self.assertIn("🔴", html_output)


class TestBatchReport(unittest.TestCase):
    """Tests for generate_batch_report()."""

    def test_empty_inventory(self):
        from diff_generator import generate_batch_report
        result = generate_batch_report("/nonexistent.json")
        self.assertEqual(result["summary"]["total"], 0)

    def test_with_mappings(self):
        from diff_generator import generate_batch_report
        inv = {"mappings": [
            {"name": "M_A", "sources": ["s"], "targets": ["t"], "complexity": "Simple",
             "transformations": []},
            {"name": "M_B", "sources": ["s"], "targets": ["t"], "complexity": "Complex",
             "transformations": []},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_batch_report(f.name)
        os.unlink(f.name)
        self.assertEqual(result["summary"]["total"], 2)
        self.assertGreater(len(result["mappings"]), 0)

    def test_filter_by_status(self):
        from diff_generator import generate_batch_report
        inv = {"mappings": [
            {"name": "M_A", "sources": [], "targets": [], "complexity": "Simple", "transformations": []},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_batch_report(f.name, filter_status="todo")
        os.unlink(f.name)
        self.assertEqual(result["filter"], "todo")

    def test_batch_html(self):
        from diff_generator import generate_batch_html
        html_output = generate_batch_html("/nonexistent.json")
        self.assertIn("<!DOCTYPE html>", html_output)
        self.assertIn("Batch Review Report", html_output)


class TestLineageHtml(unittest.TestCase):
    """Tests for interactive lineage HTML generation."""

    def test_lineage_html_output(self):
        from diff_generator import generate_lineage_html
        html_output = generate_lineage_html("/nonexistent.json")
        self.assertIn("<!DOCTYPE html>", html_output)
        self.assertIn("cytoscape", html_output)
        self.assertIn("Lineage Explorer", html_output)

    def test_lineage_html_with_data(self):
        from diff_generator import generate_lineage_html
        inv = {"mappings": [{"name": "M_X", "sources": ["src"], "targets": ["tgt"], "complexity": "Simple"}]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            html_output = generate_lineage_html(f.name)
        os.unlink(f.name)
        self.assertIn("M_X", html_output)


class TestGenerateReview(unittest.TestCase):
    """Tests for the full generate_review() orchestrator."""

    def test_generates_files(self):
        from diff_generator import generate_review, OUTPUT_DIR
        result = generate_review("/nonexistent.json")
        self.assertGreater(len(result["generated"]), 0)
        self.assertTrue((OUTPUT_DIR / "batch_review.html").exists())
        self.assertTrue((OUTPUT_DIR / "lineage_graph.json").exists())


if __name__ == "__main__":
    unittest.main()
