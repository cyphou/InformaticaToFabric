"""
Tests for Phase 17 — ML Pipeline Templates, Cost Advisor, GA Release (Sprints 98–100)
Tests feature detection, notebook generation, cost estimation, TCO, reserved capacity, and dashboards.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path


# ─────────────────────────────────────────────
#  Sprint 98 — ML Pipeline Detection & Templates
# ─────────────────────────────────────────────

class TestFeatureDetection(unittest.TestCase):
    """Tests for detect_feature_mappings()."""

    def test_import(self):
        import ml_pipeline
        self.assertTrue(hasattr(ml_pipeline, "detect_feature_mappings"))

    def test_empty_inventory(self):
        from ml_pipeline import detect_feature_mappings
        result = detect_feature_mappings("/nonexistent.json")
        self.assertEqual(result["feature_engineering"], [])
        self.assertEqual(result["summary"]["total"], 0)

    def test_detects_feat_prefix(self):
        from ml_pipeline import detect_feature_mappings
        inv = {"mappings": [
            {"name": "FEAT_CUSTOMER_AGG", "sources": [], "targets": [], "complexity": "Simple",
             "transformations": []},
            {"name": "M_LOAD_ORDERS", "sources": [], "targets": [], "complexity": "Simple",
             "transformations": []},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = detect_feature_mappings(f.name)
        os.unlink(f.name)
        names = [m["name"] for m in result["feature_engineering"]]
        self.assertIn("FEAT_CUSTOMER_AGG", names)

    def test_detects_ml_prefix(self):
        from ml_pipeline import detect_feature_mappings
        inv = {"mappings": [
            {"name": "ML_SCORING_PIPELINE", "sources": [], "targets": [], "complexity": "Medium",
             "transformations": []},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = detect_feature_mappings(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["feature_engineering"]) + len(result["scoring"]), 1)

    def test_detects_aggregation_heuristic(self):
        from ml_pipeline import detect_feature_mappings
        inv = {"mappings": [
            {"name": "M_AGG_STATS", "sources": [], "targets": [],
             "complexity": "Complex", "transformations": [
                 {"type": "Aggregator", "name": "AGG_STATS"},
                 {"type": "Expression", "name": "EXP_CALC"},
                 {"type": "Sorter", "name": "SRT_ORDER"},
             ]},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = detect_feature_mappings(f.name)
        os.unlink(f.name)
        names = [m["name"] for m in result["feature_engineering"]]
        self.assertIn("M_AGG_STATS", names)


class TestFeatureStoreNotebook(unittest.TestCase):
    """Tests for Feature Store notebook generation."""

    def test_basic_generation(self):
        from ml_pipeline import generate_feature_store_notebook
        mapping = {"name": "FEAT_CUSTOMER", "sources": ["customers"],
                   "targets": ["feature_store_customers"]}
        result = generate_feature_store_notebook(mapping)
        self.assertIn("FeatureEngineeringClient", result)
        self.assertIn("FEAT_CUSTOMER", result)

    def test_includes_target_table(self):
        from ml_pipeline import generate_feature_store_notebook
        mapping = {"name": "FEAT_X", "sources": ["s"], "targets": ["feature_table_x"]}
        result = generate_feature_store_notebook(mapping)
        self.assertIn("feature_table_x", result)


class TestMLflowTemplate(unittest.TestCase):
    """Tests for MLflow experiment template generation."""

    def test_basic_template(self):
        from ml_pipeline import generate_mlflow_template
        mapping = {"name": "ML_TRAIN_MODEL", "sources": ["training_data"],
                   "targets": ["model_output"]}
        result = generate_mlflow_template(mapping)
        self.assertIn("mlflow", result)
        self.assertIn("start_run", result)
        self.assertIn("log_param", result)
        self.assertIn("ML_TRAIN_MODEL", result)

    def test_includes_model_logging(self):
        from ml_pipeline import generate_mlflow_template
        mapping = {"name": "ML_X", "sources": [], "targets": []}
        result = generate_mlflow_template(mapping)
        self.assertIn("log_model", result)


class TestScoringPipeline(unittest.TestCase):
    """Tests for batch scoring pipeline generation."""

    def test_basic_pipeline(self):
        from ml_pipeline import generate_scoring_pipeline
        mapping = {"name": "SCORE_CUSTOMERS", "sources": ["features"],
                   "targets": ["predictions"]}
        result = generate_scoring_pipeline(mapping)
        self.assertIn("predict", result)
        self.assertIn("SCORE_CUSTOMERS", result)
        self.assertIn("delta", result.lower())


# ─────────────────────────────────────────────
#  Sprint 99 — Cost Optimization Advisor
# ─────────────────────────────────────────────

class TestCostEstimation(unittest.TestCase):
    """Tests for estimate_mapping_cost()."""

    def test_simple_fabric(self):
        from ml_pipeline import estimate_mapping_cost
        mapping = {"name": "M_SIMPLE", "complexity": "Simple",
                   "estimated_runtime_minutes": 5}
        result = estimate_mapping_cost(mapping, target="fabric")
        self.assertIn("cost_usd", result)
        self.assertGreater(result["cost_usd"], 0)

    def test_complex_higher_cost(self):
        from ml_pipeline import estimate_mapping_cost
        m_simple = {"name": "M_S", "complexity": "Simple", "estimated_runtime_minutes": 10}
        m_complex = {"name": "M_C", "complexity": "Complex", "estimated_runtime_minutes": 10}
        c_s = estimate_mapping_cost(m_simple, target="fabric")
        c_c = estimate_mapping_cost(m_complex, target="fabric")
        self.assertGreater(c_c["cost_usd"], c_s["cost_usd"])

    def test_databricks_target(self):
        from ml_pipeline import estimate_mapping_cost
        mapping = {"name": "M_X", "complexity": "Medium", "estimated_runtime_minutes": 15}
        result = estimate_mapping_cost(mapping, target="databricks")
        self.assertIn("cost_usd", result)
        self.assertEqual(result["target"], "databricks")


class TestTCOComparison(unittest.TestCase):
    """Tests for TCO comparison report."""

    def test_empty_inventory(self):
        from ml_pipeline import generate_tco_comparison
        result = generate_tco_comparison("/nonexistent.json")
        self.assertEqual(result["mappings"], [])
        self.assertIn("fabric_total", result)

    def test_comparison_with_mappings(self):
        from ml_pipeline import generate_tco_comparison
        inv = {"mappings": [
            {"name": "M_A", "complexity": "Simple", "estimated_runtime_minutes": 5,
             "sources": [], "targets": [], "transformations": []},
            {"name": "M_B", "complexity": "Complex", "estimated_runtime_minutes": 30,
             "sources": [], "targets": [], "transformations": []},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_tco_comparison(f.name)
        os.unlink(f.name)
        self.assertEqual(len(result["mappings"]), 2)
        self.assertIn("fabric_total", result)
        self.assertIn("databricks_total", result)

    def test_recommendation_present(self):
        from ml_pipeline import generate_tco_comparison
        inv = {"mappings": [
            {"name": "M_X", "complexity": "Simple", "estimated_runtime_minutes": 5,
             "sources": [], "targets": [], "transformations": []},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_tco_comparison(f.name)
        os.unlink(f.name)
        self.assertIn("cheaper", result["mappings"][0])


class TestReservedCapacity(unittest.TestCase):
    """Tests for reserved capacity plan generation."""

    def test_plan_structure(self):
        from ml_pipeline import generate_reserved_capacity_plan
        plan = generate_reserved_capacity_plan()
        self.assertIn("pay_as_you_go", plan)
        self.assertIn("reserved_1yr", plan)
        self.assertIn("reserved_3yr", plan)

    def test_discounts(self):
        from ml_pipeline import generate_reserved_capacity_plan
        plan = generate_reserved_capacity_plan()
        payg = plan["pay_as_you_go"]["yearly"]
        one_yr = plan["reserved_1yr"]["yearly"]
        three_yr = plan["reserved_3yr"]["yearly"]
        self.assertLessEqual(one_yr, payg)
        self.assertLessEqual(three_yr, one_yr)

    def test_savings_percentage(self):
        from ml_pipeline import generate_reserved_capacity_plan
        plan = generate_reserved_capacity_plan()
        self.assertIn("savings_vs_paygo", plan["reserved_1yr"])
        self.assertIn("savings_vs_paygo", plan["reserved_3yr"])


class TestIdleResources(unittest.TestCase):
    """Tests for idle resource detection."""

    def test_empty_inventory(self):
        from ml_pipeline import detect_idle_resources
        result = detect_idle_resources("/nonexistent.json")
        self.assertEqual(result["recommendations"], [])

    def test_detects_simple_mappings(self):
        from ml_pipeline import detect_idle_resources
        inv = {"mappings": [
            {"name": "M_TINY", "complexity": "Simple", "estimated_runtime_minutes": 1,
             "sources": [], "targets": [], "transformations": []},
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = detect_idle_resources(f.name)
        os.unlink(f.name)
        # With 1/1 mappings being Simple (>50%), right_size recommendation triggers
        self.assertGreater(len(result["recommendations"]), 0)


class TestCostTags(unittest.TestCase):
    """Tests for cost allocation tag generation."""

    def test_basic_tags(self):
        from ml_pipeline import generate_cost_tags
        inv = {"mappings": [
            {"name": "M_SALES_LOAD", "complexity": "Medium",
             "sources": [], "targets": [], "transformations": []}
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_cost_tags(f.name)
        os.unlink(f.name)
        self.assertGreater(len(result["tags"]), 0)
        self.assertIn("tags", result["tags"][0])

    def test_tags_contain_mapping_name(self):
        from ml_pipeline import generate_cost_tags
        inv = {"mappings": [
            {"name": "M_FINANCE_AGG", "complexity": "Complex",
             "sources": [], "targets": [], "transformations": []}
        ]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(inv, f)
            f.flush()
            result = generate_cost_tags(f.name)
        os.unlink(f.name)
        self.assertEqual(result["tags"][0]["resource"], "M_FINANCE_AGG")


class TestCostDashboard(unittest.TestCase):
    """Tests for cost dashboard HTML output."""

    def test_dashboard_html(self):
        from ml_pipeline import generate_cost_dashboard_html
        html_output = generate_cost_dashboard_html("/nonexistent.json")
        self.assertIn("<!DOCTYPE html>", html_output)
        self.assertIn("Cost", html_output)

    def test_dashboard_has_sections(self):
        from ml_pipeline import generate_cost_dashboard_html
        html_output = generate_cost_dashboard_html("/nonexistent.json")
        self.assertIn("Reserved Capacity", html_output)


class TestCostAdvisorFacade(unittest.TestCase):
    """Tests that cost_advisor.py re-exports correctly."""

    def test_imports(self):
        from cost_advisor import (
            estimate_mapping_cost,
            generate_tco_comparison,
            generate_reserved_capacity_plan,
            detect_idle_resources,
            generate_cost_tags,
            generate_cost_dashboard_html,
        )
        self.assertTrue(callable(estimate_mapping_cost))
        self.assertTrue(callable(generate_tco_comparison))
        self.assertTrue(callable(generate_reserved_capacity_plan))
        self.assertTrue(callable(detect_idle_resources))
        self.assertTrue(callable(generate_cost_tags))
        self.assertTrue(callable(generate_cost_dashboard_html))


class TestMLArtifacts(unittest.TestCase):
    """Tests for the orchestrator function."""

    def test_generate_ml_artifacts(self):
        from ml_pipeline import generate_ml_artifacts
        result = generate_ml_artifacts("/nonexistent.json")
        self.assertIn("detection", result)
        self.assertIn("tco", result)
        self.assertIn("generated", result)


if __name__ == "__main__":
    unittest.main()
