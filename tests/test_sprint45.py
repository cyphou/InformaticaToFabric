"""
Tests for Sprint 45 — Cross-Platform Comparison & Migration Advisor.

45.1  Target comparison report (Databricks PySpark vs DBT)
45.2  Dual-target generation (--target all)
45.3  Migration advisor (per-mapping recommendations)
45.4  Unified deployment manifest (multi-target)
45.5  Cross-platform integration (all components together)
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORKSPACE))

import run_target_comparison as rtc


# ═══════════════════════════════════════════════════════════════
#  Fixtures
# ═══════════════════════════════════════════════════════════════

def _make_inventory(mappings=None):
    """Create a minimal inventory dict for testing."""
    if mappings is None:
        mappings = [
            {
                "name": "M_LOAD_CUSTOMERS",
                "sources": ["Oracle.HR.CUSTOMERS"],
                "targets": ["DIM_CUSTOMER"],
                "transformations": ["SQ", "EXP", "FIL"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Simple",
            },
            {
                "name": "M_AGG_SALES",
                "sources": ["Oracle.SALES.ORDERS"],
                "targets": ["FACT_SALES_AGG"],
                "transformations": ["SQ", "AGG", "LKP", "EXP"],
                "has_sql_override": True,
                "has_stored_proc": False,
                "complexity": "Medium",
            },
            {
                "name": "M_COMPLEX_JAVA",
                "sources": ["Oracle.APP.DATA"],
                "targets": ["STG_COMPLEX"],
                "transformations": ["SQ", "JTX", "EXP", "CT", "FIL", "AGG", "LKP"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Complex",
            },
            {
                "name": "M_SP_REFRESH",
                "sources": ["Oracle.DW.FACT"],
                "targets": ["GOLD_REFRESH"],
                "transformations": ["SQ", "SP", "EXP"],
                "has_sql_override": False,
                "has_stored_proc": True,
                "complexity": "Complex",
            },
            {
                "name": "M_SIMPLE_LOAD",
                "sources": ["Oracle.HR.EMP"],
                "targets": ["DIM_EMPLOYEE"],
                "transformations": ["SQ", "FIL"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Simple",
            },
        ]
    return {
        "generated": "2026-04-02T12:00:00Z",
        "source_platform": "PowerCenter",
        "target_platform": "Databricks",
        "mappings": mappings,
        "workflows": [
            {"name": "WF_DAILY_LOAD", "sessions": ["S_M_LOAD_CUSTOMERS"]},
        ],
        "connections": [],
        "sql_files": [],
        "summary": {"total_mappings": len(mappings)},
    }


class _TempDirMixin:
    """Mixin providing tmpdir with output structure."""

    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.output = self.tmpdir / "output"
        self.output.mkdir()
        (self.output / "inventory").mkdir()
        (self.output / "notebooks").mkdir()
        (self.output / "dbt").mkdir()
        (self.output / "pipelines").mkdir()
        (self.output / "sql").mkdir()
        (self.output / "validation").mkdir()
        # Write sample artifacts
        (self.output / "notebooks" / "NB_M_LOAD_CUSTOMERS.py").write_text("# notebook", encoding="utf-8")
        (self.output / "notebooks" / "NB_M_AGG_SALES.py").write_text("# notebook2", encoding="utf-8")
        (self.output / "dbt" / "M_LOAD_CUSTOMERS.sql").write_text("-- dbt model", encoding="utf-8")
        (self.output / "dbt" / "M_SIMPLE_LOAD.sql").write_text("-- dbt model2", encoding="utf-8")
        (self.output / "pipelines" / "PL_WF_DAILY_LOAD.json").write_text("{}", encoding="utf-8")
        (self.output / "sql" / "SQL_OVERRIDE.sql").write_text("-- sql", encoding="utf-8")
        (self.output / "validation" / "VAL_DIM_CUSTOMER.py").write_text("# val", encoding="utf-8")
        self.inventory = _make_inventory()
        (self.output / "inventory" / "inventory.json").write_text(
            json.dumps(self.inventory), encoding="utf-8")
        self._orig_output = rtc.OUTPUT_DIR
        rtc.OUTPUT_DIR = self.output

    def tearDown(self):
        rtc.OUTPUT_DIR = self._orig_output
        shutil.rmtree(self.tmpdir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════
#  45.1 — Target Comparison Report
# ═══════════════════════════════════════════════════════════════

class TestComparisonReport(_TempDirMixin, unittest.TestCase):

    def test_comparison_report_generated(self):
        result = rtc.generate_comparison_report(self.inventory, output_path=self.output / "comparison_report.md")
        self.assertTrue((self.output / "comparison_report.md").exists())
        self.assertIn("report_path", result)

    def test_comparison_counts(self):
        result = rtc.generate_comparison_report(self.inventory, output_path=self.output / "comp.md")
        self.assertEqual(result["total"], 5)
        self.assertGreater(result["dbt_suited"] + result["pyspark_suited"] + result["either"], 0)

    def test_comparison_report_content(self):
        rtc.generate_comparison_report(self.inventory, output_path=self.output / "comp.md")
        text = (self.output / "comp.md").read_text(encoding="utf-8")
        self.assertIn("Cross-Platform Comparison", text)
        self.assertIn("PySpark Equivalent", text)
        self.assertIn("DBT Equivalent", text)
        self.assertIn("Cost Projection", text)

    def test_comparison_includes_all_mappings(self):
        rtc.generate_comparison_report(self.inventory, output_path=self.output / "comp.md")
        text = (self.output / "comp.md").read_text(encoding="utf-8")
        for m in self.inventory["mappings"]:
            self.assertIn(m["name"], text)

    def test_comparison_dbu_estimates(self):
        result = rtc.generate_comparison_report(self.inventory, output_path=self.output / "comp.md")
        self.assertGreater(result["est_daily_dbu_pyspark"], 0)
        self.assertGreater(result["est_daily_dbu_dbt"], 0)
        # DBT should be cheaper (0.6x factor)
        self.assertLess(result["est_daily_dbu_dbt"], result["est_daily_dbu_pyspark"])

    def test_comparison_artifact_section(self):
        rtc.generate_comparison_report(self.inventory, output_path=self.output / "comp.md")
        text = (self.output / "comp.md").read_text(encoding="utf-8")
        self.assertIn("Artifact Comparison", text)
        self.assertIn("Databricks Notebooks", text)
        self.assertIn("DBT Models", text)


# ═══════════════════════════════════════════════════════════════
#  45.3 — Migration Advisor
# ═══════════════════════════════════════════════════════════════

class TestMigrationAdvisor(unittest.TestCase):

    def test_classify_simple_sql_mapping(self):
        m = {"transformations": ["SQ", "EXP", "FIL"], "has_sql_override": False,
             "has_stored_proc": False, "complexity": "Simple"}
        target, reason = rtc.classify_mapping_target(m)
        self.assertEqual(target, "dbt")

    def test_classify_java_transform(self):
        m = {"transformations": ["SQ", "JTX", "EXP"], "has_sql_override": False,
             "has_stored_proc": False, "complexity": "Complex"}
        target, reason = rtc.classify_mapping_target(m)
        self.assertEqual(target, "pyspark")
        self.assertIn("JTX", reason)

    def test_classify_stored_proc(self):
        m = {"transformations": ["SQ", "SP"], "has_sql_override": False,
             "has_stored_proc": True, "complexity": "Complex"}
        target, reason = rtc.classify_mapping_target(m)
        self.assertEqual(target, "pyspark")
        self.assertIn("stored procedure", reason.lower())

    def test_classify_sql_override_no_complex(self):
        m = {"transformations": ["SQ", "AGG", "LKP"], "has_sql_override": True,
             "has_stored_proc": False, "complexity": "Medium"}
        target, reason = rtc.classify_mapping_target(m)
        self.assertEqual(target, "dbt")

    def test_classify_http_transform(self):
        m = {"transformations": ["SQ", "HTTP", "EXP"], "has_sql_override": False,
             "has_stored_proc": False, "complexity": "Medium"}
        target, reason = rtc.classify_mapping_target(m)
        self.assertEqual(target, "pyspark")

    def test_classify_empty_transforms(self):
        m = {"transformations": [], "has_sql_override": False,
             "has_stored_proc": False, "complexity": "Simple"}
        target, reason = rtc.classify_mapping_target(m)
        self.assertEqual(target, "either")


class TestAdvisorReport(_TempDirMixin, unittest.TestCase):

    def test_advisor_report_generated(self):
        result = rtc.generate_advisor_report(self.inventory,
                                             output_path=self.output / "inventory" / "target_recommendation.md")
        self.assertTrue((self.output / "inventory" / "target_recommendation.md").exists())

    def test_advisor_summary_counts(self):
        result = rtc.generate_advisor_report(self.inventory,
                                             output_path=self.output / "advisor.md")
        s = result["summary"]
        self.assertEqual(s["total_mappings"], 5)
        self.assertEqual(s["dbt_recommended"] + s["pyspark_recommended"] + s["either"], 5)

    def test_advisor_recommendations_per_mapping(self):
        result = rtc.generate_advisor_report(self.inventory,
                                             output_path=self.output / "advisor.md")
        self.assertEqual(len(result["recommendations"]), 5)
        for rec in result["recommendations"]:
            self.assertIn(rec["recommended_target"], ("dbt", "pyspark", "either"))
            self.assertIn("reason", rec)

    def test_advisor_report_markdown_content(self):
        rtc.generate_advisor_report(self.inventory,
                                    output_path=self.output / "advisor.md")
        text = (self.output / "advisor.md").read_text(encoding="utf-8")
        self.assertIn("Migration Target Advisor", text)
        self.assertIn("Per-Mapping Recommendations", text)
        self.assertIn("M_LOAD_CUSTOMERS", text)

    def test_advisor_dbt_percentage(self):
        result = rtc.generate_advisor_report(self.inventory,
                                             output_path=self.output / "advisor.md")
        s = result["summary"]
        self.assertGreaterEqual(s["dbt_percentage"], 0)
        self.assertLessEqual(s["dbt_percentage"], 100)


# ═══════════════════════════════════════════════════════════════
#  45.4 — Unified Deployment Manifest
# ═══════════════════════════════════════════════════════════════

class TestUnifiedManifest(_TempDirMixin, unittest.TestCase):

    def test_manifest_generated(self):
        manifest = rtc.generate_unified_manifest(self.inventory,
                                                  output_path=self.output / "manifest.json")
        self.assertTrue((self.output / "manifest.json").exists())

    def test_manifest_schema_version(self):
        manifest = rtc.generate_unified_manifest(self.inventory,
                                                  output_path=self.output / "manifest.json")
        self.assertEqual(manifest["schema_version"], "2.0")

    def test_manifest_targets(self):
        manifest = rtc.generate_unified_manifest(self.inventory, targets=["databricks", "dbt"],
                                                  output_path=self.output / "manifest.json")
        self.assertEqual(manifest["targets"], ["databricks", "dbt"])

    def test_manifest_artifact_categories(self):
        manifest = rtc.generate_unified_manifest(self.inventory,
                                                  output_path=self.output / "manifest.json")
        self.assertIn("notebooks", manifest["artifacts"])
        self.assertIn("dbt", manifest["artifacts"])

    def test_manifest_total_count(self):
        manifest = rtc.generate_unified_manifest(self.inventory,
                                                  output_path=self.output / "manifest.json")
        self.assertGreater(manifest["total_artifacts"], 0)
        # Should include notebooks + dbt + pipelines + sql + validation
        self.assertGreaterEqual(manifest["total_artifacts"], 5)

    def test_manifest_deployment_order(self):
        manifest = rtc.generate_unified_manifest(self.inventory,
                                                  output_path=self.output / "manifest.json")
        self.assertIsInstance(manifest["deployment_order"], list)
        self.assertGreater(len(manifest["deployment_order"]), 0)

    def test_manifest_file_entries(self):
        manifest = rtc.generate_unified_manifest(self.inventory,
                                                  output_path=self.output / "manifest.json")
        nb_entries = manifest["artifacts"]["notebooks"]["files"]
        for entry in nb_entries:
            self.assertIn("name", entry)
            self.assertIn("path", entry)
            self.assertIn("size_bytes", entry)
            self.assertIn("target", entry)
            self.assertIn("deploy_order", entry)

    def test_manifest_json_valid(self):
        rtc.generate_unified_manifest(self.inventory,
                                       output_path=self.output / "manifest.json")
        text = (self.output / "manifest.json").read_text(encoding="utf-8")
        parsed = json.loads(text)
        self.assertIn("artifacts", parsed)


# ═══════════════════════════════════════════════════════════════
#  45.2 — Dual-Target (--target all)
# ═══════════════════════════════════════════════════════════════

class TestDualTarget(unittest.TestCase):
    """Test --target all support in run_migration.py."""

    def test_target_all_choice_accepted(self):
        """Verify 'all' is a valid --target choice."""
        import run_migration as rm
        # Check the argparse accepts 'all'
        with patch("sys.argv", ["run_migration.py", "--target", "all", "--dry-run"]):
            args = rm._parse_args()
            self.assertEqual(args.target, "all")

    def test_compare_flag_accepted(self):
        import run_migration as rm
        with patch("sys.argv", ["run_migration.py", "--compare", "--dry-run"]):
            args = rm._parse_args()
            self.assertTrue(args.compare)

    def test_advisor_flag_accepted(self):
        import run_migration as rm
        with patch("sys.argv", ["run_migration.py", "--advisor", "--dry-run"]):
            args = rm._parse_args()
            self.assertTrue(args.advisor)


# ═══════════════════════════════════════════════════════════════
#  45.5 — Integration: end-to-end
# ═══════════════════════════════════════════════════════════════

class TestCrossPlatformIntegration(_TempDirMixin, unittest.TestCase):
    """Test all Sprint 45 components together."""

    def test_full_pipeline(self):
        """Advisor + Comparison + Manifest in sequence."""
        advisor = rtc.generate_advisor_report(self.inventory,
                                              output_path=self.output / "advisor.md")
        comparison = rtc.generate_comparison_report(self.inventory,
                                                     output_path=self.output / "comp.md")
        manifest = rtc.generate_unified_manifest(self.inventory,
                                                  output_path=self.output / "manifest.json")
        # All three should succeed
        self.assertEqual(advisor["summary"]["total_mappings"], 5)
        self.assertEqual(comparison["total"], 5)
        self.assertGreater(manifest["total_artifacts"], 0)

    def test_all_dbt_workload(self):
        """All mappings are SQL-only → advisor should recommend DBT primary."""
        sql_mappings = [
            {"name": f"M_SQL_{i}", "sources": ["SRC"], "targets": ["TGT"],
             "transformations": ["SQ", "EXP", "FIL"],
             "has_sql_override": False, "has_stored_proc": False, "complexity": "Simple"}
            for i in range(10)
        ]
        inv = _make_inventory(sql_mappings)
        result = rtc.generate_advisor_report(inv, output_path=self.output / "all_dbt.md")
        self.assertEqual(result["summary"]["dbt_recommended"], 10)
        self.assertEqual(result["summary"]["pyspark_recommended"], 0)

    def test_all_pyspark_workload(self):
        """All mappings have Java transforms → advisor should recommend PySpark."""
        java_mappings = [
            {"name": f"M_JAVA_{i}", "sources": ["SRC"], "targets": ["TGT"],
             "transformations": ["SQ", "JTX", "CT"],
             "has_sql_override": False, "has_stored_proc": False, "complexity": "Complex"}
            for i in range(5)
        ]
        inv = _make_inventory(java_mappings)
        result = rtc.generate_advisor_report(inv, output_path=self.output / "all_py.md")
        self.assertEqual(result["summary"]["pyspark_recommended"], 5)
        self.assertEqual(result["summary"]["dbt_recommended"], 0)

    def test_empty_inventory(self):
        """Empty inventory shouldn't crash."""
        inv = _make_inventory([])
        advisor = rtc.generate_advisor_report(inv, output_path=self.output / "empty.md")
        self.assertEqual(advisor["summary"]["total_mappings"], 0)
        comparison = rtc.generate_comparison_report(inv, output_path=self.output / "emp_comp.md")
        self.assertEqual(comparison["total"], 0)
        manifest = rtc.generate_unified_manifest(inv, output_path=self.output / "emp_mf.json")
        self.assertGreaterEqual(manifest["total_artifacts"], 0)


if __name__ == "__main__":
    unittest.main()
