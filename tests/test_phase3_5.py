"""
Tests for Phase 3 (Sprints 47-50), Phase 4 (Sprints 52-60), and Phase 5 (Sprints 62-65).

Sprint 47: Advanced Databricks — DLT, UC lineage, SQL dashboards, cluster policies, advanced workflows
Sprint 48: Integration testing — multi-target, benchmarks, regression
Sprint 49: Enterprise polish — dashboard v2, plugin-ready
Sprint 50: Release & docs — packaging, ADR references
Sprint 52-53: Core + Advanced DBT model generation
Sprint 54: SQL dialect conversion for DBT
Sprint 55: DBT testing & validation integration
Sprint 56: DBT macros, incremental models, snapshots
Sprint 57: Mixed dbt + notebook workflows
Sprint 58: DBT deployment & CI/CD
Sprint 59-60: Integration tests & release
Sprint 62: AutoSys → Pipeline/Workflow conversion enhancements
Sprint 63: Calendar, profile, machine mapping
Sprint 64-65: AutoSys E2E validation & docs
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add workspace root to path
WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORKSPACE))


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 47 — Advanced Databricks
# ═════════════════════════════════════════════════════════════════════════════

class TestUCLineage(unittest.TestCase):
    """Sprint 47: Unity Catalog lineage metadata generation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.inv_dir = Path(self.tmpdir) / "output" / "inventory"
        self.inv_dir.mkdir(parents=True)
        self.inventory = {
            "mappings": [
                {
                    "name": "M_LOAD_CUSTOMERS",
                    "sources": ["Oracle.HR.EMPLOYEES"],
                    "targets": ["DIM_CUSTOMER"],
                    "transformations": ["SQ", "EXP", "FIL"],
                    "complexity": "Simple",
                },
                {
                    "name": "M_AGG_SALES",
                    "sources": ["Oracle.SALES.ORDERS"],
                    "targets": ["FACT_SALES_AGG"],
                    "transformations": ["SQ", "AGG", "LKP"],
                    "complexity": "Medium",
                },
            ],
            "workflows": [],
            "connections": [],
        }
        (self.inv_dir / "inventory.json").write_text(
            json.dumps(self.inventory), encoding="utf-8")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_lineage_generation(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            result = dtd.generate_uc_lineage(self.inv_dir / "inventory.json", "test_catalog")
            self.assertEqual(result["catalog"], "test_catalog")
            self.assertEqual(result["total_mappings"], 2)
            self.assertEqual(len(result["lineage"]), 2)
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_lineage_source_tables(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            result = dtd.generate_uc_lineage(self.inv_dir / "inventory.json", "main")
            entry = result["lineage"][0]
            self.assertEqual(entry["mapping_name"], "M_LOAD_CUSTOMERS")
            self.assertEqual(len(entry["source_tables"]), 1)
            self.assertEqual(entry["source_tables"][0]["schema"], "hr")
            self.assertEqual(entry["source_tables"][0]["table"], "employees")
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_lineage_target_tables(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            result = dtd.generate_uc_lineage(self.inv_dir / "inventory.json", "main")
            entry = result["lineage"][0]
            self.assertEqual(len(entry["target_tables"]), 1)
            self.assertEqual(entry["target_tables"][0]["schema"], "silver")
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_lineage_agg_goes_gold(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            result = dtd.generate_uc_lineage(self.inv_dir / "inventory.json", "main")
            agg_entry = result["lineage"][1]
            self.assertEqual(agg_entry["target_tables"][0]["schema"], "gold")
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_lineage_missing_inventory(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            result = dtd.generate_uc_lineage(Path("/nonexistent/inv.json"))
            self.assertIn("error", result)
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_lineage_writes_file(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            dtd.generate_uc_lineage(self.inv_dir / "inventory.json")
            self.assertTrue((Path(self.tmpdir) / "output" / "inventory" / "uc_lineage.json").exists())
        finally:
            dtd.OUTPUT_DIR = orig_output


class TestDLTNotebooks(unittest.TestCase):
    """Sprint 47: Delta Live Tables notebook generation."""

    def test_dlt_notebook_content(self):
        import deploy_to_databricks as dtd
        mapping = {
            "name": "M_LOAD_CUSTOMERS",
            "sources": ["Oracle.HR.EMPLOYEES"],
            "targets": ["DIM_CUSTOMER"],
            "transformations": ["SQ", "EXP", "FIL"],
        }
        content = dtd.generate_dlt_notebook(mapping, "main")
        self.assertIn("import dlt", content)
        self.assertIn("@dlt.table", content)
        self.assertIn("raw_employees", content)
        self.assertIn("clean_employees", content)
        self.assertIn("dim_customer", content)

    def test_dlt_notebook_quality_expectations(self):
        import deploy_to_databricks as dtd
        mapping = {
            "name": "M_TEST",
            "sources": ["Oracle.SCHEMA.TABLE1"],
            "targets": ["TARGET1"],
            "transformations": ["SQ"],
        }
        content = dtd.generate_dlt_notebook(mapping)
        self.assertIn("expect_or_drop", content)

    def test_dlt_notebook_agg_gold(self):
        import deploy_to_databricks as dtd
        mapping = {
            "name": "M_AGG",
            "sources": ["Oracle.S.T"],
            "targets": ["AGG_TABLE"],
            "transformations": ["SQ", "AGG"],
        }
        content = dtd.generate_dlt_notebook(mapping)
        self.assertIn('"quality": "gold"', content)

    def test_dlt_notebooks_batch(self):
        import deploy_to_databricks as dtd
        tmpdir = tempfile.mkdtemp()
        inv_dir = Path(tmpdir) / "output" / "inventory"
        inv_dir.mkdir(parents=True)
        inventory = {
            "mappings": [
                {"name": "M1", "sources": ["S.T1"], "targets": ["T1"], "transformations": ["SQ"]},
                {"name": "M2", "sources": ["S.T2"], "targets": ["T2"], "transformations": ["SQ", "AGG"]},
            ],
        }
        (inv_dir / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(tmpdir) / "output"
        try:
            generated = dtd.generate_dlt_notebooks(inv_dir / "inventory.json")
            self.assertEqual(len(generated), 2)
            self.assertTrue((Path(tmpdir) / "output" / "notebooks" / "dlt" / "DLT_M1.py").exists())
        finally:
            dtd.OUTPUT_DIR = orig_output
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestClusterPolicies(unittest.TestCase):
    """Sprint 47: Cluster policy recommendation engine."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.inv_dir = Path(self.tmpdir) / "output" / "inventory"
        self.inv_dir.mkdir(parents=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_inventory(self, mappings):
        inventory = {"mappings": mappings, "workflows": [], "connections": []}
        (self.inv_dir / "inventory.json").write_text(
            json.dumps(inventory), encoding="utf-8")

    def test_etl_policy(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        self._write_inventory([
            {"name": "M1", "transformations": ["SQ", "AGG", "JNR"], "complexity": "Complex"},
        ])
        try:
            result = dtd.recommend_cluster_policies(self.inv_dir / "inventory.json")
            policy_names = [p["name"] for p in result["policies"]]
            self.assertIn("etl-migration-policy", policy_names)
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_sql_warehouse_policy(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        self._write_inventory([
            {"name": "M1", "transformations": ["SQ", "EXP"], "complexity": "Simple"},
        ])
        try:
            result = dtd.recommend_cluster_policies(self.inv_dir / "inventory.json")
            policy_names = [p["name"] for p in result["policies"]]
            self.assertIn("sql-warehouse-policy", policy_names)
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_ml_policy(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        self._write_inventory([
            {"name": "M1", "transformations": ["SQ", "JTX"], "complexity": "Custom"},
        ])
        try:
            result = dtd.recommend_cluster_policies(self.inv_dir / "inventory.json")
            policy_names = [p["name"] for p in result["policies"]]
            self.assertIn("ml-migration-policy", policy_names)
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_missing_inventory(self):
        import deploy_to_databricks as dtd
        result = dtd.recommend_cluster_policies(Path("/nonexistent"))
        self.assertIn("error", result)


class TestSQLDashboardQueries(unittest.TestCase):
    """Sprint 47: Databricks SQL dashboard query generation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.inv_dir = Path(self.tmpdir) / "output" / "inventory"
        self.inv_dir.mkdir(parents=True)
        inventory = {
            "mappings": [
                {"name": "M1", "targets": ["DIM_TABLE"], "transformations": ["SQ"]},
            ],
        }
        (self.inv_dir / "inventory.json").write_text(
            json.dumps(inventory), encoding="utf-8")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generates_queries(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            queries = dtd.generate_sql_dashboard_queries(self.inv_dir / "inventory.json")
            self.assertGreater(len(queries), 0)
            names = [q["name"] for q in queries]
            self.assertTrue(any("row_count" in n for n in names))
            self.assertTrue(any("null_check" in n for n in names))
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_query_has_sql(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            queries = dtd.generate_sql_dashboard_queries(self.inv_dir / "inventory.json")
            for q in queries:
                self.assertIn("query", q)
                self.assertIn("SELECT", q["query"])
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_writes_json_file(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        try:
            dtd.generate_sql_dashboard_queries(self.inv_dir / "inventory.json")
            out_path = Path(self.tmpdir) / "output" / "databricks" / "dashboards" / "validation_queries.json"
            self.assertTrue(out_path.exists())
        finally:
            dtd.OUTPUT_DIR = orig_output


class TestAdvancedWorkflow(unittest.TestCase):
    """Sprint 47: Advanced Databricks Workflow generation."""

    def test_job_clusters(self):
        import deploy_to_databricks as dtd
        workflow = {
            "name": "WF_TEST",
            "sessions": [{"mapping": "M1"}, {"mapping": "M2"}],
        }
        result = dtd.generate_advanced_workflow(workflow)
        self.assertIn("job_clusters", result)
        self.assertEqual(result["job_clusters"][0]["job_cluster_key"], "migration_cluster")

    def test_task_dependencies(self):
        import deploy_to_databricks as dtd
        workflow = {
            "name": "WF_TEST",
            "sessions": [{"mapping": "M1"}, {"mapping": "M2"}],
        }
        result = dtd.generate_advanced_workflow(workflow)
        # Second task should depend on first
        self.assertEqual(len(result["tasks"]), 2)
        self.assertIn("depends_on", result["tasks"][1])

    def test_health_rules(self):
        import deploy_to_databricks as dtd
        workflow = {"name": "WF_TEST", "sessions": [{"mapping": "M1"}]}
        result = dtd.generate_advanced_workflow(workflow)
        self.assertIn("health", result)

    def test_schedule(self):
        import deploy_to_databricks as dtd
        workflow = {
            "name": "WF_TEST",
            "sessions": [{"mapping": "M1"}],
            "schedule_cron": {"cron": "0 2 * * ?"},
        }
        result = dtd.generate_advanced_workflow(workflow)
        self.assertIn("schedule", result)

    def test_max_concurrent_runs(self):
        import deploy_to_databricks as dtd
        workflow = {"name": "WF_TEST", "sessions": [{"mapping": "M1"}]}
        result = dtd.generate_advanced_workflow(workflow)
        self.assertEqual(result["max_concurrent_runs"], 1)


class TestDBUCostEstimator(unittest.TestCase):
    """Sprint 48: DBU cost estimation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.inv_dir = Path(self.tmpdir) / "output" / "inventory"
        self.inv_dir.mkdir(parents=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_cost_simple(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        inventory = {
            "mappings": [
                {"name": "M1", "transformations": ["SQ"], "complexity": "Simple"},
            ],
        }
        (self.inv_dir / "inventory.json").write_text(
            json.dumps(inventory), encoding="utf-8")
        try:
            result = dtd.estimate_dbu_cost(self.inv_dir / "inventory.json")
            self.assertIn("estimated_monthly_cost_usd", result)
            self.assertGreater(result["estimated_monthly_cost_usd"], 0)
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_cost_complex_higher(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        inv_simple = {"mappings": [{"name": "M1", "transformations": ["SQ"], "complexity": "Simple"}]}
        inv_complex = {"mappings": [{"name": "M1", "transformations": ["SQ", "AGG", "JNR"], "complexity": "Complex"}]}

        (self.inv_dir / "inventory.json").write_text(json.dumps(inv_simple), encoding="utf-8")
        try:
            r_simple = dtd.estimate_dbu_cost(self.inv_dir / "inventory.json")
            (self.inv_dir / "inventory.json").write_text(json.dumps(inv_complex), encoding="utf-8")
            r_complex = dtd.estimate_dbu_cost(self.inv_dir / "inventory.json")
            self.assertGreater(r_complex["estimated_monthly_cost_usd"],
                             r_simple["estimated_monthly_cost_usd"])
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_cost_writes_file(self):
        import deploy_to_databricks as dtd
        orig_output = dtd.OUTPUT_DIR
        dtd.OUTPUT_DIR = Path(self.tmpdir) / "output"
        inventory = {"mappings": [{"name": "M1", "transformations": ["SQ"], "complexity": "Simple"}]}
        (self.inv_dir / "inventory.json").write_text(json.dumps(inventory), encoding="utf-8")
        try:
            dtd.estimate_dbu_cost(self.inv_dir / "inventory.json")
            self.assertTrue((self.inv_dir / "cost_estimate.json").exists())
        finally:
            dtd.OUTPUT_DIR = orig_output

    def test_cost_missing_inventory(self):
        import deploy_to_databricks as dtd
        result = dtd.estimate_dbu_cost(Path("/nonexistent"))
        self.assertIn("error", result)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 49 — Enterprise Polish (Dashboard v2)
# ═════════════════════════════════════════════════════════════════════════════

class TestDashboardV2(unittest.TestCase):
    """Sprint 49: Dashboard with multi-target support."""

    def test_collect_status_has_dbt_field(self):
        import dashboard
        orig_output = dashboard.OUTPUT_DIR
        dashboard.OUTPUT_DIR = Path(tempfile.mkdtemp()) / "output"
        dashboard.OUTPUT_DIR.mkdir(parents=True)
        try:
            status = dashboard._collect_status()
            self.assertIn("dbt_models", status["artifacts"])
            self.assertIn("autosys_pipelines", status["artifacts"])
            self.assertIn("dlt_notebooks", status["artifacts"])
        finally:
            shutil.rmtree(str(dashboard.OUTPUT_DIR.parent), ignore_errors=True)
            dashboard.OUTPUT_DIR = orig_output

    def test_collect_status_has_cost_estimate(self):
        import dashboard
        orig_output = dashboard.OUTPUT_DIR
        dashboard.OUTPUT_DIR = Path(tempfile.mkdtemp()) / "output"
        dashboard.OUTPUT_DIR.mkdir(parents=True)
        try:
            status = dashboard._collect_status()
            self.assertIn("cost_estimate", status)
        finally:
            shutil.rmtree(str(dashboard.OUTPUT_DIR.parent), ignore_errors=True)
            dashboard.OUTPUT_DIR = orig_output

    def test_html_includes_dbt_kpi(self):
        import dashboard
        status = {
            "generated": "2026-01-01T00:00:00",
            "phases": [],
            "inventory": {"total_mappings": 5, "total_workflows": 2, "total_connections": 3, "complexity": {"Simple": 3, "Complex": 2}},
            "artifacts": {"notebooks": ["NB_1.py"], "pipelines": ["PL_1.json"], "sql": [], "validation": [],
                          "dbt_models": ["stg_m1.sql", "int_m1.sql"], "autosys_pipelines": ["PL_AUTOSYS_BOX1.json"],
                          "dlt_notebooks": ["DLT_M1.py"]},
            "checkpoint": None,
            "test_matrix": None,
            "deployment": None,
            "cost_estimate": None,
        }
        html = dashboard._generate_html(status)
        self.assertIn("DBT Models", html)
        self.assertIn("AutoSys Pipelines", html)
        self.assertIn("DLT Notebooks", html)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 52-53 — Core + Advanced DBT Model Generation
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTCoreModels(unittest.TestCase):
    """Sprint 52: Core DBT model generation (SQ/EXP/FIL/AGG/LKP/SRT)."""

    def test_staging_model_basic(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_LOAD_CUSTOMERS",
            "sources": ["Oracle.HR.EMPLOYEES"],
            "targets": ["DIM_CUSTOMER"],
            "transformations": ["SQ"],
            "complexity": "Simple",
        }
        sql = rdm.generate_staging_model(mapping)
        self.assertIn("source('hr', 'employees')", sql)
        self.assertIn("config(", sql)
        self.assertIn("staging", sql)

    def test_staging_model_with_sql_override(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_WITH_SQL",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ"],
            "complexity": "Simple",
            "sql_overrides": [{"type": "Sql Query", "value": "SELECT NVL(a, 0) FROM t"}],
        }
        sql = rdm.generate_staging_model(mapping)
        self.assertIn("COALESCE(a, 0)", sql)

    def test_intermediate_model_exp(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_TEST",
            "sources": ["S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "EXP", "FIL"],
            "complexity": "Simple",
        }
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("expressions_", sql)
        self.assertIn("filtered_", sql)
        self.assertIn("ref('stg_m_test')", sql)

    def test_intermediate_model_agg(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_AGG",
            "sources": ["S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "AGG"],
        }
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("aggregated_", sql)
        self.assertIn("GROUP BY", sql)

    def test_intermediate_model_lkp(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_LKP",
            "sources": ["S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "LKP"],
        }
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("with_lookup_", sql)
        self.assertIn("LEFT JOIN", sql)

    def test_intermediate_model_srt(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_SORT",
            "sources": ["S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "SRT"],
        }
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("ORDER BY", sql)

    def test_mart_model_table(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_BASIC",
            "sources": ["S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "EXP"],
        }
        sql = rdm.generate_mart_model(mapping)
        self.assertIn('materialized="table"', sql)
        self.assertIn("ref('int_m_basic')", sql)

    def test_mart_model_incremental(self):
        import run_dbt_migration as rdm
        mapping = {
            "name": "M_UPSERT",
            "sources": ["S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "UPD"],
        }
        sql = rdm.generate_mart_model(mapping)
        self.assertIn('materialized="incremental"', sql)
        self.assertIn("is_incremental()", sql)


class TestDBTAdvancedModels(unittest.TestCase):
    """Sprint 53: Advanced DBT model generation (JNR/UNI/RNK/RTR/NRM/SEQ)."""

    def test_joiner_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_JOIN", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "JNR"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("joined_", sql)
        self.assertIn("JOIN", sql)

    def test_union_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_UNI", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "UNI"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("unioned_", sql)
        self.assertIn("UNION ALL", sql)

    def test_rank_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_RNK", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "RNK"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("ranked_", sql)
        self.assertIn("ROW_NUMBER()", sql)

    def test_router_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_RTR", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "RTR"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("routed_", sql)

    def test_normalizer_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_NRM", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "NRM"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("normalized_", sql)
        self.assertIn("EXPLODE", sql)

    def test_sequence_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_SEQ", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "SEQ"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("with_seq_", sql)
        self.assertIn("ROW_NUMBER()", sql)

    def test_mapplet_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_MPLT", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "MPLT"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("mapplet_", sql)

    def test_dm_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_DM", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "DM"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("masked_", sql)
        self.assertIn("MD5", sql)

    def test_multi_transform_chain(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_CHAIN", "sources": ["S.T"], "targets": ["TGT"],
                   "transformations": ["SQ", "EXP", "FIL", "LKP", "AGG", "SRT"]}
        sql = rdm.generate_intermediate_model(mapping)
        self.assertIn("expressions_", sql)
        self.assertIn("filtered_", sql)
        self.assertIn("with_lookup_", sql)
        self.assertIn("aggregated_", sql)
        self.assertIn("ORDER BY", sql)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 54 — SQL Dialect Conversion for DBT
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTSQLDialect(unittest.TestCase):
    """Sprint 54: SQL dialect conversion for DBT models."""

    def test_nvl_to_coalesce(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT NVL(a, 0) FROM t")
        self.assertIn("COALESCE(a, 0)", result)

    def test_sysdate(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT SYSDATE FROM dual")
        self.assertIn("CURRENT_TIMESTAMP()", result)

    def test_decode(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT DECODE(x, 1, 'a') FROM t")
        self.assertIn("CASE", result)

    def test_trunc(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT TRUNC(dt) FROM t")
        self.assertIn("DATE_TRUNC", result)

    def test_rownum(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT * FROM t WHERE ROWNUM <= 10")
        self.assertIn("ROW_NUMBER()", result)

    def test_substr(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT SUBSTR(name, 1, 5) FROM t")
        self.assertIn("SUBSTRING(name, 1, 5)", result)

    def test_to_char(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT TO_CHAR(dt, 'YYYY-MM-DD') FROM t")
        self.assertIn("DATE_FORMAT", result)

    def test_nvl2(self):
        import run_dbt_migration as rdm
        result = rdm.convert_sql_to_dbsql("SELECT NVL2(x, y, z) FROM t")
        self.assertIn("IF(", result)

    def test_oracle_date_fmt_conversion(self):
        import run_dbt_migration as rdm
        result = rdm._oracle_date_fmt("YYYY-MM-DD HH24:MI:SS")
        self.assertEqual(result, "yyyy-MM-dd HH:mm:ss")


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 55 — DBT Testing & Validation
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTTestGeneration(unittest.TestCase):
    """Sprint 55: DBT test generation (L1-L5 mapping)."""

    def test_row_count_test(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_TEST", "targets": ["TGT"], "transformations": ["SQ"]}
        sql = rdm.generate_data_test_row_count(mapping)
        self.assertIn("ref('mart_m_test')", sql)
        self.assertIn("COUNT(*)", sql)

    def test_transform_test(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_TEST", "targets": ["TGT"], "transformations": ["SQ", "EXP"]}
        sql = rdm.generate_data_test_transform(mapping)
        self.assertIn("ref('mart_m_test')", sql)

    def test_enhanced_schema_yml(self):
        import run_dbt_migration as rdm
        mappings = [
            {"name": "M_TEST", "targets": ["TGT"], "transformations": ["SQ"]},
        ]
        yml = rdm.generate_enhanced_schema_yml(mappings)
        self.assertIn("stg_m_test", yml)
        self.assertIn("int_m_test", yml)
        self.assertIn("mart_m_test", yml)
        self.assertIn("unique", yml)
        self.assertIn("not_null", yml)
        self.assertIn("sla_seconds", yml)

    def test_enhanced_schema_multiple_mappings(self):
        import run_dbt_migration as rdm
        mappings = [
            {"name": "M1", "targets": ["T1"], "transformations": ["SQ"]},
            {"name": "M2", "targets": ["T2"], "transformations": ["SQ", "EXP"]},
        ]
        yml = rdm.generate_enhanced_schema_yml(mappings)
        self.assertIn("stg_m1", yml)
        self.assertIn("mart_m2", yml)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 56 — Macros, Incremental, Snapshots
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTMacrosAndSnapshots(unittest.TestCase):
    """Sprint 56: Macros, incremental models, and snapshots."""

    def test_utility_macros_generated(self):
        import run_dbt_migration as rdm
        macros = rdm.generate_utility_macros()
        self.assertIn("clean_string.sql", macros)
        self.assertIn("safe_divide.sql", macros)
        self.assertIn("hash_key.sql", macros)
        self.assertIn("surrogate_key.sql", macros)
        self.assertIn("informatica_iif.sql", macros)

    def test_clean_string_macro(self):
        import run_dbt_migration as rdm
        macros = rdm.generate_utility_macros()
        self.assertIn("TRIM(UPPER(", macros["clean_string.sql"])

    def test_safe_divide_macro(self):
        import run_dbt_migration as rdm
        macros = rdm.generate_utility_macros()
        self.assertIn("CASE WHEN", macros["safe_divide.sql"])

    def test_mapplet_macro(self):
        import run_dbt_migration as rdm
        macro = rdm.generate_mapplet_macro("MP_DERIVE_NAME")
        self.assertIn("macro mapplet_mp_derive_name", macro)
        self.assertIn("source_model", macro)

    def test_snapshot_generation(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_SCD2", "targets": ["DIM_CUSTOMER"], "transformations": ["SQ", "UPD"]}
        sql = rdm.generate_snapshot(mapping)
        self.assertIn("snapshot snp_m_scd2", sql)
        self.assertIn("strategy='timestamp'", sql)
        self.assertIn("unique_key='id'", sql)

    def test_incremental_model(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_INC", "targets": ["FACT_TABLE"], "transformations": ["SQ", "UPD"]}
        sql = rdm.generate_incremental_model(mapping)
        self.assertIn("incremental", sql)
        self.assertIn("incremental_strategy='merge'", sql)
        self.assertIn("is_incremental()", sql)
        self.assertIn("OPTIMIZE", sql)

    def test_incremental_model_has_hooks(self):
        import run_dbt_migration as rdm
        mapping = {"name": "M_INC", "targets": ["T"], "transformations": ["SQ", "UPD"]}
        sql = rdm.generate_incremental_model(mapping)
        self.assertIn("pre_hook", sql)
        self.assertIn("post_hook", sql)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 57 — Mixed dbt + Notebook Workflows
# ═════════════════════════════════════════════════════════════════════════════

class TestMixedWorkflows(unittest.TestCase):
    """Sprint 57: Mixed dbt + notebook task Databricks Workflows."""

    def test_mixed_workflow_generation(self):
        import run_dbt_migration as rdm
        workflow = {
            "name": "WF_DAILY",
            "sessions": [
                {"mapping": "M_SIMPLE"},
                {"mapping": "M_COMPLEX"},
            ],
        }
        dbt_mappings = [{"name": "M_SIMPLE", "complexity": "Simple"}]
        pyspark_mappings = [{"name": "M_COMPLEX", "complexity": "Complex"}]
        result = rdm.generate_mixed_workflow(workflow, dbt_mappings, pyspark_mappings)
        self.assertEqual(result["name"], "PL_WF_DAILY")
        # Should have: dbt_deps_seed, dbt_m_simple, nb_M_COMPLEX, dbt_test
        task_keys = [t["task_key"] for t in result["tasks"]]
        self.assertIn("dbt_deps_seed", task_keys)
        self.assertTrue(any("dbt_m_simple" in k for k in task_keys))
        self.assertTrue(any("M_COMPLEX" in k for k in task_keys))

    def test_mixed_workflow_dbt_task_type(self):
        import run_dbt_migration as rdm
        workflow = {"name": "WF_TEST", "sessions": [{"mapping": "M_DBT"}]}
        dbt_mappings = [{"name": "M_DBT", "complexity": "Simple"}]
        result = rdm.generate_mixed_workflow(workflow, dbt_mappings, [])
        dbt_task = [t for t in result["tasks"] if t.get("dbt_task")][1]
        self.assertIn("dbt run", dbt_task["dbt_task"]["commands"][0])

    def test_mixed_workflow_notebook_task_type(self):
        import run_dbt_migration as rdm
        workflow = {"name": "WF_TEST", "sessions": [{"mapping": "M_PY"}]}
        result = rdm.generate_mixed_workflow(workflow, [], [{"name": "M_PY"}])
        nb_task = [t for t in result["tasks"] if t.get("notebook_task")]
        self.assertEqual(len(nb_task), 1)

    def test_mixed_workflow_with_schedule(self):
        import run_dbt_migration as rdm
        workflow = {
            "name": "WF_SCHED",
            "sessions": [{"mapping": "M1"}],
            "schedule_cron": {"cron": "0 0 2 * * ?"},
        }
        result = rdm.generate_mixed_workflow(workflow, [{"name": "M1"}], [])
        self.assertIn("schedule", result)

    def test_mixed_workflow_dbt_test_step(self):
        import run_dbt_migration as rdm
        workflow = {"name": "WF_TEST", "sessions": [{"mapping": "M1"}]}
        result = rdm.generate_mixed_workflow(workflow, [{"name": "M1"}], [])
        task_keys = [t["task_key"] for t in result["tasks"]]
        self.assertIn("dbt_test", task_keys)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 58 — DBT Deployment & CI/CD
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTDeployment(unittest.TestCase):
    """Sprint 58: DBT project CI/CD and deployment."""

    def test_ci_yml_generation(self):
        import run_dbt_migration as rdm
        yml = rdm.generate_dbt_ci_yml()
        self.assertIn("dbt CI", yml)
        self.assertIn("dbt deps", yml)
        self.assertIn("dbt build", yml)
        self.assertIn("dbt test", yml)
        self.assertIn("dbt docs generate", yml)

    def test_ci_yml_has_secrets(self):
        import run_dbt_migration as rdm
        yml = rdm.generate_dbt_ci_yml()
        self.assertIn("DBT_DATABRICKS_HOST", yml)
        self.assertIn("DBT_DATABRICKS_TOKEN", yml)

    def test_deploy_script_generation(self):
        import run_dbt_migration as rdm
        script = rdm.generate_deploy_dbt_script()
        self.assertIn("deploy_to_repos", script)
        self.assertIn("workspace_url", script)
        self.assertIn("Databricks Repo", script)

    def test_deploy_script_has_update_logic(self):
        import run_dbt_migration as rdm
        script = rdm.generate_deploy_dbt_script()
        self.assertIn("already exists", script)
        self.assertIn("branch", script)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 59-60 — E2E DBT + Release
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTE2E(unittest.TestCase):
    """Sprint 59-60: End-to-end dbt integration tests."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_full_dbt_project_generation(self):
        import run_dbt_migration as rdm
        orig_output = rdm.OUTPUT_DIR
        rdm.OUTPUT_DIR = Path(self.tmpdir) / "dbt"
        try:
            dbt_mappings = [
                {"name": "M_SIMPLE", "sources": ["S.T"], "targets": ["TGT"],
                 "transformations": ["SQ", "EXP"], "complexity": "Simple"},
            ]
            summary = rdm.write_dbt_project(dbt_mappings, dbt_mappings)
            self.assertEqual(summary["total_dbt_mappings"], 1)

            # Verify files exist
            base = Path(self.tmpdir) / "dbt"
            self.assertTrue((base / "dbt_project.yml").exists())
            self.assertTrue((base / "profiles.yml").exists())
            self.assertTrue((base / "packages.yml").exists())
            self.assertTrue((base / "models" / "sources.yml").exists())
            self.assertTrue((base / "models" / "schema.yml").exists())
            self.assertTrue((base / "models" / "staging" / "stg_m_simple.sql").exists())
            self.assertTrue((base / "models" / "intermediate" / "int_m_simple.sql").exists())
            self.assertTrue((base / "models" / "marts" / "mart_m_simple.sql").exists())
        finally:
            rdm.OUTPUT_DIR = orig_output

    def test_dbt_project_with_upsert(self):
        import run_dbt_migration as rdm
        orig_output = rdm.OUTPUT_DIR
        rdm.OUTPUT_DIR = Path(self.tmpdir) / "dbt"
        try:
            dbt_mappings = [
                {"name": "M_UPD", "sources": ["S.T"], "targets": ["TGT"],
                 "transformations": ["SQ", "UPD"], "complexity": "Medium"},
            ]
            rdm.write_dbt_project(dbt_mappings, dbt_mappings)

            # Should have snapshot + incremental mart
            base = Path(self.tmpdir) / "dbt"
            mart = (base / "models" / "marts" / "mart_m_upd.sql").read_text(encoding="utf-8")
            self.assertIn("incremental", mart)
            self.assertTrue((base / "snapshots" / "snp_m_upd.sql").exists())
        finally:
            rdm.OUTPUT_DIR = orig_output

    def test_dbt_project_macros_generated(self):
        import run_dbt_migration as rdm
        orig_output = rdm.OUTPUT_DIR
        rdm.OUTPUT_DIR = Path(self.tmpdir) / "dbt"
        try:
            rdm.write_dbt_project([
                {"name": "M1", "sources": ["S.T"], "targets": ["T"],
                 "transformations": ["SQ"], "complexity": "Simple"},
            ], [])
            base = Path(self.tmpdir) / "dbt"
            self.assertTrue((base / "macros" / "clean_string.sql").exists())
            self.assertTrue((base / "macros" / "safe_divide.sql").exists())
        finally:
            rdm.OUTPUT_DIR = orig_output

    def test_dbt_project_ci_yml(self):
        import run_dbt_migration as rdm
        orig_output = rdm.OUTPUT_DIR
        rdm.OUTPUT_DIR = Path(self.tmpdir) / "dbt"
        try:
            rdm.write_dbt_project([
                {"name": "M1", "sources": ["S.T"], "targets": ["T"],
                 "transformations": ["SQ"], "complexity": "Simple"},
            ], [])
            base = Path(self.tmpdir) / "dbt"
            self.assertTrue((base / ".github" / "workflows" / "dbt_ci.yml").exists())
        finally:
            rdm.OUTPUT_DIR = orig_output

    def test_dbt_project_tests_generated(self):
        import run_dbt_migration as rdm
        orig_output = rdm.OUTPUT_DIR
        rdm.OUTPUT_DIR = Path(self.tmpdir) / "dbt"
        try:
            rdm.write_dbt_project([
                {"name": "M1", "sources": ["S.T"], "targets": ["T"],
                 "transformations": ["SQ", "EXP"], "complexity": "Simple"},
            ], [])
            base = Path(self.tmpdir) / "dbt"
            self.assertTrue((base / "tests" / "assert_row_count_m1.sql").exists())
            self.assertTrue((base / "tests" / "assert_transform_m1.sql").exists())
        finally:
            rdm.OUTPUT_DIR = orig_output

    def test_dbt_project_mapplet_macro(self):
        import run_dbt_migration as rdm
        orig_output = rdm.OUTPUT_DIR
        rdm.OUTPUT_DIR = Path(self.tmpdir) / "dbt"
        try:
            rdm.write_dbt_project([
                {"name": "M_MPLT", "sources": ["S.T"], "targets": ["T"],
                 "transformations": ["SQ", "MPLT"], "complexity": "Simple"},
            ], [])
            base = Path(self.tmpdir) / "dbt"
            self.assertTrue((base / "macros" / "mapplet_m_mplt.sql").exists())
        finally:
            rdm.OUTPUT_DIR = orig_output

    def test_classify_routing(self):
        import run_dbt_migration as rdm
        mappings = [
            {"name": "M_SIMPLE", "transformations": ["SQ", "EXP"], "complexity": "Simple"},
            {"name": "M_COMPLEX", "transformations": ["SQ", "JTX"], "complexity": "Complex"},
            {"name": "M_MEDIUM", "transformations": ["SQ", "AGG"], "complexity": "Medium"},
        ]
        dbt_list, pyspark_list = rdm.classify_mappings(mappings)
        self.assertEqual(len(dbt_list), 2)  # Simple + Medium
        self.assertEqual(len(pyspark_list), 1)  # Complex


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 62 — AutoSys Enhanced Conversion
# ═════════════════════════════════════════════════════════════════════════════

class TestAutoSysConditionConversion(unittest.TestCase):
    """Sprint 62: Enhanced condition conversion."""

    def test_success_condition(self):
        import run_autosys_migration as ram
        result = ram.convert_condition_to_expression("s(JOB_A)")
        self.assertIn("Succeeded", result)
        self.assertIn("JOB_A", result)

    def test_failure_condition(self):
        import run_autosys_migration as ram
        result = ram.convert_condition_to_expression("f(JOB_B)")
        self.assertIn("Failed", result)

    def test_notrunning_condition(self):
        import run_autosys_migration as ram
        result = ram.convert_condition_to_expression("n(JOB_C)")
        self.assertIn("InProgress", result)

    def test_done_condition(self):
        import run_autosys_migration as ram
        result = ram.convert_condition_to_expression("d(JOB_D)")
        self.assertIn("Skipped", result)

    def test_compound_condition(self):
        import run_autosys_migration as ram
        result = ram.convert_condition_to_expression("s(JOB_A) AND s(JOB_B)")
        self.assertIn("&&", result)

    def test_or_condition(self):
        import run_autosys_migration as ram
        result = ram.convert_condition_to_expression("s(JOB_A) OR f(JOB_B)")
        self.assertIn("||", result)

    def test_empty_condition(self):
        import run_autosys_migration as ram
        result = ram.convert_condition_to_expression("")
        self.assertEqual(result, "")


class TestAutoSysAlarmActivity(unittest.TestCase):
    """Sprint 62: Alarm/notification activity generation."""

    def test_alarm_fabric(self):
        import run_autosys_migration as ram
        job = {"name": "JOB_A", "alarm_if_fail": "1"}
        result = ram.generate_alarm_activity(job, "fabric")
        self.assertIsNotNone(result)
        self.assertEqual(result["type"], "WebActivity")
        self.assertIn("Failed", str(result["dependsOn"]))

    def test_alarm_databricks(self):
        import run_autosys_migration as ram
        job = {"name": "JOB_A", "alarm_if_fail": "1"}
        result = ram.generate_alarm_activity(job, "databricks")
        self.assertIsNotNone(result)
        self.assertIn("notification_settings", result)

    def test_no_alarm(self):
        import run_autosys_migration as ram
        job = {"name": "JOB_A", "alarm_if_fail": "0"}
        result = ram.generate_alarm_activity(job)
        self.assertIsNone(result)

    def test_send_notification(self):
        import run_autosys_migration as ram
        job = {"name": "JOB_A", "send_notification": "1"}
        result = ram.generate_alarm_activity(job, "fabric")
        self.assertIsNotNone(result)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 63 — Calendar, Profile & Machine Mapping
# ═════════════════════════════════════════════════════════════════════════════

class TestAutoSysCalendarParsing(unittest.TestCase):
    """Sprint 63: Calendar definition parsing."""

    def test_business_calendar(self):
        import run_autosys_migration as ram
        cal = {"name": "CAL_BUSINESS", "days": ["BUSINESS_DAYS"]}
        result = ram.parse_calendar_definition(cal)
        self.assertEqual(result["type"], "business_days")
        self.assertEqual(result["name"], "CAL_BUSINESS")

    def test_holiday_calendar(self):
        import run_autosys_migration as ram
        cal = {"name": "CAL_HOLIDAY", "days": ["HOLIDAY_LIST"]}
        result = ram.parse_calendar_definition(cal)
        self.assertEqual(result["type"], "holiday_exclusion")

    def test_custom_calendar(self):
        import run_autosys_migration as ram
        cal = {"name": "CAL_CUSTOM", "days": ["2026-01-15", "2026-02-15"]}
        result = ram.parse_calendar_definition(cal)
        self.assertEqual(result["type"], "custom")


class TestAutoSysMachineMapping(unittest.TestCase):
    """Sprint 63: Machine/profile → cluster mapping."""

    def test_gpu_machine(self):
        import run_autosys_migration as ram
        result = ram.map_machine_to_cluster("gpu-server-01")
        self.assertEqual(result["cluster_type"], "gpu")

    def test_hpc_machine(self):
        import run_autosys_migration as ram
        result = ram.map_machine_to_cluster("hpc-node-01")
        self.assertEqual(result["cluster_type"], "memory_optimized")

    def test_standard_machine(self):
        import run_autosys_migration as ram
        result = ram.map_machine_to_cluster("app-server-01")
        self.assertEqual(result["cluster_type"], "standard")

    def test_ml_profile(self):
        import run_autosys_migration as ram
        result = ram.map_machine_to_cluster("server-01", "ml-workload")
        self.assertEqual(result["cluster_type"], "gpu")

    def test_empty_machine(self):
        import run_autosys_migration as ram
        result = ram.map_machine_to_cluster("")
        self.assertEqual(result["cluster_type"], "standard")


class TestAutoSysGlobalVariables(unittest.TestCase):
    """Sprint 63: Global variable extraction."""

    def test_extract_from_command(self):
        import run_autosys_migration as ram
        jobs = [
            {"name": "JOB_A", "command": "echo ${DATA_DIR}/file.csv"},
        ]
        result = ram.extract_global_variables(jobs)
        self.assertIn("DATA_DIR", result)

    def test_extract_from_std_out(self):
        import run_autosys_migration as ram
        jobs = [
            {"name": "JOB_A", "command": "ls", "std_out_file": "/logs/${ENV}/output.log"},
        ]
        result = ram.extract_global_variables(jobs)
        self.assertIn("ENV", result)

    def test_no_variables(self):
        import run_autosys_migration as ram
        jobs = [
            {"name": "JOB_A", "command": "echo hello"},
        ]
        result = ram.extract_global_variables(jobs)
        self.assertEqual(len(result), 0)

    def test_multiple_variables(self):
        import run_autosys_migration as ram
        jobs = [
            {"name": "JOB_A", "command": "${BIN_DIR}/script.sh ${DATA_DIR}/input.csv"},
        ]
        result = ram.extract_global_variables(jobs)
        self.assertIn("BIN_DIR", result)
        self.assertIn("DATA_DIR", result)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 64 — AutoSys Coverage Report
# ═════════════════════════════════════════════════════════════════════════════

class TestAutoSysCoverageReport(unittest.TestCase):
    """Sprint 64: AutoSys migration coverage report."""

    def test_basic_report(self):
        import run_autosys_migration as ram
        jobs = [
            {"name": "BOX_A", "job_type": "BOX"},
            {"name": "CMD_1", "job_type": "CMD", "command": "pmcmd startworkflow -w WF_A"},
            {"name": "CMD_2", "job_type": "CMD", "command": "echo hello"},
        ]
        linkage = {
            "CMD_1": {"workflow": "WF_A", "linked": True},
        }
        dep_graph = {"CMD_1": [{"job": "BOX_A", "status": "success"}]}
        calendars = [{"name": "CAL_1"}]
        report = ram.generate_coverage_report(jobs, linkage, dep_graph, calendars)
        self.assertEqual(report["total_jobs"], 3)
        self.assertEqual(report["informatica_linkage"]["pmcmd_jobs"], 1)
        self.assertEqual(report["informatica_linkage"]["linked_to_inventory"], 1)

    def test_linkage_rate(self):
        import run_autosys_migration as ram
        jobs = [{"name": "CMD_1", "job_type": "CMD"}]
        linkage = {"CMD_1": {"workflow": "WF_A", "linked": True}}
        report = ram.generate_coverage_report(jobs, linkage, {}, [])
        self.assertEqual(report["informatica_linkage"]["linkage_rate"], 100.0)

    def test_unlinked_workflows(self):
        import run_autosys_migration as ram
        jobs = [{"name": "CMD_1", "job_type": "CMD"}]
        linkage = {"CMD_1": {"workflow": "WF_MISSING", "linked": False}}
        report = ram.generate_coverage_report(jobs, linkage, {}, [])
        self.assertEqual(report["informatica_linkage"]["unlinked_workflows"], ["WF_MISSING"])

    def test_job_types(self):
        import run_autosys_migration as ram
        jobs = [
            {"name": "B1", "job_type": "BOX"},
            {"name": "C1", "job_type": "CMD", "command": "x"},
            {"name": "C2", "job_type": "CMD", "command": "y"},
            {"name": "F1", "job_type": "FW", "watch_file": "/tmp/*.csv"},
        ]
        report = ram.generate_coverage_report(jobs, {}, {}, [])
        self.assertEqual(report["job_types"]["BOX"], 1)
        self.assertEqual(report["job_types"]["CMD"], 2)
        self.assertEqual(report["job_types"]["FW"], 1)

    def test_schedule_coverage(self):
        import run_autosys_migration as ram
        jobs = [
            {"name": "J1", "job_type": "CMD", "command": "x", "start_times": "02:00"},
        ]
        report = ram.generate_coverage_report(jobs, {}, {}, [])
        self.assertEqual(report["schedule_coverage"]["jobs_with_schedule"], 1)


# ═════════════════════════════════════════════════════════════════════════════
#  Sprint 48/50 — Multi-target integration & packaging
# ═════════════════════════════════════════════════════════════════════════════

class TestMultiTargetIntegration(unittest.TestCase):
    """Sprint 48/50: Multi-target consistency checks."""

    def test_phases_list_complete(self):
        import run_migration
        phases = run_migration.PHASES
        self.assertEqual(len(phases), 9)
        names = [p["name"] for p in phases]
        self.assertIn("Assessment", names)
        self.assertIn("DBT Migration", names)
        self.assertIn("AutoSys Migration", names)
        self.assertIn("Functions Migration", names)
        self.assertIn("Validation", names)

    def test_dbt_module_importable(self):
        import run_dbt_migration
        self.assertTrue(hasattr(run_dbt_migration, "should_use_dbt"))
        self.assertTrue(hasattr(run_dbt_migration, "classify_mappings"))
        self.assertTrue(hasattr(run_dbt_migration, "generate_staging_model"))

    def test_autosys_module_importable(self):
        import run_autosys_migration
        self.assertTrue(hasattr(run_autosys_migration, "parse_jil"))
        self.assertTrue(hasattr(run_autosys_migration, "classify_job"))
        self.assertTrue(hasattr(run_autosys_migration, "generate_coverage_report"))

    def test_deploy_databricks_importable(self):
        import deploy_to_databricks
        self.assertTrue(hasattr(deploy_to_databricks, "generate_uc_lineage"))
        self.assertTrue(hasattr(deploy_to_databricks, "generate_dlt_notebook"))
        self.assertTrue(hasattr(deploy_to_databricks, "estimate_dbu_cost"))

    def test_dbt_new_functions_exist(self):
        import run_dbt_migration as rdm
        self.assertTrue(hasattr(rdm, "generate_mapplet_macro"))
        self.assertTrue(hasattr(rdm, "generate_utility_macros"))
        self.assertTrue(hasattr(rdm, "generate_snapshot"))
        self.assertTrue(hasattr(rdm, "generate_incremental_model"))
        self.assertTrue(hasattr(rdm, "generate_mixed_workflow"))
        self.assertTrue(hasattr(rdm, "generate_dbt_ci_yml"))
        self.assertTrue(hasattr(rdm, "generate_deploy_dbt_script"))

    def test_autosys_new_functions_exist(self):
        import run_autosys_migration as ram
        self.assertTrue(hasattr(ram, "convert_condition_to_expression"))
        self.assertTrue(hasattr(ram, "generate_alarm_activity"))
        self.assertTrue(hasattr(ram, "parse_calendar_definition"))
        self.assertTrue(hasattr(ram, "map_machine_to_cluster"))
        self.assertTrue(hasattr(ram, "extract_global_variables"))
        self.assertTrue(hasattr(ram, "generate_coverage_report"))

    def test_deploy_new_functions_exist(self):
        import deploy_to_databricks as dtd
        self.assertTrue(hasattr(dtd, "generate_uc_lineage"))
        self.assertTrue(hasattr(dtd, "generate_dlt_notebook"))
        self.assertTrue(hasattr(dtd, "generate_dlt_notebooks"))
        self.assertTrue(hasattr(dtd, "recommend_cluster_policies"))
        self.assertTrue(hasattr(dtd, "generate_sql_dashboard_queries"))
        self.assertTrue(hasattr(dtd, "generate_advanced_workflow"))
        self.assertTrue(hasattr(dtd, "estimate_dbu_cost"))


if __name__ == "__main__":
    unittest.main()
