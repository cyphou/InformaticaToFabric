"""
Tests for Azure Databricks target support.

Covers:
- Target platform helpers (_get_target, _get_catalog, _table_ref, _widget_get, _secret_get)
- Databricks notebook generation (dbutils, Unity Catalog 3-level namespace)
- Databricks workflow (Jobs JSON) generation
- Schema generator with Unity Catalog DDL
- Validation with 3-level namespace
- Key Vault → dbutils.secrets substitution
- Pipeline migration dispatch (Fabric vs Databricks)
- CLI --target flag parsing
"""

import json
import os
import re
import sys
import textwrap
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════

def _make_mapping(name="M_TEST"):
    """Minimal mapping dict for generation tests."""
    return {
        "name": name,
        "sources": ["Oracle.SALES.CUSTOMERS"],
        "targets": ["DIM_CUSTOMER"],
        "transformations": ["SQ", "EXP", "FIL"],
        "has_sql_override": False,
        "complexity": "Simple",
        "sql_overrides": [],
        "lookup_conditions": [],
        "parameters": ["$$LOAD_DATE"],
        "target_load_order": [],
        "has_mapplet": False,
    }


def _make_workflow(name="WF_DAILY"):
    """Minimal workflow dict for pipeline tests."""
    return {
        "name": name,
        "sessions": ["S_M_LOAD_A", "S_M_LOAD_B"],
        "session_to_mapping": {"S_M_LOAD_A": "M_LOAD_A", "S_M_LOAD_B": "M_LOAD_B"},
        "dependencies": {"S_M_LOAD_A": ["Start"], "S_M_LOAD_B": ["S_M_LOAD_A"]},
        "schedule": "DAILY 02:00",
        "schedule_cron": {"cron": "0 0 2 * * ?"},
    }


# ═══════════════════════════════════════════════
#  1. Notebook Migration — Target Helpers
# ═══════════════════════════════════════════════

class TestNotebookTargetHelpers:
    """Test _get_target, _get_catalog, _table_ref, _widget_get, _secret_get."""

    def test_get_target_default_is_fabric(self):
        from run_notebook_migration import _get_target
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("INFORMATICA_MIGRATION_TARGET", None)
            assert _get_target() == "fabric"

    def test_get_target_databricks(self):
        from run_notebook_migration import _get_target
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks"}):
            assert _get_target() == "databricks"

    def test_get_catalog_default(self):
        from run_notebook_migration import _get_catalog
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("INFORMATICA_DATABRICKS_CATALOG", None)
            assert _get_catalog() == "main"

    def test_get_catalog_custom(self):
        from run_notebook_migration import _get_catalog
        with mock.patch.dict(os.environ, {"INFORMATICA_DATABRICKS_CATALOG": "analytics"}):
            assert _get_catalog() == "analytics"

    def test_table_ref_fabric(self):
        from run_notebook_migration import _table_ref
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            assert _table_ref("silver", "customers") == "silver.customers"

    def test_table_ref_databricks(self):
        from run_notebook_migration import _table_ref
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "main"}):
            assert _table_ref("silver", "customers") == "main.silver.customers"

    def test_table_ref_databricks_custom_catalog(self):
        from run_notebook_migration import _table_ref
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "analytics"}):
            assert _table_ref("bronze", "orders") == "analytics.bronze.orders"

    def test_widget_get_fabric(self):
        from run_notebook_migration import _widget_get
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            result = _widget_get("load_date")
            assert "notebookutils.widgets.get" in result
            assert "load_date" in result

    def test_widget_get_databricks(self):
        from run_notebook_migration import _widget_get
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks"}):
            result = _widget_get("load_date")
            assert "dbutils.widgets.get" in result
            assert "load_date" in result

    def test_secret_get_fabric(self):
        from run_notebook_migration import _secret_get
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            result = _secret_get("my-kv", "db-password")
            assert "notebookutils.credentials.getSecret" in result
            assert "my-kv" in result
            assert "db-password" in result

    def test_secret_get_databricks(self):
        from run_notebook_migration import _secret_get
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks"}):
            result = _secret_get("migration-scope", "db-password")
            assert "dbutils.secrets.get" in result
            assert "migration-scope" in result
            assert "db-password" in result


# ═══════════════════════════════════════════════
#  2. Notebook Generation — Databricks Output
# ═══════════════════════════════════════════════

class TestNotebookDatabricksGeneration:
    """Test that generated notebooks use Databricks APIs when target=databricks."""

    def test_metadata_cell_databricks(self):
        from run_notebook_migration import _metadata_cell
        mapping = _make_mapping()
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks"}):
            cell = _metadata_cell(mapping)
            assert "Databricks notebook source" in cell
            assert "python3" in cell  # kernel_info
            assert "dbutils.widgets.get" in cell  # parameter widget

    def test_metadata_cell_fabric(self):
        from run_notebook_migration import _metadata_cell
        mapping = _make_mapping()
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            cell = _metadata_cell(mapping)
            assert "Fabric notebook source" in cell
            assert "synapse_pyspark" in cell
            assert "notebookutils.widgets.get" in cell

    def test_source_cell_databricks_3level(self):
        from run_notebook_migration import _source_cell
        mapping = _make_mapping()
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "main"}):
            cell = _source_cell(mapping, 2)
            assert "main.bronze." in cell

    def test_target_cell_databricks_3level(self):
        from run_notebook_migration import _target_cell
        mapping = _make_mapping()
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "main"}):
            cell = _target_cell(mapping, 4)
            assert "main." in cell

    def test_source_cell_fabric_2level(self):
        from run_notebook_migration import _source_cell
        mapping = _make_mapping()
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            cell = _source_cell(mapping, 2)
            assert "bronze." in cell
            # Fabric uses 2-level namespace — no catalog prefix
            assert "main.bronze." not in cell


# ═══════════════════════════════════════════════
#  3. Pipeline Migration — Databricks Workflows
# ═══════════════════════════════════════════════

class TestDatabricksWorkflowGeneration:
    """Test generate_databricks_workflow() produces valid Databricks Jobs JSON."""

    def test_workflow_basic_structure(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        assert job["name"] == "PL_WF_DAILY"
        assert job["format"] == "MULTI_TASK"
        assert len(job["tasks"]) == 2

    def test_workflow_task_dependencies(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        task_a = next(t for t in job["tasks"] if "LOAD_A" in t["task_key"])
        task_b = next(t for t in job["tasks"] if "LOAD_B" in t["task_key"])

        # Task A depends on Start → no depends_on
        assert "depends_on" not in task_a or task_a.get("depends_on", []) == []
        # Task B depends on Task A
        assert len(task_b.get("depends_on", [])) > 0

    def test_workflow_notebook_path(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        for task in job["tasks"]:
            assert "notebook_task" in task
            assert task["notebook_task"]["notebook_path"].startswith("/Workspace/")

    def test_workflow_schedule(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        assert "schedule" in job
        assert job["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"
        assert job["schedule"]["pause_status"] == "PAUSED"

    def test_workflow_tags(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        assert job["tags"]["migrated_from"] == "informatica"
        assert job["tags"]["original_workflow"] == "WF_DAILY"

    def test_workflow_parameters(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        for task in job["tasks"]:
            params = task["notebook_task"]["base_parameters"]
            assert isinstance(params, dict)
            # The mapping has $$LOAD_DATE → should have load_date key
            assert "load_date" in params

    def test_workflow_no_schedule(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        wf["schedule_cron"] = {}
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        assert "schedule" not in job

    def test_workflow_retry_config(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        for task in job["tasks"]:
            assert task["timeout_seconds"] > 0
            assert task["max_retries"] >= 0

    def test_workflow_json_serializable(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)

        # Must be valid JSON
        serialized = json.dumps(job, indent=2)
        deserialized = json.loads(serialized)
        assert deserialized["name"] == "PL_WF_DAILY"


# ═══════════════════════════════════════════════
#  4. Schema Generator — Unity Catalog DDL
# ═══════════════════════════════════════════════

class TestSchemaGeneratorDatabricks:
    """Test schema generation uses Unity Catalog 3-level namespace for Databricks."""

    def _make_table(self):
        return {
            "tier": "silver",
            "name": "DIM_CUSTOMER",
            "mapping": "M_LOAD_CUSTOMERS",
            "columns": [
                {"name": "CUST_ID", "type": "NUMBER", "source": "CUSTOMERS.CUST_ID"},
                {"name": "CUST_NAME", "type": "VARCHAR2", "source": "CUSTOMERS.CUST_NAME"},
            ],
            "partition_key": None,
        }

    def test_get_target_helpers_exist(self):
        from run_schema_generator import _get_target, _get_catalog
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "analytics"}):
            assert _get_target() == "databricks"
            assert _get_catalog() == "analytics"

    def test_generate_ddl_databricks_namespace(self):
        from run_schema_generator import generate_ddl
        table = self._make_table()
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "main"}):
            ddl = generate_ddl(table)
            assert "main.silver.dim_customer" in ddl

    def test_generate_ddl_fabric_namespace(self):
        from run_schema_generator import generate_ddl
        table = self._make_table()
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            ddl = generate_ddl(table)
            assert "silver.dim_customer" in ddl
            assert "main." not in ddl

    def test_setup_notebook_databricks_catalog(self):
        from run_schema_generator import generate_setup_notebook
        tables = [self._make_table()]
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "migration_catalog"}):
            nb = generate_setup_notebook(tables)
            assert "CREATE CATALOG" in nb or "migration_catalog" in nb


# ═══════════════════════════════════════════════
#  5. Validation — Databricks Namespace
# ═══════════════════════════════════════════════

class TestValidationDatabricks:
    """Test validation module uses 3-level namespace for Databricks."""

    def test_infer_target_table_databricks(self):
        from run_validation import _infer_target_table
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "main"}):
            result = _infer_target_table("DIM_CUSTOMER", ["SQ", "EXP", "FIL"])
            assert result.startswith("main.")
            assert "silver" in result or "gold" in result

    def test_infer_target_table_fabric(self):
        from run_validation import _infer_target_table
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            result = _infer_target_table("DIM_CUSTOMER", ["SQ", "EXP"])
            assert not result.startswith("main.")
            assert "silver" in result

    def test_infer_target_table_gold_tier(self):
        from run_validation import _infer_target_table
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "analytics"}):
            result = _infer_target_table("AGG_SALES_SUMMARY", ["SQ", "AGG"])
            assert result.startswith("analytics.gold.")

    def test_infer_target_table_silver_tier(self):
        from run_validation import _infer_target_table
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "main"}):
            result = _infer_target_table("FACT_ORDERS", ["SQ", "EXP"])
            assert result.startswith("main.silver.")

    def test_validation_helpers_exist(self):
        from run_validation import _get_target, _get_catalog
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "test_catalog"}):
            assert _get_target() == "databricks"
            assert _get_catalog() == "test_catalog"


# ═══════════════════════════════════════════════
#  6. Migration Orchestrator — Key Vault + Target
# ═══════════════════════════════════════════════

class TestMigrationOrchestratorDatabricks:
    """Test run_migration.py Databricks integration."""

    def test_substitute_keyvault_databricks(self):
        from run_migration import substitute_keyvault_refs
        config = {
            "target": "databricks",
            "databricks": {"secret_scope": "my-scope"},
        }
        result = substitute_keyvault_refs({"password": "{{KV:db-password}}", **config}, "tenant123")
        assert "dbutils.secrets.get" in result.get("password", "")
        assert "my-scope" in result.get("password", "")

    def test_substitute_keyvault_fabric(self):
        from run_migration import substitute_keyvault_refs
        config = {"target": "fabric"}
        result = substitute_keyvault_refs({"password": "{{KV:db-password}}", **config}, "tenant123")
        assert "notebookutils.credentials.getSecret" in result.get("password", "")

    def test_substitute_keyvault_no_placeholder(self):
        from run_migration import substitute_keyvault_refs
        config = {"target": "databricks", "databricks": {"secret_scope": "myscope"}}
        result = substitute_keyvault_refs({"password": "plain-text", **config}, "tenant1")
        assert result.get("password") == "plain-text"

    def test_generate_summary_databricks(self):
        from run_migration import generate_summary
        results = [
            {"id": 0, "name": "Assessment", "status": "ok", "duration": 1.0, "error": None},
            {"id": 1, "name": "SQL Conversion", "status": "ok", "duration": 0.5, "error": None},
        ]
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks"}):
            path = generate_summary(results, target="databricks")
            content = path.read_text(encoding="utf-8")
            assert "Databricks" in content or "databricks" in content

    def test_generate_summary_fabric(self):
        from run_migration import generate_summary
        results = [
            {"id": 0, "name": "Assessment", "status": "ok", "duration": 1.0, "error": None},
        ]
        path = generate_summary(results, target="fabric")
        content = path.read_text(encoding="utf-8")
        assert "Fabric" in content


# ═══════════════════════════════════════════════
#  7. SQL Migration — Target-Aware Banner
# ═══════════════════════════════════════════════

class TestSqlMigrationDatabricks:
    """Test SQL migration is target-aware."""

    def test_sql_migration_has_target_label(self):
        import run_sql_migration
        source = open(run_sql_migration.__file__, encoding="utf-8").read()
        assert "Databricks Spark SQL" in source
        assert "Fabric Spark SQL" in source


# ═══════════════════════════════════════════════
#  8. Templates — Databricks-Specific
# ═══════════════════════════════════════════════

class TestDatabricksTemplates:
    """Test that Databricks template files exist and have correct structure."""

    def test_notebook_template_exists(self):
        template_path = PROJECT_ROOT / "templates" / "notebook_template_databricks.py"
        assert template_path.exists(), "Databricks notebook template should exist"

    def test_notebook_template_has_dbutils(self):
        template_path = PROJECT_ROOT / "templates" / "notebook_template_databricks.py"
        content = template_path.read_text(encoding="utf-8")
        assert "dbutils" in content
        assert "Unity Catalog" in content or "catalog" in content

    def test_pipeline_template_exists(self):
        template_path = PROJECT_ROOT / "templates" / "pipeline_template_databricks.json"
        assert template_path.exists(), "Databricks pipeline template should exist"

    def test_pipeline_template_valid_json(self):
        template_path = PROJECT_ROOT / "templates" / "pipeline_template_databricks.json"
        content = template_path.read_text(encoding="utf-8")
        data = json.loads(content)
        assert "tasks" in data or "name" in data

    def test_pipeline_template_has_notebook_task(self):
        template_path = PROJECT_ROOT / "templates" / "pipeline_template_databricks.json"
        content = template_path.read_text(encoding="utf-8")
        assert "notebook_task" in content or "notebook_path" in content


# ═══════════════════════════════════════════════
#  9. Configuration — migration.yaml
# ═══════════════════════════════════════════════

class TestMigrationConfig:
    """Test migration.yaml supports Databricks configuration."""

    def test_yaml_has_target_field(self):
        import yaml
        config_path = PROJECT_ROOT / "migration.yaml"
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        assert "target" in config

    def test_yaml_has_databricks_section(self):
        import yaml
        config_path = PROJECT_ROOT / "migration.yaml"
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        assert "databricks" in config
        db = config["databricks"]
        assert "workspace_url" in db
        assert "catalog" in db
        assert "secret_scope" in db


# ═══════════════════════════════════════════════
#  10. Pipeline Dispatch — Target Routing
# ═══════════════════════════════════════════════

class TestPipelineDispatch:
    """Test pipeline migration dispatches to correct generator based on target."""

    def test_get_target_pipeline(self):
        from run_pipeline_migration import _get_target
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks"}):
            assert _get_target() == "databricks"

    def test_generate_databricks_workflow_callable(self):
        from run_pipeline_migration import generate_databricks_workflow
        assert callable(generate_databricks_workflow)

    def test_fabric_pipeline_still_default(self):
        from run_pipeline_migration import _get_target
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("INFORMATICA_MIGRATION_TARGET", None)
            assert _get_target() == "fabric"


# ═══════════════════════════════════════════════
#  11. Databricks Deployment Script
# ═══════════════════════════════════════════════

class TestDeployToDatabricks:
    """Test deploy_to_databricks.py functions."""

    def test_module_imports(self):
        import deploy_to_databricks
        assert hasattr(deploy_to_databricks, "deploy_notebooks")
        assert hasattr(deploy_to_databricks, "deploy_workflows")
        assert hasattr(deploy_to_databricks, "deploy_sql_scripts")
        assert hasattr(deploy_to_databricks, "setup_secret_scope")
        assert hasattr(deploy_to_databricks, "generate_uc_permissions")
        assert hasattr(deploy_to_databricks, "recommend_cluster_config")

    def test_headers(self):
        from deploy_to_databricks import _headers
        h = _headers("dapi_test_token_123")
        assert h["Authorization"] == "Bearer dapi_test_token_123"
        assert h["Content-Type"] == "application/json"

    def test_deploy_notebooks_dry_run(self):
        from deploy_to_databricks import deploy_notebooks
        results = deploy_notebooks("https://adb-test.azuredatabricks.net", "fake-token",
                                   notebook_path="/Shared/test", dry_run=True)
        assert len(results) > 0
        for r in results:
            assert r["status"] == "dry-run"
            assert r["type"] == "Notebook"
            assert "/Shared/test/" in r["path"]

    def test_deploy_workflows_dry_run(self):
        from deploy_to_databricks import deploy_workflows
        results = deploy_workflows("https://adb-test.azuredatabricks.net", "fake-token", dry_run=True)
        assert len(results) > 0
        for r in results:
            assert r["status"] == "dry-run"
            assert r["type"] == "Job"

    def test_deploy_sql_scripts_dry_run(self):
        from deploy_to_databricks import deploy_sql_scripts
        results = deploy_sql_scripts("https://adb-test.azuredatabricks.net", "fake-token",
                                     notebook_path="/Shared/test/sql", dry_run=True)
        assert len(results) > 0
        for r in results:
            assert r["status"] == "dry-run"
            assert r["type"] == "SQL Notebook"

    def test_deploy_notebooks_count_matches_files(self):
        from deploy_to_databricks import deploy_notebooks, OUTPUT_DIR
        nb_files = list((OUTPUT_DIR / "notebooks").glob("NB_*.py"))
        results = deploy_notebooks("https://adb-test.azuredatabricks.net", "t", dry_run=True)
        assert len(results) == len(nb_files)

    def test_deploy_workflows_count_matches_files(self):
        from deploy_to_databricks import deploy_workflows, OUTPUT_DIR
        pl_files = list((OUTPUT_DIR / "pipelines").glob("PL_*.json"))
        results = deploy_workflows("https://adb-test.azuredatabricks.net", "t", dry_run=True)
        assert len(results) == len(pl_files)

    def test_deploy_sql_scripts_count_matches_files(self):
        from deploy_to_databricks import deploy_sql_scripts, OUTPUT_DIR
        sql_files = list((OUTPUT_DIR / "sql").glob("SQL_*.sql"))
        results = deploy_sql_scripts("https://adb-test.azuredatabricks.net", "t", dry_run=True)
        assert len(results) == len(sql_files)

    def test_setup_secret_scope_dry_run(self):
        from deploy_to_databricks import setup_secret_scope
        results = setup_secret_scope("https://adb-test.azuredatabricks.net", "fake-token",
                                     "test-scope", {"key1": "val1", "key2": "val2"}, dry_run=True)
        assert len(results) == 1
        assert results[0]["status"] == "dry-run"
        assert results[0]["count"] == 2

    def test_setup_secret_scope_empty_secrets(self):
        from deploy_to_databricks import setup_secret_scope
        results = setup_secret_scope("https://adb-test.azuredatabricks.net", "t",
                                     "empty-scope", {}, dry_run=True)
        assert results[0]["count"] == 0

    def test_notebook_path_default(self):
        from deploy_to_databricks import deploy_notebooks
        results = deploy_notebooks("https://adb-test.azuredatabricks.net", "t", dry_run=True)
        for r in results:
            assert r["path"].startswith("/Shared/migration/")

    def test_notebook_path_custom(self):
        from deploy_to_databricks import deploy_notebooks
        results = deploy_notebooks("https://adb-test.azuredatabricks.net", "t",
                                   notebook_path="/Users/me/project", dry_run=True)
        for r in results:
            assert r["path"].startswith("/Users/me/project/")


# ═══════════════════════════════════════════════
#  12. Unity Catalog Permissions
# ═══════════════════════════════════════════════

class TestUCPermissions:
    """Test Unity Catalog GRANT statement generation."""

    def test_generate_uc_permissions_basic(self):
        from deploy_to_databricks import generate_uc_permissions, OUTPUT_DIR
        sql = generate_uc_permissions(catalog="test_catalog")
        assert "test_catalog" in sql
        assert "GRANT USE CATALOG" in sql
        assert "GRANT USE SCHEMA" in sql
        assert "GRANT SELECT" in sql
        assert "GRANT MODIFY" in sql

    def test_generate_uc_permissions_file_written(self):
        from deploy_to_databricks import generate_uc_permissions, OUTPUT_DIR
        generate_uc_permissions(catalog="main")
        out_path = OUTPUT_DIR / "scripts" / "uc_permissions.sql"
        assert out_path.exists()
        content = out_path.read_text(encoding="utf-8")
        assert "GRANT" in content

    def test_generate_uc_permissions_schemas(self):
        from deploy_to_databricks import generate_uc_permissions
        sql = generate_uc_permissions(catalog="analytics")
        # Should have silver and/or gold schemas
        assert "silver" in sql or "gold" in sql

    def test_generate_uc_permissions_data_engineers(self):
        from deploy_to_databricks import generate_uc_permissions
        sql = generate_uc_permissions(catalog="main")
        assert "data-engineers" in sql

    def test_generate_uc_permissions_data_analysts(self):
        from deploy_to_databricks import generate_uc_permissions
        sql = generate_uc_permissions(catalog="main")
        assert "data-analysts" in sql

    def test_generate_uc_permissions_function_grants(self):
        from deploy_to_databricks import generate_uc_permissions
        sql = generate_uc_permissions(catalog="main")
        assert "EXECUTE ON ALL FUNCTIONS" in sql

    def test_generate_uc_permissions_missing_inventory(self):
        from deploy_to_databricks import generate_uc_permissions
        fake_path = Path("/nonexistent/inventory.json")
        sql = generate_uc_permissions(inventory_path=fake_path)
        assert sql == ""


# ═══════════════════════════════════════════════
#  13. Cluster Configuration Recommender
# ═══════════════════════════════════════════════

class TestClusterConfigRecommender:
    """Test cluster config recommendation based on inventory."""

    def test_recommend_cluster_basic(self):
        from deploy_to_databricks import recommend_cluster_config
        config = recommend_cluster_config()
        assert "profile" in config
        assert "recommendation" in config
        assert "rationale" in config

    def test_recommend_cluster_has_node_types(self):
        from deploy_to_databricks import recommend_cluster_config
        config = recommend_cluster_config()
        rec = config["recommendation"]
        assert "driver_node_type" in rec
        assert "worker_node_type" in rec
        assert "autoscale" in rec
        assert "min_workers" in rec["autoscale"]
        assert "max_workers" in rec["autoscale"]

    def test_recommend_cluster_has_photon(self):
        from deploy_to_databricks import recommend_cluster_config
        config = recommend_cluster_config()
        rec = config["recommendation"]
        assert "enable_photon" in rec
        assert isinstance(rec["enable_photon"], bool)

    def test_recommend_cluster_has_spark_version(self):
        from deploy_to_databricks import recommend_cluster_config
        config = recommend_cluster_config()
        rec = config["recommendation"]
        assert "spark_version" in rec
        assert "14.3" in rec["spark_version"]

    def test_recommend_cluster_file_written(self):
        from deploy_to_databricks import recommend_cluster_config, OUTPUT_DIR
        recommend_cluster_config()
        out_path = OUTPUT_DIR / "inventory" / "cluster_config.json"
        assert out_path.exists()
        data = json.loads(out_path.read_text(encoding="utf-8"))
        assert data["profile"] in ("small", "medium", "large")

    def test_recommend_cluster_complexity_breakdown(self):
        from deploy_to_databricks import recommend_cluster_config
        config = recommend_cluster_config()
        assert "complexity_breakdown" in config
        assert "total_mappings" in config

    def test_recommend_cluster_missing_inventory(self):
        from deploy_to_databricks import recommend_cluster_config
        fake_path = Path("/nonexistent/inventory.json")
        config = recommend_cluster_config(inventory_path=fake_path)
        assert "error" in config

    def test_recommend_cluster_rationale(self):
        from deploy_to_databricks import recommend_cluster_config
        config = recommend_cluster_config()
        rationale = config["rationale"]
        assert "profile" in rationale
        assert "photon" in rationale
        assert "autoscale" in rationale


# ═══════════════════════════════════════════════
#  14. Databricks Notebook Content Deep Checks
# ═══════════════════════════════════════════════

class TestDatabricksNotebookContent:
    """Deep content validation of generated Databricks notebooks."""

    def test_audit_cell_databricks(self):
        from run_notebook_migration import _audit_cell
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks"}):
            cell = _audit_cell(_make_mapping("M_TEST"), 5)
            assert "Cell 5" in cell or "M_TEST" in cell

    def test_full_notebook_databricks_no_notebookutils(self):
        from run_notebook_migration import generate_notebook
        mapping = _make_mapping("M_DB_TEST")
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "databricks", "INFORMATICA_DATABRICKS_CATALOG": "main"}):
            cells = generate_notebook(mapping)
            full = "\n".join(cells)
            assert "notebookutils" not in full

    def test_full_notebook_fabric_no_dbutils(self):
        from run_notebook_migration import generate_notebook
        mapping = _make_mapping("M_FB_TEST")
        with mock.patch.dict(os.environ, {"INFORMATICA_MIGRATION_TARGET": "fabric"}):
            cells = generate_notebook(mapping)
            full = "\n".join(cells)
            assert "dbutils.widgets" not in full
            assert "dbutils.secrets" not in full


# ═══════════════════════════════════════════════
#  15. Databricks Workflow Edge Cases
# ═══════════════════════════════════════════════

class TestDatabricksWorkflowEdgeCases:
    """Additional edge case tests for Databricks workflow generation."""

    def test_single_session_workflow(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = {
            "name": "WF_SINGLE",
            "sessions": ["S_ONLY"],
            "session_to_mapping": {"S_ONLY": "M_ONLY"},
            "dependencies": {"S_ONLY": ["Start"]},
            "schedule": "",
            "schedule_cron": {},
        }
        mappings = {"M_ONLY": _make_mapping("M_ONLY")}
        job = generate_databricks_workflow(wf, mappings)
        assert len(job["tasks"]) == 1
        assert "schedule" not in job

    def test_workflow_name_prefix(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow("WF_NIGHTLY_BATCH")
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)
        assert job["name"].startswith("PL_")

    def test_workflow_task_keys_unique(self):
        from run_pipeline_migration import generate_databricks_workflow
        wf = _make_workflow()
        mappings = {"M_LOAD_A": _make_mapping("M_LOAD_A"), "M_LOAD_B": _make_mapping("M_LOAD_B")}
        job = generate_databricks_workflow(wf, mappings)
        keys = [t["task_key"] for t in job["tasks"]]
        assert len(keys) == len(set(keys))
