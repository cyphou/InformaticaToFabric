"""
Tests for artifact validation (run_artifact_validation.py).

Tests cover:
  - Fabric pipeline JSON validation (structure, types, deps, circular refs, nesting)
  - Databricks workflow JSON validation (tasks, depends_on, task types)
  - DBT SQL model validation (syntax, Jinja, CTE, parentheses, trailing commas)
  - DBT YAML validation (tabs, indentation, version key)
  - Notebook structure validation (headers, cells, imports, I/O, credentials)
  - Batch validation across output/ directory
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORKSPACE))

import run_artifact_validation as rav


# ═════════════════════════════════════════════════════════════════════════════
#  Fabric Pipeline Validation
# ═════════════════════════════════════════════════════════════════════════════

class TestFabricPipelineValidation(unittest.TestCase):
    """Fabric Data Pipeline JSON schema validation."""

    def _valid_pipeline(self):
        return {
            "name": "PL_TEST",
            "properties": {
                "activities": [
                    {
                        "name": "NB_Step1",
                        "type": "TridentNotebook",
                        "dependsOn": [],
                        "policy": {"timeout": "0.02:00:00", "retry": 2},
                        "typeProperties": {
                            "notebook": {
                                "referenceName": "NB_MAPPING_A",
                                "type": "NotebookReference"
                            }
                        }
                    }
                ]
            }
        }

    def test_valid_pipeline(self):
        issues = rav.validate_fabric_pipeline(self._valid_pipeline())
        self.assertEqual(issues, [])

    def test_missing_name(self):
        p = self._valid_pipeline()
        del p["name"]
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("name" in i for i in issues))

    def test_missing_properties(self):
        p = {"name": "PL"}
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("properties" in i for i in issues))

    def test_missing_activities(self):
        p = {"name": "PL", "properties": {}}
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("activities" in i for i in issues))

    def test_invalid_activity_type(self):
        p = self._valid_pipeline()
        p["properties"]["activities"][0]["type"] = "BogusType"
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("BogusType" in i for i in issues))

    def test_missing_activity_name(self):
        p = self._valid_pipeline()
        del p["properties"]["activities"][0]["name"]
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("name" in i for i in issues))

    def test_duplicate_activity_name(self):
        p = self._valid_pipeline()
        act = p["properties"]["activities"][0].copy()
        p["properties"]["activities"].append(act)
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("Duplicate" in i for i in issues))

    def test_invalid_dependency_condition(self):
        p = self._valid_pipeline()
        p["properties"]["activities"][0]["dependsOn"] = [
            {"activity": "X", "dependencyConditions": ["InvalidCond"]}
        ]
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("InvalidCond" in i for i in issues))

    def test_valid_dependency_conditions(self):
        p = self._valid_pipeline()
        p["properties"]["activities"].append({
            "name": "NB_Step2",
            "type": "TridentNotebook",
            "dependsOn": [
                {"activity": "NB_Step1", "dependencyConditions": ["Succeeded"]}
            ],
            "typeProperties": {
                "notebook": {"referenceName": "NB_B", "type": "NotebookReference"}
            }
        })
        issues = rav.validate_fabric_pipeline(p)
        self.assertEqual(issues, [])

    def test_notebook_missing_reference_name(self):
        p = self._valid_pipeline()
        p["properties"]["activities"][0]["typeProperties"]["notebook"] = {}
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("referenceName" in i for i in issues))

    def test_if_condition_missing_expression(self):
        p = self._valid_pipeline()
        p["properties"]["activities"][0] = {
            "name": "IF_CHECK",
            "type": "IfCondition",
            "dependsOn": [],
            "typeProperties": {"ifTrueActivities": [], "ifFalseActivities": []}
        }
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("expression" in i for i in issues))

    def test_invalid_timeout_format(self):
        p = self._valid_pipeline()
        p["properties"]["activities"][0]["policy"]["timeout"] = "2hours"
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("timeout" in i for i in issues))

    def test_valid_timeout_format(self):
        p = self._valid_pipeline()
        p["properties"]["activities"][0]["policy"]["timeout"] = "0.02:00:00"
        issues = rav.validate_fabric_pipeline(p)
        self.assertEqual(issues, [])

    def test_negative_retry(self):
        p = self._valid_pipeline()
        p["properties"]["activities"][0]["policy"]["retry"] = -1
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("retry" in i for i in issues))

    def test_circular_dependency(self):
        p = {
            "name": "PL_CIRCULAR",
            "properties": {
                "activities": [
                    {"name": "A", "type": "TridentNotebook", "dependsOn": [
                        {"activity": "B", "dependencyConditions": ["Succeeded"]}
                    ], "typeProperties": {"notebook": {"referenceName": "X"}}},
                    {"name": "B", "type": "TridentNotebook", "dependsOn": [
                        {"activity": "A", "dependencyConditions": ["Succeeded"]}
                    ], "typeProperties": {"notebook": {"referenceName": "Y"}}},
                ]
            }
        }
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("Circular" in i for i in issues))

    def test_nested_if_condition_activities(self):
        p = {
            "name": "PL_NESTED",
            "properties": {
                "activities": [{
                    "name": "IF_1",
                    "type": "IfCondition",
                    "dependsOn": [],
                    "typeProperties": {
                        "expression": {"value": "@true", "type": "Expression"},
                        "ifTrueActivities": [
                            {"name": "NB_NESTED", "type": "TridentNotebook",
                             "typeProperties": {"notebook": {"referenceName": "NB_X"}}}
                        ],
                        "ifFalseActivities": []
                    }
                }]
            }
        }
        issues = rav.validate_fabric_pipeline(p)
        self.assertEqual(issues, [])

    def test_nested_missing_type(self):
        p = {
            "name": "PL_NESTED",
            "properties": {
                "activities": [{
                    "name": "IF_1",
                    "type": "IfCondition",
                    "dependsOn": [],
                    "typeProperties": {
                        "expression": {"value": "@true", "type": "Expression"},
                        "ifTrueActivities": [
                            {"name": "MISSING_TYPE"}  # no "type" field
                        ],
                        "ifFalseActivities": []
                    }
                }]
            }
        }
        issues = rav.validate_fabric_pipeline(p)
        self.assertTrue(any("type" in i for i in issues))


# ═════════════════════════════════════════════════════════════════════════════
#  Databricks Workflow Validation
# ═════════════════════════════════════════════════════════════════════════════

class TestDatabricksWorkflowValidation(unittest.TestCase):
    """Databricks Workflow (Jobs API) JSON schema validation."""

    def _valid_workflow(self):
        return {
            "name": "WF_TEST",
            "tasks": [
                {
                    "task_key": "step_1",
                    "notebook_task": {
                        "notebook_path": "/Workspace/migration/NB_M1"
                    },
                    "timeout_seconds": 3600,
                    "max_retries": 1,
                }
            ]
        }

    def test_valid_workflow(self):
        issues = rav.validate_databricks_workflow(self._valid_workflow())
        self.assertEqual(issues, [])

    def test_missing_name(self):
        w = self._valid_workflow()
        del w["name"]
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("name" in i for i in issues))

    def test_missing_tasks(self):
        w = {"name": "WF"}
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("tasks" in i for i in issues))

    def test_missing_task_key(self):
        w = self._valid_workflow()
        del w["tasks"][0]["task_key"]
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("task_key" in i for i in issues))

    def test_duplicate_task_key(self):
        w = self._valid_workflow()
        w["tasks"].append(w["tasks"][0].copy())
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("Duplicate" in i for i in issues))

    def test_no_task_type(self):
        w = {"name": "WF", "tasks": [{"task_key": "x"}]}
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("No task type" in i for i in issues))

    def test_multiple_task_types(self):
        w = {"name": "WF", "tasks": [{
            "task_key": "x",
            "notebook_task": {"notebook_path": "/a"},
            "dbt_task": {"commands": ["dbt run"]},
        }]}
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("Multiple" in i for i in issues))

    def test_dbt_task_missing_commands(self):
        w = {"name": "WF", "tasks": [{"task_key": "x", "dbt_task": {}}]}
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("commands" in i for i in issues))

    def test_notebook_task_missing_path(self):
        w = {"name": "WF", "tasks": [{"task_key": "x", "notebook_task": {}}]}
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("notebook_path" in i for i in issues))

    def test_depends_on_unknown_task(self):
        w = {"name": "WF", "tasks": [{
            "task_key": "step1",
            "notebook_task": {"notebook_path": "/a"},
            "depends_on": [{"task_key": "nonexistent"}]
        }]}
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("nonexistent" in i for i in issues))

    def test_valid_depends_on(self):
        w = {"name": "WF", "tasks": [
            {"task_key": "A", "notebook_task": {"notebook_path": "/a"}},
            {"task_key": "B", "notebook_task": {"notebook_path": "/b"},
             "depends_on": [{"task_key": "A"}]},
        ]}
        issues = rav.validate_databricks_workflow(w)
        self.assertEqual(issues, [])

    def test_circular_dependency(self):
        w = {"name": "WF", "tasks": [
            {"task_key": "A", "notebook_task": {"notebook_path": "/a"},
             "depends_on": [{"task_key": "B"}]},
            {"task_key": "B", "notebook_task": {"notebook_path": "/b"},
             "depends_on": [{"task_key": "A"}]},
        ]}
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("Circular" in i for i in issues))

    def test_invalid_max_retries(self):
        w = self._valid_workflow()
        w["tasks"][0]["max_retries"] = -5
        issues = rav.validate_databricks_workflow(w)
        self.assertTrue(any("max_retries" in i for i in issues))


# ═════════════════════════════════════════════════════════════════════════════
#  DBT SQL Model Validation
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTModelValidation(unittest.TestCase):
    """DBT SQL model syntax validation."""

    def test_valid_model(self):
        sql = """{{ config(materialized="view") }}
SELECT * FROM {{ source('schema', 'table') }}"""
        issues = rav.validate_dbt_model(sql)
        self.assertEqual(issues, [])

    def test_empty_model(self):
        issues = rav.validate_dbt_model("")
        self.assertTrue(any("Empty" in i for i in issues))

    def test_missing_select(self):
        sql = """{{ config(materialized="view") }}
-- just a comment"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("SELECT" in i for i in issues))

    def test_trailing_comma_before_from(self):
        sql = """{{ config(materialized="view") }}
SELECT a, b,
FROM {{ source('s', 't') }}"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("Trailing comma before FROM" in i for i in issues))

    def test_trailing_comma_before_where(self):
        sql = """{{ config(materialized="view") }}
SELECT a, b,
WHERE x = 1"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("WHERE" in i for i in issues))

    def test_trailing_comma_before_group_by(self):
        sql = """{{ config(materialized="view") }}
SELECT a, COUNT(*),
GROUP BY a"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("GROUP BY" in i for i in issues))

    def test_unmatched_jinja_braces(self):
        sql = """{{ config(materialized="view") }}
SELECT * FROM {{ source('s', 't')"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("Unmatched Jinja braces" in i for i in issues))

    def test_unmatched_jinja_blocks(self):
        sql = """{% if is_incremental()
SELECT * FROM t"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("Unmatched Jinja blocks" in i for i in issues))

    def test_unmatched_parentheses(self):
        sql = """{{ config(materialized="view") }}
SELECT COALESCE(a, ( b )
FROM {{ ref('t') }}"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("parenthesis" in i.lower() for i in issues))

    def test_duplicate_cte(self):
        sql = """{{ config(materialized="view") }}
WITH step1 AS (SELECT 1), step1 AS (SELECT 2)
SELECT * FROM step1"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("Duplicate CTE" in i for i in issues))

    def test_valid_cte_chain(self):
        sql = """{{ config(materialized="view") }}
WITH source AS (
    SELECT * FROM {{ ref('stg_m') }}
), expressions AS (
    SELECT *, 'x' AS derived FROM source
), filtered AS (
    SELECT * FROM expressions WHERE derived IS NOT NULL
)
SELECT * FROM filtered"""
        issues = rav.validate_dbt_model(sql)
        self.assertEqual(issues, [])

    def test_filename_in_issue(self):
        issues = rav.validate_dbt_model("", "my_model.sql")
        self.assertTrue(any("my_model.sql" in i for i in issues))

    def test_valid_jinja_ref(self):
        sql = """{{ config(materialized="table") }}
SELECT * FROM {{ ref('stg_model') }}"""
        issues = rav.validate_dbt_model(sql)
        self.assertEqual(issues, [])

    def test_trailing_comma_before_closing_paren(self):
        sql = """{{ config(materialized="view") }}
SELECT COALESCE(a, b, ) FROM t"""
        issues = rav.validate_dbt_model(sql)
        self.assertTrue(any("closing parenthesis" in i for i in issues))


# ═════════════════════════════════════════════════════════════════════════════
#  DBT YAML Validation
# ═════════════════════════════════════════════════════════════════════════════

class TestDBTYamlValidation(unittest.TestCase):
    """DBT YAML file validation."""

    def test_valid_schema_yml(self):
        yml = """version: 2
models:
  - name: stg_model
    columns:
      - name: id
        tests:
          - unique
          - not_null"""
        issues = rav.validate_dbt_yaml(yml, "schema.yml")
        self.assertEqual(issues, [])

    def test_empty_yaml(self):
        issues = rav.validate_dbt_yaml("", "schema.yml")
        self.assertTrue(any("Empty" in i for i in issues))

    def test_tab_characters(self):
        yml = "version: 2\nmodels:\n\t- name: x"
        issues = rav.validate_dbt_yaml(yml, "schema.yml")
        self.assertTrue(any("Tab" in i for i in issues))

    def test_missing_version_schema(self):
        yml = "models:\n  - name: x"
        issues = rav.validate_dbt_yaml(yml, "schema.yml")
        self.assertTrue(any("version" in i for i in issues))

    def test_missing_version_sources(self):
        yml = "sources:\n  - name: x"
        issues = rav.validate_dbt_yaml(yml, "sources.yml")
        self.assertTrue(any("version" in i for i in issues))

    def test_profiles_yml_no_version_ok(self):
        yml = "databricks_migration:\n  target: dev"
        issues = rav.validate_dbt_yaml(yml, "profiles.yml")
        # profiles.yml doesn't need version:
        self.assertFalse(any("version" in i for i in issues))


# ═════════════════════════════════════════════════════════════════════════════
#  Notebook Validation
# ═════════════════════════════════════════════════════════════════════════════

class TestNotebookValidation(unittest.TestCase):
    """PySpark notebook structure validation."""

    def _valid_notebook(self, target="fabric"):
        header = "# Fabric notebook source" if target == "fabric" else "# Databricks notebook source"
        return f"""{header}

# CELL 1
from pyspark.sql.functions import col
# COMMAND ----------

# CELL 2 — Source Read
df = spark.table("bronze.customers")
# COMMAND ----------

# CELL 3 — Write
df.write.mode("overwrite").format("delta").save("/path")
# COMMAND ----------
"""

    def test_valid_fabric_notebook(self):
        issues = rav.validate_notebook(self._valid_notebook("fabric"), target="fabric")
        self.assertEqual(issues, [])

    def test_valid_databricks_notebook(self):
        issues = rav.validate_notebook(self._valid_notebook("databricks"), target="databricks")
        self.assertEqual(issues, [])

    def test_empty_notebook(self):
        issues = rav.validate_notebook("")
        self.assertTrue(any("Empty" in i for i in issues))

    def test_wrong_header(self):
        nb = self._valid_notebook("fabric").replace(
            "# Fabric notebook source", "# Some other header")
        issues = rav.validate_notebook(nb, target="fabric")
        self.assertTrue(any("header" in i for i in issues))

    def test_too_few_cells(self):
        nb = """# Fabric notebook source
from pyspark.sql.functions import col
df = spark.table("t")
df.write.mode("overwrite").save("/x")"""
        issues = rav.validate_notebook(nb, target="fabric")
        self.assertTrue(any("Too few cells" in i for i in issues))

    def test_missing_pyspark_imports(self):
        nb = """# Fabric notebook source
import json
# COMMAND ----------
df = spark.table("t")
# COMMAND ----------
df.write.mode("overwrite").save("/x")
# COMMAND ----------"""
        issues = rav.validate_notebook(nb, target="fabric")
        self.assertTrue(any("PySpark imports" in i for i in issues))

    def test_missing_source_read(self):
        nb = """# Fabric notebook source
from pyspark.sql.functions import col
# COMMAND ----------
df = None
# COMMAND ----------
df.write.mode("overwrite").save("/x")
# COMMAND ----------"""
        issues = rav.validate_notebook(nb, target="fabric")
        self.assertTrue(any("source read" in i for i in issues))

    def test_missing_target_write(self):
        nb = """# Fabric notebook source
from pyspark.sql.functions import col
# COMMAND ----------
df = spark.table("t")
# COMMAND ----------
result = df.count()
# COMMAND ----------"""
        issues = rav.validate_notebook(nb, target="fabric")
        self.assertTrue(any("target write" in i for i in issues))

    def test_hardcoded_password(self):
        nb = self._valid_notebook()
        nb += '\npassword = "supersecret123"\n'
        issues = rav.validate_notebook(nb, target="fabric")
        self.assertTrue(any("credential" in i for i in issues))

    def test_filename_in_issue(self):
        issues = rav.validate_notebook("", "NB_TEST.py")
        self.assertTrue(any("NB_TEST.py" in i for i in issues))


# ═════════════════════════════════════════════════════════════════════════════
#  Circular Dependency Detection
# ═════════════════════════════════════════════════════════════════════════════

class TestCircularDependencyDetection(unittest.TestCase):
    """Tests for the _check_circular_deps helper."""

    def test_no_deps(self):
        issues = rav._check_circular_deps({"A": [], "B": []})
        self.assertEqual(issues, [])

    def test_linear_chain(self):
        issues = rav._check_circular_deps({"A": [], "B": ["A"], "C": ["B"]})
        self.assertEqual(issues, [])

    def test_two_node_cycle(self):
        issues = rav._check_circular_deps({"A": ["B"], "B": ["A"]})
        self.assertTrue(any("Circular" in i for i in issues))

    def test_three_node_cycle(self):
        issues = rav._check_circular_deps({"A": ["B"], "B": ["C"], "C": ["A"]})
        self.assertTrue(any("Circular" in i for i in issues))

    def test_self_loop(self):
        issues = rav._check_circular_deps({"A": ["A"]})
        self.assertTrue(any("Circular" in i for i in issues))

    def test_diamond_no_cycle(self):
        issues = rav._check_circular_deps({
            "A": [], "B": ["A"], "C": ["A"], "D": ["B", "C"]
        })
        self.assertEqual(issues, [])


# ═════════════════════════════════════════════════════════════════════════════
#  Batch Validation
# ═════════════════════════════════════════════════════════════════════════════

class TestBatchValidation(unittest.TestCase):
    """Batch validation across output/ directory."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.output = Path(self.tmpdir) / "output"
        (self.output / "pipelines").mkdir(parents=True)
        (self.output / "dbt" / "models").mkdir(parents=True)
        (self.output / "notebooks").mkdir(parents=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_validate_all_empty(self):
        report = rav.validate_all(self.output)
        self.assertEqual(report["summary"]["total_artifacts"], 0)

    def test_validate_valid_pipeline(self):
        pipeline = {
            "name": "PL_X", "properties": {
                "activities": [{"name": "A", "type": "TridentNotebook",
                                "dependsOn": [],
                                "typeProperties": {"notebook": {"referenceName": "NB_X"}}}]
            }
        }
        (self.output / "pipelines" / "PL_X.json").write_text(
            json.dumps(pipeline), encoding="utf-8")
        result = rav.validate_all_pipelines(self.output)
        self.assertEqual(result["valid"], 1)
        self.assertEqual(result["invalid"], 0)

    def test_validate_invalid_json(self):
        (self.output / "pipelines" / "PL_BAD.json").write_text(
            "not valid json{{{", encoding="utf-8")
        result = rav.validate_all_pipelines(self.output)
        self.assertEqual(result["invalid"], 1)
        self.assertIn("PL_BAD.json", result["issues"])

    def test_validate_dbt_models(self):
        (self.output / "dbt" / "models" / "stg_x.sql").write_text(
            '{{ config(materialized="view") }}\nSELECT * FROM {{ source("s", "t") }}',
            encoding="utf-8")
        result = rav.validate_all_dbt_models(self.output)
        self.assertEqual(result["valid"], 1)

    def test_validate_notebooks(self):
        nb = """# Fabric notebook source
from pyspark.sql.functions import col
# COMMAND ----------
df = spark.table("t")
# COMMAND ----------
df.write.mode("overwrite").save("/x")
# COMMAND ----------"""
        (self.output / "notebooks" / "NB_TEST.py").write_text(nb, encoding="utf-8")
        result = rav.validate_all_notebooks(self.output)
        self.assertEqual(result["valid"], 1)

    def test_validate_all_combined(self):
        # Add one of each
        pipeline = {"name": "PL_X", "properties": {"activities": [
            {"name": "A", "type": "TridentNotebook", "dependsOn": [],
             "typeProperties": {"notebook": {"referenceName": "NB_X"}}}
        ]}}
        (self.output / "pipelines" / "PL_X.json").write_text(
            json.dumps(pipeline), encoding="utf-8")
        (self.output / "dbt" / "models" / "stg_x.sql").write_text(
            '{{ config(materialized="view") }}\nSELECT * FROM {{ source("s", "t") }}',
            encoding="utf-8")
        nb = """# Fabric notebook source
from pyspark.sql.functions import col
# COMMAND ----------
df = spark.table("t")
# COMMAND ----------
df.write.mode("overwrite").save("/x")
# COMMAND ----------"""
        (self.output / "notebooks" / "NB_X.py").write_text(nb, encoding="utf-8")

        report = rav.validate_all(self.output)
        self.assertEqual(report["summary"]["total_artifacts"], 3)
        self.assertEqual(report["summary"]["total_valid"], 3)
        self.assertEqual(report["summary"]["pass_rate"], 100.0)

    def test_validate_databricks_workflow_file(self):
        workflow = {"name": "WF_X", "tasks": [
            {"task_key": "s1", "notebook_task": {"notebook_path": "/x"}}
        ]}
        (self.output / "pipelines" / "PL_WF.json").write_text(
            json.dumps(workflow), encoding="utf-8")
        result = rav.validate_all_pipelines(self.output)
        self.assertEqual(result["valid"], 1)


# ═════════════════════════════════════════════════════════════════════════════
#  Real Output Validation (integration-style)
# ═════════════════════════════════════════════════════════════════════════════

class TestRealOutputValidation(unittest.TestCase):
    """Validate the actual generated output/ artifacts."""

    def test_real_pipelines_valid(self):
        result = rav.validate_all_pipelines()
        self.assertEqual(result["invalid"], 0,
                        f"Pipeline issues: {result['issues']}")

    def test_real_dbt_models_valid(self):
        result = rav.validate_all_dbt_models()
        self.assertEqual(result["invalid"], 0,
                        f"DBT issues: {result['issues']}")

    def test_real_output_pass_rate(self):
        report = rav.validate_all()
        # Allow up to 2 invalid (the known DQ notebooks)
        self.assertGreaterEqual(report["summary"]["pass_rate"], 90.0)


if __name__ == "__main__":
    unittest.main()
