"""
Unit Test Suite — Informatica to Fabric Migration Scripts
Covers: SQL conversion, notebook generation, pipeline generation,
        validation generation, and the orchestrator.

Run:
    pytest tests/ -v
    pytest tests/ -v --tb=short
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add project root to sys.path so we can import run_* modules
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ─────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def sample_inventory():
    """Minimal but realistic inventory for testing."""
    return {
        "generated": "2026-03-23",
        "source_platform": "Informatica PowerCenter",
        "target_platform": "Microsoft Fabric",
        "mappings": [
            {
                "name": "M_TEST_SIMPLE",
                "sources": ["Oracle.SALES.CUSTOMERS"],
                "targets": ["DIM_CUSTOMER"],
                "transformations": ["SQ", "EXP", "FIL"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Simple",
                "sql_overrides": [],
                "lookup_conditions": [],
                "parameters": [],
                "target_load_order": ["DIM_CUSTOMER"],
            },
            {
                "name": "M_TEST_COMPLEX",
                "sources": ["Oracle.SALES.ORDERS", "Oracle.SALES.PRODUCTS"],
                "targets": ["FACT_ORDERS", "AGG_ORDERS_BY_REGION"],
                "transformations": ["SQ", "LKP", "EXP", "AGG", "UPD"],
                "has_sql_override": True,
                "has_stored_proc": False,
                "complexity": "Complex",
                "sql_overrides": [
                    {"type": "Sql Query", "value": "SELECT ORDER_ID, NVL(STATUS, 'PENDING') FROM SALES.ORDERS WHERE SYSDATE > ORDER_DATE"},
                    {"type": "Lookup Sql Override", "value": "SELECT PRODUCT_ID, NVL(NAME, 'Unknown') FROM PRODUCTS"},
                ],
                "lookup_conditions": [
                    {"lookup": "LKP_PRODUCTS", "sql": "SELECT * FROM PRODUCTS"},
                    {"lookup": "LKP_PRODUCTS", "condition": "IN_PRODUCT_ID = PRODUCT_ID"},
                ],
                "parameters": ["$$LOAD_DATE"],
                "target_load_order": ["FACT_ORDERS", "AGG_ORDERS_BY_REGION"],
            },
        ],
        "workflows": [
            {
                "name": "WF_TEST_FLOW",
                "sessions": ["S_M_TEST_SIMPLE", "S_M_TEST_COMPLEX"],
                "session_to_mapping": {
                    "S_M_TEST_SIMPLE": "M_TEST_SIMPLE",
                    "S_M_TEST_COMPLEX": "M_TEST_COMPLEX",
                },
                "dependencies": {
                    "S_M_TEST_SIMPLE": ["Start"],
                    "S_M_TEST_COMPLEX": ["S_M_TEST_SIMPLE"],
                },
                "has_timer": False,
                "has_decision": False,
                "decision_tasks": [],
                "email_tasks": [],
                "pre_post_sql": [],
                "schedule": "SCHED_DAILY",
            }
        ],
        "connections": [
            {"name": "CONN_SALES", "type": "Oracle", "schema": "SALES"}
        ],
        "sql_files": [],
        "summary": {
            "total_mappings": 2,
            "total_workflows": 1,
            "total_sessions": 2,
            "total_sql_files": 0,
            "complexity_breakdown": {"Simple": 1, "Medium": 0, "Complex": 1, "Custom": 0},
        },
        "parameter_files": [],
        "mapplets": {},
    }


@pytest.fixture
def tmp_workspace(sample_inventory, tmp_path):
    """Create a temporary workspace mirroring the project layout."""
    # Create directories
    (tmp_path / "input" / "sql").mkdir(parents=True)
    (tmp_path / "input" / "workflows").mkdir(parents=True)
    (tmp_path / "input" / "mappings").mkdir(parents=True)
    (tmp_path / "output" / "inventory").mkdir(parents=True)
    (tmp_path / "output" / "sql").mkdir(parents=True)
    (tmp_path / "output" / "notebooks").mkdir(parents=True)
    (tmp_path / "output" / "pipelines").mkdir(parents=True)
    (tmp_path / "output" / "validation").mkdir(parents=True)

    # Write inventory
    inv_path = tmp_path / "output" / "inventory" / "inventory.json"
    with open(inv_path, "w") as f:
        json.dump(sample_inventory, f, indent=2)

    # Write a test SQL file
    sql_content = """\
CREATE OR REPLACE PROCEDURE SP_TEST AS
  v_count NUMBER;
BEGIN
  SELECT NVL(count(*), 0) INTO v_count
  FROM ORDERS WHERE ORDER_DATE >= SYSDATE - 30;
  UPDATE STATS SET ROW_COUNT = v_count, UPDATED = SYSDATE;
  DBMS_OUTPUT.PUT_LINE('Done: ' || v_count);
EXCEPTION WHEN OTHERS THEN
  DBMS_OUTPUT.PUT_LINE('Error');
END;
"""
    test_sql_path = tmp_path / "input" / "sql" / "SP_TEST.sql"
    test_sql_path.write_text(sql_content, encoding="utf-8")

    # Update inventory to reference the SQL file
    sample_inventory["sql_files"] = [
        {"file": "SP_TEST.sql", "path": str(test_sql_path.relative_to(tmp_path)), "db_type": "oracle"}
    ]
    with open(inv_path, "w") as f:
        json.dump(sample_inventory, f, indent=2)

    return tmp_path


# ─────────────────────────────────────────────
#  SQL Migration Tests
# ─────────────────────────────────────────────

class TestSQLConversion:
    """Test Oracle and SQL Server → Spark SQL regex conversions."""

    def setup_method(self):
        import run_sql_migration
        self.mod = run_sql_migration

    # --- Oracle conversions ---

    def test_nvl_to_coalesce(self):
        result = self.mod.convert_sql("SELECT NVL(col, 0) FROM t", "oracle")
        assert "COALESCE(col, 0)" in result
        assert "NVL" not in result

    def test_nvl2_to_case(self):
        result = self.mod.convert_sql("SELECT NVL2(col, 'Y', 'N') FROM t", "oracle")
        assert "CASE WHEN" in result
        assert "NVL2" not in result

    def test_sysdate_to_current_timestamp(self):
        result = self.mod.convert_sql("WHERE created_date > SYSDATE", "oracle")
        assert "current_timestamp()" in result
        assert "SYSDATE" not in result

    def test_decode_simple(self):
        result = self.mod.convert_sql("SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown') FROM t", "oracle")
        assert "CASE" in result
        assert "WHEN" in result
        assert "ELSE" in result
        assert "END" in result
        assert "DECODE" not in result

    def test_decode_two_pairs(self):
        result = self.mod.convert_sql("DECODE(x, 1, 'one', 2, 'two')", "oracle")
        assert "CASE" in result
        assert "WHEN x = 1 THEN 'one'" in result
        assert "WHEN x = 2 THEN 'two'" in result

    def test_substr_to_substring(self):
        result = self.mod.convert_sql("SUBSTR(name, 1, 3)", "oracle")
        assert "SUBSTRING(" in result

    def test_to_number_to_cast(self):
        result = self.mod.convert_sql("TO_NUMBER(amount)", "oracle")
        assert "CAST(amount AS DECIMAL)" in result

    def test_varchar2_to_string(self):
        result = self.mod.convert_sql("col VARCHAR2(100)", "oracle")
        assert "STRING" in result
        assert "VARCHAR2" not in result

    def test_date_format_conversion(self):
        result = self.mod.convert_sql("TO_DATE(d, 'YYYY-MM-DD')", "oracle")
        assert "'yyyy-MM-dd'" in result

    def test_from_dual_removed(self):
        result = self.mod.convert_sql("SELECT 1 FROM DUAL", "oracle")
        assert "DUAL" not in result

    def test_trunc_date(self):
        result = self.mod.convert_sql("TRUNC(hire_date)", "oracle")
        assert "date_trunc('day', hire_date)" in result

    def test_dbms_output_flagged(self):
        result = self.mod.convert_sql("DBMS_OUTPUT.PUT_LINE('hello')", "oracle")
        assert "TODO" in result

    def test_regexp_like(self):
        result = self.mod.convert_sql("REGEXP_LIKE(name, '^A')", "oracle")
        assert "RLIKE" in result

    # --- SQL Server conversions ---

    def test_getdate_to_current_timestamp(self):
        result = self.mod.convert_sql("WHERE d > GETDATE()", "sqlserver")
        assert "current_timestamp()" in result

    def test_isnull_to_coalesce(self):
        result = self.mod.convert_sql("ISNULL(col, 0)", "sqlserver")
        assert "COALESCE(col, 0)" in result

    def test_charindex_to_locate(self):
        result = self.mod.convert_sql("CHARINDEX('@', email)", "sqlserver")
        assert "LOCATE('@', email)" in result

    def test_len_to_length(self):
        result = self.mod.convert_sql("LEN(name)", "sqlserver")
        assert "LENGTH(RTRIM(name))" in result

    def test_nolock_removed(self):
        result = self.mod.convert_sql("FROM orders (NOLOCK)", "sqlserver")
        assert "NOLOCK" not in result

    def test_nvarchar_to_string(self):
        result = self.mod.convert_sql("col NVARCHAR(MAX)", "sqlserver")
        assert "STRING" in result

    def test_bit_to_boolean(self):
        result = self.mod.convert_sql("is_active BIT", "sqlserver")
        assert "BOOLEAN" in result

    def test_iif_to_case(self):
        result = self.mod.convert_sql("IIF(x > 0, 'pos', 'neg')", "sqlserver")
        assert "CASE WHEN" in result

    def test_cross_apply_flagged(self):
        result = self.mod.convert_sql("CROSS APPLY fn(t.id)", "sqlserver")
        assert "TODO" in result

    # --- Edge cases ---

    def test_empty_sql(self):
        result = self.mod.convert_sql("", "oracle")
        assert result == ""

    def test_no_conversions_needed(self):
        sql = "SELECT id, name FROM users WHERE active = 1"
        result = self.mod.convert_sql(sql, "oracle")
        assert result == sql

    def test_multiple_conversions_in_one_line(self):
        sql = "SELECT NVL(a, 0), NVL(b, 1) FROM t WHERE d > SYSDATE"
        result = self.mod.convert_sql(sql, "oracle")
        assert result.count("COALESCE") == 2
        assert "current_timestamp()" in result

    # --- File-level conversion ---

    def test_convert_sql_overrides(self, tmp_workspace):
        """Test override conversion produces output file."""
        import run_sql_migration as mod
        # Temporarily point OUTPUT_DIR
        original_output = mod.OUTPUT_DIR
        mod.OUTPUT_DIR = tmp_workspace / "output" / "sql"
        try:
            overrides = [
                {"type": "Sql Query", "value": "SELECT NVL(x, 0) FROM t WHERE SYSDATE > d"},
            ]
            out = mod.convert_sql_overrides("M_TEST", overrides, "oracle")
            assert out is not None
            assert out.exists()
            content = out.read_text()
            assert "COALESCE" in content
            assert "current_timestamp()" in content
        finally:
            mod.OUTPUT_DIR = original_output


# ─────────────────────────────────────────────
#  Notebook Migration Tests
# ─────────────────────────────────────────────

class TestNotebookGeneration:
    """Test PySpark notebook generation from inventory mappings."""

    def setup_method(self):
        import run_notebook_migration
        self.mod = run_notebook_migration

    def test_simple_mapping_generates_notebook(self, sample_inventory):
        mapping = sample_inventory["mappings"][0]  # M_TEST_SIMPLE
        content = self.mod.generate_notebook(mapping)
        assert "NB_M_TEST_SIMPLE" in content
        assert "METADATA_START" in content
        assert "current_timestamp" in content
        assert "COMMAND ----------" in content

    def test_complex_mapping_has_all_transformations(self, sample_inventory):
        mapping = sample_inventory["mappings"][1]  # M_TEST_COMPLEX
        content = self.mod.generate_notebook(mapping)
        assert "Lookup" in content or "broadcast" in content
        assert "Expression" in content or "withColumn" in content
        assert "Aggregator" in content or "groupBy" in content
        assert "Update Strategy" in content or "MERGE" in content

    def test_notebook_has_source_cell(self, sample_inventory):
        mapping = sample_inventory["mappings"][0]
        content = self.mod.generate_notebook(mapping)
        assert "Source Read" in content
        assert "spark.table" in content

    def test_notebook_has_target_cell(self, sample_inventory):
        mapping = sample_inventory["mappings"][0]
        content = self.mod.generate_notebook(mapping)
        assert "Target Write" in content
        assert "saveAsTable" in content or "MERGE" in content

    def test_notebook_has_audit_cell(self, sample_inventory):
        mapping = sample_inventory["mappings"][0]
        content = self.mod.generate_notebook(mapping)
        assert "Audit" in content
        assert "completed successfully" in content

    def test_parameters_included(self, sample_inventory):
        mapping = sample_inventory["mappings"][1]  # has $$LOAD_DATE
        content = self.mod.generate_notebook(mapping)
        assert "load_date" in content
        assert "notebookutils" in content

    def test_sql_override_reference(self, sample_inventory):
        mapping = sample_inventory["mappings"][1]  # has sql_overrides
        content = self.mod.generate_notebook(mapping)
        assert "SQL_OVERRIDES_M_TEST_COMPLEX" in content or "SQL Override" in content

    def test_all_transformation_types_handled(self):
        """Ensure every known TX type produces output without errors."""
        for tx_type in self.mod.TX_TEMPLATES:
            mapping = {
                "name": f"M_TX_{tx_type}",
                "sources": ["Oracle.SCHEMA.TABLE"],
                "targets": ["TARGET"],
                "transformations": [tx_type],
                "sql_overrides": [],
                "lookup_conditions": [],
                "parameters": [],
                "complexity": "Simple",
            }
            content = self.mod.generate_notebook(mapping)
            assert content is not None
            assert len(content) > 50

    def test_notebook_end_to_end(self, tmp_workspace, sample_inventory):
        """Test main() writes files to disk."""
        import run_notebook_migration as mod
        original_output = mod.OUTPUT_DIR
        original_inv = mod.INVENTORY_PATH
        mod.OUTPUT_DIR = tmp_workspace / "output" / "notebooks"
        mod.INVENTORY_PATH = tmp_workspace / "output" / "inventory" / "inventory.json"
        saved_argv = sys.argv
        sys.argv = ["run_notebook_migration.py"]
        try:
            mod.main()
            nb_files = list((tmp_workspace / "output" / "notebooks").glob("NB_*.py"))
            assert len(nb_files) == 2
            names = {f.name for f in nb_files}
            assert "NB_M_TEST_SIMPLE.py" in names
            assert "NB_M_TEST_COMPLEX.py" in names
        finally:
            mod.OUTPUT_DIR = original_output
            mod.INVENTORY_PATH = original_inv
            sys.argv = saved_argv


# ─────────────────────────────────────────────
#  Pipeline Migration Tests
# ─────────────────────────────────────────────

class TestPipelineGeneration:
    """Test Fabric Pipeline JSON generation from workflow inventory."""

    def setup_method(self):
        import run_pipeline_migration
        self.mod = run_pipeline_migration

    def test_basic_pipeline_structure(self, sample_inventory):
        mappings_by_name = {m["name"]: m for m in sample_inventory["mappings"]}
        wf = sample_inventory["workflows"][0]
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)

        assert pipeline["name"] == "PL_WF_TEST_FLOW"
        assert "properties" in pipeline
        assert "activities" in pipeline["properties"]
        assert "parameters" in pipeline["properties"]

    def test_activities_are_trident_notebook(self, sample_inventory):
        mappings_by_name = {m["name"]: m for m in sample_inventory["mappings"]}
        wf = sample_inventory["workflows"][0]
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)

        activities = pipeline["properties"]["activities"]
        for act in activities:
            assert act["type"] == "TridentNotebook"

    def test_dependency_chain(self, sample_inventory):
        mappings_by_name = {m["name"]: m for m in sample_inventory["mappings"]}
        wf = sample_inventory["workflows"][0]
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)

        activities = pipeline["properties"]["activities"]
        # First activity (S_M_TEST_SIMPLE) depends on Start → no dependsOn
        first = [a for a in activities if a["name"] == "NB_M_TEST_SIMPLE"][0]
        assert first["dependsOn"] == []

        # Second activity depends on first
        second = [a for a in activities if a["name"] == "NB_M_TEST_COMPLEX"][0]
        assert len(second["dependsOn"]) == 1
        assert second["dependsOn"][0]["activity"] == "NB_M_TEST_SIMPLE"

    def test_parameters_propagated(self, sample_inventory):
        mappings_by_name = {m["name"]: m for m in sample_inventory["mappings"]}
        wf = sample_inventory["workflows"][0]
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)

        # M_TEST_COMPLEX has $$LOAD_DATE parameter
        complex_act = [a for a in pipeline["properties"]["activities"]
                       if a["name"] == "NB_M_TEST_COMPLEX"][0]
        params = complex_act["typeProperties"]["parameters"]
        assert "load_date" in params

    def test_pipeline_has_load_date_parameter(self, sample_inventory):
        mappings_by_name = {m["name"]: m for m in sample_inventory["mappings"]}
        wf = sample_inventory["workflows"][0]
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)
        assert "load_date" in pipeline["properties"]["parameters"]

    def test_annotations_include_migration_tag(self, sample_inventory):
        mappings_by_name = {m["name"]: m for m in sample_inventory["mappings"]}
        wf = sample_inventory["workflows"][0]
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)
        annotations = pipeline["properties"]["annotations"]
        assert "MigratedFromInformatica" in annotations

    def test_pipeline_json_serializable(self, sample_inventory):
        mappings_by_name = {m["name"]: m for m in sample_inventory["mappings"]}
        wf = sample_inventory["workflows"][0]
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)
        # Should not raise
        serialized = json.dumps(pipeline, indent=2)
        assert len(serialized) > 100

    def test_email_task_generates_web_activity(self):
        """When workflow has email tasks, a WebActivity should be generated."""
        wf = {
            "name": "WF_WITH_EMAIL",
            "sessions": ["S_LOAD"],
            "session_to_mapping": {"S_LOAD": "M_LOAD"},
            "dependencies": {
                "S_LOAD": ["Start"],
                "EMAIL_ON_FAIL": ["S_LOAD"],
            },
            "decision_tasks": [],
            "email_tasks": ["EMAIL_ON_FAIL"],
            "schedule": "",
        }
        mappings_by_name = {"M_LOAD": {"parameters": []}}
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)
        types = [a["type"] for a in pipeline["properties"]["activities"]]
        assert "WebActivity" in types
        assert "alert_webhook_url" in pipeline["properties"]["parameters"]

    def test_decision_task_generates_if_condition(self):
        """When workflow has decision tasks, an IfCondition should be generated."""
        wf = {
            "name": "WF_WITH_DEC",
            "sessions": ["S_LOAD", "S_NEXT"],
            "session_to_mapping": {"S_LOAD": "M_LOAD", "S_NEXT": "M_NEXT"},
            "dependencies": {
                "S_LOAD": ["Start"],
                "DEC_CHECK": ["S_LOAD"],
                "S_NEXT": ["DEC_CHECK"],
            },
            "decision_tasks": ["DEC_CHECK"],
            "email_tasks": [],
            "schedule": "",
        }
        mappings_by_name = {"M_LOAD": {"parameters": []}, "M_NEXT": {"parameters": []}}
        pipeline = self.mod.generate_pipeline(wf, mappings_by_name)
        types = [a["type"] for a in pipeline["properties"]["activities"]]
        assert "IfCondition" in types


# ─────────────────────────────────────────────
#  Validation Generation Tests
# ─────────────────────────────────────────────

class TestValidationGeneration:
    """Test validation notebook generation from inventory."""

    def setup_method(self):
        import run_validation
        self.mod = run_validation

    def test_infer_target_table_silver(self):
        assert self.mod._infer_target_table("DIM_CUSTOMER", []).startswith("silver.")

    def test_infer_target_table_gold(self):
        assert self.mod._infer_target_table("AGG_SALES_SUMMARY", []).startswith("gold.")

    def test_infer_key_columns(self):
        assert self.mod._infer_key_columns("DIM_CUSTOMER") == ["customer_id"]
        assert self.mod._infer_key_columns("FACT_ORDERS") == ["order_id"]
        assert self.mod._infer_key_columns("DIM_EMPLOYEE") == ["employee_id"]

    def test_source_connection_oracle(self):
        assert self.mod._source_connection(["Oracle.SALES.TABLE"]) == "oracle"

    def test_source_connection_sqlserver(self):
        assert self.mod._source_connection(["SqlServer.DBO.TABLE"]) == "sqlserver"

    def test_source_connection_default(self):
        assert self.mod._source_connection(["some_table"]) == "oracle"

    def test_validation_notebook_content(self, sample_inventory):
        mapping = sample_inventory["mappings"][0]
        notebooks = self.mod.generate_validation(mapping, "oracle")
        assert len(notebooks) == 1
        name, content = notebooks[0]
        assert "VAL_" in f"VAL_{name}"
        assert "Row Count" in content
        assert "Checksum" in content
        assert "Data Quality" in content
        assert "Summary" in content

    def test_multi_target_generates_multiple_notebooks(self, sample_inventory):
        mapping = sample_inventory["mappings"][1]  # 2 targets
        notebooks = self.mod.generate_validation(mapping, "oracle")
        assert len(notebooks) == 2

    def test_validation_end_to_end(self, tmp_workspace, sample_inventory):
        """Test main() writes validation files to disk."""
        import run_validation as mod
        original_output = mod.OUTPUT_DIR
        original_inv = mod.INVENTORY_PATH
        mod.OUTPUT_DIR = tmp_workspace / "output" / "validation"
        mod.INVENTORY_PATH = tmp_workspace / "output" / "inventory" / "inventory.json"
        saved_argv = sys.argv
        sys.argv = ["run_validation.py"]
        try:
            mod.main()
            val_files = list((tmp_workspace / "output" / "validation").glob("VAL_*.py"))
            # M_TEST_SIMPLE has 1 target, M_TEST_COMPLEX has 2
            assert len(val_files) == 3
            # test_matrix.md should exist
            assert (tmp_workspace / "output" / "validation" / "test_matrix.md").exists()
        finally:
            mod.OUTPUT_DIR = original_output
            mod.INVENTORY_PATH = original_inv
            sys.argv = saved_argv

    def test_test_matrix_content(self, tmp_workspace, sample_inventory):
        """Test the generated test_matrix.md has correct structure."""
        import run_validation as mod
        original_output = mod.OUTPUT_DIR
        original_inv = mod.INVENTORY_PATH
        mod.OUTPUT_DIR = tmp_workspace / "output" / "validation"
        mod.INVENTORY_PATH = tmp_workspace / "output" / "inventory" / "inventory.json"
        saved_argv = sys.argv
        sys.argv = ["run_validation.py"]
        try:
            mod.main()
            matrix = (tmp_workspace / "output" / "validation" / "test_matrix.md").read_text(encoding="utf-8")
            assert "Test Matrix" in matrix
            assert "M_TEST_SIMPLE" in matrix
            assert "M_TEST_COMPLEX" in matrix
            assert "L1" in matrix
        finally:
            mod.OUTPUT_DIR = original_output
            mod.INVENTORY_PATH = original_inv
            sys.argv = saved_argv


# ─────────────────────────────────────────────
#  Orchestrator Tests
# ─────────────────────────────────────────────

class TestOrchestrator:
    """Test run_migration.py argument parsing and summary generation."""

    def setup_method(self):
        import run_migration
        self.mod = run_migration

    def test_parse_args_skip(self):
        with patch.object(sys, "argv", ["run_migration.py", "--skip", "0", "2"]):
            parsed = self.mod._parse_args()
            assert set(parsed.skip) == {0, 2}
            assert parsed.only is None

    def test_parse_args_only(self):
        with patch.object(sys, "argv", ["run_migration.py", "--only", "1", "3"]):
            parsed = self.mod._parse_args()
            assert parsed.skip == []
            assert set(parsed.only) == {1, 3}

    def test_parse_args_empty(self):
        with patch.object(sys, "argv", ["run_migration.py"]):
            parsed = self.mod._parse_args()
            assert parsed.skip == []
            assert parsed.only is None

    def test_parse_args_verbose(self):
        with patch.object(sys, "argv", ["run_migration.py", "--verbose"]):
            parsed = self.mod._parse_args()
            assert parsed.verbose is True

    def test_parse_args_dry_run(self):
        with patch.object(sys, "argv", ["run_migration.py", "--dry-run"]):
            parsed = self.mod._parse_args()
            assert parsed.dry_run is True

    def test_parse_args_config(self):
        with patch.object(sys, "argv", ["run_migration.py", "--config", "my.yaml"]):
            parsed = self.mod._parse_args()
            assert parsed.config == "my.yaml"

    def test_parse_args_log_format(self):
        with patch.object(sys, "argv", ["run_migration.py", "--log-format", "json"]):
            parsed = self.mod._parse_args()
            assert parsed.log_format == "json"

    def test_generate_summary(self, tmp_path):
        original_workspace = self.mod.WORKSPACE
        self.mod.WORKSPACE = tmp_path
        (tmp_path / "output").mkdir()
        try:
            results = [
                {"id": 0, "name": "Assessment", "status": "skipped", "duration": 0, "error": None},
                {"id": 1, "name": "SQL Migration", "status": "ok", "duration": 1.5, "error": None},
                {"id": 2, "name": "Notebook Migration", "status": "ok", "duration": 0.8, "error": None},
                {"id": 3, "name": "Pipeline Migration", "status": "error", "duration": 0.1, "error": "test error"},
                {"id": 4, "name": "Validation", "status": "ok", "duration": 0.3, "error": None},
            ]
            path = self.mod.generate_summary(results)
            assert path.exists()
            content = path.read_text(encoding="utf-8")
            assert "Migration Summary" in content
            assert "✅ OK" in content
            assert "⏭️ Skipped" in content
            assert "❌" in content
        finally:
            self.mod.WORKSPACE = original_workspace

    def test_phases_list_complete(self):
        assert len(self.mod.PHASES) == 9
        ids = [p["id"] for p in self.mod.PHASES]
        assert ids == [0, 1, 2, 3, 4, 5, 6, 7, 8]


# ─────────────────────────────────────────────
#  Integration: Full SQL Pipeline
# ─────────────────────────────────────────────

class TestSQLEndToEnd:
    """Integration test: run_sql_migration.main() against tmp workspace."""

    def test_full_sql_migration(self, tmp_workspace):
        import run_sql_migration as mod
        original_output = mod.OUTPUT_DIR
        original_input = mod.INPUT_DIR
        original_inv = mod.INVENTORY_PATH
        original_workspace = mod.WORKSPACE

        mod.WORKSPACE = tmp_workspace
        mod.INPUT_DIR = tmp_workspace / "input"
        mod.OUTPUT_DIR = tmp_workspace / "output" / "sql"
        mod.INVENTORY_PATH = tmp_workspace / "output" / "inventory" / "inventory.json"
        saved_argv = sys.argv
        sys.argv = ["run_sql_migration.py"]
        try:
            mod.main()
            sql_files = list((tmp_workspace / "output" / "sql").glob("SQL_*.sql"))
            # 1 standalone + 1 override file
            assert len(sql_files) >= 2

            # Check standalone conversion
            sp_file = tmp_workspace / "output" / "sql" / "SQL_SP_TEST.sql"
            assert sp_file.exists()
            content = sp_file.read_text()
            assert "COALESCE" in content
            assert "current_timestamp()" in content

            # Check override conversion
            ovr_file = tmp_workspace / "output" / "sql" / "SQL_OVERRIDES_M_TEST_COMPLEX.sql"
            assert ovr_file.exists()
            ovr_content = ovr_file.read_text()
            assert "COALESCE" in ovr_content
        finally:
            mod.WORKSPACE = original_workspace
            mod.INPUT_DIR = original_input
            mod.OUTPUT_DIR = original_output
            mod.INVENTORY_PATH = original_inv
            sys.argv = saved_argv
