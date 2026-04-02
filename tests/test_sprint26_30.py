"""
Sprints 26–30 — Tests for Placeholder Templates, Schema Generator,
Wave Planner, Validation L4/L5, and Production Hardening.

Sprint 26: Transformation templates (JTX, CT, HTTP, XMLG, XMLP, TC, ULKP)
Sprint 27: Schema generator (type mapping, DDL, lakehouse tier, setup notebook)
Sprint 28: Wave planner (dependency graph, topological sort, critical path)
Sprint 29: Validation L4 Key Sampling + L5 Aggregates + HTML report
Sprint 30: Audit log, credential sanitization, PHASES expansion
"""

import json
import re
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════
#  Sprint 26 — Transformation Template Tests
# ═══════════════════════════════════════════════

from run_notebook_migration import _transformation_cell, TX_TEMPLATES


def _make_mapping(name="M_TEST"):
    """Minimal mapping dict for template tests."""
    return {
        "name": name,
        "sources": ["Oracle.SALES.CUSTOMERS"],
        "targets": ["DIM_CUSTOMER"],
        "transformations": ["SQ", "EXP"],
        "has_sql_override": False,
        "complexity": "Simple",
        "sql_overrides": [],
        "lookup_conditions": [],
        "parameters": [],
        "target_load_order": [],
        "has_mapplet": False,
    }


class TestTxTemplates:
    """TX_TEMPLATES dict should have named types for promoted transformations."""

    def test_jtx_promoted(self):
        assert TX_TEMPLATES.get("JTX") == "java_transformation"

    def test_ct_promoted(self):
        assert TX_TEMPLATES.get("CT") == "custom_transformation"

    def test_http_promoted(self):
        assert TX_TEMPLATES.get("HTTP") == "http_transformation"

    def test_xmlg_promoted(self):
        assert TX_TEMPLATES.get("XMLG") == "xml_generator"

    def test_xmlp_promoted(self):
        assert TX_TEMPLATES.get("XMLP") == "xml_parser"

    def test_tc_promoted(self):
        assert TX_TEMPLATES.get("TC") == "transaction_control"

    def test_ulkp_promoted(self):
        assert TX_TEMPLATES.get("ULKP") == "unconnected_lookup"


class TestJTXTemplate:
    """JTX — Java Transformation → PySpark UDF."""

    def test_generates_udf(self):
        cell = _transformation_cell("JTX", 1, _make_mapping(), 3)
        assert "java_transform_udf" in cell
        assert "@udf" in cell

    def test_contains_return_type(self):
        cell = _transformation_cell("JTX", 1, _make_mapping(), 3)
        assert "StringType" in cell

    def test_references_prev_df(self):
        cell = _transformation_cell("JTX", 0, _make_mapping(), 3)
        assert "df_source" in cell


class TestCTTemplate:
    """CT — Custom Transformation → pandas UDF."""

    def test_generates_pandas_udf(self):
        cell = _transformation_cell("CT", 1, _make_mapping(), 3)
        assert "pandas_udf" in cell

    def test_contains_vectorized_hint(self):
        cell = _transformation_cell("CT", 1, _make_mapping(), 3)
        assert "pd.Series" in cell

    def test_contains_module_todo(self):
        cell = _transformation_cell("CT", 1, _make_mapping(), 3)
        assert "TODO" in cell


class TestHTTPTemplate:
    """HTTP — HTTP Transformation → requests UDF."""

    def test_generates_retry_strategy(self):
        cell = _transformation_cell("HTTP", 1, _make_mapping(), 3)
        assert "Retry" in cell
        assert "backoff_factor" in cell

    def test_contains_session_mount(self):
        cell = _transformation_cell("HTTP", 1, _make_mapping(), 3)
        assert "HTTPAdapter" in cell
        assert "mount" in cell

    def test_contains_error_handling(self):
        cell = _transformation_cell("HTTP", 1, _make_mapping(), 3)
        assert "raise_for_status" in cell


class TestXMLGTemplate:
    """XMLG — XML Generator → PySpark XML construction."""

    def test_generates_concat(self):
        cell = _transformation_cell("XMLG", 1, _make_mapping(), 3)
        assert "concat" in cell

    def test_contains_xml_tags(self):
        cell = _transformation_cell("XMLG", 1, _make_mapping(), 3)
        assert "<record>" in cell

    def test_xml_output_column(self):
        cell = _transformation_cell("XMLG", 1, _make_mapping(), 3)
        assert "xml_output" in cell


class TestXMLPTemplate:
    """XMLP — XML Parser → spark.read.format('xml')."""

    def test_references_xml_format(self):
        cell = _transformation_cell("XMLP", 1, _make_mapping(), 3)
        assert "com.databricks.spark.xml" in cell

    def test_contains_schema(self):
        cell = _transformation_cell("XMLP", 1, _make_mapping(), 3)
        assert "StructType" in cell

    def test_contains_rowtag(self):
        cell = _transformation_cell("XMLP", 1, _make_mapping(), 3)
        assert "rowTag" in cell


class TestTCTemplate:
    """TC — Transaction Control → Delta ACID pattern."""

    def test_generates_delta_import(self):
        cell = _transformation_cell("TC", 1, _make_mapping(), 3)
        assert "DeltaTable" in cell

    def test_contains_try_except(self):
        cell = _transformation_cell("TC", 1, _make_mapping(), 3)
        assert "try:" in cell
        assert "except" in cell

    def test_contains_commit_message(self):
        cell = _transformation_cell("TC", 1, _make_mapping(), 3)
        assert "committed" in cell.lower() or "Transaction committed" in cell


class TestULKPTemplate:
    """ULKP — Unconnected Lookup → broadcast join."""

    def test_generates_broadcast_join(self):
        cell = _transformation_cell("ULKP", 1, _make_mapping(), 3)
        assert "broadcast" in cell

    def test_contains_coalesce_default(self):
        cell = _transformation_cell("ULKP", 1, _make_mapping(), 3)
        assert "coalesce" in cell

    def test_contains_drop_cleanup(self):
        cell = _transformation_cell("ULKP", 1, _make_mapping(), 3)
        assert ".drop(" in cell


class TestUnknownTemplate:
    """Unknown transformation types get a generic UNKNOWN cell."""

    def test_unknown_type_flagged(self):
        cell = _transformation_cell("ZZZZZ", 1, _make_mapping(), 3)
        assert "UNKNOWN" in cell

    def test_sq_returns_none(self):
        cell = _transformation_cell("SQ", 0, _make_mapping(), 3)
        assert cell is None


# ═══════════════════════════════════════════════
#  Sprint 27 — Schema Generator Tests
# ═══════════════════════════════════════════════

from run_schema_generator import (
    map_type_to_delta,
    infer_lakehouse_tier,
    infer_partition_key,
    extract_target_schemas,
    generate_ddl,
    generate_setup_notebook,
)


class TestMapTypeToDelta:
    """Tests for map_type_to_delta()."""

    def test_oracle_varchar2(self):
        assert map_type_to_delta("VARCHAR2") == "STRING"

    def test_oracle_number(self):
        assert map_type_to_delta("NUMBER") == "DECIMAL(38,10)"

    def test_number_with_precision(self):
        assert map_type_to_delta("NUMBER(10,2)") == "DECIMAL(10,2)"

    def test_oracle_date(self):
        assert map_type_to_delta("DATE") == "TIMESTAMP"

    def test_sqlserver_int(self):
        assert map_type_to_delta("INT") == "INT"

    def test_sqlserver_bit(self):
        assert map_type_to_delta("BIT") == "BOOLEAN"

    def test_sqlserver_money(self):
        assert map_type_to_delta("MONEY") == "DECIMAL(19,4)"

    def test_teradata_byteint(self):
        assert map_type_to_delta("BYTEINT") == "TINYINT"

    def test_mysql_json(self):
        assert map_type_to_delta("JSON") == "STRING"

    def test_postgresql_uuid(self):
        assert map_type_to_delta("UUID") == "STRING"

    def test_postgresql_jsonb(self):
        assert map_type_to_delta("JSONB") == "STRING"

    def test_postgresql_serial(self):
        assert map_type_to_delta("SERIAL") == "INT"

    def test_varchar_with_length_returns_string(self):
        assert map_type_to_delta("VARCHAR2(100)") == "STRING"

    def test_timestamp_with_precision(self):
        assert map_type_to_delta("TIMESTAMP(6)") == "TIMESTAMP"

    def test_empty_returns_string(self):
        assert map_type_to_delta("") == "STRING"

    def test_none_returns_string(self):
        assert map_type_to_delta(None) == "STRING"

    def test_unknown_type_fallback(self):
        assert map_type_to_delta("XYZZY_UNKNOWN") == "STRING"

    def test_case_insensitive(self):
        assert map_type_to_delta("varchar2") == "STRING"

    def test_raw_with_size(self):
        assert map_type_to_delta("RAW(16)") == "BINARY"

    def test_float_with_precision(self):
        assert map_type_to_delta("FLOAT(53)") == "DOUBLE"


class TestInferLakehouseTier:
    """Tests for infer_lakehouse_tier()."""

    def test_gold_agg(self):
        assert infer_lakehouse_tier("AGG_MONTHLY_SALES") == "gold"

    def test_gold_rpt(self):
        assert infer_lakehouse_tier("RPT_DAILY_REVENUE") == "gold"

    def test_gold_kpi(self):
        assert infer_lakehouse_tier("KPI_CUSTOMER_SCORE") == "gold"

    def test_gold_summary(self):
        assert infer_lakehouse_tier("SUMMARY_ORDERS") == "gold"

    def test_bronze_raw(self):
        assert infer_lakehouse_tier("RAW_CUSTOMERS") == "bronze"

    def test_bronze_stg(self):
        assert infer_lakehouse_tier("STG_ORDERS") == "bronze"

    def test_bronze_src(self):
        assert infer_lakehouse_tier("SRC_INVENTORY") == "bronze"

    def test_silver_default(self):
        assert infer_lakehouse_tier("DIM_CUSTOMER") == "silver"

    def test_silver_fact(self):
        assert infer_lakehouse_tier("FACT_ORDERS") == "silver"


class TestInferPartitionKey:
    """Tests for infer_partition_key()."""

    def test_detects_load_date(self):
        cols = [{"name": "ID"}, {"name": "LOAD_DATE"}, {"name": "VALUE"}]
        assert infer_partition_key("DIM_CUSTOMER", cols) == "load_date"

    def test_detects_event_date(self):
        cols = [{"name": "ID"}, {"name": "EVENT_DATE"}]
        assert infer_partition_key("FACT_EVENTS", cols) == "event_date"

    def test_no_date_column(self):
        cols = [{"name": "ID"}, {"name": "NAME"}]
        assert infer_partition_key("DIM_CUSTOMER", cols) is None


class TestExtractTargetSchemas:
    """Tests for extract_target_schemas()."""

    def test_basic_extraction(self):
        inv = {
            "mappings": [{
                "name": "M_TEST",
                "targets": ["DIM_CUSTOMER"],
                "complexity": "Simple",
                "field_lineage": [
                    {"target_field": "CUST_ID", "source_field": "ID", "target_instance": "TGT_DIM_CUSTOMER"},
                    {"target_field": "CUST_NAME", "source_field": "NAME", "target_instance": "TGT_DIM_CUSTOMER"},
                ],
            }],
        }
        tables = extract_target_schemas(inv)
        assert len(tables) == 1
        assert tables[0]["name"] == "DIM_CUSTOMER"
        assert len(tables[0]["columns"]) == 2
        assert tables[0]["tier"] == "silver"

    def test_empty_mappings(self):
        tables = extract_target_schemas({"mappings": []})
        assert tables == []

    def test_no_lineage_gets_default_column(self):
        inv = {
            "mappings": [{
                "name": "M_TEST",
                "targets": ["SOME_TABLE"],
                "complexity": "Simple",
                "field_lineage": [],
            }],
        }
        tables = extract_target_schemas(inv)
        assert len(tables) == 1
        assert tables[0]["columns"][0]["name"] == "id"

    def test_deduplicates_targets(self):
        inv = {
            "mappings": [
                {"name": "M_A", "targets": ["SHARED_TABLE"], "complexity": "Simple", "field_lineage": []},
                {"name": "M_B", "targets": ["SHARED_TABLE"], "complexity": "Simple", "field_lineage": []},
            ],
        }
        tables = extract_target_schemas(inv)
        assert len(tables) == 1


class TestGenerateDDL:
    """Tests for generate_ddl()."""

    def test_basic_ddl(self):
        table = {
            "name": "DIM_CUSTOMER",
            "tier": "silver",
            "columns": [
                {"name": "CUST_ID", "type": "BIGINT", "source": "ID"},
                {"name": "CUST_NAME", "type": "STRING", "source": "NAME"},
            ],
            "partition_key": None,
            "mapping": "M_TEST",
            "complexity": "Simple",
        }
        ddl = generate_ddl(table)
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "silver.dim_customer" in ddl
        assert "USING DELTA" in ddl
        assert "_etl_load_timestamp" in ddl
        assert "_etl_source_mapping" in ddl

    def test_ddl_with_partition(self):
        table = {
            "name": "FACT_ORDERS",
            "tier": "silver",
            "columns": [{"name": "ORDER_ID", "type": "BIGINT", "source": "ID"}],
            "partition_key": "load_date",
            "mapping": "M_ORDERS",
            "complexity": "Complex",
        }
        ddl = generate_ddl(table)
        assert "PARTITIONED BY" in ddl
        assert "load_date" in ddl

    def test_ddl_comment_contains_mapping(self):
        table = {
            "name": "DIM_X",
            "tier": "silver",
            "columns": [{"name": "ID", "type": "INT", "source": "SRC_ID"}],
            "partition_key": None,
            "mapping": "M_MY_MAPPING",
            "complexity": "Simple",
        }
        ddl = generate_ddl(table)
        assert "M_MY_MAPPING" in ddl


class TestGenerateSetupNotebook:
    """Tests for generate_setup_notebook()."""

    def test_generates_cells(self):
        tables = [
            {"name": "T1", "tier": "silver", "columns": [{"name": "ID", "type": "INT", "source": "SRC"}],
             "partition_key": None, "mapping": "M1", "complexity": "Simple"},
            {"name": "T2", "tier": "bronze", "columns": [{"name": "ID", "type": "INT", "source": "SRC"}],
             "partition_key": None, "mapping": "M2", "complexity": "Simple"},
        ]
        nb = generate_setup_notebook(tables)
        assert "CREATE SCHEMA IF NOT EXISTS" in nb
        assert "silver" in nb
        assert "bronze" in nb

    def test_empty_tables(self):
        nb = generate_setup_notebook([])
        assert "CREATE SCHEMA" in nb or nb  # Empty tables still produces output


# ═══════════════════════════════════════════════
#  Sprint 28 — Wave Planner Tests
# ═══════════════════════════════════════════════

from run_assessment import (
    _build_mapping_dependency_graph,
    topological_sort_waves,
    find_critical_path,
    generate_wave_plan,
)


def _make_mappings(*names):
    """Create minimal mapping dicts with optional sources/targets."""
    mappings = []
    for n in names:
        mappings.append({
            "name": n,
            "sources": [],
            "targets": [],
            "transformations": ["SQ", "EXP"],
            "complexity": "Simple",
            "manual_effort_hours": 1,
        })
    return mappings


class TestBuildMappingDependencyGraph:
    """Tests for _build_mapping_dependency_graph()."""

    def test_no_deps_for_independent_mappings(self):
        mappings = _make_mappings("M_A", "M_B")
        deps = _build_mapping_dependency_graph(mappings, [])
        assert deps["M_A"] == set()
        assert deps["M_B"] == set()

    def test_workflow_edge_creates_dependency(self):
        mappings = _make_mappings("M_A", "M_B")
        workflows = [{
            "name": "WF_1",
            "session_to_mapping": {"S_A": "M_A", "S_B": "M_B"},
            "links": [{"from": "S_A", "to": "S_B"}],
        }]
        deps = _build_mapping_dependency_graph(mappings, workflows)
        assert "M_A" in deps["M_B"]

    def test_shared_table_creates_dependency(self):
        m1 = {"name": "M_PRODUCER", "sources": ["SRC"], "targets": ["SHARED_TABLE"],
               "transformations": ["SQ"], "complexity": "Simple", "manual_effort_hours": 1}
        m2 = {"name": "M_CONSUMER", "sources": ["SHARED_TABLE"], "targets": ["TGT"],
               "transformations": ["SQ"], "complexity": "Simple", "manual_effort_hours": 1}
        deps = _build_mapping_dependency_graph([m1, m2], [])
        assert "M_PRODUCER" in deps["M_CONSUMER"]


class TestTopologicalSortWaves:
    """Tests for topological_sort_waves()."""

    def test_independent_mappings_single_wave(self):
        deps = {"M_A": set(), "M_B": set(), "M_C": set()}
        waves = topological_sort_waves(deps)
        assert len(waves) == 1
        assert set(waves[0]) == {"M_A", "M_B", "M_C"}

    def test_linear_chain_three_waves(self):
        deps = {"M_A": set(), "M_B": {"M_A"}, "M_C": {"M_B"}}
        waves = topological_sort_waves(deps)
        assert len(waves) == 3
        assert waves[0] == ["M_A"]
        assert waves[1] == ["M_B"]
        assert waves[2] == ["M_C"]

    def test_mixed_deps(self):
        deps = {"M_A": set(), "M_B": set(), "M_C": {"M_A", "M_B"}}
        waves = topological_sort_waves(deps)
        assert len(waves) == 2
        assert "M_C" in waves[1]

    def test_cycle_handled(self):
        deps = {"M_A": {"M_B"}, "M_B": {"M_A"}}
        waves = topological_sort_waves(deps)
        # Should still produce waves (break cycle)
        assert len(waves) >= 1
        all_mappings = [m for w in waves for m in w]
        assert set(all_mappings) == {"M_A", "M_B"}

    def test_empty_deps(self):
        waves = topological_sort_waves({})
        assert waves == []


class TestFindCriticalPath:
    """Tests for find_critical_path()."""

    def test_single_node(self):
        deps = {"M_A": set()}
        effort = {"M_A": 5}
        path, total = find_critical_path(deps, effort)
        assert path == ["M_A"]
        assert total == 5

    def test_linear_chain(self):
        deps = {"M_A": set(), "M_B": {"M_A"}, "M_C": {"M_B"}}
        effort = {"M_A": 1, "M_B": 2, "M_C": 3}
        path, total = find_critical_path(deps, effort)
        assert path == ["M_A", "M_B", "M_C"]
        assert total == 6

    def test_picks_longest_branch(self):
        deps = {"M_A": set(), "M_B": {"M_A"}, "M_C": {"M_A"}}
        effort = {"M_A": 1, "M_B": 10, "M_C": 2}
        path, total = find_critical_path(deps, effort)
        assert "M_B" in path
        assert total == 11

    def test_default_effort(self):
        deps = {"M_A": set(), "M_B": {"M_A"}}
        path, total = find_critical_path(deps, {})
        # Default effort is 1 per mapping
        assert total == 2


class TestGenerateWavePlan:
    """Tests for generate_wave_plan()."""

    def test_basic_plan(self):
        mappings = _make_mappings("M_A", "M_B")
        plan = generate_wave_plan(mappings, [])
        assert plan["total_waves"] >= 1
        assert plan["total_mappings"] == 2
        assert "critical_path" in plan
        assert "waves" in plan

    def test_plan_with_deps(self):
        mappings = _make_mappings("M_A", "M_B")
        workflows = [{
            "name": "WF_1",
            "session_to_mapping": {"S_A": "M_A", "S_B": "M_B"},
            "links": [{"from": "S_A", "to": "S_B"}],
        }]
        plan = generate_wave_plan(mappings, workflows)
        assert plan["total_waves"] == 2

    def test_plan_complexity_breakdown(self):
        mappings = _make_mappings("M_A")
        plan = generate_wave_plan(mappings, [])
        wave = plan["waves"][0]
        assert "complexity_breakdown" in wave
        assert wave["complexity_breakdown"].get("Simple", 0) == 1


# ═══════════════════════════════════════════════
#  Sprint 29 — Validation L4/L5 + HTML Report
# ═══════════════════════════════════════════════

from run_validation import generate_validation, _generate_html_report


class TestValidationL4L5:
    """Tests for L4 Key Sampling and L5 Aggregate Comparison cells."""

    def _make_mapping(self):
        return {
            "name": "M_TEST",
            "targets": ["DIM_TEST"],
            "sources": ["Oracle.S.SRC"],
            "transformations": ["SQ", "EXP"],
            "complexity": "Simple",
        }

    def _get_notebook(self):
        notebooks = generate_validation(self._make_mapping(), "oracle")
        assert len(notebooks) >= 1
        return notebooks[0][1]  # (filename, content) tuple

    def test_notebook_contains_l4(self):
        nb = self._get_notebook()
        assert "Level 4" in nb

    def test_notebook_contains_key_sampling(self):
        nb = self._get_notebook()
        assert "Key" in nb and ("Sampling" in nb or "sample" in nb.lower())

    def test_notebook_contains_l5(self):
        nb = self._get_notebook()
        assert "Level 5" in nb

    def test_notebook_contains_aggregate(self):
        nb = self._get_notebook()
        assert "Aggregate" in nb or "SUM" in nb

    def test_notebook_contains_summary(self):
        nb = self._get_notebook()
        assert "Summary" in nb


class TestValidationHTMLReport:
    """Tests for _generate_html_report()."""

    def test_generates_html(self, tmp_path):
        out = tmp_path / "report.html"
        files = [("M_A", "TGT_A", "VAL_M_A.py"), ("M_B", "TGT_B", "VAL_M_B.py")]
        _generate_html_report(files, out)
        html = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in html
        assert "M_A" in html
        assert "TGT_B" in html

    def test_contains_l4_l5_labels(self, tmp_path):
        out = tmp_path / "report.html"
        _generate_html_report([("M_X", "T", "V.py")], out)
        html = out.read_text(encoding="utf-8")
        assert "L4" in html
        assert "L5" in html

    def test_empty_files(self, tmp_path):
        out = tmp_path / "report.html"
        _generate_html_report([], out)
        html = out.read_text(encoding="utf-8")
        assert "Total validation notebooks" in html
        assert "0" in html


# ═══════════════════════════════════════════════
#  Sprint 30 — Production Hardening Tests
# ═══════════════════════════════════════════════

from run_migration import (
    sanitize_output,
    _write_audit_log,
    _CREDENTIAL_PATTERNS,
    PHASES,
    generate_summary,
)


class TestPHASES:
    """PHASES list should include Schema Generation."""

    def test_has_eight_phases(self):
        assert len(PHASES) == 8

    def test_schema_generation_present(self):
        names = [p["name"] for p in PHASES]
        assert "Schema Generation" in names

    def test_validation_is_last(self):
        assert PHASES[-1]["name"] == "Validation"


class TestSanitizeOutput:
    """Tests for sanitize_output() credential scrubbing."""

    def test_redacts_password(self):
        result = sanitize_output("password= MyS3cret123 extra")
        assert "MyS3cret123" not in result
        assert "***REDACTED***" in result

    def test_redacts_secret(self):
        result = sanitize_output("secret: topsecretvalue end")
        assert "topsecretvalue" not in result
        assert "***REDACTED***" in result

    def test_redacts_token(self):
        result = sanitize_output("token=abc123xyz end")
        assert "abc123xyz" not in result
        assert "***REDACTED***" in result

    def test_redacts_jdbc_host(self):
        result = sanitize_output("jdbc:oracle:thin:@myhost.db.com:1521/orcl next")
        assert "myhost.db.com" not in result
        assert "***REDACTED***" in result

    def test_redacts_account_key(self):
        result = sanitize_output("AccountKey= mykey123+ABC== end")
        assert "mykey123+ABC==" not in result
        assert "***REDACTED***" in result

    def test_no_credentials_unchanged(self):
        text = "Normal log line with no secrets"
        assert sanitize_output(text) == text

    def test_multiple_credentials(self):
        text = "password=abc123 and token=xyz789"
        result = sanitize_output(text)
        assert "abc123" not in result
        assert "xyz789" not in result
        assert result.count("***REDACTED***") == 2


class TestCredentialPatterns:
    """Verify the compiled regex patterns."""

    def test_five_patterns(self):
        assert len(_CREDENTIAL_PATTERNS) == 5

    def test_all_compiled(self):
        for pattern, _ in _CREDENTIAL_PATTERNS:
            assert hasattr(pattern, "sub")


class TestWriteAuditLog:
    """Tests for _write_audit_log()."""

    def test_writes_json(self, tmp_path, monkeypatch):
        import run_migration
        monkeypatch.setattr(run_migration, "WORKSPACE", tmp_path)
        (tmp_path / "output").mkdir()
        results = [
            {"id": 0, "name": "Assessment", "status": "ok", "duration": 1.5, "error": None},
            {"id": 1, "name": "SQL", "status": "error", "duration": 0.2, "error": "password=secret123 fail"},
        ]
        audit_path = _write_audit_log(results, {})
        assert audit_path.exists()
        data = json.loads(audit_path.read_text(encoding="utf-8"))
        assert data["summary"]["succeeded"] == 1
        assert data["summary"]["failed"] == 1
        # Credentials should be sanitized in error messages
        assert "secret123" not in json.dumps(data)

    def test_audit_log_structure(self, tmp_path, monkeypatch):
        import run_migration
        monkeypatch.setattr(run_migration, "WORKSPACE", tmp_path)
        (tmp_path / "output").mkdir()
        results = [{"id": 0, "name": "Test", "status": "ok", "duration": 0.1, "error": None}]
        _write_audit_log(results, {"_config_file": "test.yaml"})
        data = json.loads((tmp_path / "output" / "audit_log.json").read_text(encoding="utf-8"))
        assert "migration_run" in data
        assert "phases" in data
        assert "summary" in data
        assert data["config_file"] == "test.yaml"


class TestGenerateSummary:
    """Tests for migration summary generation."""

    def test_summary_includes_schema_dir(self, tmp_path, monkeypatch):
        import run_migration
        monkeypatch.setattr(run_migration, "WORKSPACE", tmp_path)
        (tmp_path / "output").mkdir()
        results = [{"id": 0, "name": "Assessment", "status": "ok", "duration": 1.0, "error": None}]
        path = generate_summary(results)
        content = path.read_text(encoding="utf-8")
        assert "output/schema/" in content

    def test_summary_includes_audit_log(self, tmp_path, monkeypatch):
        import run_migration
        monkeypatch.setattr(run_migration, "WORKSPACE", tmp_path)
        (tmp_path / "output").mkdir()
        results = [{"id": 0, "name": "Assessment", "status": "ok", "duration": 1.0, "error": None}]
        path = generate_summary(results)
        content = path.read_text(encoding="utf-8")
        assert "audit_log" in content
