"""
Sprints 31–40 — Tests for Remaining Object Gaps, Advanced PL/SQL,
Multi-Tenant Enterprise, Performance, Web UI, DQ & Governance, Docs.

Sprint 31: EP, AEP, ASSOC, KEYGEN, ADDRVAL templates + Oracle Object Types
Sprint 33: Advanced PL/SQL conversion (EXECUTE IMMEDIATE, CURSOR, BULK COLLECT, CONNECT BY, EXCEPTION)
Sprint 35: Multi-tenant (Key Vault substitution, manifest, batch, parallel waves)
Sprint 37: Performance (profiling, parallel notebook generation)
Sprint 38: Web UI (Streamlit wizard structure)
Sprint 39: DQ & Governance (PII detection, sensitivity classification, DQ rule extraction)
Sprint 40: Enterprise docs (RUNBOOK.md, ENTERPRISE_PLAYBOOK.md)
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
#  Sprint 31 — Remaining Object Gap Tests
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


class TestSprint31TxTemplates:
    """Sprint 31: TX_TEMPLATES should include EP, AEP, ASSOC, KEYGEN, ADDRVAL."""

    def test_ep_in_templates(self):
        assert TX_TEMPLATES.get("EP") == "external_procedure"

    def test_aep_in_templates(self):
        assert TX_TEMPLATES.get("AEP") == "advanced_external_procedure"

    def test_assoc_in_templates(self):
        assert TX_TEMPLATES.get("ASSOC") == "association"

    def test_keygen_in_templates(self):
        assert TX_TEMPLATES.get("KEYGEN") == "key_generator"

    def test_addrval_in_templates(self):
        assert TX_TEMPLATES.get("ADDRVAL") == "address_validator"


class TestEPTemplate:
    """EP — External Procedure → subprocess UDF."""

    def test_generates_subprocess(self):
        cell = _transformation_cell("EP", 1, _make_mapping(), 3)
        assert "subprocess" in cell

    def test_generates_udf(self):
        cell = _transformation_cell("EP", 1, _make_mapping(), 3)
        assert "external_proc_udf" in cell

    def test_contains_return_type(self):
        cell = _transformation_cell("EP", 1, _make_mapping(), 3)
        assert "StringType" in cell

    def test_references_prev_df(self):
        cell = _transformation_cell("EP", 0, _make_mapping(), 3)
        assert "df_source" in cell


class TestAEPTemplate:
    """AEP — Advanced External Procedure → library wrapper UDF."""

    def test_generates_udf(self):
        cell = _transformation_cell("AEP", 1, _make_mapping(), 3)
        assert "advanced_ext_proc_udf" in cell

    def test_mentions_ctypes(self):
        cell = _transformation_cell("AEP", 1, _make_mapping(), 3)
        assert "ctypes" in cell

    def test_mentions_jpype(self):
        cell = _transformation_cell("AEP", 1, _make_mapping(), 3)
        assert "jpype" in cell


class TestASSOCTemplate:
    """ASSOC — Association → window grouping."""

    def test_generates_window(self):
        cell = _transformation_cell("ASSOC", 1, _make_mapping(), 3)
        assert "Window" in cell

    def test_generates_dense_rank(self):
        cell = _transformation_cell("ASSOC", 1, _make_mapping(), 3)
        assert "dense_rank" in cell

    def test_generates_group_rank(self):
        cell = _transformation_cell("ASSOC", 1, _make_mapping(), 3)
        assert "ASSOC_GROUP_RANK" in cell


class TestKEYGENTemplate:
    """KEYGEN — Key Generator → surrogate keys."""

    def test_generates_monotonically(self):
        cell = _transformation_cell("KEYGEN", 1, _make_mapping(), 3)
        assert "monotonically_increasing_id" in cell

    def test_generates_sha2_option(self):
        cell = _transformation_cell("KEYGEN", 1, _make_mapping(), 3)
        assert "sha2" in cell

    def test_generates_key_column(self):
        cell = _transformation_cell("KEYGEN", 1, _make_mapping(), 3)
        assert "GENERATED_KEY" in cell


class TestADDRVALTemplate:
    """ADDRVAL — Address Validator → Azure Maps / regex."""

    def test_mentions_azure_maps(self):
        cell = _transformation_cell("ADDRVAL", 1, _make_mapping(), 3)
        assert "azure" in cell.lower() or "Maps" in cell

    def test_generates_regex_fallback(self):
        cell = _transformation_cell("ADDRVAL", 1, _make_mapping(), 3)
        assert "regexp_replace" in cell

    def test_generates_std_address(self):
        cell = _transformation_cell("ADDRVAL", 1, _make_mapping(), 3)
        assert "STD_ADDRESS" in cell


class TestSprint31OracleAssessment:
    """Sprint 31: Oracle Object Types detected in assessment patterns."""

    from run_assessment import ORACLE_PATTERNS

    def test_create_type_pattern(self):
        assert "CREATE TYPE" in self.ORACLE_PATTERNS

    def test_object_type_pattern(self):
        assert "OBJECT TYPE" in self.ORACLE_PATTERNS

    def test_create_type_matches(self):
        pattern = re.compile(self.ORACLE_PATTERNS["CREATE TYPE"], re.IGNORECASE)
        assert pattern.search("CREATE TYPE my_type")
        assert pattern.search("CREATE OR REPLACE TYPE my_type")

    def test_object_type_matches(self):
        pattern = re.compile(self.ORACLE_PATTERNS["OBJECT TYPE"], re.IGNORECASE)
        assert pattern.search("AS OBJECT (col1 NUMBER)")


from run_sql_migration import convert_sql as _convert_sql


class TestSprint31SQLConversion:
    """Sprint 31: Oracle Object Type → StructType SQL conversion."""

    def test_create_type_as_object(self):
        sql = "CREATE OR REPLACE TYPE address_type AS OBJECT (street VARCHAR2(100))"
        result = _convert_sql(sql, "oracle")
        assert "StructType" in result or "Flatten" in result

    def test_create_type_as_table(self):
        sql = "CREATE TYPE address_list AS TABLE OF address_type"
        result = _convert_sql(sql, "oracle")
        assert "ArrayType" in result or "collection type" in result


# ═══════════════════════════════════════════════
#  Sprint 33 — Advanced PL/SQL Conversion Tests
# ═══════════════════════════════════════════════

class TestSprint33DynamicSQL:
    """Sprint 33: Dynamic SQL conversion."""

    def test_execute_immediate_literal(self):
        sql = "EXECUTE IMMEDIATE 'SELECT * FROM t1'"
        result = _convert_sql(sql, "oracle")
        assert "spark.sql" in result

    def test_execute_immediate_variable(self):
        sql = "EXECUTE IMMEDIATE v_sql_stmt"
        result = _convert_sql(sql, "oracle")
        assert "spark.sql" in result

    def test_dbms_sql_replaced(self):
        sql = "DBMS_SQL.OPEN_CURSOR"
        result = _convert_sql(sql, "oracle")
        assert "TODO" in result or "spark.sql" in result


class TestSprint33Cursors:
    """Sprint 33: PL/SQL Cursor → PySpark iterator."""

    def test_cursor_is_select(self):
        sql = "CURSOR c_emp IS SELECT name FROM employees"
        result = _convert_sql(sql, "oracle")
        assert "df_" in result or "PySpark" in result

    def test_open_cursor(self):
        sql = "OPEN c_emp;"
        result = _convert_sql(sql, "oracle")
        assert "Cursor" in result or "DataFrame" in result

    def test_fetch_into(self):
        sql = "FETCH c_emp INTO v_name"
        result = _convert_sql(sql, "oracle")
        assert "collect" in result or "foreach" in result or "FETCH" in result

    def test_close_cursor(self):
        sql = "CLOSE c_emp;"
        result = _convert_sql(sql, "oracle")
        assert "closed" in result or "no action" in result

    def test_for_in_loop(self):
        sql = "FOR rec IN c_emp LOOP"
        result = _convert_sql(sql, "oracle")
        assert "collect" in result


class TestSprint33BulkAndForall:
    """Sprint 33: BULK COLLECT and FORALL conversion."""

    def test_bulk_collect(self):
        sql = "BULK COLLECT INTO emp_table"
        result = _convert_sql(sql, "oracle")
        assert "DataFrame" in result or "collect" in result

    def test_forall(self):
        sql = "FORALL i IN 1..emp_table.COUNT"
        result = _convert_sql(sql, "oracle")
        assert "batch" in result.lower() or "DataFrame" in result


class TestSprint33ConnectBy:
    """Sprint 33: CONNECT BY → Recursive CTE."""

    def test_connect_by_prior(self):
        sql = "CONNECT BY PRIOR employee_id = manager_id"
        result = _convert_sql(sql, "oracle")
        assert "RECURSIVE" in result or "recursive" in result or "CTE" in result

    def test_connect_by_nocycle(self):
        sql = "CONNECT BY NOCYCLE PRIOR id = parent_id"
        result = _convert_sql(sql, "oracle")
        assert "RECURSIVE" in result or "recursive" in result or "CTE" in result

    def test_start_with(self):
        sql = "START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id"
        result = _convert_sql(sql, "oracle")
        assert "anchor" in result or "START WITH" in result


class TestSprint33ExceptionHandling:
    """Sprint 33: EXCEPTION WHEN and RAISE conversion."""

    def test_exception_when(self):
        sql = "EXCEPTION WHEN NO_DATA_FOUND THEN"
        result = _convert_sql(sql, "oracle")
        assert "except" in result

    def test_raise_application_error(self):
        sql = "RAISE_APPLICATION_ERROR(-20001, 'Custom error')"
        result = _convert_sql(sql, "oracle")
        assert "ValueError" in result or "raise" in result

    def test_raise_bare(self):
        sql = "RAISE;"
        result = _convert_sql(sql, "oracle")
        assert "raise" in result


# ═══════════════════════════════════════════════
#  Sprint 35 — Multi-Tenant & Enterprise Tests
# ═══════════════════════════════════════════════

from run_migration import (
    substitute_keyvault_refs,
    generate_manifest,
    run_batch,
    _get_memory_mb,
    sanitize_output,
)


class TestKeyVaultSubstitution:
    """Sprint 35: Key Vault template substitution."""

    def test_simple_substitution(self):
        config = {"jdbc_url": "{{KV:oracle-jdbc-url}}"}
        result = substitute_keyvault_refs(config, "my-vault")
        assert 'notebookutils.credentials.getSecret("my-vault", "oracle-jdbc-url")' in result["jdbc_url"]

    def test_nested_substitution(self):
        config = {"fabric": {"secret": "{{KV:workspace-secret}}"}}
        result = substitute_keyvault_refs(config, "vault-a")
        assert "notebookutils.credentials.getSecret" in result["fabric"]["secret"]

    def test_no_placeholder_unchanged(self):
        config = {"key": "plain_value"}
        result = substitute_keyvault_refs(config, "vault")
        assert result["key"] == "plain_value"

    def test_list_substitution(self):
        config = {"secrets": ["{{KV:secret1}}", "plain"]}
        result = substitute_keyvault_refs(config, "v")
        assert "notebookutils" in result["secrets"][0]
        assert result["secrets"][1] == "plain"

    def test_multiple_placeholders(self):
        config = {"url": "host={{KV:host}}&key={{KV:key}}"}
        result = substitute_keyvault_refs(config, "v")
        assert result["url"].count("notebookutils") == 2


class TestManifestGeneration:
    """Sprint 35: Deployment manifest."""

    def test_manifest_structure(self, tmp_path):
        # Create mock output directories
        (tmp_path / "output" / "notebooks").mkdir(parents=True)
        (tmp_path / "output" / "notebooks" / "NB_TEST.py").write_text("# test")
        (tmp_path / "output" / "pipelines").mkdir(parents=True)

        import run_migration
        original_ws = run_migration.WORKSPACE
        run_migration.WORKSPACE = tmp_path
        try:
            config = {"fabric": {"workspace_id": "test-ws"}}
            manifest_path = generate_manifest([], config)
            assert manifest_path.exists()
            manifest = json.loads(manifest_path.read_text())
            assert manifest["total_artifacts"] >= 1
            assert manifest["workspace_id"] == "test-ws"
            assert "artifacts" in manifest
            assert "deployment_order" in manifest
        finally:
            run_migration.WORKSPACE = original_ws

    def test_manifest_has_schema_version(self, tmp_path):
        (tmp_path / "output").mkdir(parents=True)
        import run_migration
        original_ws = run_migration.WORKSPACE
        run_migration.WORKSPACE = tmp_path
        try:
            manifest_path = generate_manifest([], {})
            manifest = json.loads(manifest_path.read_text())
            assert manifest["schema_version"] == "1.0"
        finally:
            run_migration.WORKSPACE = original_ws


class TestBatchMode:
    """Sprint 35: Batch migration."""

    def test_batch_valid_dir(self, tmp_path):
        dir1 = tmp_path / "tenant_a"
        dir1.mkdir()
        results = run_batch([str(dir1)], None, {})
        assert len(results) == 1
        assert results[0]["status"] == "ok"

    def test_batch_missing_dir(self):
        results = run_batch(["/nonexistent/path123"], None, {})
        assert results[0]["status"] == "error"

    def test_batch_multiple_dirs(self, tmp_path):
        dir1 = tmp_path / "a"
        dir2 = tmp_path / "b"
        dir1.mkdir()
        dir2.mkdir()
        results = run_batch([str(dir1), str(dir2)], None, {})
        assert len(results) == 2
        assert all(r["status"] == "ok" for r in results)


class TestSanitizeOutput:
    """Sprint 35: Credential sanitization in audit logs."""

    def test_password_redacted(self):
        assert "REDACTED" in sanitize_output("password=secret123")

    def test_token_redacted(self):
        assert "REDACTED" in sanitize_output("token=abc123xyz")

    def test_plain_text_unchanged(self):
        assert sanitize_output("hello world") == "hello world"

    def test_jdbc_url_redacted(self):
        assert "REDACTED" in sanitize_output("jdbc:oracle:thin:@host/db")


# ═══════════════════════════════════════════════
#  Sprint 37 — Performance at Scale Tests
# ═══════════════════════════════════════════════

class TestPerformanceProfiling:
    """Sprint 37: Memory profiling helper."""

    def test_get_memory_returns_float(self):
        result = _get_memory_mb()
        assert isinstance(result, float)

    def test_get_memory_non_negative(self):
        assert _get_memory_mb() >= 0.0


from run_notebook_migration import _write_notebook as _nb_write, generate_notebook as _gen_notebook


class TestParallelNotebookGeneration:
    """Sprint 37: Parallel notebook generation support."""

    def test_write_notebook_returns_tuple(self, tmp_path):
        mapping = _make_mapping("M_PARALLEL_TEST")
        result = _nb_write((mapping, str(tmp_path)))
        assert result[0] == "M_PARALLEL_TEST"
        assert isinstance(result[1], str)  # complexity
        assert isinstance(result[2], int)  # tx_count

    def test_write_notebook_creates_file(self, tmp_path):
        mapping = _make_mapping("M_FILE_CHECK")
        _nb_write((mapping, str(tmp_path)))
        assert (tmp_path / "NB_M_FILE_CHECK.py").exists()

    def test_generate_notebook_basic(self):
        mapping = _make_mapping("M_BASIC")
        content = _gen_notebook(mapping)
        assert "NB_M_BASIC" in content
        assert "CELL 1" in content


from run_migration import _parse_args as _do_parse_args


class TestProfileCLIArg:
    """Sprint 37: --profile CLI argument accepted."""

    def test_profile_flag_exists(self):
        """Ensure _parse_args accepts --profile."""
        original_argv = sys.argv
        sys.argv = ["run_migration.py", "--profile"]
        try:
            args = _do_parse_args()
            assert args.profile is True
        finally:
            sys.argv = original_argv


# ═══════════════════════════════════════════════
#  Sprint 38 — Web UI Tests
# ═══════════════════════════════════════════════

class TestWebUIStructure:
    """Sprint 38: Web app module structure."""

    def test_web_app_exists(self):
        web_app = PROJECT_ROOT / "web" / "app.py"
        assert web_app.exists(), "web/app.py should exist"

    def test_web_app_importable(self):
        sys.path.insert(0, str(PROJECT_ROOT / "web"))
        try:
            import app
            assert hasattr(app, "STEPS")
            assert hasattr(app, "run_streamlit")
            assert hasattr(app, "run_fallback_server")
            assert hasattr(app, "_load_inventory")
            assert hasattr(app, "_list_artifacts")
        finally:
            sys.path.pop(0)

    def test_steps_count(self):
        sys.path.insert(0, str(PROJECT_ROOT / "web"))
        try:
            import app
            assert len(app.STEPS) == 6
        finally:
            sys.path.pop(0)

    def test_steps_have_required_keys(self):
        sys.path.insert(0, str(PROJECT_ROOT / "web"))
        try:
            import app
            for step in app.STEPS:
                assert "id" in step
                assert "title" in step
                assert "icon" in step
                assert "desc" in step
        finally:
            sys.path.pop(0)


class TestWebHelpers:
    """Sprint 38: Web helper functions."""

    def test_list_artifacts_empty_dir(self):
        sys.path.insert(0, str(PROJECT_ROOT / "web"))
        try:
            import app
            result = app._list_artifacts("nonexistent_dir_12345")
            assert result == []
        finally:
            sys.path.pop(0)

    def test_load_inventory_returns_dict_or_none(self):
        sys.path.insert(0, str(PROJECT_ROOT / "web"))
        try:
            import app
            result = app._load_inventory()
            assert result is None or isinstance(result, dict)
        finally:
            sys.path.pop(0)


# ═══════════════════════════════════════════════
#  Sprint 39 — DQ & Governance Tests
# ═══════════════════════════════════════════════

from run_assessment import (
    PII_COLUMN_PATTERNS,
    SENSITIVITY_LEVELS,
    detect_pii_columns,
    extract_dq_rules,
)


class TestPIIPatterns:
    """Sprint 39: PII column detection patterns."""

    def test_ssn_pattern(self):
        pattern = re.compile(PII_COLUMN_PATTERNS["SSN"])
        assert pattern.search("ssn")
        assert pattern.search("social_security")

    def test_email_pattern(self):
        pattern = re.compile(PII_COLUMN_PATTERNS["EMAIL"])
        assert pattern.search("email")
        assert pattern.search("email_addr")

    def test_phone_pattern(self):
        pattern = re.compile(PII_COLUMN_PATTERNS["PHONE"])
        assert pattern.search("phone")
        assert pattern.search("mobile")

    def test_credit_card_pattern(self):
        pattern = re.compile(PII_COLUMN_PATTERNS["CREDIT_CARD"])
        assert pattern.search("credit_card")
        assert pattern.search("cc_num")

    def test_dob_pattern(self):
        pattern = re.compile(PII_COLUMN_PATTERNS["DOB"])
        assert pattern.search("date_of_birth")
        assert pattern.search("dob")

    def test_passport_pattern(self):
        pattern = re.compile(PII_COLUMN_PATTERNS["PASSPORT"])
        assert pattern.search("passport_no")

    def test_national_id_pattern(self):
        pattern = re.compile(PII_COLUMN_PATTERNS["NATIONAL_ID"])
        assert pattern.search("tax_id")
        assert pattern.search("national_id")


class TestSensitivityLevels:
    """Sprint 39: Sensitivity classification."""

    def test_ssn_highly_confidential(self):
        assert SENSITIVITY_LEVELS["SSN"] == "Highly Confidential"

    def test_credit_card_highly_confidential(self):
        assert SENSITIVITY_LEVELS["CREDIT_CARD"] == "Highly Confidential"

    def test_email_confidential(self):
        assert SENSITIVITY_LEVELS["EMAIL"] == "Confidential"

    def test_name_internal(self):
        assert SENSITIVITY_LEVELS["NAME"] == "Internal"

    def test_all_categories_have_levels(self):
        for cat in PII_COLUMN_PATTERNS:
            assert cat in SENSITIVITY_LEVELS, f"Missing sensitivity for {cat}"


class TestDetectPIIColumns:
    """Sprint 39: detect_pii_columns function."""

    def test_finds_email_in_sources(self):
        mappings = [{
            "name": "M_TEST",
            "sources": ["Oracle.HR.email_addr"],
            "targets": ["DIM_EMPLOYEE"],
            "transformations": [],
            "sql_overrides": [],
            "field_lineage": [],
        }]
        findings = detect_pii_columns(mappings)
        assert len(findings) >= 1
        assert any(f["pii_category"] == "EMAIL" for f in findings)

    def test_finds_ssn_in_targets(self):
        mappings = [{
            "name": "M_TEST",
            "sources": [],
            "targets": ["silver.ssn"],
            "transformations": [],
            "sql_overrides": [],
            "field_lineage": [],
        }]
        findings = detect_pii_columns(mappings)
        assert any(f["pii_category"] == "SSN" for f in findings)

    def test_no_pii_in_clean_mapping(self):
        mappings = [{
            "name": "M_CLEAN",
            "sources": ["Oracle.SALES.ORDERS"],
            "targets": ["FACT_ORDERS"],
            "transformations": [],
            "sql_overrides": [],
            "field_lineage": [],
        }]
        findings = detect_pii_columns(mappings)
        assert len(findings) == 0

    def test_finding_includes_sensitivity(self):
        mappings = [{
            "name": "M_PII",
            "sources": ["Oracle.HR.credit_card"],
            "targets": [],
            "transformations": [],
            "sql_overrides": [],
            "field_lineage": [],
        }]
        findings = detect_pii_columns(mappings)
        cc_findings = [f for f in findings if f["pii_category"] == "CREDIT_CARD"]
        assert len(cc_findings) >= 1
        assert cc_findings[0]["sensitivity"] == "Highly Confidential"


class TestExtractDQRules:
    """Sprint 39: DQ rule extraction."""

    def test_filter_as_dq_rule(self):
        mappings = [{
            "name": "M_FIL",
            "transformations": ["SQ", "FIL", "EXP"],
            "sql_overrides": [],
        }]
        rules = extract_dq_rules(mappings)
        assert any(r["type"] == "filter" for r in rules)

    def test_dq_transformation(self):
        mappings = [{
            "name": "M_DQ",
            "transformations": ["SQ", "DQ"],
            "sql_overrides": [],
        }]
        rules = extract_dq_rules(mappings)
        assert any(r["type"] == "data_quality" for r in rules)

    def test_not_null_in_override(self):
        mappings = [{
            "name": "M_NN",
            "transformations": [],
            "sql_overrides": [{"value": "SELECT * FROM t WHERE id IS NOT NULL"}],
        }]
        rules = extract_dq_rules(mappings)
        assert any(r["type"] == "not_null_check" for r in rules)

    def test_check_constraint(self):
        mappings = [{
            "name": "M_CHK",
            "transformations": [],
            "sql_overrides": [{"value": "ALTER TABLE t ADD CHECK(amount > 0)"}],
        }]
        rules = extract_dq_rules(mappings)
        assert any(r["type"] == "check_constraint" for r in rules)

    def test_no_rules_clean_mapping(self):
        mappings = [{
            "name": "M_CLEAN",
            "transformations": ["SQ", "EXP"],
            "sql_overrides": [],
        }]
        rules = extract_dq_rules(mappings)
        assert len(rules) == 0


# ═══════════════════════════════════════════════
#  Sprint 40 — Enterprise Docs Tests
# ═══════════════════════════════════════════════

class TestRunbookExists:
    """Sprint 40: RUNBOOK.md exists and has required sections."""

    def test_runbook_exists(self):
        assert (PROJECT_ROOT / "docs" / "RUNBOOK.md").exists()

    def test_runbook_has_quick_reference(self):
        content = (PROJECT_ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        assert "Quick Reference" in content

    def test_runbook_has_preflight(self):
        content = (PROJECT_ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        assert "Pre-Flight" in content

    def test_runbook_has_troubleshooting(self):
        content = (PROJECT_ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        assert "Troubleshooting" in content

    def test_runbook_has_monitoring(self):
        content = (PROJECT_ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        assert "Monitoring" in content

    def test_runbook_has_recovery(self):
        content = (PROJECT_ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        assert "Recovery" in content

    def test_runbook_has_multitenant(self):
        content = (PROJECT_ROOT / "docs" / "RUNBOOK.md").read_text(encoding="utf-8")
        assert "Multi-Tenant" in content


class TestEnterprisePlaybookExists:
    """Sprint 40: ENTERPRISE_PLAYBOOK.md exists and has required sections."""

    def test_playbook_exists(self):
        assert (PROJECT_ROOT / "docs" / "ENTERPRISE_PLAYBOOK.md").exists()

    def test_playbook_has_phases(self):
        content = (PROJECT_ROOT / "docs" / "ENTERPRISE_PLAYBOOK.md").read_text(encoding="utf-8")
        assert "Phase 1" in content
        assert "Phase 2" in content
        assert "Phase 3" in content

    def test_playbook_has_wave_strategy(self):
        content = (PROJECT_ROOT / "docs" / "ENTERPRISE_PLAYBOOK.md").read_text(encoding="utf-8")
        assert "Wave Strategy" in content

    def test_playbook_has_risk_register(self):
        content = (PROJECT_ROOT / "docs" / "ENTERPRISE_PLAYBOOK.md").read_text(encoding="utf-8")
        assert "Risk Register" in content

    def test_playbook_has_governance(self):
        content = (PROJECT_ROOT / "docs" / "ENTERPRISE_PLAYBOOK.md").read_text(encoding="utf-8")
        assert "Governance" in content

    def test_playbook_has_data_classification(self):
        content = (PROJECT_ROOT / "docs" / "ENTERPRISE_PLAYBOOK.md").read_text(encoding="utf-8")
        assert "Highly Confidential" in content
        assert "Confidential" in content
        assert "Internal" in content


# ═══════════════════════════════════════════════
#  Sprint 35/37 — CLI Flag Integration Tests
# ═══════════════════════════════════════════════

class TestCLIFlags:
    """Sprints 35+37: CLI flags accepted by argparse."""

    def _parse(self, *args):
        original = sys.argv
        sys.argv = ["run_migration.py"] + list(args)
        try:
            return _do_parse_args()
        finally:
            sys.argv = original

    def test_batch_flag(self):
        args = self._parse("--batch", "dir1", "dir2")
        assert args.batch == ["dir1", "dir2"]

    def test_manifest_flag(self):
        args = self._parse("--manifest")
        assert args.manifest is True

    def test_tenant_flag(self):
        args = self._parse("--tenant", "my-vault")
        assert args.tenant == "my-vault"

    def test_parallel_waves_flag(self):
        args = self._parse("--parallel-waves", "4")
        assert args.parallel_waves == 4

    def test_profile_flag(self):
        args = self._parse("--profile")
        assert args.profile is True

    def test_default_values(self):
        args = self._parse()
        assert args.batch is None
        assert args.manifest is False
        assert args.tenant is None
        assert args.parallel_waves is None
        assert args.profile is False
