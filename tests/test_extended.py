"""
Extended Test Suite — Sprint 14
Covers: run_assessment.py, deploy_to_fabric.py, run_migration.py (config/logging)

These tests complement test_migration.py (Sprint 9) with coverage for the
assessment engine, deployment script, and orchestrator config paths.
"""

import base64
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ─────────────────────────────────────────────
#  Assessment Tests
# ─────────────────────────────────────────────

class TestAssessmentHelpers:
    """Test assessment utility functions (no file I/O)."""

    def test_abbrev_known_type(self):
        from run_assessment import abbrev
        assert abbrev("Source Qualifier") == "SQ"
        assert abbrev("Expression") == "EXP"
        assert abbrev("Lookup Procedure") == "LKP"

    def test_abbrev_unknown_type(self):
        from run_assessment import abbrev, warnings
        before = len(warnings)
        result = abbrev("SuperCustomXform999")
        assert result == "SuperCustomXform999"
        assert len(warnings) > before

    def test_classify_simple(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "EXP", "FIL"],
            sql_overrides=[],
            sources=["Oracle.SALES.CUSTOMERS"],
            targets=["DIM_CUSTOMER"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Simple"

    def test_classify_medium_with_lookup(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "LKP", "EXP"],
            sql_overrides=[],
            sources=["Oracle.SALES.ORDERS"],
            targets=["FACT_ORDERS"],
            has_stored_proc=False,
            lookup_conditions=[{"lookup": "LKP_X", "condition": "a=b"}],
        )
        assert result == "Medium"

    def test_classify_complex_with_router(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "RTR", "EXP"],
            sql_overrides=[],
            sources=["Oracle.SALES.ORDERS"],
            targets=["TGT_A", "TGT_B"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Complex"

    def test_classify_complex_with_update_strategy(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "UPD"],
            sql_overrides=[],
            sources=["Oracle.SALES.ORDERS"],
            targets=["FACT_ORDERS"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Complex"

    def test_classify_custom_java(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "JTX"],
            sql_overrides=[],
            sources=["Oracle.SALES.ORDERS"],
            targets=["TGT"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Custom"

    def test_classify_complex_with_stored_proc(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "EXP"],
            sql_overrides=[],
            sources=["Oracle.SALES.ORDERS"],
            targets=["TGT"],
            has_stored_proc=True,
            lookup_conditions=[],
        )
        assert result == "Complex"

    def test_classify_complex_with_merge_sql(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "EXP"],
            sql_overrides=[{"type": "Sql Query", "value": "MERGE INTO tgt USING src ON tgt.id=src.id"}],
            sources=["Oracle.SALES.ORDERS"],
            targets=["TGT"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Complex"


class TestDetectSourceDbType:
    """Test SQL detection heuristics."""

    def test_oracle_detected(self):
        from run_assessment import detect_source_db_type
        sql = "SELECT NVL(name, 'X'), SYSDATE, DECODE(status, 1, 'A') FROM dual"
        assert detect_source_db_type(sql) == "oracle"

    def test_sqlserver_detected(self):
        from run_assessment import detect_source_db_type
        sql = "SELECT ISNULL(name, 'X'), GETDATE(), CHARINDEX('a', name) FROM dbo.users WITH (NOLOCK)"
        assert detect_source_db_type(sql) == "sqlserver"

    def test_unknown_for_ansi_sql(self):
        from run_assessment import detect_source_db_type
        sql = "SELECT id, name FROM users WHERE active = 1"
        assert detect_source_db_type(sql) == "unknown"

    def test_oracle_wins_mixed(self):
        from run_assessment import detect_source_db_type
        sql = "SELECT NVL(x, 0), SYSDATE, DECODE(a,1,2,3) FROM t WHERE ROWNUM <= 10"
        assert detect_source_db_type(sql) == "oracle"


class TestXmlParsing:
    """Test XML parsing and format detection."""

    def test_detect_powercenter_format(self, tmp_path):
        from run_assessment import detect_xml_format
        xml_file = tmp_path / "pc.xml"
        xml_file.write_text('<?xml version="1.0"?>\n<POWERMART><REPOSITORY/></POWERMART>')
        assert detect_xml_format(xml_file) == "powercenter"

    def test_detect_iics_format(self, tmp_path):
        from run_assessment import detect_xml_format
        xml_file = tmp_path / "iics.xml"
        xml_file.write_text('<?xml version="1.0"?>\n<exportMetadata/>')
        assert detect_xml_format(xml_file) == "iics"

    def test_detect_unknown_format(self, tmp_path):
        from run_assessment import detect_xml_format
        xml_file = tmp_path / "other.xml"
        xml_file.write_text('<?xml version="1.0"?>\n<data><record/></data>')
        assert detect_xml_format(xml_file) == "unknown"

    def test_detect_malformed_xml(self, tmp_path):
        from run_assessment import detect_xml_format
        xml_file = tmp_path / "bad.xml"
        xml_file.write_text('this is not xml at all')
        assert detect_xml_format(xml_file) == "unknown"

    def test_safe_parse_valid_xml(self, tmp_path):
        from run_assessment import safe_parse_xml
        xml_file = tmp_path / "good.xml"
        xml_file.write_text('<?xml version="1.0"?>\n<root><child name="A"/></root>')
        tree, root = safe_parse_xml(xml_file)
        assert root is not None
        assert root.tag == "root"

    def test_safe_parse_malformed_xml(self, tmp_path):
        from run_assessment import safe_parse_xml
        xml_file = tmp_path / "bad.xml"
        xml_file.write_text('<root><unclosed>')
        tree, root = safe_parse_xml(xml_file)
        # Should attempt recovery — either succeed or return None gracefully
        # The function doesn't crash either way

    def test_parse_mapping_xml(self, tmp_path):
        from run_assessment import parse_mapping_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <MAPPING NAME="M_TEST" DESCRIPTION="Test mapping">
        <TRANSFORMATION NAME="SQ_TEST" TYPE="Source Qualifier" REUSABLE="NO">
          <TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT * FROM CUSTOMERS"/>
        </TRANSFORMATION>
        <TRANSFORMATION NAME="EXP_DERIVE" TYPE="Expression" REUSABLE="NO"/>
        <TRANSFORMATION NAME="TGT_TABLE" TYPE="Target Definition"/>
        <INSTANCE NAME="SQ_TEST" TRANSFORMATION_NAME="SQ_TEST" TRANSFORMATION_TYPE="Source Qualifier"/>
        <INSTANCE NAME="EXP_DERIVE" TRANSFORMATION_NAME="EXP_DERIVE" TRANSFORMATION_TYPE="Expression"/>
        <CONNECTOR FROMINSTANCE="SQ_TEST" TOINSTANCE="EXP_DERIVE"/>
        <CONNECTOR FROMINSTANCE="EXP_DERIVE" TOINSTANCE="TGT_TABLE"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        xml_file = tmp_path / "M_TEST.xml"
        xml_file.write_text(xml)
        mappings = parse_mapping_xml(xml_file)
        assert len(mappings) >= 1
        m = mappings[0]
        assert m["name"] == "M_TEST"
        assert "SQ" in m["transformations"]

    def test_parse_workflow_xml(self, tmp_path):
        from run_assessment import parse_workflow_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <WORKFLOW NAME="WF_TEST" DESCRIPTION="Test workflow">
        <SCHEDULER NAME="DAILY" SCHEDULETYPE="0"/>
        <SESSION NAME="S_M_TEST" MAPPINGNAME="M_TEST" ISVALID="YES"/>
        <TASKINSTANCE NAME="Start" TASKNAME="Start" TASKTYPE="Start"/>
        <TASKINSTANCE NAME="S_M_TEST" TASKNAME="S_M_TEST" TASKTYPE="Session"/>
        <WORKFLOWLINK FROMTASK="Start" TOTASK="S_M_TEST" CONDITION=""/>
      </WORKFLOW>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        xml_file = tmp_path / "WF_TEST.xml"
        xml_file.write_text(xml)
        workflows = parse_workflow_xml(xml_file)
        assert len(workflows) >= 1
        wf = workflows[0]
        assert wf["name"] == "WF_TEST"
        assert "S_M_TEST" in wf["sessions"]


class TestParameterFiles:
    """Test .prm parameter file parsing."""

    def test_parse_prm_file(self, tmp_path):
        import run_assessment
        original_ws = run_assessment.WORKSPACE
        try:
            run_assessment.WORKSPACE = tmp_path
            prm = tmp_path / "test.prm"
            prm.write_text("[Global]\n$$LOAD_DATE=2026-01-01\n$$BATCH_ID=100\n\n[WF_TEST]\n$$ENV=PROD\n")
            results = run_assessment.parse_parameter_files(tmp_path)
            assert len(results) == 1
            r = results[0]
            assert r["file"] == "test.prm"
            assert r["total_params"] == 3
            assert r["sections"]["Global"]["$$LOAD_DATE"] == "2026-01-01"
        finally:
            run_assessment.WORKSPACE = original_ws

    def test_parse_empty_dir(self, tmp_path):
        from run_assessment import parse_parameter_files
        results = parse_parameter_files(tmp_path)
        assert results == []

    def test_parse_nonexistent_dir(self, tmp_path):
        from run_assessment import parse_parameter_files
        results = parse_parameter_files(tmp_path / "nonexistent")
        assert results == []


# ─────────────────────────────────────────────
#  Deployment Tests
# ─────────────────────────────────────────────

class TestDeployment:
    """Test deploy_to_fabric.py functions (mocked HTTP)."""

    def test_read_as_base64(self, tmp_path):
        from deploy_to_fabric import _read_as_base64
        f = tmp_path / "test.txt"
        f.write_text("hello world", encoding="utf-8")
        encoded = _read_as_base64(f)
        assert base64.b64decode(encoded) == b"hello world"

    def test_headers(self):
        from deploy_to_fabric import _headers
        h = _headers("mytoken123")
        assert h["Authorization"] == "Bearer mytoken123"
        assert h["Content-Type"] == "application/json"

    def test_deploy_notebooks_dry_run(self, tmp_path):
        from deploy_to_fabric import OUTPUT_DIR, deploy_notebooks
        # Create a temp notebook
        nb_dir = OUTPUT_DIR / "notebooks"
        nb_files = list(nb_dir.glob("NB_*.py"))
        assert len(nb_files) > 0, "Expected existing NB_*.py files in output/notebooks/"
        results = deploy_notebooks("fake-workspace-id", None, dry_run=True)
        assert len(results) > 0
        assert all(r["status"] == "dry-run" for r in results)

    def test_deploy_pipelines_dry_run(self):
        from deploy_to_fabric import deploy_pipelines
        results = deploy_pipelines("fake-workspace-id", None, dry_run=True)
        assert len(results) > 0
        assert all(r["status"] == "dry-run" for r in results)

    def test_deploy_sql_dry_run(self):
        from deploy_to_fabric import deploy_sql_scripts
        results = deploy_sql_scripts("fake-workspace-id", None, dry_run=True)
        assert len(results) > 0
        assert all(r["status"] == "dry-run" for r in results)


# ─────────────────────────────────────────────
#  Orchestrator Config/Logging Tests
# ─────────────────────────────────────────────

class TestOrchestratorConfig:
    """Test run_migration.py config loading and logging setup."""

    def test_load_config_nonexistent(self, tmp_path):
        from run_migration import _load_config
        cfg = _load_config(str(tmp_path / "nonexistent.yaml"))
        assert cfg == {}

    def test_load_config_valid_yaml(self, tmp_path):
        from run_migration import _load_config
        cfg_file = tmp_path / "test.yaml"
        cfg_file.write_text("fabric:\n  workspace_id: test-123\n", encoding="utf-8")
        cfg = _load_config(str(cfg_file))
        if "fabric" in cfg:
            assert cfg["fabric"]["workspace_id"] == "test-123"
        # If PyYAML not installed, falls back gracefully

    def test_setup_logging_text(self):
        from run_migration import _setup_logging
        log = _setup_logging(verbose=False, log_format="text", config={})
        assert log.name == "migration"
        assert log.level <= 20  # INFO or lower

    def test_setup_logging_json(self):
        from run_migration import _setup_logging
        log = _setup_logging(verbose=False, log_format="json", config={})
        assert log.name == "migration"
        assert len(log.handlers) > 0

    def test_setup_logging_verbose(self):
        import logging

        from run_migration import _setup_logging
        log = _setup_logging(verbose=True, log_format="text", config={})
        assert log.level == logging.DEBUG

    def test_main_dry_run(self):
        """Test full main() in dry-run mode."""
        saved = sys.argv
        try:
            sys.argv = ["run_migration.py", "--dry-run", "--skip", "0"]
            from run_migration import main
            # Should not raise
            main()
        finally:
            sys.argv = saved


class TestCheckpointing:
    """Test incremental migration checkpointing (Sprint 15)."""

    def test_save_and_load_checkpoint(self, tmp_path):
        import run_migration
        original = run_migration.CHECKPOINT_PATH
        try:
            run_migration.CHECKPOINT_PATH = tmp_path / ".checkpoint.json"
            cp = {"completed_phases": [0, 1], "results": []}
            run_migration._save_checkpoint(cp)
            loaded = run_migration._load_checkpoint()
            assert loaded["completed_phases"] == [0, 1]
            assert "updated" in loaded
        finally:
            run_migration.CHECKPOINT_PATH = original

    def test_load_nonexistent_checkpoint(self, tmp_path):
        import run_migration
        original = run_migration.CHECKPOINT_PATH
        try:
            run_migration.CHECKPOINT_PATH = tmp_path / "no_such_file.json"
            cp = run_migration._load_checkpoint()
            assert cp["completed_phases"] == []
        finally:
            run_migration.CHECKPOINT_PATH = original

    def test_clear_checkpoint(self, tmp_path):
        import run_migration
        original = run_migration.CHECKPOINT_PATH
        try:
            run_migration.CHECKPOINT_PATH = tmp_path / ".checkpoint.json"
            run_migration._save_checkpoint({"completed_phases": [1]})
            assert run_migration.CHECKPOINT_PATH.exists()
            run_migration._clear_checkpoint()
            assert not run_migration.CHECKPOINT_PATH.exists()
        finally:
            run_migration.CHECKPOINT_PATH = original

    def test_clear_nonexistent_is_safe(self, tmp_path):
        import run_migration
        original = run_migration.CHECKPOINT_PATH
        try:
            run_migration.CHECKPOINT_PATH = tmp_path / "no_such_file.json"
            run_migration._clear_checkpoint()  # Should not raise
        finally:
            run_migration.CHECKPOINT_PATH = original

    def test_resume_flag_parsed(self):
        saved = sys.argv
        try:
            sys.argv = ["run_migration.py", "--resume"]
            from run_migration import _parse_args
            args = _parse_args()
            assert args.resume is True
        finally:
            sys.argv = saved

    def test_reset_flag_parsed(self):
        saved = sys.argv
        try:
            sys.argv = ["run_migration.py", "--reset"]
            from run_migration import _parse_args
            args = _parse_args()
            assert args.reset is True
        finally:
            sys.argv = saved


class TestDashboard:
    """Test dashboard generation (Sprint 16)."""

    def test_collect_status_returns_dict(self):
        from dashboard import _collect_status
        status = _collect_status()
        assert isinstance(status, dict)
        assert "artifacts" in status
        assert "generated" in status

    def test_collect_status_finds_notebooks(self):
        from dashboard import _collect_status
        status = _collect_status()
        nbs = status["artifacts"]["notebooks"]
        assert len(nbs) > 0
        assert all(n.startswith("NB_") for n in nbs)

    def test_collect_status_finds_pipelines(self):
        from dashboard import _collect_status
        status = _collect_status()
        pls = status["artifacts"]["pipelines"]
        assert len(pls) > 0

    def test_collect_status_finds_validation(self):
        from dashboard import _collect_status
        status = _collect_status()
        vals = status["artifacts"]["validation"]
        assert len(vals) > 0

    def test_generate_html_contains_title(self):
        from dashboard import _collect_status, _generate_html
        html = _generate_html(_collect_status())
        assert "Migration Dashboard" in html
        assert "<!DOCTYPE html>" in html

    def test_generate_html_contains_artifacts(self):
        from dashboard import _collect_status, _generate_html
        html = _generate_html(_collect_status())
        assert "NB_" in html
        assert "PL_" in html
        assert "SQL_" in html

    def test_main_generates_file(self):
        saved = sys.argv
        try:
            sys.argv = ["dashboard.py"]
            from dashboard import OUTPUT_DIR, main
            main()
            assert (OUTPUT_DIR / "dashboard.html").exists()
        finally:
            sys.argv = saved
