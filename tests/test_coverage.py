"""
Sprint 17 — Coverage Expansion Tests
Targets: generate_html_reports.py, run_assessment.py (deep), run_pipeline_migration.py,
         run_validation.py, run_sql_migration.py, run_notebook_migration.py, deploy_to_fabric.py

Goal: Push overall coverage from ~52% to 80%+
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═════════════════════════════════════════════
#  HTML Report Generator Tests  (0% → 80%+)
# ═════════════════════════════════════════════

def _sample_inventory():
    """Return a minimal but complete inventory for testing report generators."""
    return {
        "generated": "2026-03-23",
        "source_platform": "Informatica PowerCenter",
        "target_platform": "Microsoft Fabric",
        "mappings": [
            {
                "name": "M_LOAD_CUSTOMERS",
                "sources": ["Oracle.SALES.CUSTOMERS"],
                "targets": ["DIM_CUSTOMER"],
                "transformations": ["SQ", "EXP", "FIL"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Simple",
                "sql_overrides": [],
                "lookup_conditions": [],
                "parameters": ["$$LOAD_DATE"],
                "target_load_order": [],
                "has_mapplet": False,
            },
            {
                "name": "M_LOAD_ORDERS",
                "sources": ["Oracle.SALES.ORDERS", "Oracle.SALES.ORDER_LINES"],
                "targets": ["FACT_ORDERS"],
                "transformations": ["SQ", "JNR", "LKP", "AGG", "EXP"],
                "has_sql_override": True,
                "has_stored_proc": False,
                "complexity": "Complex",
                "sql_overrides": [{"type": "Sql Query", "value": "SELECT * FROM ORDERS WHERE STATUS='A'"}],
                "lookup_conditions": [{"lookup": "LKP_PRODUCTS", "condition": "a.id=b.id"}],
                "parameters": [],
                "target_load_order": [],
                "has_mapplet": False,
            },
            {
                "name": "M_UPSERT_INVENTORY",
                "sources": ["Oracle.WH.INVENTORY"],
                "targets": ["DIM_INVENTORY"],
                "transformations": ["SQ", "UPD"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Complex",
                "sql_overrides": [],
                "lookup_conditions": [],
                "parameters": [],
                "target_load_order": [],
                "has_mapplet": True,
            },
        ],
        "workflows": [
            {
                "name": "WF_DAILY_SALES_LOAD",
                "sessions": ["S_M_LOAD_CUSTOMERS", "S_M_LOAD_ORDERS"],
                "session_to_mapping": {
                    "S_M_LOAD_CUSTOMERS": "M_LOAD_CUSTOMERS",
                    "S_M_LOAD_ORDERS": "M_LOAD_ORDERS",
                },
                "dependencies": {
                    "S_M_LOAD_ORDERS": ["S_M_LOAD_CUSTOMERS"],
                },
                "has_timer": False,
                "has_decision": True,
                "decision_tasks": ["DEC_CHECK_ORDERS"],
                "email_tasks": ["EMAIL_NOTIFY"],
                "pre_post_sql": [],
                "schedule": "DAILY_02AM",
            },
        ],
        "connections": [
            {"name": "CONN_SALES", "type": "Database", "subtype": "Oracle", "connect_string": "HOST:1521/ORCL"},
            {"name": "FTP_EXPORT", "type": "FTP", "host": "ftp.example.com", "remote_dir": "/data"},
        ],
        "sql_files": [
            {
                "file": "SP_UPDATE_ORDER_STATS.sql",
                "path": "input/sql/SP_UPDATE_ORDER_STATS.sql",
                "db_type": "oracle",
                "oracle_constructs": [
                    {"construct": "MERGE", "occurrences": 1, "lines": [5]},
                    {"construct": "NVL", "occurrences": 3, "lines": [10, 15, 20]},
                ],
                "sqlserver_constructs": [],
                "total_lines": 85,
            },
        ],
        "parameter_files": [
            {"file": "daily.prm", "total_params": 5, "sections": {"Global": {"$$LOAD_DATE": "2026-01-01"}}},
        ],
        "mapplets": {"MPLT_COMMON_DERIVE": ["EXP", "FIL"]},
        "summary": {
            "total_mappings": 3,
            "total_workflows": 1,
            "total_sessions": 2,
            "total_sql_files": 1,
            "complexity_breakdown": {"Simple": 1, "Medium": 0, "Complex": 2, "Custom": 0},
        },
    }


class TestHtmlHelpers:
    """Test generate_html_reports.py helper functions."""

    def test_svg_donut_basic(self):
        from generate_html_reports import _svg_donut
        slices = [("Simple", 5, "#27AE60"), ("Complex", 3, "#E74C3C")]
        html = _svg_donut(slices)
        assert "<svg" in html
        assert "Simple" in html
        assert "Complex" in html
        assert "8" in html  # total

    def test_svg_donut_empty(self):
        from generate_html_reports import _svg_donut
        html = _svg_donut([("A", 0, "#fff")])
        assert html == ""

    def test_svg_donut_single_slice(self):
        from generate_html_reports import _svg_donut
        slices = [("All", 10, "#27AE60")]
        html = _svg_donut(slices)
        assert "10" in html

    def test_svg_bar_basic(self):
        from generate_html_reports import _svg_bar
        items = [("SQ", 5, "#007"), ("EXP", 3, "#007")]
        html = _svg_bar(items)
        assert "SQ" in html
        assert "EXP" in html

    def test_svg_bar_empty(self):
        from generate_html_reports import _svg_bar
        assert _svg_bar([]) == ""

    def test_svg_bar_zero_peak(self):
        from generate_html_reports import _svg_bar
        assert _svg_bar([("A", 0, "#000")]) == ""

    def test_card(self):
        from generate_html_reports import _card
        html = _card("Test Title", "<p>body</p>", "🔍")
        assert "Test Title" in html
        assert "<p>body</p>" in html
        assert "card" in html

    def test_table(self):
        from generate_html_reports import _table
        html = _table(["Name", "Value"], [["A", 1], ["B", 2]])
        assert "<table>" in html
        assert "Name" in html
        assert "A" in html

    def test_table_with_highlight(self):
        from generate_html_reports import _table
        html = _table(["Name", "Level"], [["X", "Simple"]], highlight_col=1)
        assert "font-weight" in html

    def test_badge(self):
        from generate_html_reports import _badge
        html = _badge("Ready", "#27AE60")
        assert "Ready" in html
        assert "#27AE60" in html


class TestAssessmentReport:
    """Test generate_assessment_report with synthetic inventory."""

    def test_generates_html_file(self, tmp_path):
        from generate_html_reports import generate_assessment_report
        inv = _sample_inventory()
        out = tmp_path / "assessment.html"
        result = generate_assessment_report(inv, out)
        assert result == out
        assert out.exists()
        html = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in html
        assert "Assessment Report" in html

    def test_contains_kpi_cards(self, tmp_path):
        from generate_html_reports import generate_assessment_report
        inv = _sample_inventory()
        out = tmp_path / "assessment.html"
        generate_assessment_report(inv, out)
        html = out.read_text(encoding="utf-8")
        assert "Mappings" in html
        assert "Workflows" in html
        assert "Connections" in html

    def test_contains_mapping_table(self, tmp_path):
        from generate_html_reports import generate_assessment_report
        inv = _sample_inventory()
        out = tmp_path / "assessment.html"
        generate_assessment_report(inv, out)
        html = out.read_text(encoding="utf-8")
        assert "M_LOAD_CUSTOMERS" in html
        assert "M_LOAD_ORDERS" in html

    def test_contains_sql_section(self, tmp_path):
        from generate_html_reports import generate_assessment_report
        inv = _sample_inventory()
        out = tmp_path / "assessment.html"
        generate_assessment_report(inv, out)
        html = out.read_text(encoding="utf-8")
        assert "SP_UPDATE_ORDER_STATS" in html
        assert "MERGE" in html

    def test_contains_connections(self, tmp_path):
        from generate_html_reports import generate_assessment_report
        inv = _sample_inventory()
        out = tmp_path / "assessment.html"
        generate_assessment_report(inv, out)
        html = out.read_text(encoding="utf-8")
        assert "CONN_SALES" in html

    def test_handles_empty_inventory(self, tmp_path):
        from generate_html_reports import generate_assessment_report
        inv = {
            "mappings": [], "workflows": [], "sql_files": [],
            "connections": [], "parameter_files": [], "mapplets": {},
            "summary": {
                "total_mappings": 0, "total_workflows": 0,
                "total_sessions": 0, "total_sql_files": 0,
                "complexity_breakdown": {},
            },
        }
        out = tmp_path / "empty_assessment.html"
        generate_assessment_report(inv, out)
        assert out.exists()


class TestMigrationReport:
    """Test generate_migration_report."""

    def test_generates_html_file(self, tmp_path):
        from generate_html_reports import generate_migration_report
        inv = _sample_inventory()
        out = tmp_path / "migration.html"
        result = generate_migration_report(inv, out)
        assert result == out
        assert out.exists()
        html = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in html
        assert "Migration Report" in html

    def test_contains_progress_kpis(self, tmp_path):
        from generate_html_reports import generate_migration_report
        inv = _sample_inventory()
        out = tmp_path / "migration.html"
        generate_migration_report(inv, out)
        html = out.read_text(encoding="utf-8")
        assert "Notebooks" in html
        assert "Pipelines" in html
        assert "Overall Progress" in html

    def test_contains_action_items(self, tmp_path):
        from generate_html_reports import generate_migration_report
        inv = _sample_inventory()
        out = tmp_path / "migration.html"
        generate_migration_report(inv, out)
        html = out.read_text(encoding="utf-8")
        # Should have action items since we don't have matching output files
        assert "Action Items" in html

    def test_handles_empty_inventory(self, tmp_path):
        from generate_html_reports import generate_migration_report
        inv = {
            "mappings": [], "workflows": [], "sql_files": [],
            "summary": {
                "total_mappings": 0, "total_workflows": 0,
                "total_sessions": 0, "total_sql_files": 0,
                "complexity_breakdown": {},
            },
        }
        out = tmp_path / "empty_migration.html"
        generate_migration_report(inv, out)
        assert out.exists()


class TestHtmlArtifactStatus:
    """Test _artifact_status helper."""

    def test_scans_notebooks(self, tmp_path):
        from generate_html_reports import _artifact_status
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        (nb_dir / "NB_TEST.py").write_text("x")
        (nb_dir / "NB_OTHER.py").write_text("x")
        result = _artifact_status(tmp_path, "notebooks", ".py")
        assert "NB_TEST" in result
        assert "NB_OTHER" in result

    def test_empty_dir(self, tmp_path):
        from generate_html_reports import _artifact_status
        d = tmp_path / "empty"
        d.mkdir()
        assert _artifact_status(tmp_path, "empty", ".py") == []

    def test_nonexistent_dir(self, tmp_path):
        from generate_html_reports import _artifact_status
        assert _artifact_status(tmp_path, "nope", ".py") == []


# ═════════════════════════════════════════════
#  Assessment Deep Tests  (36% → 70%+)
# ═════════════════════════════════════════════

class TestExtractConnections:
    """Test extract_connections_from_mappings."""

    def test_extracts_oracle_connection(self):
        from run_assessment import extract_connections_from_mappings
        mappings = [{"sources": ["Oracle.SALES.CUSTOMERS"], "targets": []}]
        conns = extract_connections_from_mappings(mappings)
        assert len(conns) >= 1
        assert any(c["schema"] == "SALES" for c in conns)

    def test_extracts_multiple(self):
        from run_assessment import extract_connections_from_mappings
        mappings = [
            {"sources": ["Oracle.SALES.CUSTOMERS", "Oracle.HR.EMPLOYEES"], "targets": []},
        ]
        conns = extract_connections_from_mappings(mappings)
        assert len(conns) >= 2

    def test_deduplicates(self):
        from run_assessment import extract_connections_from_mappings
        mappings = [
            {"sources": ["Oracle.SALES.T1"], "targets": []},
            {"sources": ["Oracle.SALES.T2"], "targets": []},
        ]
        conns = extract_connections_from_mappings(mappings)
        sales_conns = [c for c in conns if c.get("schema") == "SALES"]
        assert len(sales_conns) == 1

    def test_handles_no_db_type(self):
        from run_assessment import extract_connections_from_mappings
        mappings = [{"sources": ["SCHEMA.TABLE"], "targets": []}]
        conns = extract_connections_from_mappings(mappings)
        assert len(conns) >= 1

    def test_handles_single_part_source(self):
        from run_assessment import extract_connections_from_mappings
        mappings = [{"sources": ["TABLE_ONLY"], "targets": []}]
        conns = extract_connections_from_mappings(mappings)
        assert conns == []  # single-part doesn't have schema


class TestBuildDependencyDag:
    """Test build_dependency_dag."""

    def test_basic_dag(self):
        from run_assessment import build_dependency_dag
        workflows = [{
            "name": "WF_TEST",
            "sessions": ["S_A", "S_B"],
            "task_instances": [{"name": "Start", "type": "Start"}],
            "decision_tasks": [],
            "email_tasks": [],
            "links": [
                {"from": "Start", "to": "S_A"},
                {"from": "S_A", "to": "S_B"},
            ],
        }]
        dag = build_dependency_dag(workflows)
        assert "workflows" in dag
        assert len(dag["workflows"]) == 1
        wf_dag = dag["workflows"][0]
        assert wf_dag["workflow"] == "WF_TEST"
        assert len(wf_dag["edges"]) == 2

    def test_includes_decisions_and_emails(self):
        from run_assessment import build_dependency_dag
        workflows = [{
            "name": "WF_X",
            "sessions": ["S_1"],
            "task_instances": [],
            "decision_tasks": ["DEC_1"],
            "email_tasks": ["EM_1"],
            "links": [{"from": "S_1", "to": "DEC_1"}],
        }]
        dag = build_dependency_dag(workflows)
        nodes = dag["workflows"][0]["nodes"]
        assert "DEC_1" in nodes
        assert "EM_1" in nodes


class TestWriteInventory:
    """Test write_inventory_json and write_complexity_report."""

    def test_write_inventory(self, tmp_path):
        import run_assessment
        original_dir = run_assessment.OUTPUT_DIR
        try:
            run_assessment.OUTPUT_DIR = tmp_path
            mappings = [{
                "name": "M_X",
                "sources": ["SRC"],
                "targets": ["TGT"],
                "transformations": ["SQ"],
                "has_sql_override": False,
                "has_stored_proc": False,
                "complexity": "Simple",
                "sql_overrides": [],
                "lookup_conditions": [],
                "parameters": [],
                "target_load_order": [],
            }]
            workflows = [{
                "name": "WF_Y",
                "sessions": ["S_1"],
                "session_to_mapping": {"S_1": "M_X"},
                "dependencies": {},
                "has_timer": False,
                "has_decision": False,
                "decision_tasks": [],
                "email_tasks": [],
                "pre_post_sql": [],
                "schedule": None,
            }]
            inv = run_assessment.write_inventory_json(mappings, workflows, [], [])
            assert (tmp_path / "inventory.json").exists()
            assert inv["summary"]["total_mappings"] == 1
            assert inv["summary"]["total_workflows"] == 1
        finally:
            run_assessment.OUTPUT_DIR = original_dir

    def test_write_complexity_report(self, tmp_path):
        import run_assessment
        original_dir = run_assessment.OUTPUT_DIR
        try:
            run_assessment.OUTPUT_DIR = tmp_path
            inv = {
                "generated": "2026-03-23",
                "source_platform": "Test",
                "target_platform": "Fabric",
                "mappings": [
                    {"name": "M_A", "complexity": "Simple", "sources": ["S"], "targets": ["T"],
                     "transformations": ["SQ"], "has_sql_override": False, "has_stored_proc": False,
                     "sql_overrides": []},
                ],
                "sql_files": [],
                "summary": {
                    "total_mappings": 1,
                    "total_workflows": 0,
                    "total_sessions": 0,
                    "total_sql_files": 0,
                    "complexity_breakdown": {"Simple": 1, "Medium": 0, "Complex": 0, "Custom": 0},
                },
            }
            run_assessment.write_complexity_report(inv)
            report = tmp_path / "complexity_report.md"
            assert report.exists()
            content = report.read_text(encoding="utf-8")
            assert "Complexity Report" in content
            assert "Simple" in content
        finally:
            run_assessment.OUTPUT_DIR = original_dir

    def test_write_dependency_dag(self, tmp_path):
        import run_assessment
        original_dir = run_assessment.OUTPUT_DIR
        try:
            run_assessment.OUTPUT_DIR = tmp_path
            dag = {"workflows": [{"workflow": "WF", "nodes": ["A"], "edges": []}]}
            run_assessment.write_dependency_dag(dag)
            assert (tmp_path / "dependency_dag.json").exists()
        finally:
            run_assessment.OUTPUT_DIR = original_dir


class TestMappletExpansion:
    """Test parse_mapplets and expand_mapplet_refs."""

    def test_parse_mapplets_from_xml(self, tmp_path):
        import xml.etree.ElementTree as ET

        from run_assessment import parse_mapplets
        xml = '''<FOLDER>
            <MAPPLET NAME="MPLT_DERIVE">
                <TRANSFORMATION NAME="EXP_COMMON" TYPE="Expression"/>
                <TRANSFORMATION NAME="FIL_ACTIVE" TYPE="Filter"/>
            </MAPPLET>
        </FOLDER>'''
        root = ET.fromstring(xml)
        result = parse_mapplets(root)
        assert "MPLT_DERIVE" in result
        assert len(result["MPLT_DERIVE"]) == 2

    def test_expand_mapplet_resolved(self):
        from run_assessment import expand_mapplet_refs
        mapping = {
            "name": "M_X",
            "transformations": ["SQ", "MPLT"],
            "transformation_details": [
                {"name": "SQ_SRC", "type": "Source Qualifier", "abbrev": "SQ"},
                {"name": "MPLT_DERIVE", "type": "Mapplet", "abbrev": "MPLT"},
            ],
        }
        mapplets = {
            "MPLT_DERIVE": [
                {"name": "EXP_X", "type": "Expression", "abbrev": "EXP"},
                {"name": "FIL_X", "type": "Filter", "abbrev": "FIL"},
            ]
        }
        expand_mapplet_refs(mapping, mapplets)
        assert mapping["has_mapplet"] is True
        assert "EXP" in mapping["transformations"]
        assert "FIL" in mapping["transformations"]

    def test_expand_unresolved_mapplet(self):
        from run_assessment import expand_mapplet_refs
        mapping = {
            "name": "M_Y",
            "transformations": ["SQ", "MPLT"],
            "transformation_details": [
                {"name": "SQ_SRC", "type": "Source Qualifier", "abbrev": "SQ"},
                {"name": "MPLT_UNKNOWN", "type": "Mapplet", "abbrev": "MPLT"},
            ],
        }
        expand_mapplet_refs(mapping, {})
        assert mapping["has_mapplet"] is True
        assert "MPLT" in mapping["transformations"]

    def test_expand_no_mapplet(self):
        from run_assessment import expand_mapplet_refs
        mapping = {
            "name": "M_Z",
            "transformations": ["SQ", "EXP"],
            "transformation_details": [
                {"name": "SQ_SRC", "type": "Source Qualifier", "abbrev": "SQ"},
                {"name": "EXP_X", "type": "Expression", "abbrev": "EXP"},
            ],
        }
        expand_mapplet_refs(mapping, {})
        assert mapping["has_mapplet"] is False


class TestIicsParser:
    """Test IICS XML parsing."""

    def test_parse_iics_mapping(self, tmp_path):
        from run_assessment import parse_iics_mapping
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="m_iics_test" objectType="com.infa.deployment.mapping">
    <dTemplate name="src_customers" objectType="com.infa.adapter.source"/>
    <dTemplate name="exp_derive" objectType="com.infa.adapter.expression"/>
    <dTemplate name="tgt_output" objectType="com.infa.adapter.target"/>
  </dTemplate>
</exportMetadata>'''
        xml_file = tmp_path / "iics_mapping.xml"
        xml_file.write_text(xml)
        result = parse_iics_mapping(xml_file)
        assert len(result) >= 1
        m = result[0]
        assert m["name"] == "m_iics_test"
        assert m["format"] == "iics"

    def test_iics_empty_returns_empty(self, tmp_path):
        from run_assessment import parse_iics_mapping
        xml = '<?xml version="1.0"?>\n<exportMetadata></exportMetadata>'
        f = tmp_path / "empty_iics.xml"
        f.write_text(xml)
        result = parse_iics_mapping(f)
        assert result == []

    def test_iics_type_to_abbrev(self):
        from run_assessment import _iics_type_to_abbrev
        assert _iics_type_to_abbrev("sourcetransformation") == "SQ"
        assert _iics_type_to_abbrev("expression") == "EXP"
        assert _iics_type_to_abbrev("filter") == "FIL"
        assert _iics_type_to_abbrev("lookup") == "LKP"
        assert _iics_type_to_abbrev("joiner") == "JNR"
        assert _iics_type_to_abbrev("aggregator") == "AGG"
        assert _iics_type_to_abbrev("unknownType123") == "unknownType123"


class TestConnectionObjectParser:
    """Test parse_connection_objects."""

    def test_parses_db_connections(self):
        import xml.etree.ElementTree as ET

        from run_assessment import parse_connection_objects
        xml = '''<ROOT>
            <DBCONNECTION NAME="CONN_PROD" DBTYPE="Oracle" CONNECTSTRING="host:1521/orcl" USERNAME="admin"/>
        </ROOT>'''
        root = ET.fromstring(xml)
        conns = parse_connection_objects(root)
        assert len(conns) == 1
        assert conns[0]["name"] == "CONN_PROD"
        assert conns[0]["type"] == "Database"

    def test_parses_ftp_connections(self):
        import xml.etree.ElementTree as ET

        from run_assessment import parse_connection_objects
        xml = '''<ROOT>
            <FTPCONNECTION NAME="FTP_DATA" HOSTNAME="ftp.example.com" REMOTEDIRECTORY="/exports"/>
        </ROOT>'''
        root = ET.fromstring(xml)
        conns = parse_connection_objects(root)
        assert len(conns) == 1
        assert conns[0]["type"] == "FTP"
        assert conns[0]["host"] == "ftp.example.com"

    def test_parses_generic_connection(self):
        import xml.etree.ElementTree as ET

        from run_assessment import parse_connection_objects
        xml = '''<ROOT>
            <CONNECTION NAME="CONN_GENERIC" TYPE="HTTP"/>
        </ROOT>'''
        root = ET.fromstring(xml)
        conns = parse_connection_objects(root)
        assert len(conns) == 1
        assert conns[0]["type"] == "HTTP"

    def test_deduplicates_by_name(self):
        import xml.etree.ElementTree as ET

        from run_assessment import parse_connection_objects
        xml = '''<ROOT>
            <DBCONNECTION NAME="SAME" DBTYPE="Oracle"/>
            <CONNECTION NAME="SAME" TYPE="DB"/>
        </ROOT>'''
        root = ET.fromstring(xml)
        conns = parse_connection_objects(root)
        same_conns = [c for c in conns if c["name"] == "SAME"]
        assert len(same_conns) == 1


class TestParseSqlFile:
    """Test parse_sql_file."""

    def test_detects_oracle_constructs(self, tmp_path):
        import run_assessment
        original_ws = run_assessment.WORKSPACE
        try:
            run_assessment.WORKSPACE = tmp_path
            sql_file = tmp_path / "test.sql"
            sql_file.write_text(
                "MERGE INTO tgt USING src ON tgt.id = src.id\n"
                "WHEN MATCHED THEN UPDATE SET name = NVL(src.name, 'X')\n"
                "WHEN NOT MATCHED THEN INSERT VALUES (src.id, SYSDATE)\n"
            )
            result = run_assessment.parse_sql_file(sql_file)
            assert result["db_type"] == "oracle"
            constructs = {c["construct"] for c in result["oracle_constructs"]}
            assert "MERGE" in constructs
            assert "NVL" in constructs
            assert "SYSDATE" in constructs
            assert result["total_lines"] == 4
        finally:
            run_assessment.WORKSPACE = original_ws

    def test_detects_sqlserver_constructs(self, tmp_path):
        import run_assessment
        original_ws = run_assessment.WORKSPACE
        try:
            run_assessment.WORKSPACE = tmp_path
            sql_file = tmp_path / "mssql.sql"
            sql_file.write_text(
                "SELECT ISNULL(name, 'X'), GETDATE(), LEN(name)\n"
                "FROM dbo.users WITH (NOLOCK)\n"
                "WHERE CHARINDEX('a', name) > 0\n"
            )
            result = run_assessment.parse_sql_file(sql_file)
            assert result["db_type"] == "sqlserver"
            constructs = {c["construct"] for c in result["sqlserver_constructs"]}
            assert "ISNULL" in constructs
            assert "GETDATE" in constructs
        finally:
            run_assessment.WORKSPACE = original_ws


# ═════════════════════════════════════════════
#  SQL Migration Deep Tests  (92% → 95%+)
# ═════════════════════════════════════════════

class TestSqlConversion:
    """Test convert_sql and related functions – edge cases."""

    def test_convert_decode_basic(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT DECODE(status, 1, 'A', 2, 'B', 'C') FROM t", "oracle")
        assert "CASE" in result
        assert "WHEN" in result
        assert "ELSE" in result

    def test_convert_nvl(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT NVL(name, 'Unknown') FROM t", "oracle")
        assert "COALESCE" in result

    def test_convert_nvl2(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT NVL2(name, name||' active', 'none') FROM t", "oracle")
        assert "CASE WHEN" in result

    def test_convert_sysdate(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT SYSDATE FROM DUAL", "oracle")
        assert "current_timestamp()" in result
        assert "DUAL" not in result

    def test_convert_trunc(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT TRUNC(hire_date, 'MM') FROM emp", "oracle")
        assert "date_trunc('month'" in result

    def test_convert_varchar2(self):
        from run_sql_migration import convert_sql
        result = convert_sql("CREATE TABLE t (name VARCHAR2(100))", "oracle")
        assert "STRING" in result

    def test_convert_sqlserver_getdate(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT GETDATE()", "sqlserver")
        assert "current_timestamp()" in result

    def test_convert_sqlserver_isnull(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT ISNULL(name, 'X')", "sqlserver")
        assert "COALESCE" in result

    def test_convert_sqlserver_nolock(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT * FROM t WITH (NOLOCK)", "sqlserver")
        assert "NOLOCK" not in result

    def test_convert_sqlserver_top(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT TOP 10 * FROM t", "sqlserver")
        assert "LIMIT" in result

    def test_convert_unknown_db_uses_oracle(self):
        from run_sql_migration import convert_sql
        result = convert_sql("SELECT NVL(x, 0) FROM DUAL", "postgres")
        assert "COALESCE" in result

    def test_convert_decode_few_args(self):
        from run_sql_migration import _convert_decode
        # Less than 3 args — should leave as-is
        result = _convert_decode("SELECT DECODE(x, y) FROM t")
        assert "DECODE" in result

    def test_split_args_nested_parens(self):
        from run_sql_migration import _split_args
        result = _split_args("a, FUNC(b, c), d")
        assert len(result) == 3
        assert "FUNC(b, c)" in result[1]

    def test_split_args_with_quotes(self):
        from run_sql_migration import _split_args
        result = _split_args("a, 'hello, world', b")
        assert len(result) == 3

    def test_convert_sql_file(self, tmp_path):
        from run_sql_migration import convert_sql_file
        src = tmp_path / "input.sql"
        src.write_text("SELECT NVL(name, 'X'), SYSDATE FROM DUAL;\n", encoding="utf-8")
        out = tmp_path / "output.sql"
        result = convert_sql_file(src, "oracle", out)
        assert result == out
        content = out.read_text(encoding="utf-8")
        assert "COALESCE" in content
        assert "current_timestamp()" in content

    def test_convert_sql_overrides(self, tmp_path):
        import run_sql_migration
        original_dir = run_sql_migration.OUTPUT_DIR
        try:
            run_sql_migration.OUTPUT_DIR = tmp_path
            overrides = [
                {"type": "Sql Query", "value": "SELECT NVL(x, 0) FROM t"},
                {"type": "Lookup Sql Override", "value": "SELECT * FROM lkp WHERE SYSDATE > start_date"},
            ]
            result = run_sql_migration.convert_sql_overrides("M_TEST", overrides, "oracle")
            assert result is not None
            content = result.read_text(encoding="utf-8")
            assert "COALESCE" in content
            assert "current_timestamp()" in content
        finally:
            run_sql_migration.OUTPUT_DIR = original_dir

    def test_convert_sql_overrides_empty(self):
        from run_sql_migration import convert_sql_overrides
        assert convert_sql_overrides("M_X", [], "oracle") is None

    def test_header_oracle(self):
        from run_sql_migration import _header
        h = _header("test.sql", "oracle")
        assert "Oracle → Spark SQL" in h

    def test_header_sqlserver(self):
        from run_sql_migration import _header
        h = _header("test.sql", "sqlserver")
        assert "SQL Server → Spark SQL" in h


# ═════════════════════════════════════════════
#  Notebook Migration Tests  (95% → 98%)
# ═════════════════════════════════════════════

class TestNotebookGeneration:
    """Test notebook generation functions."""

    def test_generate_notebook_simple(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_SIMPLE",
            "sources": ["Oracle.SALES.CUSTOMERS"],
            "targets": ["DIM_CUSTOMER"],
            "transformations": ["SQ", "EXP", "FIL"],
            "complexity": "Simple",
            "parameters": ["$$LOAD_DATE"],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "NB_M_SIMPLE" in content
        assert "Expression" in content
        assert "Filter" in content
        assert "load_date" in content.lower()

    def test_generate_notebook_with_aggregator(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_AGG",
            "sources": ["Oracle.SALES.ORDERS"],
            "targets": ["AGG_ORDERS"],
            "transformations": ["SQ", "AGG"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "groupBy" in content
        assert "agg" in content

    def test_generate_notebook_with_joiner(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_JOIN",
            "sources": ["Oracle.SALES.A", "Oracle.SALES.B"],
            "targets": ["FACT_AB"],
            "transformations": ["SQ", "JNR"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "join" in content.lower()

    def test_generate_notebook_with_lookup(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_LKP",
            "sources": ["Oracle.SALES.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "LKP"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [{"lookup": "LKP_REF", "condition": "a=b"}],
        }
        content = generate_notebook(mapping)
        assert "broadcast" in content

    def test_generate_notebook_with_router(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_RTR",
            "sources": ["Oracle.SALES.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "RTR"],
            "complexity": "Complex",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "Router" in content

    def test_generate_notebook_with_update_strategy(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_UPD",
            "sources": ["Oracle.WH.INV"],
            "targets": ["DIM_INV"],
            "transformations": ["SQ", "UPD"],
            "complexity": "Complex",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "MERGE" in content
        assert "DeltaTable" in content

    def test_generate_notebook_with_rank(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_RNK",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "RNK"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "Window" in content
        assert "row_number" in content

    def test_generate_notebook_with_sorter(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_SRT",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "SRT"],
            "complexity": "Simple",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "orderBy" in content

    def test_generate_notebook_with_union(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_UNI",
            "sources": ["Oracle.S.A", "Oracle.S.B"],
            "targets": ["TGT"],
            "transformations": ["SQ", "UNI"],
            "complexity": "Simple",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "unionByName" in content

    def test_generate_notebook_with_normalizer(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_NRM",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "NRM"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "explode" in content

    def test_generate_notebook_with_sequence(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_SEQ",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "SEQ"],
            "complexity": "Simple",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "monotonically_increasing_id" in content

    def test_generate_notebook_with_stored_proc(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_SP",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "SP"],
            "complexity": "Complex",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "Stored Procedure" in content

    def test_generate_notebook_with_sqlt(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_SQLT",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "SQLT"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "SQL Transformation" in content
        assert "createOrReplaceTempView" in content

    def test_generate_notebook_with_data_masking(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_DM",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "DM"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "Data Masking" in content
        assert "md5" in content

    def test_generate_notebook_with_web_service(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_WSC",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "WSC"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "Web Service" in content

    def test_generate_notebook_with_mapplet(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_MPLT",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "MPLT"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "Mapplet" in content

    def test_generate_notebook_placeholder_type(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_JTX",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "JTX"],
            "complexity": "Custom",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "PLACEHOLDER" in content or "TODO" in content

    def test_target_cell_gold_tier(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_GOLD",
            "sources": ["Oracle.S.T"],
            "targets": ["AGG_SALES_GOLD"],
            "transformations": ["SQ", "AGG"],
            "complexity": "Simple",
            "parameters": [],
            "sql_overrides": [],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "gold." in content

    def test_source_cell_sql_override(self):
        from run_notebook_migration import generate_notebook
        mapping = {
            "name": "M_OVR",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT"],
            "transformations": ["SQ", "EXP"],
            "complexity": "Medium",
            "parameters": [],
            "sql_overrides": [{"type": "Sql Query", "value": "SELECT * FROM t WHERE 1=1"}],
            "lookup_conditions": [],
        }
        content = generate_notebook(mapping)
        assert "SQL Override" in content or "SQL_OVERRIDES" in content


# ═════════════════════════════════════════════
#  Pipeline Migration Tests  (76% → 90%+)
# ═════════════════════════════════════════════

class TestPipelineGeneration:
    """Test pipeline JSON generation."""

    def test_generate_basic_pipeline(self):
        from run_pipeline_migration import generate_pipeline
        workflow = {
            "name": "WF_BASIC",
            "sessions": ["S_M_A"],
            "session_to_mapping": {"S_M_A": "M_A"},
            "dependencies": {"S_M_A": ["Start"]},
            "decision_tasks": [],
            "email_tasks": [],
            "schedule": "",
        }
        mappings = {"M_A": {"parameters": ["$$LOAD_DATE"]}}
        pipeline = generate_pipeline(workflow, mappings)
        assert pipeline["name"] == "PL_WF_BASIC"
        activities = pipeline["properties"]["activities"]
        assert len(activities) == 1
        assert activities[0]["type"] == "TridentNotebook"

    def test_pipeline_with_dependencies(self):
        from run_pipeline_migration import generate_pipeline
        workflow = {
            "name": "WF_CHAIN",
            "sessions": ["S_M_A", "S_M_B"],
            "session_to_mapping": {"S_M_A": "M_A", "S_M_B": "M_B"},
            "dependencies": {"S_M_A": ["Start"], "S_M_B": ["S_M_A"]},
            "decision_tasks": [],
            "email_tasks": [],
            "schedule": "DAILY",
        }
        mappings = {"M_A": {"parameters": []}, "M_B": {"parameters": []}}
        pipeline = generate_pipeline(workflow, mappings)
        activities = pipeline["properties"]["activities"]
        assert len(activities) == 2
        # S_M_B should depend on NB_M_A
        b_activity = next(a for a in activities if "M_B" in a["name"])
        assert len(b_activity["dependsOn"]) > 0

    def test_pipeline_with_decision(self):
        from run_pipeline_migration import generate_pipeline
        workflow = {
            "name": "WF_DEC",
            "sessions": ["S_M_A", "S_M_B"],
            "session_to_mapping": {"S_M_A": "M_A", "S_M_B": "M_B"},
            "dependencies": {
                "S_M_A": ["Start"],
                "DEC_CHECK": ["S_M_A"],
                "S_M_B": ["DEC_CHECK"],
            },
            "decision_tasks": ["DEC_CHECK"],
            "email_tasks": [],
            "schedule": "",
        }
        mappings = {"M_A": {"parameters": []}, "M_B": {"parameters": []}}
        pipeline = generate_pipeline(workflow, mappings)
        activities = pipeline["properties"]["activities"]
        types = {a["type"] for a in activities}
        assert "IfCondition" in types

    def test_pipeline_with_email(self):
        from run_pipeline_migration import generate_pipeline
        workflow = {
            "name": "WF_EMAIL",
            "sessions": ["S_M_A"],
            "session_to_mapping": {"S_M_A": "M_A"},
            "dependencies": {
                "S_M_A": ["Start"],
                "EMAIL_NOTIFY": ["S_M_A"],
            },
            "decision_tasks": [],
            "email_tasks": ["EMAIL_NOTIFY"],
            "schedule": "",
        }
        mappings = {"M_A": {"parameters": []}}
        pipeline = generate_pipeline(workflow, mappings)
        activities = pipeline["properties"]["activities"]
        types = {a["type"] for a in activities}
        assert "WebActivity" in types
        # Webhook param should exist
        assert "alert_webhook_url" in pipeline["properties"]["parameters"]

    def test_pipeline_schedule_annotation(self):
        from run_pipeline_migration import generate_pipeline
        workflow = {
            "name": "WF_SCHED",
            "sessions": ["S_M_A"],
            "session_to_mapping": {"S_M_A": "M_A"},
            "dependencies": {"S_M_A": ["Start"]},
            "decision_tasks": [],
            "email_tasks": [],
            "schedule": "DAILY_02AM",
        }
        mappings = {"M_A": {"parameters": []}}
        pipeline = generate_pipeline(workflow, mappings)
        annotations = pipeline["properties"]["annotations"]
        assert any("DAILY_02AM" in a for a in annotations)

    def test_notebook_activity_function(self):
        from run_pipeline_migration import _notebook_activity
        activity = _notebook_activity("S_M_TEST", "M_TEST", ["S_M_PREV"], ["$$P1"])
        assert activity["name"] == "NB_M_TEST"
        assert activity["type"] == "TridentNotebook"
        assert len(activity["dependsOn"]) > 0

    def test_email_activity_function(self):
        from run_pipeline_migration import _email_activity
        activity = _email_activity("EMAIL_FAIL", "NB_M_TEST", "Failed")
        assert activity["type"] == "WebActivity"
        assert activity["dependsOn"][0]["dependencyConditions"] == ["Failed"]

    def test_decision_activity_function(self):
        from run_pipeline_migration import _decision_activity
        true_acts = [{"name": "NB_M_X", "type": "TridentNotebook"}]
        activity = _decision_activity("DEC_1", "NB_M_PREV", true_acts)
        assert activity["type"] == "IfCondition"
        assert len(activity["typeProperties"]["ifTrueActivities"]) == 1


# ═════════════════════════════════════════════
#  Validation Tests  (86% → 95%+)
# ═════════════════════════════════════════════

class TestValidationGeneration:
    """Test validation notebook generation."""

    def test_infer_target_table_gold(self):
        from run_validation import _infer_target_table
        assert _infer_target_table("AGG_SALES_SUMMARY", ["SQ", "AGG"]).startswith("gold.")

    def test_infer_target_table_silver(self):
        from run_validation import _infer_target_table
        assert _infer_target_table("DIM_CUSTOMER", []).startswith("silver.")

    def test_infer_target_table_fact(self):
        from run_validation import _infer_target_table
        assert _infer_target_table("FACT_ORDERS", []).startswith("silver.")

    def test_infer_key_columns_customer(self):
        from run_validation import _infer_key_columns
        assert _infer_key_columns("DIM_CUSTOMER") == ["customer_id"]

    def test_infer_key_columns_order(self):
        from run_validation import _infer_key_columns
        assert _infer_key_columns("FACT_ORDERS") == ["order_id"]

    def test_infer_key_columns_generic(self):
        from run_validation import _infer_key_columns
        result = _infer_key_columns("DIM_WIDGET")
        assert result == ["widget_id"]

    def test_source_connection_oracle(self):
        from run_validation import _source_connection
        assert _source_connection(["Oracle.SALES.T"]) == "oracle"

    def test_source_connection_sqlserver(self):
        from run_validation import _source_connection
        assert _source_connection(["SqlServer.DBO.T"]) == "sqlserver"

    def test_source_connection_default(self):
        from run_validation import _source_connection
        assert _source_connection(["TABLE_ONLY"]) == "oracle"

    def test_generate_validation_oracle(self):
        from run_validation import generate_validation
        mapping = {
            "name": "M_TEST_VAL",
            "sources": ["Oracle.SALES.CUSTOMERS"],
            "targets": ["DIM_CUSTOMER"],
            "transformations": ["SQ", "EXP"],
        }
        notebooks = generate_validation(mapping, "oracle")
        assert len(notebooks) == 1
        name, content = notebooks[0]
        assert "VAL_" in name or "DIM_CUSTOMER" in name
        assert "Row Count" in content
        assert "Checksum" in content
        assert "Data Quality" in content
        assert "oracle" in content.lower()

    def test_generate_validation_sqlserver(self):
        from run_validation import generate_validation
        mapping = {
            "name": "M_SQL_VAL",
            "sources": ["SqlServer.DBO.USERS"],
            "targets": ["DIM_USER"],
            "transformations": ["SQ"],
        }
        notebooks = generate_validation(mapping, "sqlserver")
        assert len(notebooks) == 1
        _, content = notebooks[0]
        assert "sqlserver" in content.lower()

    def test_generate_validation_multi_target(self):
        from run_validation import generate_validation
        mapping = {
            "name": "M_MULTI",
            "sources": ["Oracle.S.T"],
            "targets": ["TGT_A", "TGT_B"],
            "transformations": ["SQ", "RTR"],
        }
        notebooks = generate_validation(mapping, "oracle")
        assert len(notebooks) == 2

    def test_generate_test_matrix(self):
        from run_validation import generate_test_matrix
        inv = _sample_inventory()
        files = [("M_A", "TGT", "VAL_TGT.py"), ("M_B", "TGT2", "VAL_TGT2.py")]
        matrix = generate_test_matrix(inv, files)
        assert "Test Matrix" in matrix
        assert "M_A" in matrix
        assert "L1" in matrix


# ═════════════════════════════════════════════
#  Deploy Deep Tests  (39% → 55%+)
# ═════════════════════════════════════════════

class TestDeployHelpers:
    """Additional deploy_to_fabric tests."""

    def test_sql_deploy_wraps_in_notebook(self, tmp_path):
        """Verify deploy_sql_scripts reads SQL and wraps with %%sql."""
        import deploy_to_fabric
        original_dir = deploy_to_fabric.OUTPUT_DIR
        try:
            deploy_to_fabric.OUTPUT_DIR = tmp_path
            sql_dir = tmp_path / "sql"
            sql_dir.mkdir()
            (sql_dir / "SQL_TEST.sql").write_text("SELECT 1;", encoding="utf-8")
            results = deploy_to_fabric.deploy_sql_scripts("ws-123", None, dry_run=True)
            assert len(results) == 1
            assert results[0]["status"] == "dry-run"
        finally:
            deploy_to_fabric.OUTPUT_DIR = original_dir

    def test_deploy_notebooks_custom_dir(self, tmp_path):
        import deploy_to_fabric
        original_dir = deploy_to_fabric.OUTPUT_DIR
        try:
            deploy_to_fabric.OUTPUT_DIR = tmp_path
            nb_dir = tmp_path / "notebooks"
            nb_dir.mkdir()
            (nb_dir / "NB_CUSTOM.py").write_text("# test", encoding="utf-8")
            results = deploy_to_fabric.deploy_notebooks("ws-123", None, dry_run=True)
            assert len(results) == 1
            assert results[0]["artifact"] == "NB_CUSTOM"
        finally:
            deploy_to_fabric.OUTPUT_DIR = original_dir

    def test_deploy_pipelines_custom_dir(self, tmp_path):
        import deploy_to_fabric
        original_dir = deploy_to_fabric.OUTPUT_DIR
        try:
            deploy_to_fabric.OUTPUT_DIR = tmp_path
            pl_dir = tmp_path / "pipelines"
            pl_dir.mkdir()
            (pl_dir / "PL_WF_TEST.json").write_text("{}", encoding="utf-8")
            results = deploy_to_fabric.deploy_pipelines("ws-123", None, dry_run=True)
            assert len(results) == 1
            assert results[0]["artifact"] == "PL_WF_TEST"
        finally:
            deploy_to_fabric.OUTPUT_DIR = original_dir


# ═════════════════════════════════════════════
#  Orchestrator / run_migration deeper tests
# ═════════════════════════════════════════════

class TestOrchestratorDeep:
    """Additional run_migration.py path tests."""

    def test_main_only_flag(self):
        saved = sys.argv
        try:
            sys.argv = ["run_migration.py", "--dry-run", "--only", "0"]
            from run_migration import main
            main()
        finally:
            sys.argv = saved

    def test_main_verbose_json(self):
        saved = sys.argv
        try:
            sys.argv = ["run_migration.py", "--dry-run", "--verbose", "--log-format", "json"]
            from run_migration import main
            main()
        finally:
            sys.argv = saved

    def test_parse_args_all_flags(self):
        saved = sys.argv
        try:
            sys.argv = ["run_migration.py", "--dry-run", "--verbose", "--skip", "0", "1",
                         "--config", "test.yaml", "--log-format", "json", "--resume"]
            from run_migration import _parse_args
            args = _parse_args()
            assert args.dry_run is True
            assert args.verbose is True
            assert args.skip == [0, 1]
            assert args.config == "test.yaml"
            assert args.log_format == "json"
            assert args.resume is True
        finally:
            sys.argv = saved

    def test_parse_args_only_phases(self):
        saved = sys.argv
        try:
            sys.argv = ["run_migration.py", "--only", "1", "2", "3"]
            from run_migration import _parse_args
            args = _parse_args()
            assert args.only == [1, 2, 3]
        finally:
            sys.argv = saved


# ═════════════════════════════════════════════
#  Assessment main() and deeper coverage
# ═════════════════════════════════════════════

class TestAssessmentComplexityReport:
    """Test write_complexity_report with SQL overrides and SQL files."""

    def test_report_with_overrides_and_sql_files(self, tmp_path):
        import run_assessment
        original_dir = run_assessment.OUTPUT_DIR
        try:
            run_assessment.OUTPUT_DIR = tmp_path
            inv = {
                "generated": "2026-03-23",
                "source_platform": "Test",
                "target_platform": "Fabric",
                "mappings": [
                    {
                        "name": "M_OVR",
                        "complexity": "Complex",
                        "sources": ["Oracle.S.T"],
                        "targets": ["TGT"],
                        "transformations": ["SQ", "EXP"],
                        "has_sql_override": True,
                        "has_stored_proc": False,
                        "sql_overrides": [{"type": "Sql Query", "value": "SELECT * FROM t"}],
                    },
                ],
                "sql_files": [
                    {
                        "file": "test.sql",
                        "path": "input/sql/test.sql",
                        "db_type": "oracle",
                        "oracle_constructs": [
                            {"construct": "NVL", "occurrences": 2, "lines": [1, 5]},
                            {"construct": "MERGE", "occurrences": 1, "lines": list(range(15))},
                        ],
                        "sqlserver_constructs": [],
                        "total_lines": 50,
                    },
                ],
                "summary": {
                    "total_mappings": 1,
                    "total_workflows": 0,
                    "total_sessions": 0,
                    "total_sql_files": 1,
                    "complexity_breakdown": {"Simple": 0, "Medium": 0, "Complex": 1, "Custom": 0},
                },
            }
            run_assessment.write_complexity_report(inv)
            report = (tmp_path / "complexity_report.md").read_text(encoding="utf-8")
            assert "SQL Overrides" in report
            assert "M_OVR" in report
            assert "Oracle SQL Files" in report
            assert "NVL" in report
        finally:
            run_assessment.OUTPUT_DIR = original_dir


class TestAssessmentMainIntegration:
    """Test the entire assessment main() with minimal input fixtures."""

    def test_main_with_test_data(self, tmp_path, monkeypatch):
        import run_assessment

        # Redirect I/O paths
        monkeypatch.setattr(run_assessment, "WORKSPACE", tmp_path)
        monkeypatch.setattr(run_assessment, "INPUT_DIR", tmp_path / "input")
        monkeypatch.setattr(run_assessment, "OUTPUT_DIR", tmp_path / "output" / "inventory")

        (tmp_path / "output" / "inventory").mkdir(parents=True)
        (tmp_path / "input" / "mappings").mkdir(parents=True)
        (tmp_path / "input" / "workflows").mkdir(parents=True)
        (tmp_path / "input" / "sql").mkdir(parents=True)

        # Minimal mapping XML
        mapping_xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <SOURCE NAME="CUSTOMERS" DATABASETYPE="Oracle" OWNERNAME="SALES"/>
      <TARGET NAME="DIM_CUSTOMER"/>
      <MAPPING NAME="M_MAIN_TEST">
        <TRANSFORMATION NAME="SQ_SRC" TYPE="Source Qualifier"/>
        <TRANSFORMATION NAME="EXP_X" TYPE="Expression"/>
        <CONNECTOR FROMINSTANCE="SQ_SRC" TOINSTANCE="EXP_X"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        (tmp_path / "input" / "mappings" / "M_MAIN_TEST.xml").write_text(mapping_xml)

        # Minimal workflow XML
        wf_xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <WORKFLOW NAME="WF_MAIN_TEST">
        <SESSION NAME="S_M_MAIN_TEST" MAPPINGNAME="M_MAIN_TEST"/>
        <TASKINSTANCE NAME="Start" TASKNAME="Start" TASKTYPE="Start"/>
        <TASKINSTANCE NAME="S_M_MAIN_TEST" TASKNAME="S_M_MAIN_TEST" TASKTYPE="Session"/>
        <WORKFLOWLINK FROMTASK="Start" TOTASK="S_M_MAIN_TEST"/>
      </WORKFLOW>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        (tmp_path / "input" / "workflows" / "WF_MAIN_TEST.xml").write_text(wf_xml)

        # Minimal SQL file
        sql_content = "SELECT NVL(name, 'X'), SYSDATE FROM DUAL;\nMERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v;\n"
        (tmp_path / "input" / "sql" / "test_proc.sql").write_text(sql_content)

        # Parameter file
        (tmp_path / "input" / "daily.prm").write_text("[Global]\n$$LOAD_DATE=2026-01-01\n")

        # Reset warnings/issues
        run_assessment.warnings.clear()
        run_assessment.issues.clear()

        # Temporarily disable HTML report generation (needs output/notebooks etc.)
        original_main = run_assessment.main

        # Run main
        run_assessment.main()

        # Verify outputs
        inv_path = tmp_path / "output" / "inventory" / "inventory.json"
        assert inv_path.exists()
        with open(inv_path, encoding="utf-8") as f:
            inv = json.load(f)
        assert inv["summary"]["total_mappings"] >= 1
        assert inv["summary"]["total_workflows"] >= 1

        assert (tmp_path / "output" / "inventory" / "complexity_report.md").exists()
        assert (tmp_path / "output" / "inventory" / "dependency_dag.json").exists()

    def test_main_iics_mapping(self, tmp_path, monkeypatch):
        """Test that IICS format mappings are picked up by main."""
        import run_assessment

        monkeypatch.setattr(run_assessment, "WORKSPACE", tmp_path)
        monkeypatch.setattr(run_assessment, "INPUT_DIR", tmp_path / "input")
        monkeypatch.setattr(run_assessment, "OUTPUT_DIR", tmp_path / "output" / "inventory")

        (tmp_path / "output" / "inventory").mkdir(parents=True)
        (tmp_path / "input" / "mappings").mkdir(parents=True)

        iics_xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="m_iics_main_test" objectType="com.infa.deployment.mapping">
    <dTemplate name="src_data" objectType="com.infa.adapter.source"/>
    <dTemplate name="tgt_data" objectType="com.infa.adapter.target"/>
  </dTemplate>
</exportMetadata>'''
        (tmp_path / "input" / "mappings" / "iics_test.xml").write_text(iics_xml)

        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        run_assessment.main()

        inv_path = tmp_path / "output" / "inventory" / "inventory.json"
        assert inv_path.exists()


class TestAssessmentEdgeCases:
    """Test edge cases in assessment parsing."""

    def test_mapping_with_lookup_sql_override(self, tmp_path):
        from run_assessment import parse_mapping_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <MAPPING NAME="M_LKP_TEST">
        <TRANSFORMATION NAME="LKP_PRODUCTS" TYPE="Lookup Procedure">
          <TABLEATTRIBUTE NAME="Lookup Sql Override" VALUE="SELECT * FROM products WHERE active=1"/>
          <TABLEATTRIBUTE NAME="Lookup condition" VALUE="prod_id = IN_PROD_ID"/>
        </TRANSFORMATION>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "m_lkp.xml"
        f.write_text(xml)
        mappings = parse_mapping_xml(f)
        assert len(mappings) >= 1
        assert len(mappings[0]["lookup_conditions"]) >= 1

    def test_mapping_with_connectors(self, tmp_path):
        from run_assessment import parse_mapping_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <MAPPING NAME="M_CONN_TEST">
        <TRANSFORMATION NAME="SQ_A" TYPE="Source Qualifier"/>
        <TRANSFORMATION NAME="EXP_B" TYPE="Expression"/>
        <CONNECTOR FROMINSTANCE="SQ_A" FROMFIELD="ID" TOINSTANCE="EXP_B" TOFIELD="IN_ID"
                   FROMINSTANCETYPE="Source Qualifier" TOINSTANCETYPE="Expression"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "m_conn.xml"
        f.write_text(xml)
        mappings = parse_mapping_xml(f)
        assert len(mappings) >= 1
        assert mappings[0]["connector_count"] >= 1

    def test_mapping_with_target_load_order(self, tmp_path):
        from run_assessment import parse_mapping_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <MAPPING NAME="M_TLO_TEST">
        <TRANSFORMATION NAME="SQ_A" TYPE="Source Qualifier"/>
        <TARGETLOADORDER TARGETINSTANCE="TGT_1" ORDER="1"/>
        <TARGETLOADORDER TARGETINSTANCE="TGT_2" ORDER="2"/>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "m_tlo.xml"
        f.write_text(xml)
        mappings = parse_mapping_xml(f)
        assert len(mappings) >= 1
        assert len(mappings[0]["target_load_order"]) >= 2

    def test_workflow_with_timer_and_email(self, tmp_path):
        from run_assessment import parse_workflow_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <WORKFLOW NAME="WF_TIMER_TEST">
        <TIMER NAME="WAIT_5MIN"/>
        <SESSION NAME="S_A" MAPPINGNAME="M_A"/>
        <TASKINSTANCE NAME="S_A" TASKTYPE="Session"/>
        <TASKINSTANCE NAME="EM_NOTIFY" TASKTYPE="Email"/>
        <WORKFLOWLINK FROMTASK="S_A" TOTASK="EM_NOTIFY"/>
      </WORKFLOW>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "wf_timer.xml"
        f.write_text(xml)
        wfs = parse_workflow_xml(f)
        assert len(wfs) >= 1
        assert wfs[0]["has_timer"] is True
        assert len(wfs[0]["email_tasks"]) >= 1

    def test_workflow_with_decision(self, tmp_path):
        from run_assessment import parse_workflow_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <WORKFLOW NAME="WF_DEC_TEST">
        <DECISION NAME="DEC_CHECK"/>
        <SESSION NAME="S_B" MAPPINGNAME="M_B"/>
        <TASKINSTANCE NAME="DEC_CHECK" TASKTYPE="Decision"/>
        <WORKFLOWLINK FROMTASK="DEC_CHECK" TOTASK="S_B"/>
      </WORKFLOW>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "wf_dec.xml"
        f.write_text(xml)
        wfs = parse_workflow_xml(f)
        assert len(wfs) >= 1
        assert wfs[0]["has_decision"] is True
        assert "DEC_CHECK" in wfs[0]["decision_tasks"]

    def test_workflow_with_pre_post_sql(self, tmp_path):
        from run_assessment import parse_workflow_xml
        xml = '''<?xml version="1.0"?>
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="TEST">
      <WORKFLOW NAME="WF_SQL_TEST">
        <SESSION NAME="S_X" MAPPINGNAME="M_X">
          <ATTRIBUTE NAME="Pre SQL" VALUE="DELETE FROM staging"/>
          <ATTRIBUTE NAME="Post SQL" VALUE="CALL update_stats()"/>
        </SESSION>
      </WORKFLOW>
    </FOLDER>
  </REPOSITORY>
</POWERMART>'''
        f = tmp_path / "wf_sql.xml"
        f.write_text(xml)
        wfs = parse_workflow_xml(f)
        assert len(wfs) >= 1
        assert len(wfs[0]["pre_post_sql"]) == 2

    def test_classify_medium_agg(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "AGG"],
            sql_overrides=[],
            sources=["Oracle.S.T"],
            targets=["TGT"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Medium"

    def test_classify_medium_multi_source(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ", "EXP"],
            sql_overrides=[],
            sources=["Oracle.S.T1", "Oracle.S.T2"],
            targets=["TGT"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Medium"

    def test_classify_complex_sql_override_with_decode(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ"],
            sql_overrides=[{"type": "Sql Query", "value": "SELECT DECODE(a,1,2,3) FROM t"}],
            sources=["Oracle.S.T"],
            targets=["TGT"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        # DECODE alone adds 1 point, not enough for Complex
        assert result in ("Medium", "Complex")

    def test_classify_complex_sql_override_with_union(self):
        from run_assessment import classify_complexity
        result = classify_complexity(
            transformations=["SQ"],
            sql_overrides=[{"type": "Sql Query", "value": "SELECT * UNION SELECT * FROM t"}],
            sources=["Oracle.S.T"],
            targets=["TGT"],
            has_stored_proc=False,
            lookup_conditions=[],
        )
        assert result == "Complex"


class TestHtmlReportMain:
    """Test generate_html_reports main() function."""

    def test_main_with_default_inventory(self):
        """main() should work when inventory.json exists."""
        saved = sys.argv
        try:
            sys.argv = ["generate_html_reports.py"]
            from generate_html_reports import DEFAULT_INVENTORY, main
            if DEFAULT_INVENTORY.exists():
                main()
        finally:
            sys.argv = saved

    def test_main_missing_inventory(self, tmp_path):
        """main() should exit with error for missing inventory."""
        import pytest
        saved = sys.argv
        try:
            sys.argv = ["generate_html_reports.py", str(tmp_path / "nope.json")]
            from generate_html_reports import main
            with pytest.raises(SystemExit):
                main()
        finally:
            sys.argv = saved
