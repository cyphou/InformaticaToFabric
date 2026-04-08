"""
Tests for Sprint 66 — Gap Closure & Documentation.

66.1  ULKP promoted to AUTO_CONVERTIBLE_TX
66.2  TC Transaction Control enhanced template
66.3  Event Wait/Raise XML parsing in workflow assessment
66.4  Event Wait/Raise pipeline activity generation
66.5  Standalone session config XML parser
66.6  ADR documentation files exist
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

# Add workspace root to path
WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORKSPACE))


# ═════════════════════════════════════════════════════════════════════════════
#  66.1 — ULKP Promoted to AUTO_CONVERTIBLE
# ═════════════════════════════════════════════════════════════════════════════

class TestULKPPromotion(unittest.TestCase):
    """ULKP should be in AUTO_CONVERTIBLE_TX, not PLACEHOLDER_TX."""

    def test_ulkp_in_auto_convertible(self):
        import run_assessment as ra
        self.assertIn("ULKP", ra.AUTO_CONVERTIBLE_TX)

    def test_ulkp_not_in_placeholder(self):
        import run_assessment as ra
        self.assertNotIn("ULKP", ra.PLACEHOLDER_TX)

    def test_ulkp_conversion_score_not_penalized(self):
        import run_assessment as ra
        mapping = {
            "name": "M_WITH_ULKP",
            "transformations": ["SQ", "EXP", "ULKP", "FIL"],
            "sql_overrides": [],
            "has_stored_proc": False,
            "complexity": "Medium",
        }
        score = ra.calculate_conversion_score(mapping)
        # ULKP is auto-convertible, so score should be high (>= 80)
        self.assertGreaterEqual(score, 80)

    def test_ulkp_notebook_template_has_broadcast_join(self):
        import run_notebook_migration as rnm
        mapping = {
            "name": "M_ULKP_TEST",
            "sources": ["Oracle.HR.EMP"],
            "targets": ["DIM_EMP"],
            "transformations": ["SQ", "ULKP"],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Medium",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
        }
        content = rnm.generate_notebook(mapping)
        self.assertIn("broadcast", content)
        self.assertIn("Unconnected Lookup", content)
        self.assertIn("coalesce", content)


# ═════════════════════════════════════════════════════════════════════════════
#  66.2 — TC Transaction Control Enhanced Template
# ═════════════════════════════════════════════════════════════════════════════

class TestTCEnhancedTemplate(unittest.TestCase):
    """TC template should include Delta ACID patterns and migration checklist."""

    def test_tc_template_has_checklist(self):
        import run_notebook_migration as rnm
        mapping = {
            "name": "M_TC_TEST",
            "sources": ["Oracle.HR.EMP"],
            "targets": ["DIM_EMP"],
            "transformations": ["SQ", "EXP", "TC"],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Medium",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
        }
        content = rnm.generate_notebook(mapping)
        self.assertIn("Migration checklist", content)
        self.assertIn("TC_COMMIT_BEFORE", content)
        self.assertIn("TC_COMMIT_AFTER", content)
        self.assertIn("TC_ROLLBACK", content)

    def test_tc_template_has_multiple_patterns(self):
        import run_notebook_migration as rnm
        mapping = {
            "name": "M_TC_PATTERNS",
            "sources": ["Oracle.DW.FACT"],
            "targets": ["SILVER_FACT"],
            "transformations": ["SQ", "TC"],
            "has_sql_override": False,
            "has_stored_proc": False,
            "complexity": "Simple",
            "sql_overrides": [],
            "lookup_conditions": [],
            "parameters": [],
            "target_load_order": [],
        }
        content = rnm.generate_notebook(mapping)
        self.assertIn("Pattern A", content)
        self.assertIn("Pattern B", content)
        self.assertIn("Pattern C", content)
        self.assertIn("DeltaTable", content)


# ═════════════════════════════════════════════════════════════════════════════
#  66.3 — Event Wait/Raise XML Parsing
# ═════════════════════════════════════════════════════════════════════════════

class TestEventWaitRaiseParsing(unittest.TestCase):
    """Workflow parser should detect Event Wait and Event Raise tasks."""

    def _make_workflow_xml(self, tmpdir, extra_xml=""):
        """Create a test workflow XML with optional extra elements."""
        xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
<REPOSITORY NAME="TEST" VERSION="186" DATABASETYPE="Oracle">
<FOLDER NAME="TEST_FOLDER">
<WORKFLOW NAME="WF_EVENT_TEST">
    <SESSION NAME="S_LOAD_DATA" MAPPINGNAME="M_LOAD"/>
    <TASKINSTANCE NAME="S_LOAD_DATA" TASKTYPE="Session"/>
    <TASKINSTANCE NAME="EW_WAIT_FOR_FILE" TASKTYPE="Event Wait"/>
    <TASKINSTANCE NAME="ER_SIGNAL_DONE" TASKTYPE="Event Raise"/>
    {extra_xml}
    <WORKFLOWLINK FROMTASK="Start" TOTASK="EW_WAIT_FOR_FILE"/>
    <WORKFLOWLINK FROMTASK="EW_WAIT_FOR_FILE" TOTASK="S_LOAD_DATA"/>
    <WORKFLOWLINK FROMTASK="S_LOAD_DATA" TOTASK="ER_SIGNAL_DONE"/>
</WORKFLOW>
</FOLDER>
</REPOSITORY>
</POWERMART>"""
        xml_path = Path(tmpdir) / "wf_event_test.xml"
        xml_path.write_text(xml_content, encoding="utf-8")
        return xml_path

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_event_wait_detected(self):
        import run_assessment as ra
        ra.warnings.clear()
        ra.issues.clear()
        xml_path = self._make_workflow_xml(self.tmpdir)
        workflows = ra.parse_workflow_xml(xml_path)
        self.assertEqual(len(workflows), 1)
        wf = workflows[0]
        self.assertIn("EW_WAIT_FOR_FILE", wf["event_wait_tasks"])

    def test_event_raise_detected(self):
        import run_assessment as ra
        ra.warnings.clear()
        ra.issues.clear()
        xml_path = self._make_workflow_xml(self.tmpdir)
        workflows = ra.parse_workflow_xml(xml_path)
        wf = workflows[0]
        self.assertIn("ER_SIGNAL_DONE", wf["event_raise_tasks"])

    def test_event_tasks_in_workflow_info(self):
        import run_assessment as ra
        ra.warnings.clear()
        ra.issues.clear()
        xml_path = self._make_workflow_xml(self.tmpdir)
        workflows = ra.parse_workflow_xml(xml_path)
        wf = workflows[0]
        self.assertIn("event_wait_tasks", wf)
        self.assertIn("event_raise_tasks", wf)

    def test_event_wait_alternative_type_name(self):
        """TASKTYPE='EventWait' (no space) should also be detected."""
        import run_assessment as ra
        ra.warnings.clear()
        ra.issues.clear()
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
<REPOSITORY NAME="TEST" VERSION="186" DATABASETYPE="Oracle">
<FOLDER NAME="TEST_FOLDER">
<WORKFLOW NAME="WF_ALT_EVENT">
    <TASKINSTANCE NAME="EW_ALT" TASKTYPE="EventWait"/>
    <TASKINSTANCE NAME="ER_ALT" TASKTYPE="EventRaise"/>
</WORKFLOW>
</FOLDER>
</REPOSITORY>
</POWERMART>"""
        xml_path = Path(self.tmpdir) / "wf_alt_event.xml"
        xml_path.write_text(xml_content, encoding="utf-8")
        workflows = ra.parse_workflow_xml(xml_path)
        wf = workflows[0]
        self.assertIn("EW_ALT", wf["event_wait_tasks"])
        self.assertIn("ER_ALT", wf["event_raise_tasks"])

    def test_workflow_without_events_has_empty_lists(self):
        import run_assessment as ra
        ra.warnings.clear()
        ra.issues.clear()
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
<REPOSITORY NAME="TEST" VERSION="186" DATABASETYPE="Oracle">
<FOLDER NAME="TEST_FOLDER">
<WORKFLOW NAME="WF_NO_EVENTS">
    <SESSION NAME="S_LOAD" MAPPINGNAME="M_LOAD"/>
    <TASKINSTANCE NAME="S_LOAD" TASKTYPE="Session"/>
</WORKFLOW>
</FOLDER>
</REPOSITORY>
</POWERMART>"""
        xml_path = Path(self.tmpdir) / "wf_no_events.xml"
        xml_path.write_text(xml_content, encoding="utf-8")
        workflows = ra.parse_workflow_xml(xml_path)
        wf = workflows[0]
        self.assertEqual(wf["event_wait_tasks"], [])
        self.assertEqual(wf["event_raise_tasks"], [])


# ═════════════════════════════════════════════════════════════════════════════
#  66.4 — Event Wait/Raise Pipeline Activity Generation
# ═════════════════════════════════════════════════════════════════════════════

class TestEventPipelineActivities(unittest.TestCase):
    """Pipeline generation should include Wait/WebActivity for events."""

    def test_event_wait_activity_structure(self):
        import run_pipeline_migration as rpm
        activity = rpm._event_wait_activity("EW_FILE_READY", "NB_M_LOAD")
        self.assertEqual(activity["name"], "EW_FILE_READY")
        self.assertEqual(activity["type"], "Wait")
        self.assertIn("waitTimeInSeconds", activity["typeProperties"])
        self.assertEqual(len(activity["dependsOn"]), 1)

    def test_event_raise_activity_structure(self):
        import run_pipeline_migration as rpm
        activity = rpm._event_raise_activity("ER_DONE", "NB_M_LOAD")
        self.assertEqual(activity["name"], "ER_DONE")
        self.assertEqual(activity["type"], "WebActivity")
        self.assertEqual(activity["typeProperties"]["method"], "POST")

    def test_event_wait_no_dependency(self):
        import run_pipeline_migration as rpm
        activity = rpm._event_wait_activity("EW_START", "")
        self.assertEqual(activity["dependsOn"], [])

    def test_pipeline_with_events(self):
        import run_pipeline_migration as rpm
        workflow = {
            "name": "WF_EVENT_FLOW",
            "sessions": ["S_M_LOAD"],
            "session_to_mapping": {"S_M_LOAD": "M_LOAD"},
            "dependencies": {
                "EW_WAIT": ["Start"],
                "S_M_LOAD": ["EW_WAIT"],
                "ER_DONE": ["S_M_LOAD"],
            },
            "links": [],
            "decision_tasks": [],
            "email_tasks": [],
            "event_wait_tasks": ["EW_WAIT"],
            "event_raise_tasks": ["ER_DONE"],
            "schedule": "",
        }
        mappings_by_name = {
            "M_LOAD": {
                "name": "M_LOAD",
                "parameters": [],
            }
        }
        pipeline = rpm.generate_pipeline(workflow, mappings_by_name)
        activities = pipeline["properties"]["activities"]
        activity_names = [a["name"] for a in activities]
        self.assertIn("EW_WAIT", activity_names)
        self.assertIn("ER_DONE", activity_names)

        # Check event_webhook_url parameter exists
        params = pipeline["properties"]["parameters"]
        self.assertIn("event_webhook_url", params)

    def test_pipeline_without_events_no_webhook_param(self):
        import run_pipeline_migration as rpm
        workflow = {
            "name": "WF_NO_EVENT",
            "sessions": ["S_M_LOAD"],
            "session_to_mapping": {"S_M_LOAD": "M_LOAD"},
            "dependencies": {"S_M_LOAD": ["Start"]},
            "links": [],
            "decision_tasks": [],
            "email_tasks": [],
            "event_wait_tasks": [],
            "event_raise_tasks": [],
            "schedule": "",
        }
        mappings_by_name = {"M_LOAD": {"name": "M_LOAD", "parameters": []}}
        pipeline = rpm.generate_pipeline(workflow, mappings_by_name)
        params = pipeline["properties"]["parameters"]
        self.assertNotIn("event_webhook_url", params)


# ═════════════════════════════════════════════════════════════════════════════
#  66.5 — Standalone Session Config XML Parser
# ═════════════════════════════════════════════════════════════════════════════

class TestStandaloneSessionConfig(unittest.TestCase):
    """Standalone session XML with CONFIG elements should be parsed."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_session_xml(self):
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART>
<REPOSITORY NAME="REPO" VERSION="186" DATABASETYPE="Oracle">
<FOLDER NAME="SHARED">
<CONFIG NAME="default_config" DESCRIPTION="Standard ETL config">
  <ATTRIBUTE NAME="DTM buffer size" VALUE="64000000"/>
  <ATTRIBUTE NAME="Commit Interval" VALUE="10000"/>
  <ATTRIBUTE NAME="Sorter Cache Size" VALUE="4000000"/>
  <ATTRIBUTE NAME="Target Load Type" VALUE="Bulk"/>
  <ATTRIBUTE NAME="Truncate Target Table Option" VALUE="YES"/>
  <ATTRIBUTE NAME="Recovery Strategy" VALUE="Restart task"/>
</CONFIG>
<CONFIG NAME="high_perf_config" DESCRIPTION="High volume config">
  <ATTRIBUTE NAME="DTM buffer size" VALUE="256000000"/>
  <ATTRIBUTE NAME="Maximum Memory Allowed" VALUE="512000000"/>
  <ATTRIBUTE NAME="Pushdown Optimization" VALUE="Full"/>
</CONFIG>
</FOLDER>
</REPOSITORY>
</POWERMART>"""
        xml_path = Path(self.tmpdir) / "session_config.xml"
        xml_path.write_text(xml_content, encoding="utf-8")
        return xml_path

    def test_parse_standalone_configs(self):
        import run_assessment as ra
        xml_path = self._make_session_xml()
        tree = ET.parse(xml_path)
        root = tree.getroot()
        configs = ra.parse_standalone_session_configs(root)
        self.assertGreater(len(configs), 0)

    def test_config_names_extracted(self):
        import run_assessment as ra
        xml_path = self._make_session_xml()
        tree = ET.parse(xml_path)
        root = tree.getroot()
        configs = ra.parse_standalone_session_configs(root)
        config_names = {c["config_name"] for c in configs}
        self.assertIn("default_config", config_names)
        self.assertIn("high_perf_config", config_names)

    def test_dtm_buffer_mapped_to_spark(self):
        import run_assessment as ra
        xml_path = self._make_session_xml()
        tree = ET.parse(xml_path)
        root = tree.getroot()
        configs = ra.parse_standalone_session_configs(root)
        dtm_configs = [c for c in configs if c["infa_property"] == "DTM buffer size"]
        self.assertGreater(len(dtm_configs), 0)
        self.assertEqual(dtm_configs[0]["spark_property"], "spark.sql.shuffle.partitions")

    def test_target_load_type_mapped(self):
        import run_assessment as ra
        xml_path = self._make_session_xml()
        tree = ET.parse(xml_path)
        root = tree.getroot()
        configs = ra.parse_standalone_session_configs(root)
        tlt = [c for c in configs if c["infa_property"] == "Target Load Type"]
        self.assertEqual(len(tlt), 1)
        self.assertEqual(tlt[0]["infa_value"], "Bulk")

    def test_recovery_strategy_mapped(self):
        import run_assessment as ra
        xml_path = self._make_session_xml()
        tree = ET.parse(xml_path)
        root = tree.getroot()
        configs = ra.parse_standalone_session_configs(root)
        rec = [c for c in configs if c["infa_property"] == "Recovery Strategy"]
        self.assertEqual(len(rec), 1)
        self.assertIn("checkpoint", rec[0]["note"])

    def test_pushdown_optimization_mapped(self):
        import run_assessment as ra
        xml_path = self._make_session_xml()
        tree = ET.parse(xml_path)
        root = tree.getroot()
        configs = ra.parse_standalone_session_configs(root)
        pd = [c for c in configs if c["infa_property"] == "Pushdown Optimization"]
        self.assertEqual(len(pd), 1)
        self.assertIn("AQE", pd[0]["note"])

    def test_session_field_contains_config_name(self):
        import run_assessment as ra
        xml_path = self._make_session_xml()
        tree = ET.parse(xml_path)
        root = tree.getroot()
        configs = ra.parse_standalone_session_configs(root)
        for c in configs:
            self.assertIn("[shared config]", c["session"])

    def test_empty_xml_returns_empty(self):
        import run_assessment as ra
        xml_content = '<?xml version="1.0"?><POWERMART/>'
        root = ET.fromstring(xml_content)
        configs = ra.parse_standalone_session_configs(root)
        self.assertEqual(configs, [])

    def test_real_session_config_file(self):
        """Parse the actual input/sessions/session_config.xml."""
        import run_assessment as ra
        config_path = WORKSPACE / "input" / "sessions" / "session_config.xml"
        if config_path.exists():
            tree = ET.parse(config_path)
            root = tree.getroot()
            configs = ra.parse_standalone_session_configs(root)
            self.assertGreater(len(configs), 0)
            # Should find both default and high-perf configs
            config_names = {c["config_name"] for c in configs}
            self.assertIn("default_session_config", config_names)
            self.assertIn("high_perf_session_config", config_names)


# ═════════════════════════════════════════════════════════════════════════════
#  66.6 — ADR Files Exist
# ═════════════════════════════════════════════════════════════════════════════

class TestADRDocumentation(unittest.TestCase):
    """ADR files for Databricks, DBT, and AutoSys should exist."""

    def test_adr_004_databricks_exists(self):
        path = WORKSPACE / "docs" / "ADR" / "004-databricks-second-target.md"
        self.assertTrue(path.exists(), f"Missing ADR: {path}")

    def test_adr_005_dbt_exists(self):
        path = WORKSPACE / "docs" / "ADR" / "005-dbt-third-target.md"
        self.assertTrue(path.exists(), f"Missing ADR: {path}")

    def test_adr_006_autosys_exists(self):
        path = WORKSPACE / "docs" / "ADR" / "006-autosys-jil-migration.md"
        self.assertTrue(path.exists(), f"Missing ADR: {path}")

    def test_adr_004_has_status(self):
        path = WORKSPACE / "docs" / "ADR" / "004-databricks-second-target.md"
        content = path.read_text(encoding="utf-8")
        self.assertIn("## Status", content)
        self.assertIn("Accepted", content)

    def test_adr_005_has_decision(self):
        path = WORKSPACE / "docs" / "ADR" / "005-dbt-third-target.md"
        content = path.read_text(encoding="utf-8")
        self.assertIn("## Decision", content)

    def test_adr_006_has_mapping_table(self):
        path = WORKSPACE / "docs" / "ADR" / "006-autosys-jil-migration.md"
        content = path.read_text(encoding="utf-8")
        self.assertIn("BOX job", content)
        self.assertIn("CMD job", content)


# =====================================================================
#  Sprint 66b — Lineage in HTML Reports
# =====================================================================

class TestSvgLineageFlow(unittest.TestCase):
    """Test the inline SVG lineage flow renderer."""

    def test_empty_lineage_returns_message(self):
        from generate_html_reports import _svg_lineage_flow
        result = _svg_lineage_flow("M_TEST", [])
        self.assertIn("No field-level lineage", result)

    def test_basic_lineage_renders_svg(self):
        from generate_html_reports import _svg_lineage_flow
        lineage = [
            {
                "source_field": "ID",
                "source_instance": "SQ_CUST",
                "target_field": "CUST_ID",
                "target_instance": "DIM_CUSTOMER",
                "transformations": [
                    {"instance": "EXP_DERIVE", "type": "Expression"},
                ],
            }
        ]
        result = _svg_lineage_flow("M_LOAD", lineage)
        self.assertIn("<svg", result)
        self.assertIn("SQ_CUST", result)
        self.assertIn("EXP_DERIVE", result)
        self.assertIn("DIM_CUSTOMER", result)

    def test_svg_contains_arrow_marker(self):
        from generate_html_reports import _svg_lineage_flow
        lineage = [
            {
                "source_field": "A",
                "source_instance": "SQ",
                "target_field": "B",
                "target_instance": "TGT",
                "transformations": [],
            }
        ]
        result = _svg_lineage_flow("M_SIMPLE", lineage)
        self.assertIn("marker", result)
        self.assertIn("arrow", result)

    def test_source_and_target_styled_differently(self):
        from generate_html_reports import _svg_lineage_flow
        lineage = [
            {
                "source_field": "X",
                "source_instance": "SRC_1",
                "target_field": "Y",
                "target_instance": "TGT_1",
                "transformations": [{"instance": "FIL_1", "type": "Filter"}],
            }
        ]
        result = _svg_lineage_flow("M_TEST", lineage)
        # Source gets blue fill, target gets green fill
        self.assertIn("#E8F4FD", result)  # source color
        self.assertIn("#E8F8E8", result)  # target color


class TestCrossMappingLineage(unittest.TestCase):
    """Test the cross-mapping lineage summary builder."""

    def test_basic_cross_lineage(self):
        from generate_html_reports import _build_cross_mapping_lineage
        mappings = [
            {"name": "M_A", "sources": ["SRC_X"], "targets": ["TGT_Y"]},
            {"name": "M_B", "sources": ["SRC_X", "SRC_Z"], "targets": ["TGT_W"]},
        ]
        result = _build_cross_mapping_lineage(mappings)
        # SRC_X appears in both mappings
        src_x = [r for r in result if r["source"] == "SRC_X"]
        self.assertEqual(len(src_x), 1)
        self.assertIn("M_A", src_x[0]["mappings"])
        self.assertIn("M_B", src_x[0]["mappings"])
        self.assertIn("TGT_Y", src_x[0]["targets"])
        self.assertIn("TGT_W", src_x[0]["targets"])

    def test_empty_mappings(self):
        from generate_html_reports import _build_cross_mapping_lineage
        result = _build_cross_mapping_lineage([])
        self.assertEqual(result, [])

    def test_no_sources(self):
        from generate_html_reports import _build_cross_mapping_lineage
        mappings = [{"name": "M_A", "sources": [], "targets": ["TGT"]}]
        result = _build_cross_mapping_lineage(mappings)
        self.assertEqual(result, [])


class TestLineageReport(unittest.TestCase):
    """Test the standalone lineage report generation."""

    def test_generate_lineage_report_creates_file(self):
        import tempfile
        from generate_html_reports import generate_lineage_report
        inv = {
            "mappings": [
                {
                    "name": "M_TEST",
                    "sources": ["SRC_A"],
                    "targets": ["TGT_B"],
                    "complexity": "Simple",
                    "conversion_score": 95,
                    "field_lineage": [
                        {
                            "source_field": "ID",
                            "source_instance": "SQ_A",
                            "target_field": "ID",
                            "target_instance": "TGT_B",
                            "transformations": [],
                        }
                    ],
                },
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "lineage_report.html"
            result = generate_lineage_report(inv, out)
            self.assertTrue(out.exists())
            content = out.read_text(encoding="utf-8")
            self.assertIn("Data Lineage Report", content)
            self.assertIn("M_TEST", content)
            self.assertIn("SRC_A", content)
            self.assertIn("TGT_B", content)

    def test_lineage_report_has_kpi_section(self):
        import tempfile
        from generate_html_reports import generate_lineage_report
        inv = {
            "mappings": [
                {
                    "name": "M_1",
                    "sources": ["S1"],
                    "targets": ["T1"],
                    "complexity": "Medium",
                    "conversion_score": 80,
                    "field_lineage": [],
                },
                {
                    "name": "M_2",
                    "sources": ["S2"],
                    "targets": ["T2"],
                    "complexity": "Simple",
                    "conversion_score": 100,
                    "field_lineage": [
                        {
                            "source_field": "A",
                            "source_instance": "SQ",
                            "target_field": "B",
                            "target_instance": "TGT",
                            "transformations": [],
                        }
                    ],
                },
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "lr.html"
            generate_lineage_report(inv, out)
            content = out.read_text(encoding="utf-8")
            # Should show 2 total mappings, 1 with field lineage
            self.assertIn("Total Mappings", content)
            self.assertIn("With Field Lineage", content)
            self.assertIn("Field Paths Traced", content)

    def test_lineage_report_field_level_table(self):
        import tempfile
        from generate_html_reports import generate_lineage_report
        inv = {
            "mappings": [
                {
                    "name": "M_DETAIL",
                    "sources": ["SRC"],
                    "targets": ["TGT"],
                    "complexity": "Complex",
                    "conversion_score": 75,
                    "field_lineage": [
                        {
                            "source_field": "CUST_ID",
                            "source_instance": "SQ_CUST",
                            "target_field": "CUSTOMER_KEY",
                            "target_instance": "DIM_CUST",
                            "transformations": [
                                {"instance": "EXP_1", "type": "Expression"},
                                {"instance": "LKP_1", "type": "Lookup"},
                            ],
                        }
                    ],
                },
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "lr.html"
            generate_lineage_report(inv, out)
            content = out.read_text(encoding="utf-8")
            self.assertIn("CUST_ID", content)
            self.assertIn("CUSTOMER_KEY", content)
            self.assertIn("Expression", content)


class TestAssessmentReportWithLineage(unittest.TestCase):
    """Test that the assessment report now includes lineage sections."""

    def test_assessment_report_has_lineage_section(self):
        import tempfile
        from generate_html_reports import generate_assessment_report
        inv = {
            "summary": {
                "total_mappings": 1,
                "total_workflows": 0,
                "total_sessions": 0,
                "total_sql_files": 0,
                "complexity_breakdown": {"Simple": 1},
            },
            "mappings": [
                {
                    "name": "M_LIN",
                    "sources": ["SRC"],
                    "targets": ["TGT"],
                    "transformations": ["EXP"],
                    "complexity": "Simple",
                    "has_sql_override": False,
                    "conversion_score": 100,
                    "field_lineage": [
                        {
                            "source_field": "A",
                            "source_instance": "SQ",
                            "target_field": "B",
                            "target_instance": "TGT",
                            "transformations": [],
                        }
                    ],
                },
            ],
            "workflows": [],
            "sql_files": [],
            "connections": [],
            "parameter_files": [],
            "mapplets": {},
        }
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "assessment.html"
            generate_assessment_report(inv, out)
            content = out.read_text(encoding="utf-8")
            self.assertIn("Data Lineage", content)
            self.assertIn("Source Table", content)
            self.assertIn("Per-Mapping Lineage", content)


if __name__ == "__main__":
    unittest.main()
