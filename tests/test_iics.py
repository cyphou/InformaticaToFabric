"""
Sprint 19 — IICS Full Support Tests
Tests for: IICS Taskflow parsing, Sync Task, Mass Ingestion, IICS Connections,
and end-to-end pipeline generation from IICS taskflows.
"""

import json
import shutil
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

REAL_INPUT = PROJECT_ROOT / "input"


# ═════════════════════════════════════════════
#  IICS Taskflow Parser
# ═════════════════════════════════════════════

class TestIicsTaskflowParser:
    """Test parse_iics_taskflow() with the real fixture."""

    @pytest.fixture
    def real_fixture(self):
        return REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml"

    def test_parses_taskflow(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tfs = parse_iics_taskflow(real_fixture)
        assert len(tfs) == 1
        tf = tfs[0]
        assert tf["name"] == "TF_DAILY_CONTACTS_ETL"
        assert tf["format"] == "iics"

    def test_taskflow_sessions(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        # Should have mapping tasks + command task as sessions
        assert "mt_load_contacts" in tf["sessions"]
        assert "mt_sync_accounts" in tf["sessions"]
        assert "cmd_update_stats" in tf["sessions"]

    def test_taskflow_session_to_mapping(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        assert tf["session_to_mapping"]["mt_load_contacts"] == "m_load_contacts"
        assert tf["session_to_mapping"]["mt_sync_accounts"] == "m_sync_accounts"

    def test_taskflow_links(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        assert len(tf["links"]) >= 5
        # Check specific link
        start_links = [lk for lk in tf["links"] if lk["from"] == "Start"]
        assert len(start_links) >= 2  # Both mapping tasks start from Start

    def test_taskflow_decisions(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        assert tf["has_decision"] is True
        assert "gw_check_quality" in tf["decision_tasks"]

    def test_taskflow_timer(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        assert tf["has_timer"] is True

    def test_taskflow_email(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        assert "notify_complete" in tf["email_tasks"]

    def test_taskflow_parameters(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        assert "$$ENV" in tf["parameters"]
        assert "$$LOAD_DATE" in tf["parameters"]

    def test_taskflow_dependencies(self, real_fixture):
        from run_assessment import parse_iics_taskflow
        tf = parse_iics_taskflow(real_fixture)[0]
        # mt_load_contacts depends on Start
        assert "mt_load_contacts" in tf["dependencies"]
        assert "Start" in tf["dependencies"]["mt_load_contacts"]

    def test_empty_file(self, tmp_path):
        from run_assessment import parse_iics_taskflow
        f = tmp_path / "empty.xml"
        f.write_text('<?xml version="1.0"?>\n<exportMetadata/>')
        assert parse_iics_taskflow(f) == []

    def test_synthetic_taskflow(self, tmp_path):
        from run_assessment import parse_iics_taskflow
        xml = '''<?xml version="1.0"?>
<exportMetadata>
  <dTemplate name="TF_SIMPLE" objectType="com.infa.deployment.taskflow">
    <dTemplate name="mt_a" objectType="com.infa.deployment.mappingtask">
      <dAttribute name="mappingName" value="m_a"/>
    </dTemplate>
    <dLink from="Start" to="mt_a"/>
  </dTemplate>
</exportMetadata>'''
        f = tmp_path / "tf.xml"
        f.write_text(xml)
        tfs = parse_iics_taskflow(f)
        assert len(tfs) == 1
        assert tfs[0]["sessions"] == ["mt_a"]
        assert tfs[0]["session_to_mapping"]["mt_a"] == "m_a"


# ═════════════════════════════════════════════
#  IICS Synchronization Task Parser
# ═════════════════════════════════════════════

class TestIicsSyncTasks:

    def test_parses_sync_task(self):
        from run_assessment import parse_iics_sync_tasks
        p = REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml"
        tasks = parse_iics_sync_tasks(p)
        assert len(tasks) == 1
        st = tasks[0]
        assert st["name"] == "SYNC_CUSTOMER_DATA"
        assert st["iics_type"] == "SynchronizationTask"
        assert "Oracle_CRM" in st["sources"]
        assert "Lakehouse_Silver" in st["targets"]
        assert st["format"] == "iics"

    def test_empty_returns_empty(self, tmp_path):
        from run_assessment import parse_iics_sync_tasks
        f = tmp_path / "empty.xml"
        f.write_text('<?xml version="1.0"?>\n<exportMetadata/>')
        assert parse_iics_sync_tasks(f) == []

    def test_sync_task_mapping_ref(self):
        from run_assessment import parse_iics_sync_tasks
        p = REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml"
        tasks = parse_iics_sync_tasks(p)
        assert tasks[0]["mapping_ref"] == "m_sync_customers"


# ═════════════════════════════════════════════
#  IICS Mass Ingestion Parser
# ═════════════════════════════════════════════

class TestIicsMassIngestion:

    def test_parses_mass_ingestion(self):
        from run_assessment import parse_iics_mass_ingestion
        p = REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml"
        tasks = parse_iics_mass_ingestion(p)
        assert len(tasks) == 1
        mi = tasks[0]
        assert mi["name"] == "MI_BULK_LOAD_PRODUCTS"
        assert mi["iics_type"] == "MassIngestion"
        assert "S3_LANDING" in mi["sources"]
        assert "Lakehouse_Bronze" in mi["targets"]
        assert mi["file_pattern"] == "products_*.csv"
        assert mi["load_type"] == "full"

    def test_empty_returns_empty(self, tmp_path):
        from run_assessment import parse_iics_mass_ingestion
        f = tmp_path / "empty.xml"
        f.write_text('<?xml version="1.0"?>\n<exportMetadata/>')
        assert parse_iics_mass_ingestion(f) == []


# ═════════════════════════════════════════════
#  IICS Connection Parser
# ═════════════════════════════════════════════

class TestIicsConnectionParser:

    def test_parses_connections(self):
        from run_assessment import parse_iics_connections
        p = REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml"
        conns = parse_iics_connections(p)
        assert len(conns) == 3
        names = {c["name"] for c in conns}
        assert "CONN_SALESFORCE_PROD" in names
        assert "CONN_ORACLE_CRM" in names
        assert "CONN_LAKEHOUSE" in names

    def test_connection_types(self):
        from run_assessment import parse_iics_connections
        p = REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml"
        conns = parse_iics_connections(p)
        by_name = {c["name"]: c for c in conns}
        assert by_name["CONN_SALESFORCE_PROD"]["type"] == "Salesforce"
        assert by_name["CONN_ORACLE_CRM"]["type"] == "Oracle"
        assert by_name["CONN_LAKEHOUSE"]["type"] == "Lakehouse"

    def test_connection_format_tag(self):
        from run_assessment import parse_iics_connections
        p = REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml"
        conns = parse_iics_connections(p)
        for c in conns:
            assert c["format"] == "iics"

    def test_empty_returns_empty(self, tmp_path):
        from run_assessment import parse_iics_connections
        f = tmp_path / "empty.xml"
        f.write_text('<?xml version="1.0"?>\n<exportMetadata/>')
        assert parse_iics_connections(f) == []


# ═════════════════════════════════════════════
#  IICS Taskflow Type Mapping
# ═════════════════════════════════════════════

class TestIicsTaskflowTypeMap:

    def test_type_map_keys(self):
        from run_assessment import IICS_TASKFLOW_TYPE_MAP
        assert IICS_TASKFLOW_TYPE_MAP["mappingtask"] == "NotebookActivity"
        assert IICS_TASKFLOW_TYPE_MAP["commandtask"] == "ScriptActivity"
        assert IICS_TASKFLOW_TYPE_MAP["notificationtask"] == "WebActivity"
        assert IICS_TASKFLOW_TYPE_MAP["exclusivegateway"] == "IfCondition"
        assert IICS_TASKFLOW_TYPE_MAP["timerevent"] == "WaitActivity"
        assert IICS_TASKFLOW_TYPE_MAP["subflow"] == "ExecutePipeline"


# ═════════════════════════════════════════════
#  IICS E2E: Assessment → Pipeline
# ═════════════════════════════════════════════

class TestIicsEndToEnd:
    """Full flow: parse IICS taskflow → assessment → pipeline generation."""

    def test_iics_taskflow_in_inventory(self, tmp_path):
        """Assessment main() should include IICS taskflows in inventory."""
        import run_assessment

        ws = tmp_path
        (ws / "input" / "mappings").mkdir(parents=True)
        (ws / "input" / "workflows").mkdir(parents=True)
        (ws / "input" / "sql").mkdir(parents=True)
        (ws / "output" / "inventory").mkdir(parents=True)

        # Copy IICS fixtures
        shutil.copy(REAL_INPUT / "mappings" / "IICS_M_LOAD_CONTACTS.xml",
                     ws / "input" / "mappings" / "IICS_M_LOAD_CONTACTS.xml")
        shutil.copy(REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml",
                     ws / "input" / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml")

        saved = {}
        for attr in ["WORKSPACE", "INPUT_DIR", "OUTPUT_DIR"]:
            saved[attr] = getattr(run_assessment, attr)
        run_assessment.WORKSPACE = ws
        run_assessment.INPUT_DIR = ws / "input"
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            run_assessment.main()
        finally:
            for attr, val in saved.items():
                setattr(run_assessment, attr, val)

        inv = json.loads((ws / "output" / "inventory" / "inventory.json").read_text(encoding="utf-8"))

        # Should have IICS mappings
        mapping_names = [m["name"] for m in inv["mappings"]]
        assert "m_load_contacts" in mapping_names

        # Should have IICS taskflow as a workflow
        wf_names = [w["name"] for w in inv["workflows"]]
        assert "TF_DAILY_CONTACTS_ETL" in wf_names

        # Should have sync task and mass ingestion as mappings
        assert "SYNC_CUSTOMER_DATA" in mapping_names
        assert "MI_BULK_LOAD_PRODUCTS" in mapping_names

        # Should have IICS connections
        conn_names = {c["name"] for c in inv["connections"]}
        assert "CONN_SALESFORCE_PROD" in conn_names or len(conn_names) >= 1

    def test_iics_taskflow_pipeline_generation(self, tmp_path):
        """Generate pipeline JSON from an IICS taskflow."""
        import run_assessment
        import run_pipeline_migration

        ws = tmp_path
        (ws / "input" / "mappings").mkdir(parents=True)
        (ws / "input" / "workflows").mkdir(parents=True)
        (ws / "input" / "sql").mkdir(parents=True)
        (ws / "output" / "inventory").mkdir(parents=True)
        (ws / "output" / "pipelines").mkdir(parents=True)

        shutil.copy(REAL_INPUT / "mappings" / "IICS_M_LOAD_CONTACTS.xml",
                     ws / "input" / "mappings" / "IICS_M_LOAD_CONTACTS.xml")
        shutil.copy(REAL_INPUT / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml",
                     ws / "input" / "workflows" / "IICS_TF_DAILY_CONTACTS_ETL.xml")

        # Run assessment
        saved_a = {}
        for attr in ["WORKSPACE", "INPUT_DIR", "OUTPUT_DIR"]:
            saved_a[attr] = getattr(run_assessment, attr)
        run_assessment.WORKSPACE = ws
        run_assessment.INPUT_DIR = ws / "input"
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        saved_argv = sys.argv
        sys.argv = ["test"]
        try:
            run_assessment.main()
        finally:
            sys.argv = saved_argv
            for attr, val in saved_a.items():
                setattr(run_assessment, attr, val)

        # Run pipeline generation
        saved_p = {}
        for attr in ["WORKSPACE", "OUTPUT_DIR", "INVENTORY_PATH"]:
            if hasattr(run_pipeline_migration, attr):
                saved_p[attr] = getattr(run_pipeline_migration, attr)
        run_pipeline_migration.WORKSPACE = ws
        run_pipeline_migration.OUTPUT_DIR = ws / "output" / "pipelines"
        run_pipeline_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        sys.argv = ["test"]
        try:
            run_pipeline_migration.main()
        finally:
            sys.argv = saved_argv
            for attr, val in saved_p.items():
                setattr(run_pipeline_migration, attr, val)

        # Verify pipeline was generated
        pl_files = list((ws / "output" / "pipelines").glob("PL_*.json"))
        assert len(pl_files) >= 1

        # Verify pipeline for the IICS taskflow
        pl_path = ws / "output" / "pipelines" / "PL_TF_DAILY_CONTACTS_ETL.json"
        assert pl_path.exists(), f"Pipeline not found. Files: {[f.name for f in pl_files]}"

        pipeline = json.loads(pl_path.read_text(encoding="utf-8"))
        assert pipeline["name"] == "PL_TF_DAILY_CONTACTS_ETL"
        assert len(pipeline["properties"]["activities"]) >= 1
        assert "MigratedFromInformatica" in pipeline["properties"]["annotations"]
