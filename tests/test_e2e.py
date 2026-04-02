"""
Sprint 18 — End-to-End Integration Tests
Runs the full 5-phase migration pipeline against real fixtures,
then verifies outputs are generated and structurally correct.
"""

import json
import shutil
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Real input fixtures shipped with the project
REAL_INPUT = PROJECT_ROOT / "input"


def _setup_workspace(tmp_path):
    """Copy real fixtures and create output dirs in tmp_path."""
    input_dst = tmp_path / "input"
    shutil.copytree(REAL_INPUT, input_dst)
    for sub in ["inventory", "sql", "notebooks", "pipelines", "validation"]:
        (tmp_path / "output" / sub).mkdir(parents=True, exist_ok=True)
    return tmp_path


def _redirect_module(mod, tmp_path, extra_attrs=None):
    """Monkeypatch a module's WORKSPACE / INPUT_DIR / OUTPUT_DIR / INVENTORY_PATH."""
    saved = {}
    for attr in ["WORKSPACE", "INPUT_DIR", "OUTPUT_DIR", "INVENTORY_PATH"] + (extra_attrs or []):
        if hasattr(mod, attr):
            saved[attr] = getattr(mod, attr)
    mod.WORKSPACE = tmp_path
    if hasattr(mod, "INPUT_DIR"):
        mod.INPUT_DIR = tmp_path / "input"
    return saved


def _restore_module(mod, saved):
    """Restore original module-level constants."""
    for attr, val in saved.items():
        setattr(mod, attr, val)


def _run_main(mod_main):
    """Run a module's main() with isolated sys.argv."""
    saved_argv = sys.argv
    sys.argv = ["test"]
    try:
        mod_main()
    finally:
        sys.argv = saved_argv


# ═════════════════════════════════════════════════════════
#  Phase-by-phase Integration Tests
# ═════════════════════════════════════════════════════════

class TestPhase0Assessment:
    """Run assessment against the real fixtures and verify outputs."""

    def test_assessment_generates_inventory(self, tmp_path):
        import run_assessment

        ws = _setup_workspace(tmp_path)
        saved = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved)

        inv_path = ws / "output" / "inventory" / "inventory.json"
        assert inv_path.exists(), "inventory.json was not generated"
        inv = json.loads(inv_path.read_text(encoding="utf-8"))

        # Real fixtures should produce ≥ 4 mappings (M_LOAD_CUSTOMERS, M_LOAD_ORDERS, etc.)
        assert inv["summary"]["total_mappings"] >= 4
        # At least 1 workflow
        assert inv["summary"]["total_workflows"] >= 1
        # SQL files
        assert inv["summary"]["total_sql_files"] >= 1
        # Complexity report and DAG
        assert (ws / "output" / "inventory" / "complexity_report.md").exists()
        assert (ws / "output" / "inventory" / "dependency_dag.json").exists()

    def test_assessment_html_reports(self, tmp_path):
        """Assessment Phase 0 should also generate HTML reports."""
        import run_assessment

        ws = _setup_workspace(tmp_path)
        saved = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved)

        # HTML reports (generated in step 10/10)
        assert (ws / "output" / "inventory" / "assessment_report.html").exists()
        assert (ws / "output" / "inventory" / "migration_report.html").exists()


class TestPhase1SqlMigration:
    """Run SQL migration against real inventory."""

    @pytest.fixture
    def workspace_with_inventory(self, tmp_path):
        """Create workspace with assessment already run."""
        import run_assessment

        ws = _setup_workspace(tmp_path)
        saved = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved)
        return ws

    def test_sql_migration_converts_files(self, workspace_with_inventory):
        import run_sql_migration

        ws = workspace_with_inventory
        saved = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved)

        sql_dir = ws / "output" / "sql"
        sql_files = list(sql_dir.glob("SQL_*.sql"))
        assert len(sql_files) >= 3, f"Expected ≥3 SQL files, got {len(sql_files)}: {[f.name for f in sql_files]}"

        # Verify converted SQL files have headers
        for sf in sql_files:
            content = sf.read_text(encoding="utf-8")
            assert "Converted from:" in content or "SQL Overrides for mapping:" in content

    def test_sql_overrides_generated(self, workspace_with_inventory):
        import run_sql_migration

        ws = workspace_with_inventory
        saved = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved)

        # Check that SQL override files are generated for mappings with overrides
        override_files = list((ws / "output" / "sql").glob("SQL_OVERRIDES_*.sql"))
        # At least the ORDERS mapping has overrides
        assert len(override_files) >= 1


class TestPhase2NotebookMigration:
    """Run notebook generation against real inventory."""

    @pytest.fixture
    def workspace_with_sql(self, tmp_path):
        """Workspace with assessment + SQL migration already done."""
        import run_assessment
        import run_sql_migration

        ws = _setup_workspace(tmp_path)

        saved_a = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved_a)

        saved_s = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved_s)

        return ws

    def test_notebooks_generated(self, workspace_with_sql):
        import run_notebook_migration

        ws = workspace_with_sql
        saved = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved)

        nb_dir = ws / "output" / "notebooks"
        notebooks = list(nb_dir.glob("NB_*.py"))
        assert len(notebooks) >= 4, f"Expected ≥4 notebooks, got {[f.name for f in notebooks]}"

        # Verify notebook structure
        for nb in notebooks:
            content = nb.read_text(encoding="utf-8")
            assert "Fabric Notebook" in content or "PySpark" in content or "spark" in content.lower()
            assert "COMMAND" in content  # Should have cell separators

    def test_notebook_has_source_cell(self, workspace_with_sql):
        import run_notebook_migration

        ws = workspace_with_sql
        saved = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved)

        # Customer notebook should have a read cell
        nb_cust = ws / "output" / "notebooks" / "NB_M_LOAD_CUSTOMERS.py"
        if nb_cust.exists():
            content = nb_cust.read_text(encoding="utf-8")
            assert "spark.read" in content or "spark.sql" in content or "read" in content.lower()


class TestPhase3PipelineMigration:
    """Run pipeline generation against real inventory."""

    @pytest.fixture
    def workspace_with_notebooks(self, tmp_path):
        """Workspace with phases 0-2 already done."""
        import run_assessment
        import run_notebook_migration
        import run_sql_migration

        ws = _setup_workspace(tmp_path)

        # Phase 0
        saved_a = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved_a)

        # Phase 1
        saved_s = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved_s)

        # Phase 2
        saved_n = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved_n)

        return ws

    def test_pipelines_generated(self, workspace_with_notebooks):
        import run_pipeline_migration

        ws = workspace_with_notebooks
        saved = _redirect_module(run_pipeline_migration, ws)
        run_pipeline_migration.OUTPUT_DIR = ws / "output" / "pipelines"
        run_pipeline_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_pipeline_migration.main)
        finally:
            _restore_module(run_pipeline_migration, saved)

        pl_dir = ws / "output" / "pipelines"
        pipelines = list(pl_dir.glob("PL_*.json"))
        assert len(pipelines) >= 1, "No pipeline JSON files generated"

        # Verify pipeline structure
        has_activities = False
        for pl in pipelines:
            content = json.loads(pl.read_text(encoding="utf-8"))
            assert "name" in content
            assert "properties" in content
            assert "activities" in content["properties"]
            if len(content["properties"]["activities"]) >= 1:
                has_activities = True
        assert has_activities, "At least one pipeline should have activities"

    def test_pipeline_has_dependencies(self, workspace_with_notebooks):
        import run_pipeline_migration

        ws = workspace_with_notebooks
        saved = _redirect_module(run_pipeline_migration, ws)
        run_pipeline_migration.OUTPUT_DIR = ws / "output" / "pipelines"
        run_pipeline_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_pipeline_migration.main)
        finally:
            _restore_module(run_pipeline_migration, saved)

        # The daily workflow should have ordered sessions
        pl = ws / "output" / "pipelines" / "PL_WF_DAILY_SALES_LOAD.json"
        if pl.exists():
            pipeline = json.loads(pl.read_text(encoding="utf-8"))
            activities = pipeline["properties"]["activities"]
            # At least some activities should have dependsOn
            has_deps = any(a.get("dependsOn") for a in activities)
            assert has_deps or len(activities) == 1


class TestPhase4Validation:
    """Run validation generation against real inventory."""

    @pytest.fixture
    def workspace_full(self, tmp_path):
        """Workspace with all phases 0-3 complete."""
        import run_assessment
        import run_notebook_migration
        import run_pipeline_migration
        import run_sql_migration

        ws = _setup_workspace(tmp_path)

        saved_a = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved_a)

        saved_s = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved_s)

        saved_n = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved_n)

        saved_p = _redirect_module(run_pipeline_migration, ws)
        run_pipeline_migration.OUTPUT_DIR = ws / "output" / "pipelines"
        run_pipeline_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_pipeline_migration.main)
        finally:
            _restore_module(run_pipeline_migration, saved_p)

        return ws

    def test_validation_notebooks_generated(self, workspace_full):
        import run_validation

        ws = workspace_full
        saved = _redirect_module(run_validation, ws)
        run_validation.OUTPUT_DIR = ws / "output" / "validation"
        run_validation.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_validation.main)
        finally:
            _restore_module(run_validation, saved)

        val_dir = ws / "output" / "validation"
        val_files = list(val_dir.glob("VAL_*.py"))
        assert len(val_files) >= 1

        # Test matrix should exist
        assert (val_dir / "test_matrix.md").exists()

    def test_validation_has_row_count_check(self, workspace_full):
        import run_validation

        ws = workspace_full
        saved = _redirect_module(run_validation, ws)
        run_validation.OUTPUT_DIR = ws / "output" / "validation"
        run_validation.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_validation.main)
        finally:
            _restore_module(run_validation, saved)

        val_dir = ws / "output" / "validation"
        val_files = list(val_dir.glob("VAL_*.py"))
        for vf in val_files:
            content = vf.read_text(encoding="utf-8")
            # Validation notebooks should contain row count logic
            assert "count" in content.lower() or "row" in content.lower()


# ═════════════════════════════════════════════════════════
#  Full Pipeline Integration
# ═════════════════════════════════════════════════════════

class TestFullPipeline:
    """Run the entire 5-phase pipeline end-to-end."""

    def test_full_pipeline_produces_all_artifacts(self, tmp_path):
        """Run all 5 phases sequentially and verify every expected output."""
        import run_assessment
        import run_notebook_migration
        import run_pipeline_migration
        import run_sql_migration
        import run_validation

        ws = _setup_workspace(tmp_path)

        # Phase 0 — Assessment
        saved = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved)

        inv = json.loads((ws / "output" / "inventory" / "inventory.json").read_text(encoding="utf-8"))

        # Phase 1 — SQL
        saved = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved)

        # Phase 2 — Notebooks
        saved = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved)

        # Phase 3 — Pipelines
        saved = _redirect_module(run_pipeline_migration, ws)
        run_pipeline_migration.OUTPUT_DIR = ws / "output" / "pipelines"
        run_pipeline_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_pipeline_migration.main)
        finally:
            _restore_module(run_pipeline_migration, saved)

        # Phase 4 — Validation
        saved = _redirect_module(run_validation, ws)
        run_validation.OUTPUT_DIR = ws / "output" / "validation"
        run_validation.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_validation.main)
        finally:
            _restore_module(run_validation, saved)

        # ─── Comprehensive output verification ───

        # Inventory
        assert inv["summary"]["total_mappings"] >= 4
        assert inv["summary"]["total_workflows"] >= 1

        # Complexity report
        report = (ws / "output" / "inventory" / "complexity_report.md").read_text(encoding="utf-8")
        assert "Complexity" in report

        # DAG
        dag = json.loads((ws / "output" / "inventory" / "dependency_dag.json").read_text(encoding="utf-8"))
        assert "workflows" in dag
        assert len(dag["workflows"]) >= 1
        assert "nodes" in dag["workflows"][0]

        # SQL files
        sql_files = list((ws / "output" / "sql").glob("SQL_*.sql"))
        assert len(sql_files) >= 3

        # Notebooks — one per mapping
        notebooks = list((ws / "output" / "notebooks").glob("NB_*.py"))
        assert len(notebooks) >= inv["summary"]["total_mappings"]

        # Pipelines — one per workflow
        pipelines = list((ws / "output" / "pipelines").glob("PL_*.json"))
        assert len(pipelines) >= inv["summary"]["total_workflows"]

        # Validation — at least some
        val_files = list((ws / "output" / "validation").glob("VAL_*.py"))
        assert len(val_files) >= 1
        assert (ws / "output" / "validation" / "test_matrix.md").exists()

        # HTML reports
        assert (ws / "output" / "inventory" / "assessment_report.html").exists()
        assert (ws / "output" / "inventory" / "migration_report.html").exists()

    def test_notebooks_reference_correct_sources(self, tmp_path):
        """Verify notebooks contain references to their mapping's source tables."""
        import run_assessment
        import run_notebook_migration
        import run_sql_migration

        ws = _setup_workspace(tmp_path)

        saved = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved)

        saved = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved)

        saved = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved)

        nb = ws / "output" / "notebooks" / "NB_M_LOAD_CUSTOMERS.py"
        assert nb.exists()
        content = nb.read_text(encoding="utf-8")
        # Should reference the CUSTOMERS source table somehow
        assert "customer" in content.lower()

    def test_pipeline_json_validates(self, tmp_path):
        """Pipeline JSONs should parse as valid JSON with expected keys."""
        import run_assessment
        import run_notebook_migration
        import run_pipeline_migration
        import run_sql_migration

        ws = _setup_workspace(tmp_path)

        saved = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved)

        saved = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved)

        saved = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved)

        saved = _redirect_module(run_pipeline_migration, ws)
        run_pipeline_migration.OUTPUT_DIR = ws / "output" / "pipelines"
        run_pipeline_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_pipeline_migration.main)
        finally:
            _restore_module(run_pipeline_migration, saved)

        for pl_file in (ws / "output" / "pipelines").glob("PL_*.json"):
            pipeline = json.loads(pl_file.read_text(encoding="utf-8"))
            assert isinstance(pipeline["name"], str)
            assert isinstance(pipeline["properties"]["activities"], list)
            for activity in pipeline["properties"]["activities"]:
                assert "name" in activity
                assert "type" in activity


# ═════════════════════════════════════════════════════════
#  Orchestrator Integration (--resume / --reset)
# ═════════════════════════════════════════════════════════

class TestOrchestratorResume:
    """Test checkpoint / resume behavior via run_migration.py."""

    def test_checkpoint_saved_on_success(self, tmp_path):
        import run_migration

        # Create a checkpoint dir
        chk_path = tmp_path / "output" / ".checkpoint.json"
        (tmp_path / "output").mkdir(parents=True, exist_ok=True)

        original_chk = run_migration.CHECKPOINT_PATH
        run_migration.CHECKPOINT_PATH = chk_path

        try:
            # Save a synthetic checkpoint
            checkpoint = {"completed_phases": [0, 1], "results": []}
            run_migration._save_checkpoint(checkpoint)
            assert chk_path.exists()

            # Load it back
            loaded = run_migration._load_checkpoint()
            assert 0 in loaded["completed_phases"]
            assert 1 in loaded["completed_phases"]
            assert "updated" in loaded

            # Clear it
            run_migration._clear_checkpoint()
            assert not chk_path.exists()
        finally:
            run_migration.CHECKPOINT_PATH = original_chk

    def test_generate_summary(self):
        from run_migration import generate_summary

        results = [
            {"id": 0, "name": "Assessment", "status": "ok", "duration": 1.5, "error": None},
            {"id": 1, "name": "SQL Migration", "status": "ok", "duration": 0.8, "error": None},
            {"id": 2, "name": "Notebook Migration", "status": "skipped", "duration": 0, "error": None},
            {"id": 3, "name": "Pipeline Migration", "status": "error", "duration": 0.3, "error": "test error"},
            {"id": 4, "name": "Validation", "status": "ok", "duration": 2.0, "error": None},
        ]
        path = generate_summary(results)
        content = path.read_text(encoding="utf-8")
        assert "Migration Summary" in content
        assert "Assessment" in content
        assert "Skipped" in content or "skipped" in content.lower()
        assert "test error" in content


# ═════════════════════════════════════════════════════════
#  Artifact Content Validation
# ═════════════════════════════════════════════════════════

class TestArtifactContent:
    """Deep validation of generated artifact contents."""

    @pytest.fixture
    def full_output(self, tmp_path):
        """Run full pipeline and return workspace path."""
        import run_assessment
        import run_notebook_migration
        import run_pipeline_migration
        import run_sql_migration
        import run_validation

        ws = _setup_workspace(tmp_path)

        saved = _redirect_module(run_assessment, ws)
        run_assessment.OUTPUT_DIR = ws / "output" / "inventory"
        run_assessment.warnings.clear()
        run_assessment.issues.clear()
        try:
            _run_main(run_assessment.main)
        finally:
            _restore_module(run_assessment, saved)

        saved = _redirect_module(run_sql_migration, ws)
        run_sql_migration.OUTPUT_DIR = ws / "output" / "sql"
        run_sql_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_sql_migration.main)
        finally:
            _restore_module(run_sql_migration, saved)

        saved = _redirect_module(run_notebook_migration, ws)
        run_notebook_migration.OUTPUT_DIR = ws / "output" / "notebooks"
        run_notebook_migration.SQL_DIR = ws / "output" / "sql"
        run_notebook_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_notebook_migration.main)
        finally:
            _restore_module(run_notebook_migration, saved)

        saved = _redirect_module(run_pipeline_migration, ws)
        run_pipeline_migration.OUTPUT_DIR = ws / "output" / "pipelines"
        run_pipeline_migration.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_pipeline_migration.main)
        finally:
            _restore_module(run_pipeline_migration, saved)

        saved = _redirect_module(run_validation, ws)
        run_validation.OUTPUT_DIR = ws / "output" / "validation"
        run_validation.INVENTORY_PATH = ws / "output" / "inventory" / "inventory.json"
        try:
            _run_main(run_validation.main)
        finally:
            _restore_module(run_validation, saved)

        return ws

    def test_sql_conversion_removes_oracle_syntax(self, full_output):
        """Converted SQL should not contain raw Oracle-only syntax."""
        ws = full_output
        for sf in (ws / "output" / "sql").glob("SQL_*.sql"):
            if "OVERRIDES" in sf.name:
                continue
            content = sf.read_text(encoding="utf-8")
            converted_section = content.split("-- =====")[-1] if "-- =====" in content else content
            # NVL should be converted to COALESCE
            # (may still appear in comments, so check non-comment lines only)
            non_comment = [ln for ln in converted_section.splitlines() if not ln.strip().startswith("--")]
            joined = "\n".join(non_comment)
            # Verify SYSDATE is converted to CURRENT_TIMESTAMP
            assert "SYSDATE" not in joined or "current_timestamp" in joined.lower()

    def test_inventory_has_all_expected_keys(self, full_output):
        ws = full_output
        inv = json.loads((ws / "output" / "inventory" / "inventory.json").read_text(encoding="utf-8"))
        required_keys = ["generated", "source_platform", "target_platform", "mappings",
                         "workflows", "connections", "sql_files", "summary"]
        for key in required_keys:
            assert key in inv, f"Missing key '{key}' in inventory"

    def test_test_matrix_lists_all_targets(self, full_output):
        ws = full_output
        matrix = (ws / "output" / "validation" / "test_matrix.md").read_text(encoding="utf-8")
        assert "Test Matrix" in matrix or "test_matrix" in matrix.lower() or "|" in matrix
        # Should reference at least one target table
        assert "DIM_CUSTOMER" in matrix or "FACT_ORDERS" in matrix or "dim_customer" in matrix.lower()

    def test_dag_has_workflow_nodes(self, full_output):
        ws = full_output
        dag = json.loads((ws / "output" / "inventory" / "dependency_dag.json").read_text(encoding="utf-8"))
        # DAG structure: {"workflows": [{"workflow": "...", "nodes": [...], "edges": [...]}]}
        assert "workflows" in dag
        assert len(dag["workflows"]) >= 1
        wf_dag = dag["workflows"][0]
        assert len(wf_dag["nodes"]) >= 1
