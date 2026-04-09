"""
Test Suite — Monitoring Platform (DD7–DD9)
==========================================
Tests: PlatformState, HealthScore, AlertingOrchestrator, SLOTracker, status page.

Run:
    pytest tests/test_monitoring_platform.py -v
"""

import json
import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from monitoring_platform import (
    AlertingOrchestrator,
    AlertState,
    EscalationTier,
    HealthScoreCalculator,
    MigrationRunState,
    PlatformState,
    SLOTracker,
    generate_platform_report,
    generate_status_page,
    load_platform_config,
)


# ─────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def platform_config():
    return {
        "alerting_platform": {
            "escalation": {"tiers": 4},
            "slo": {
                "phase_success_rate": 0.90,
                "max_duration_minutes": 30,
                "max_error_rate": 0.10,
            },
        }
    }


@pytest.fixture
def run_state():
    state = MigrationRunState(run_id="test-run-001", target="fabric")
    state.record_phase("assessment", "ok", duration=5.0, artifacts=3)
    state.record_phase("sql", "ok", duration=10.0, artifacts=8)
    state.record_phase("notebook", "ok", duration=15.0, artifacts=16)
    state.record_artifact("notebooks", 16)
    state.record_artifact("sql", 8)
    return state


@pytest.fixture
def failed_run_state():
    state = MigrationRunState(run_id="test-fail-001", target="fabric")
    state.record_phase("assessment", "ok", duration=5.0)
    state.record_phase("sql", "error", duration=30.0, errors=["SQL timeout"])
    state.record_phase("notebook", "error", duration=60.0, errors=["OOM", "Schema mismatch"])
    return state


# ─────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────

class TestLoadPlatformConfig:

    def test_defaults(self):
        cfg = load_platform_config({})
        assert cfg["escalation"] == {}
        assert cfg["slo"] == {}

    def test_from_config(self, platform_config):
        cfg = load_platform_config(platform_config)
        assert cfg["slo"]["phase_success_rate"] == 0.90


# ─────────────────────────────────────────────
#  DD7: MigrationRunState
# ─────────────────────────────────────────────

class TestMigrationRunState:

    def test_create_state(self):
        state = MigrationRunState(target="databricks")
        assert state.target == "databricks"
        assert state.status == "running"
        assert len(state.run_id) == 12

    def test_record_phase(self, run_state):
        assert "assessment" in run_state.phases
        assert run_state.phases["assessment"]["status"] == "ok"
        assert run_state.phases["assessment"]["duration"] == 5.0

    def test_record_artifact(self, run_state):
        assert run_state.artifacts["notebooks"] == 16
        assert run_state.artifacts["sql"] == 8

    def test_complete(self, run_state):
        run_state.complete("completed")
        assert run_state.status == "completed"
        assert run_state.end_time is not None

    def test_to_dict(self, run_state):
        d = run_state.to_dict()
        assert d["run_id"] == "test-run-001"
        assert d["target"] == "fabric"
        assert "phases" in d
        assert "artifacts" in d

    def test_errors_tracked(self, failed_run_state):
        assert len(failed_run_state.errors) == 3


# ─────────────────────────────────────────────
#  DD7: Health Score
# ─────────────────────────────────────────────

class TestHealthScoreCalculator:

    def test_perfect_health(self, run_state):
        calc = HealthScoreCalculator()
        result = calc.calculate(run_state)
        assert result["overall"] >= 80
        assert result["grade"] in ("A", "B")

    def test_failed_health(self, failed_run_state):
        calc = HealthScoreCalculator()
        result = calc.calculate(failed_run_state)
        assert result["overall"] < 80
        assert result["grade"] in ("C", "D", "F")

    def test_components_present(self, run_state):
        calc = HealthScoreCalculator()
        result = calc.calculate(run_state)
        assert "phase_success_rate" in result["components"]
        assert "error_rate" in result["components"]
        assert "artifact_quality" in result["components"]

    def test_duration_ratio(self, run_state):
        calc = HealthScoreCalculator()
        result = calc.calculate(run_state, expected_duration=30)
        assert "duration_ratio" in result["components"]

    def test_todo_effect(self, run_state):
        calc = HealthScoreCalculator()
        result_low = calc.calculate(run_state, todo_count=0)
        result_high = calc.calculate(run_state, todo_count=20)
        assert result_low["overall"] >= result_high["overall"]

    def test_grade_mapping(self):
        calc = HealthScoreCalculator()
        assert calc._grade(95) == "A"
        assert calc._grade(85) == "B"
        assert calc._grade(75) == "C"
        assert calc._grade(65) == "D"
        assert calc._grade(50) == "F"


# ─────────────────────────────────────────────
#  DD7: Platform State
# ─────────────────────────────────────────────

class TestPlatformState:

    def test_start_run(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        run = ps.start_run(target="fabric")
        assert run.status == "running"
        assert ps.current_run is run
        assert run.run_id in ps.runs

    def test_end_run(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        ps.start_run()
        run = ps.end_run("completed")
        assert run.status == "completed"

    def test_get_health(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        run = ps.start_run()
        run.record_phase("assessment", "ok", duration=5.0)
        health = ps.get_health()
        assert "overall" in health

    def test_get_dashboard_data(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        ps.start_run(target="fabric")
        ps.start_run(target="databricks")
        data = ps.get_dashboard_data()
        assert data["total_runs"] == 2

    def test_saves_state_to_disk(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        run = ps.start_run()
        state_file = tmp_path / "state" / f"run_{run.run_id}.json"
        assert state_file.exists()

    def test_no_current_run_health(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        health = ps.get_health()
        assert health["overall"] == 0


# ─────────────────────────────────────────────
#  DD8: Escalation Tier
# ─────────────────────────────────────────────

class TestEscalationTier:

    def test_create_tier(self):
        tier = EscalationTier(1, "INFO", channels=["log"])
        assert tier.tier_id == 1
        assert tier.name == "INFO"

    def test_tier_to_dict(self):
        tier = EscalationTier(2, "WARN", channels=["slack", "teams"], timeout=600)
        d = tier.to_dict()
        assert d["timeout"] == 600
        assert "slack" in d["channels"]


# ─────────────────────────────────────────────
#  DD8: Alert State
# ─────────────────────────────────────────────

class TestAlertState:

    def test_create_alert(self):
        alert = AlertState("ALT-0001", "Test alert", "high")
        assert alert.status == "open"
        assert alert.severity == "high"

    def test_escalate(self):
        alert = AlertState("ALT-0001", "Test", "high")
        alert.escalate()
        assert alert.current_tier == 2
        assert alert.status == "escalated"

    def test_acknowledge(self):
        alert = AlertState("ALT-0001", "Test", "high")
        alert.acknowledge()
        assert alert.status == "acknowledged"

    def test_resolve(self):
        alert = AlertState("ALT-0001", "Test", "high")
        alert.resolve("Fixed the issue")
        assert alert.status == "resolved"

    def test_to_dict(self):
        alert = AlertState("ALT-0001", "Test", "high")
        d = alert.to_dict()
        assert d["alert_id"] == "ALT-0001"
        assert d["status"] == "open"


# ─────────────────────────────────────────────
#  DD8: Alerting Orchestrator
# ─────────────────────────────────────────────

class TestAlertingOrchestrator:

    def test_create_alert(self):
        orch = AlertingOrchestrator()
        alert = orch.create_alert("SQL timeout", "high", "Phase failed")
        assert alert.alert_id == "ALT-0001"
        assert alert.current_tier == 3  # high → tier 3

    def test_create_alert_info(self):
        orch = AlertingOrchestrator()
        alert = orch.create_alert("Phase done", "info")
        assert alert.current_tier == 1

    def test_create_alert_critical(self):
        orch = AlertingOrchestrator()
        alert = orch.create_alert("OOM", "critical")
        assert alert.current_tier == 4

    def test_escalate_alert(self):
        orch = AlertingOrchestrator()
        alert = orch.create_alert("Slow query", "medium")
        escalated = orch.escalate_alert(alert.alert_id)
        assert escalated.current_tier > 2

    def test_acknowledge_alert(self):
        orch = AlertingOrchestrator()
        alert = orch.create_alert("Test", "high")
        orch.acknowledge_alert(alert.alert_id)
        assert alert.status == "acknowledged"

    def test_resolve_alert(self):
        orch = AlertingOrchestrator()
        alert = orch.create_alert("Test", "high")
        orch.resolve_alert(alert.alert_id, "Fixed")
        assert alert.status == "resolved"

    def test_get_open_alerts(self):
        orch = AlertingOrchestrator()
        orch.create_alert("A1", "high")
        orch.create_alert("A2", "medium")
        a3 = orch.create_alert("A3", "low")
        orch.resolve_alert(a3.alert_id)
        open_alerts = orch.get_open_alerts()
        assert len(open_alerts) == 2

    def test_get_alert_summary(self):
        orch = AlertingOrchestrator()
        orch.create_alert("A1", "high")
        orch.create_alert("A2", "critical")
        summary = orch.get_alert_summary()
        assert summary["total"] == 2
        assert summary["by_severity"]["high"] == 1

    def test_escalate_nonexistent(self):
        orch = AlertingOrchestrator()
        result = orch.escalate_alert("BOGUS")
        assert result is None

    def test_notifications_recorded(self):
        orch = AlertingOrchestrator()
        alert = orch.create_alert("Test", "medium", "Details here")
        assert len(alert.notifications) > 0


# ─────────────────────────────────────────────
#  DD9: SLO Tracker
# ─────────────────────────────────────────────

class TestSLOTracker:

    def test_check_ok(self, run_state, platform_config):
        tracker = SLOTracker(platform_config)
        result = tracker.check(run_state)
        assert result["overall_met"] is True
        assert result["details"]["phase_success_rate"]["met"] is True

    def test_check_failed(self, failed_run_state, platform_config):
        tracker = SLOTracker(platform_config)
        result = tracker.check(failed_run_state)
        assert result["overall_met"] is False

    def test_compliance_report(self, run_state, platform_config):
        tracker = SLOTracker(platform_config)
        tracker.check(run_state)
        report = tracker.get_compliance_report()
        assert report["total_checks"] == 1
        assert report["compliance_rate"] > 0

    def test_compliance_multiple_checks(self, run_state, failed_run_state, platform_config):
        tracker = SLOTracker(platform_config)
        tracker.check(run_state)
        tracker.check(failed_run_state)
        report = tracker.get_compliance_report()
        assert report["total_checks"] == 2
        assert 0 <= report["compliance_rate"] <= 1

    def test_empty_compliance(self):
        tracker = SLOTracker()
        report = tracker.get_compliance_report()
        assert report["total_checks"] == 0


# ─────────────────────────────────────────────
#  DD9: Status Page & Reports
# ─────────────────────────────────────────────

class TestStatusPageGeneration:

    def test_generate_status_page(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        run = ps.start_run()
        run.record_phase("assessment", "ok", duration=5.0)
        ps.end_run()

        orch = AlertingOrchestrator()
        orch.create_alert("Test", "medium")

        slo = SLOTracker()
        slo.check(run)

        output = str(tmp_path / "status.html")
        result = generate_status_page(ps, orch, slo, output_path=output)
        assert os.path.isfile(result)
        with open(result, encoding="utf-8") as f:
            html = f.read()
        assert "Migration Platform Status" in html
        assert "Health Score" in html

    def test_generate_platform_report(self, tmp_path):
        ps = PlatformState(state_dir=str(tmp_path / "state"))
        run = ps.start_run()
        run.record_phase("assessment", "ok", duration=5.0)
        ps.end_run()

        orch = AlertingOrchestrator()
        slo = SLOTracker()
        slo.check(run)

        output = str(tmp_path / "report.json")
        result = generate_platform_report(ps, orch, slo, output_path=output)
        assert "dashboard" in result
        assert "health" in result
        assert "slo" in result
        assert os.path.isfile(output)
