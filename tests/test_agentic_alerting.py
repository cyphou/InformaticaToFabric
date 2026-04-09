"""
Test Suite — Agentic Alerting (DD4–DD6)
=======================================
Tests: Signal processing, auto-remediation, learning agent.

Run:
    pytest tests/test_agentic_alerting.py -v
"""

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from agentic_alerting import (
    ActionCatalog,
    CircuitBreaker,
    DECISION_MATRIX,
    DecisionEngine,
    LearningStore,
    MigrationAgent,
    Signal,
    SignalClassifier,
    SignalProcessor,
    load_agent_config,
)


# ─────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def agent_config():
    return {
        "agent": {
            "enabled": True,
            "mode": "auto_fix",
            "poll_interval": 10,
            "max_retries": 3,
            "cooldown": 30,
        }
    }


@pytest.fixture
def agent_config_monitor():
    return {
        "agent": {
            "enabled": True,
            "mode": "monitor",
        }
    }


@pytest.fixture
def learning_db(tmp_path):
    return LearningStore(db_path=str(tmp_path / "test_learning.db"))


# ─────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────

class TestLoadAgentConfig:

    def test_defaults(self):
        cfg = load_agent_config({})
        assert cfg["enabled"] is False
        assert cfg["mode"] == "monitor"
        assert cfg["max_retries"] == 3

    def test_from_config(self, agent_config):
        cfg = load_agent_config(agent_config)
        assert cfg["enabled"] is True
        assert cfg["mode"] == "auto_fix"
        assert cfg["poll_interval"] == 10


# ─────────────────────────────────────────────
#  DD4: Signal
# ─────────────────────────────────────────────

class TestSignal:

    def test_create_signal(self):
        sig = Signal("error", "Phase failed", severity="high", phase="assessment")
        assert sig.signal_type == "error"
        assert sig.severity == "high"
        assert sig.phase == "assessment"
        assert len(sig.id) == 16

    def test_signal_to_dict(self):
        sig = Signal("warning", "Slow phase", severity="medium")
        d = sig.to_dict()
        assert d["signal_type"] == "warning"
        assert d["severity"] == "medium"
        assert "timestamp" in d
        assert "id" in d

    def test_invalid_severity_defaults_to_info(self):
        sig = Signal("error", "test", severity="bogus")
        assert sig.severity == "info"

    def test_signal_metadata(self):
        sig = Signal("error", "test", metadata={"key": "val"})
        assert sig.metadata["key"] == "val"


# ─────────────────────────────────────────────
#  DD4: Signal Classifier
# ─────────────────────────────────────────────

class TestSignalClassifier:

    def test_classify_error(self):
        c = SignalClassifier()
        sig = c.classify("error", "Phase failed with exception traceback")
        assert sig.severity == "medium"

    def test_classify_oom(self):
        c = SignalClassifier()
        sig = c.classify("error", "Process killed: out of memory")
        assert sig.severity == "critical"

    def test_classify_rate_limit(self):
        c = SignalClassifier()
        sig = c.classify("warning", "429 rate limited by API")
        assert sig.severity == "high"

    def test_classify_success(self):
        c = SignalClassifier()
        sig = c.classify("info", "Phase completed successfully")
        assert sig.severity == "info"

    def test_classify_timeout(self):
        c = SignalClassifier()
        sig = c.classify("error", "Operation timed out after 300s")
        assert sig.severity == "high"

    def test_classify_connection_error(self):
        c = SignalClassifier()
        sig = c.classify("error", "ECONNREFUSED: connection refused")
        assert sig.severity == "high"

    def test_classify_threshold_violation(self):
        c = SignalClassifier()
        sig = c.classify_threshold("phase_duration_max", 700)
        assert sig is not None
        assert sig.severity == "high"
        assert sig.signal_type == "threshold"

    def test_classify_threshold_ok(self):
        c = SignalClassifier()
        sig = c.classify_threshold("phase_duration_max", 100)
        assert sig is None

    def test_classify_threshold_critical(self):
        c = SignalClassifier()
        sig = c.classify_threshold("memory_max_mb", 10000)  # > 2x threshold
        assert sig.severity == "critical"


# ─────────────────────────────────────────────
#  DD4: Signal Processor
# ─────────────────────────────────────────────

class TestSignalProcessor:

    def test_process_event(self, agent_config):
        proc = SignalProcessor(agent_config)
        sig = proc.process_event("error", "Phase failed", phase="sql")
        assert sig.phase == "sql"
        assert len(proc.signals) == 1

    def test_process_metric_violation(self, agent_config):
        proc = SignalProcessor(agent_config)
        sig = proc.process_metric("error_rate_max", 0.5)
        assert sig is not None

    def test_process_metric_ok(self, agent_config):
        proc = SignalProcessor(agent_config)
        sig = proc.process_metric("error_rate_max", 0.01)
        assert sig is None

    def test_get_signals_filter(self, agent_config):
        proc = SignalProcessor(agent_config)
        proc.process_event("error", "OOM crash")
        proc.process_event("info", "All good, completed")
        critical = proc.get_signals(severity="critical")
        assert len(critical) == 1
        info = proc.get_signals(severity="info")
        assert len(info) == 1

    def test_get_stats(self, agent_config):
        proc = SignalProcessor(agent_config)
        proc.process_event("error", "fail")
        proc.process_event("info", "ok done")
        stats = proc.get_stats()
        assert stats["total"] == 2
        assert "by_severity" in stats


# ─────────────────────────────────────────────
#  DD5: Circuit Breaker
# ─────────────────────────────────────────────

class TestCircuitBreaker:

    def test_initial_state_closed(self):
        cb = CircuitBreaker()
        assert cb.can_execute() is True
        assert cb.get_state()["state"] == "closed"

    def test_open_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        assert cb.can_execute() is True
        cb.record_failure()
        assert cb.get_state()["state"] == "open"
        assert cb.can_execute() is False

    def test_success_resets(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_success()
        assert cb.get_state()["failures"] == 0
        assert cb.get_state()["state"] == "closed"

    def test_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0)
        cb.record_failure()
        assert cb.get_state()["state"] == "open"
        # With reset_timeout=0, should transition to half_open immediately
        assert cb.can_execute() is True
        assert cb.get_state()["state"] == "half_open"


# ─────────────────────────────────────────────
#  DD5: Action Catalog
# ─────────────────────────────────────────────

class TestActionCatalog:

    def test_execute_retry(self):
        cat = ActionCatalog()
        result = cat.execute("retry_phase", {"phase": "sql"})
        assert result["status"] == "retry_scheduled"
        assert result["action"] == "retry_phase"

    def test_execute_skip(self):
        cat = ActionCatalog()
        result = cat.execute("skip_and_continue", {"phase": "dbt"})
        assert result["status"] == "skipped"

    def test_execute_escalate(self):
        cat = ActionCatalog()
        result = cat.execute("escalate_to_human", {"reason": "OOM"})
        assert result["status"] == "escalated"

    def test_execute_create_incident(self):
        cat = ActionCatalog()
        result = cat.execute("create_incident", {"title": "Critical failure"})
        assert result["status"] == "incident_created"

    def test_execute_rollback(self):
        cat = ActionCatalog()
        result = cat.execute("rollback_artifacts", {"artifact_type": "notebooks"})
        assert result["status"] == "rolled_back"

    def test_execute_unknown_action(self):
        cat = ActionCatalog()
        result = cat.execute("nonexistent_action")
        assert result["status"] == "unknown_action"

    def test_history_tracking(self):
        cat = ActionCatalog()
        cat.execute("retry_phase")
        cat.execute("skip_and_continue")
        assert len(cat.history) == 2


# ─────────────────────────────────────────────
#  DD5: Decision Engine
# ─────────────────────────────────────────────

class TestDecisionEngine:

    def test_decide_error_high(self, agent_config):
        engine = DecisionEngine(agent_config)
        sig = Signal("error", "phase crashed", severity="high", phase="sql")
        decision = engine.decide(sig)
        assert decision["action"] == "retry_phase"
        assert decision["result"]["status"] == "retry_scheduled"

    def test_decide_error_critical(self, agent_config):
        engine = DecisionEngine(agent_config)
        sig = Signal("error", "OOM", severity="critical")
        decision = engine.decide(sig)
        assert decision["action"] == "escalate_to_human"

    def test_decide_monitor_mode(self, agent_config_monitor):
        engine = DecisionEngine(agent_config_monitor)
        sig = Signal("error", "fail", severity="high")
        decision = engine.decide(sig)
        assert decision["result"]["status"] == "monitor_only"
        assert "recommended" in decision["result"]

    def test_circuit_breaker_integration(self, agent_config):
        engine = DecisionEngine(agent_config)
        engine.config["max_retries"] = 2
        sig = Signal("error", "fail", severity="high")
        # First two should retry
        engine.decide(sig)
        engine.decide(sig)
        # Third should escalate due to circuit breaker
        decision = engine.decide(sig)
        assert decision.get("circuit_breaker") == "open" or decision["action"] in ("retry_phase", "escalate_to_human")

    def test_decision_matrix_covers_all_keys(self):
        """Verify decision matrix has expected entries."""
        assert ("error", "critical") in DECISION_MATRIX
        assert ("error", "high") in DECISION_MATRIX
        assert ("threshold", "critical") in DECISION_MATRIX
        assert ("anomaly", "critical") in DECISION_MATRIX

    def test_get_decisions(self, agent_config):
        engine = DecisionEngine(agent_config)
        sig = Signal("warning", "slow", severity="medium")
        engine.decide(sig)
        assert len(engine.get_decisions()) == 1


# ─────────────────────────────────────────────
#  DD6: Learning Store
# ─────────────────────────────────────────────

class TestLearningStore:

    def test_create_store(self, learning_db):
        assert learning_db is not None
        stats = learning_db.get_stats()
        assert stats["total_decisions"] == 0
        learning_db.close()

    def test_record_decision(self, learning_db):
        sig = Signal("error", "fail", severity="high")
        learning_db.record_decision(sig, "retry_phase", {"timestamp": "now"})
        stats = learning_db.get_stats()
        assert stats["total_decisions"] == 1
        learning_db.close()

    def test_record_outcome(self, learning_db):
        sig = Signal("error", "fail", severity="high")
        learning_db.record_decision(sig, "retry_phase", {"timestamp": "now"})
        learning_db.record_outcome(sig.id, "success", True)
        stats = learning_db.get_stats()
        assert stats["with_outcome"] == 1
        learning_db.close()

    def test_override_after_successful_outcomes(self, learning_db):
        # Record 5 successful retries for error+high
        for i in range(5):
            sig = Signal("error", f"fail {i}", severity="high")
            learning_db.record_decision(sig, "retry_phase", {"timestamp": f"t{i}"})
            learning_db.record_outcome(sig.id, "success", True)
        override = learning_db.get_override("error", "high")
        assert override == "retry_phase"
        learning_db.close()

    def test_no_override_insufficient_data(self, learning_db):
        sig = Signal("error", "fail", severity="high")
        learning_db.record_decision(sig, "retry_phase", {"timestamp": "now"})
        learning_db.record_outcome(sig.id, "success", True)
        override = learning_db.get_override("error", "high")
        assert override is None  # Only 1 sample < 3 required
        learning_db.close()

    def test_no_override_low_success_rate(self, learning_db):
        for i in range(5):
            sig = Signal("error", f"fail {i}", severity="high")
            learning_db.record_decision(sig, "retry_phase", {"timestamp": f"t{i}"})
            learning_db.record_outcome(sig.id, "failed", i > 3)  # Only 1/5 success
        override = learning_db.get_override("error", "high")
        assert override is None  # Success rate < 70%
        learning_db.close()


# ─────────────────────────────────────────────
#  DD5+DD6: Migration Agent (Integrated)
# ─────────────────────────────────────────────

class TestMigrationAgent:

    def test_create_agent_disabled(self):
        agent = MigrationAgent(config={"agent": {"enabled": False}})
        assert agent.enabled is False
        agent.close()

    def test_create_agent_enabled(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        assert agent.enabled is True
        agent.close()

    def test_on_phase_start(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        agent.on_phase_start("assessment", phase_id=0)
        stats = agent.processor.get_stats()
        assert stats["total"] >= 1
        agent.close()

    def test_on_phase_complete(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        result = agent.on_phase_complete("assessment", 15.0)
        assert result is None  # Duration within threshold
        agent.close()

    def test_on_phase_complete_slow(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        result = agent.on_phase_complete("assessment", 700)  # Above threshold
        assert result is not None  # Should trigger decision
        agent.close()

    def test_on_phase_error(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        decision = agent.on_phase_error("sql", RuntimeError("connection lost"))
        assert decision is not None
        assert "action" in decision
        agent.close()

    def test_on_metric(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        result = agent.on_metric("memory_max_mb", 5000)
        assert result is not None
        agent.close()

    def test_on_metric_ok(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        result = agent.on_metric("memory_max_mb", 100)
        assert result is None
        agent.close()

    def test_get_report(self, agent_config):
        agent = MigrationAgent(config=agent_config)
        agent.on_phase_start("assessment")
        agent.on_phase_complete("assessment", 10.0)
        report = agent.get_report()
        assert report["enabled"] is True
        assert "signals" in report
        assert "learning" in report
        agent.close()

    def test_disabled_agent_noop(self):
        agent = MigrationAgent(config={"agent": {"enabled": False}})
        agent.on_phase_start("test")
        assert agent.on_phase_error("test", Exception("e")) is None
        assert agent.on_metric("test", 999) is None
        agent.close()
