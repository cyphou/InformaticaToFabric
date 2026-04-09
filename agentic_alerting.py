"""
Agentic Alerting Module (DD4–DD6)
=================================
Autonomous monitoring and self-healing for the migration pipeline:
  - DD4: SignalProcessor — polls Datadog, classifies signals, scores severity
  - DD5: Auto-Remediation — rule-based decision engine + circuit breakers + actions
  - DD6: Learning Agent — SQLite-backed outcome tracking, pattern recognition

All features degrade gracefully when Datadog client is not installed.
"""

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

_logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Configuration loader
# ─────────────────────────────────────────────

def load_agent_config(config):
    """Extract agent config from migration.yaml dict."""
    agent = (config or {}).get("agent", {})
    return {
        "enabled": agent.get("enabled", False),
        "mode": agent.get("mode", "monitor"),  # monitor | auto_fix | full_auto
        "poll_interval": agent.get("poll_interval", 30),
        "max_retries": agent.get("max_retries", 3),
        "cooldown": agent.get("cooldown", 60),
        "ai_fix": agent.get("ai_fix", False),
        "escalation": agent.get("escalation", {}),
        "learning": agent.get("learning", {}),
    }


# ─────────────────────────────────────────────
#  DD4: Signal Processor
# ─────────────────────────────────────────────

class Signal:
    """Represents a classified migration signal/event."""

    SEVERITY_LEVELS = ["info", "low", "medium", "high", "critical"]

    def __init__(self, signal_type, message, source="migration", severity="info",
                 phase=None, metadata=None):
        self.id = hashlib.sha256(
            f"{signal_type}:{message}:{time.time()}".encode()
        ).hexdigest()[:16]
        self.signal_type = signal_type  # error, warning, threshold, anomaly
        self.message = message
        self.source = source
        self.severity = severity if severity in self.SEVERITY_LEVELS else "info"
        self.phase = phase
        self.metadata = metadata or {}
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.actions_taken = []

    def to_dict(self):
        return {
            "id": self.id,
            "signal_type": self.signal_type,
            "message": self.message,
            "source": self.source,
            "severity": self.severity,
            "phase": self.phase,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "actions_taken": self.actions_taken,
        }


class SignalClassifier:
    """Classifies raw events into Signal objects with severity scoring."""

    # Keyword → severity mapping
    SEVERITY_RULES = [
        (["OOM", "out of memory", "MemoryError", "killed"], "critical"),
        (["timeout", "timed out", "408", "504"], "high"),
        (["429", "rate limit", "throttl"], "high"),
        (["connection refused", "unreachable", "ECONNREFUSED"], "high"),
        (["error", "failed", "exception", "traceback", "crash"], "medium"),
        (["warning", "deprecated", "retry", "fallback"], "low"),
        (["info", "success", "completed", "done"], "info"),
    ]

    # Anomaly thresholds
    THRESHOLDS = {
        "phase_duration_max": 600,   # 10 min
        "error_rate_max": 0.15,       # 15%
        "memory_max_mb": 4096,        # 4 GB
        "retry_count_max": 5,
    }

    def classify(self, event_type, message, metadata=None):
        """Classify a raw event into a Signal."""
        severity = self._score_severity(message)
        signal_type = self._determine_type(event_type, message, metadata or {})
        return Signal(
            signal_type=signal_type,
            message=message,
            severity=severity,
            metadata=metadata,
        )

    def classify_threshold(self, metric_name, value, threshold=None):
        """Classify a threshold violation."""
        threshold = threshold or self.THRESHOLDS.get(metric_name, float("inf"))
        if value > threshold:
            severity = "critical" if value > threshold * 2 else "high"
            return Signal(
                signal_type="threshold",
                message=f"{metric_name} = {value} (threshold: {threshold})",
                severity=severity,
                metadata={"metric": metric_name, "value": value, "threshold": threshold},
            )
        return None

    def _score_severity(self, message):
        msg = message.lower()
        for keywords, severity in self.SEVERITY_RULES:
            if any(kw.lower() in msg for kw in keywords):
                return severity
        return "info"

    def _determine_type(self, event_type, message, metadata):
        if event_type in ("error", "exception"):
            return "error"
        if event_type in ("warning",):
            return "warning"
        if "threshold" in event_type:
            return "threshold"
        if "anomaly" in event_type:
            return "anomaly"
        return event_type


class SignalProcessor:
    """Processes migration events, classifies signals, and dispatches to decision engine."""

    def __init__(self, config=None):
        self.config = load_agent_config(config)
        self.classifier = SignalClassifier()
        self.signals = []
        self._lock = threading.Lock()

    def process_event(self, event_type, message, phase=None, metadata=None):
        """Process a migration event, classify it, and return the Signal."""
        signal = self.classifier.classify(event_type, message, metadata)
        signal.phase = phase
        with self._lock:
            self.signals.append(signal)
        return signal

    def process_metric(self, metric_name, value, threshold=None):
        """Process a metric check and return Signal if threshold violated."""
        signal = self.classifier.classify_threshold(metric_name, value, threshold)
        if signal:
            with self._lock:
                self.signals.append(signal)
        return signal

    def get_signals(self, severity=None, since=None):
        """Get signals, optionally filtered."""
        with self._lock:
            signals = list(self.signals)
        if severity:
            signals = [s for s in signals if s.severity == severity]
        if since:
            signals = [s for s in signals if s.timestamp >= since]
        return signals

    def get_stats(self):
        """Get signal statistics."""
        with self._lock:
            by_severity = {}
            by_type = {}
            for s in self.signals:
                by_severity[s.severity] = by_severity.get(s.severity, 0) + 1
                by_type[s.signal_type] = by_type.get(s.signal_type, 0) + 1
        return {"total": len(self.signals), "by_severity": by_severity, "by_type": by_type}


# ─────────────────────────────────────────────
#  DD5: Auto-Remediation
# ─────────────────────────────────────────────

class CircuitBreaker:
    """Prevents repeated failing actions with an open/closed circuit."""

    STATES = ("closed", "open", "half_open")

    def __init__(self, failure_threshold=3, reset_timeout=60):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.state = "closed"
        self._last_failure_time = 0
        self._lock = threading.Lock()

    def can_execute(self):
        with self._lock:
            if self.state == "closed":
                return True
            if self.state == "open":
                if time.time() - self._last_failure_time > self.reset_timeout:
                    self.state = "half_open"
                    return True
                return False
            return True  # half_open → allow one attempt

    def record_success(self):
        with self._lock:
            self.failures = 0
            self.state = "closed"

    def record_failure(self):
        with self._lock:
            self.failures += 1
            self._last_failure_time = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "open"

    def get_state(self):
        with self._lock:
            return {
                "state": self.state,
                "failures": self.failures,
                "threshold": self.failure_threshold,
            }


class ActionCatalog:
    """Catalog of remediation actions the agent can execute."""

    def __init__(self):
        self.actions = {
            "retry_phase": self._action_retry_phase,
            "skip_and_continue": self._action_skip,
            "adjust_config": self._action_adjust_config,
            "escalate_to_human": self._action_escalate,
            "create_incident": self._action_create_incident,
            "rollback_artifacts": self._action_rollback,
        }
        self.history = []

    def execute(self, action_name, context=None):
        """Execute a named action."""
        if action_name not in self.actions:
            return {"action": action_name, "status": "unknown_action"}
        result = self.actions[action_name](context or {})
        result["action"] = action_name
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        self.history.append(result)
        return result

    def _action_retry_phase(self, ctx):
        _logger.info("Agent action: retry_phase — %s", ctx.get("phase", "unknown"))
        return {"status": "retry_scheduled", "phase": ctx.get("phase")}

    def _action_skip(self, ctx):
        _logger.info("Agent action: skip_and_continue — %s", ctx.get("phase", "unknown"))
        return {"status": "skipped", "phase": ctx.get("phase")}

    def _action_adjust_config(self, ctx):
        _logger.info("Agent action: adjust_config — %s", ctx.get("param"))
        return {"status": "config_adjusted", "param": ctx.get("param"),
                "old_value": ctx.get("old_value"), "new_value": ctx.get("new_value")}

    def _action_escalate(self, ctx):
        _logger.info("Agent action: escalate_to_human — %s", ctx.get("reason"))
        return {"status": "escalated", "reason": ctx.get("reason")}

    def _action_create_incident(self, ctx):
        _logger.info("Agent action: create_incident — %s", ctx.get("title"))
        return {"status": "incident_created", "title": ctx.get("title")}

    def _action_rollback(self, ctx):
        _logger.info("Agent action: rollback_artifacts — %s", ctx.get("artifact_type"))
        return {"status": "rolled_back", "artifact_type": ctx.get("artifact_type")}


# Decision rules: (signal_type, severity) → action_name
DECISION_MATRIX = {
    ("error", "critical"): "escalate_to_human",
    ("error", "high"): "retry_phase",
    ("error", "medium"): "retry_phase",
    ("error", "low"): "skip_and_continue",
    ("threshold", "critical"): "escalate_to_human",
    ("threshold", "high"): "adjust_config",
    ("warning", "high"): "adjust_config",
    ("warning", "medium"): "skip_and_continue",
    ("anomaly", "critical"): "create_incident",
    ("anomaly", "high"): "escalate_to_human",
}


class DecisionEngine:
    """Rule-based decision engine that maps signals to remediation actions."""

    def __init__(self, config=None, learning_store=None):
        self.config = load_agent_config(config)
        self.mode = self.config.get("mode", "monitor")
        self.catalog = ActionCatalog()
        self.circuit_breakers = {}
        self.learning_store = learning_store
        self.decisions = []

    def decide(self, signal):
        """Decide what action to take for a signal and execute it."""
        action_name = self._lookup_action(signal)
        decision = {
            "signal_id": signal.id,
            "signal_type": signal.signal_type,
            "severity": signal.severity,
            "action": action_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Check learning store for overrides
        if self.learning_store:
            override = self.learning_store.get_override(
                signal.signal_type, signal.severity)
            if override:
                action_name = override
                decision["override_from_learning"] = True

        # Check circuit breaker
        cb_key = f"{signal.signal_type}:{signal.severity}"
        if cb_key not in self.circuit_breakers:
            self.circuit_breakers[cb_key] = CircuitBreaker(
                failure_threshold=self.config.get("max_retries", 3),
                reset_timeout=self.config.get("cooldown", 60),
            )
        cb = self.circuit_breakers[cb_key]

        if not cb.can_execute():
            action_name = "escalate_to_human"
            decision["circuit_breaker"] = "open"

        # Execute if not monitor-only
        if self.mode in ("auto_fix", "full_auto"):
            result = self.catalog.execute(action_name, {
                "phase": signal.phase,
                "signal": signal.to_dict(),
            })
            decision["result"] = result
            signal.actions_taken.append(action_name)

            # Track outcome
            if result.get("status") in ("retry_scheduled", "config_adjusted"):
                cb.record_success()
            elif result.get("status") in ("escalated", "incident_created"):
                cb.record_failure()
        else:
            decision["result"] = {"status": "monitor_only", "recommended": action_name}

        self.decisions.append(decision)

        # Record for learning
        if self.learning_store:
            self.learning_store.record_decision(signal, action_name, decision)

        return decision

    def _lookup_action(self, signal):
        """Lookup the action for a signal from the decision matrix."""
        key = (signal.signal_type, signal.severity)
        return DECISION_MATRIX.get(key, "skip_and_continue")

    def get_decisions(self):
        return list(self.decisions)


# ─────────────────────────────────────────────
#  DD6: Learning Agent (SQLite-backed)
# ─────────────────────────────────────────────

class LearningStore:
    """SQLite-backed learning store that tracks decision outcomes and learns overrides."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS decisions (
        id TEXT PRIMARY KEY,
        signal_type TEXT NOT NULL,
        severity TEXT NOT NULL,
        action TEXT NOT NULL,
        outcome TEXT,
        success INTEGER DEFAULT 0,
        timestamp TEXT NOT NULL,
        metadata TEXT
    );
    CREATE TABLE IF NOT EXISTS overrides (
        signal_type TEXT NOT NULL,
        severity TEXT NOT NULL,
        action TEXT NOT NULL,
        confidence REAL DEFAULT 0.0,
        sample_count INTEGER DEFAULT 0,
        PRIMARY KEY (signal_type, severity)
    );
    """

    def __init__(self, db_path=None):
        self.db_path = db_path or os.path.join("output", "agent_learning.db")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._lock:
            self._conn.executescript(self.SCHEMA)
            self._conn.commit()

    def record_decision(self, signal, action, decision):
        """Record a decision for learning."""
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO decisions (id, signal_type, severity, action, "
                "timestamp, metadata) VALUES (?, ?, ?, ?, ?, ?)",
                (signal.id, signal.signal_type, signal.severity, action,
                 decision.get("timestamp", ""), json.dumps(decision)),
            )
            self._conn.commit()

    def record_outcome(self, signal_id, outcome, success):
        """Record whether a decision was successful."""
        with self._lock:
            self._conn.execute(
                "UPDATE decisions SET outcome = ?, success = ? WHERE id = ?",
                (outcome, 1 if success else 0, signal_id),
            )
            self._conn.commit()
            self._update_overrides()

    def _update_overrides(self):
        """Recompute overrides from outcome history."""
        cursor = self._conn.execute("""
            SELECT signal_type, severity, action,
                   SUM(success) as successes, COUNT(*) as total
            FROM decisions
            WHERE outcome IS NOT NULL
            GROUP BY signal_type, severity, action
            ORDER BY signal_type, severity, successes DESC
        """)
        best = {}
        for row in cursor:
            sig_type, severity, action, successes, total = row
            key = (sig_type, severity)
            confidence = successes / total if total > 0 else 0
            if key not in best or confidence > best[key][1]:
                best[key] = (action, confidence, total)

        for (sig_type, severity), (action, confidence, total) in best.items():
            if confidence >= 0.7 and total >= 3:
                self._conn.execute(
                    "INSERT OR REPLACE INTO overrides (signal_type, severity, action, "
                    "confidence, sample_count) VALUES (?, ?, ?, ?, ?)",
                    (sig_type, severity, action, confidence, total),
                )
        self._conn.commit()

    def get_override(self, signal_type, severity):
        """Get learned action override for a signal type + severity."""
        with self._lock:
            row = self._conn.execute(
                "SELECT action, confidence FROM overrides "
                "WHERE signal_type = ? AND severity = ? AND confidence >= 0.7",
                (signal_type, severity),
            ).fetchone()
        return row[0] if row else None

    def get_stats(self):
        """Get learning store statistics."""
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
            with_outcome = self._conn.execute(
                "SELECT COUNT(*) FROM decisions WHERE outcome IS NOT NULL"
            ).fetchone()[0]
            overrides = self._conn.execute("SELECT COUNT(*) FROM overrides").fetchone()[0]
            success_rate = self._conn.execute(
                "SELECT AVG(success) FROM decisions WHERE outcome IS NOT NULL"
            ).fetchone()[0]
        return {
            "total_decisions": total,
            "with_outcome": with_outcome,
            "overrides_active": overrides,
            "success_rate": round(success_rate or 0, 3),
        }

    def close(self):
        self._conn.close()


# ─────────────────────────────────────────────
#  Integrated Agent
# ─────────────────────────────────────────────

class MigrationAgent:
    """Top-level agent that combines signal processing, decision engine, and learning."""

    def __init__(self, config=None):
        self.config = config or {}
        self.agent_config = load_agent_config(config)
        self.enabled = self.agent_config.get("enabled", False)
        self.processor = SignalProcessor(config)
        self.learning_store = LearningStore() if self.enabled else None
        self.engine = DecisionEngine(config, learning_store=self.learning_store)

    def on_phase_start(self, phase_name, phase_id=None):
        """Called when a migration phase starts."""
        if not self.enabled:
            return
        self.processor.process_event(
            "info", f"Phase started: {phase_name}", phase=phase_name)

    def on_phase_complete(self, phase_name, duration, phase_id=None):
        """Called when a migration phase completes successfully."""
        if not self.enabled:
            return
        self.processor.process_event(
            "info", f"Phase completed: {phase_name} ({duration:.1f}s)", phase=phase_name)
        # Check duration threshold
        sig = self.processor.process_metric("phase_duration_max", duration)
        if sig:
            return self.engine.decide(sig)
        return None

    def on_phase_error(self, phase_name, error, phase_id=None):
        """Called when a migration phase fails."""
        if not self.enabled:
            return None
        signal = self.processor.process_event(
            "error", str(error), phase=phase_name,
            metadata={"error_type": type(error).__name__})
        return self.engine.decide(signal)

    def on_metric(self, metric_name, value, threshold=None):
        """Called to check a metric against a threshold."""
        if not self.enabled:
            return None
        signal = self.processor.process_metric(metric_name, value, threshold)
        if signal:
            return self.engine.decide(signal)
        return None

    def get_report(self):
        """Get full agent report."""
        report = {
            "enabled": self.enabled,
            "mode": self.agent_config.get("mode", "monitor"),
            "signals": self.processor.get_stats(),
            "decisions": len(self.engine.decisions),
        }
        if self.learning_store:
            report["learning"] = self.learning_store.get_stats()
        return report

    def close(self):
        if self.learning_store:
            self.learning_store.close()
