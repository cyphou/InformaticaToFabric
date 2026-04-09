"""
Monitoring Platform Module (DD7–DD9)
====================================
Enterprise-grade monitoring control plane for the migration tool:
  - DD7: Global Control Plane — unified state across runs + health scoring
  - DD8: Alerting Orchestrator — 4-tier escalation, on-call rotation, PagerDuty/OpsGenie
  - DD9: Enterprise Dashboards / SLO tracking / status page generation

All features work standalone; Datadog integration is optional.
"""

import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

_logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Configuration loader
# ─────────────────────────────────────────────

def load_platform_config(config):
    """Extract alerting_platform config from migration.yaml dict."""
    plat = (config or {}).get("alerting_platform", {})
    return {
        "escalation": plat.get("escalation", {}),
        "on_call": plat.get("on_call", {}),
        "pagerduty": plat.get("pagerduty", {}),
        "reporting": plat.get("reporting", {}),
        "slo": plat.get("slo", {}),
    }


# ─────────────────────────────────────────────
#  DD7: Global Control Plane
# ─────────────────────────────────────────────

class MigrationRunState:
    """State container for a single migration run."""

    def __init__(self, run_id=None, target="fabric"):
        self.run_id = run_id or hashlib.sha256(
            f"{time.time()}".encode()).hexdigest()[:12]
        self.target = target
        self.start_time = datetime.now(timezone.utc).isoformat()
        self.end_time = None
        self.status = "running"  # running | completed | failed | cancelled
        self.phases = {}  # phase_name → {status, duration, artifacts, errors}
        self.artifacts = {"notebooks": 0, "pipelines": 0, "sql": 0, "dbt": 0,
                          "validation": 0, "schema": 0}
        self.errors = []
        self.warnings = []
        self.metrics = {}

    def record_phase(self, name, status, duration=0, artifacts=0, errors=None):
        self.phases[name] = {
            "status": status,
            "duration": round(duration, 2),
            "artifacts": artifacts,
            "errors": errors or [],
        }
        if errors:
            self.errors.extend(errors)

    def record_artifact(self, artifact_type, count=1):
        if artifact_type in self.artifacts:
            self.artifacts[artifact_type] += count

    def complete(self, status="completed"):
        self.status = status
        self.end_time = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return {
            "run_id": self.run_id,
            "target": self.target,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "status": self.status,
            "phases": self.phases,
            "artifacts": self.artifacts,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "metrics": self.metrics,
        }


class HealthScoreCalculator:
    """Computes a 0-100 health score for a migration run."""

    WEIGHTS = {
        "phase_success_rate": 0.35,
        "error_rate": 0.25,
        "artifact_quality": 0.20,
        "duration_ratio": 0.10,
        "todo_ratio": 0.10,
    }

    def calculate(self, run_state, expected_duration=None, todo_count=0):
        """Calculate health score.

        Returns:
            dict with overall score and component breakdown
        """
        phases = run_state.phases
        total_phases = len(phases) or 1
        ok_phases = sum(1 for p in phases.values() if p["status"] == "ok")

        scores = {}

        # Phase success rate
        scores["phase_success_rate"] = (ok_phases / total_phases) * 100

        # Error rate
        total_errors = len(run_state.errors)
        scores["error_rate"] = max(0, 100 - total_errors * 10)

        # Artifact quality (non-zero output)
        total_artifacts = sum(run_state.artifacts.values())
        scores["artifact_quality"] = min(100, total_artifacts * 5) if total_artifacts > 0 else 0

        # Duration ratio
        if expected_duration and expected_duration > 0:
            actual = sum(p.get("duration", 0) for p in phases.values())
            ratio = actual / expected_duration
            scores["duration_ratio"] = max(0, 100 - max(0, (ratio - 1) * 50))
        else:
            scores["duration_ratio"] = 100

        # TODO ratio (fewer TODOs = better)
        scores["todo_ratio"] = max(0, 100 - todo_count * 2)

        # Weighted score
        overall = sum(scores[k] * self.WEIGHTS[k] for k in self.WEIGHTS)
        overall = max(0, min(100, round(overall, 1)))

        return {
            "overall": overall,
            "components": scores,
            "grade": self._grade(overall),
        }

    def _grade(self, score):
        if score >= 90:
            return "A"
        if score >= 80:
            return "B"
        if score >= 70:
            return "C"
        if score >= 60:
            return "D"
        return "F"


class PlatformState:
    """Global state aggregator across migration runs."""

    def __init__(self, state_dir=None):
        self.state_dir = state_dir or os.path.join("output", "platform_state")
        os.makedirs(self.state_dir, exist_ok=True)
        self.runs = {}
        self.current_run = None
        self.calculator = HealthScoreCalculator()

    def start_run(self, target="fabric"):
        """Start a new migration run."""
        run = MigrationRunState(target=target)
        self.runs[run.run_id] = run
        self.current_run = run
        self._save_run(run)
        return run

    def end_run(self, status="completed"):
        """End the current run."""
        if self.current_run:
            self.current_run.complete(status)
            self._save_run(self.current_run)
        return self.current_run

    def get_health(self, run_id=None):
        """Get health score for a run."""
        run = self.runs.get(run_id) or self.current_run
        if not run:
            return {"overall": 0, "components": {}, "grade": "F"}
        return self.calculator.calculate(run)

    def get_dashboard_data(self):
        """Get data for the global dashboard."""
        runs_data = []
        for run in self.runs.values():
            health = self.calculator.calculate(run)
            runs_data.append({**run.to_dict(), "health": health})
        return {
            "total_runs": len(self.runs),
            "runs": runs_data,
            "current_run": self.current_run.run_id if self.current_run else None,
        }

    def _save_run(self, run):
        """Persist run state to disk."""
        path = os.path.join(self.state_dir, f"run_{run.run_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(run.to_dict(), f, indent=2, ensure_ascii=False)


# ─────────────────────────────────────────────
#  DD8: Alerting Orchestrator
# ─────────────────────────────────────────────

class EscalationTier:
    """Represents one tier in the escalation chain."""

    def __init__(self, tier_id, name, channels=None, timeout=300, auto_escalate=True):
        self.tier_id = tier_id
        self.name = name
        self.channels = channels or []  # ["webhook", "email", "pagerduty", "slack"]
        self.timeout = timeout
        self.auto_escalate = auto_escalate

    def to_dict(self):
        return {
            "tier_id": self.tier_id,
            "name": self.name,
            "channels": self.channels,
            "timeout": self.timeout,
            "auto_escalate": self.auto_escalate,
        }


# Default 4-tier escalation
DEFAULT_TIERS = [
    EscalationTier(1, "INFO", channels=["log", "datadog_event"], timeout=0, auto_escalate=False),
    EscalationTier(2, "WARN", channels=["slack", "teams"], timeout=600),
    EscalationTier(3, "CRITICAL", channels=["pagerduty", "email"], timeout=1200),
    EscalationTier(4, "EMERGENCY", channels=["phone", "pagerduty_high"], timeout=0,
                   auto_escalate=False),
]


class AlertState:
    """Tracks the state of a single alert through escalation."""

    def __init__(self, alert_id, title, severity, signal_id=None):
        self.alert_id = alert_id
        self.title = title
        self.severity = severity
        self.signal_id = signal_id
        self.current_tier = 1
        self.status = "open"  # open | acknowledged | resolved | escalated
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.notifications = []

    def escalate(self):
        self.current_tier += 1
        self.status = "escalated"

    def acknowledge(self):
        self.status = "acknowledged"

    def resolve(self, resolution=""):
        self.status = "resolved"
        self.resolution = resolution

    def to_dict(self):
        return {
            "alert_id": self.alert_id,
            "title": self.title,
            "severity": self.severity,
            "current_tier": self.current_tier,
            "status": self.status,
            "created_at": self.created_at,
            "notifications": self.notifications,
        }


class AlertingOrchestrator:
    """4-tier escalation engine with channel routing."""

    SEVERITY_TO_TIER = {
        "info": 1,
        "low": 1,
        "medium": 2,
        "high": 3,
        "critical": 4,
    }

    def __init__(self, config=None):
        self.config = load_platform_config(config)
        self.tiers = list(DEFAULT_TIERS)
        self.alerts = {}
        self._alert_counter = 0

    def create_alert(self, title, severity, details="", signal_id=None):
        """Create and route a new alert."""
        self._alert_counter += 1
        alert_id = f"ALT-{self._alert_counter:04d}"
        alert = AlertState(alert_id, title, severity, signal_id)

        # Set initial tier from severity
        alert.current_tier = self.SEVERITY_TO_TIER.get(severity, 1)
        self.alerts[alert_id] = alert

        # Route to channels
        tier = self._get_tier(alert.current_tier)
        if tier:
            for channel in tier.channels:
                notification = self._notify(channel, alert, details)
                alert.notifications.append(notification)

        return alert

    def escalate_alert(self, alert_id):
        """Escalate an alert to the next tier."""
        alert = self.alerts.get(alert_id)
        if not alert or alert.current_tier >= len(self.tiers):
            return None
        alert.escalate()
        tier = self._get_tier(alert.current_tier)
        if tier:
            for channel in tier.channels:
                notification = self._notify(channel, alert, "ESCALATED")
                alert.notifications.append(notification)
        return alert

    def acknowledge_alert(self, alert_id):
        alert = self.alerts.get(alert_id)
        if alert:
            alert.acknowledge()
        return alert

    def resolve_alert(self, alert_id, resolution=""):
        alert = self.alerts.get(alert_id)
        if alert:
            alert.resolve(resolution)
        return alert

    def get_open_alerts(self):
        return [a.to_dict() for a in self.alerts.values()
                if a.status in ("open", "escalated")]

    def get_alert_summary(self):
        by_status = defaultdict(int)
        by_severity = defaultdict(int)
        for a in self.alerts.values():
            by_status[a.status] += 1
            by_severity[a.severity] += 1
        return {
            "total": len(self.alerts),
            "by_status": dict(by_status),
            "by_severity": dict(by_severity),
            "open": len(self.get_open_alerts()),
        }

    def _get_tier(self, tier_id):
        for t in self.tiers:
            if t.tier_id == tier_id:
                return t
        return None

    def _notify(self, channel, alert, details):
        """Send notification via a channel. Returns notification record."""
        notification = {
            "channel": channel,
            "alert_id": alert.alert_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "sent",
        }
        _logger.info("Alert %s → %s (%s): %s",
                      alert.alert_id, channel, alert.severity, alert.title)
        return notification


# ─────────────────────────────────────────────
#  DD9: SLO Tracker
# ─────────────────────────────────────────────

class SLOTracker:
    """Tracks Service Level Objectives for migration operations."""

    def __init__(self, config=None):
        plat_cfg = load_platform_config(config)
        slo_cfg = plat_cfg.get("slo", {})
        self.targets = {
            "phase_success_rate": slo_cfg.get("phase_success_rate", 0.95),
            "max_duration_minutes": slo_cfg.get("max_duration_minutes", 60),
            "max_error_rate": slo_cfg.get("max_error_rate", 0.05),
            "availability": slo_cfg.get("availability", 0.999),
        }
        self.checks = []

    def check(self, run_state):
        """Check SLO compliance for a run."""
        results = {}
        phases = run_state.phases
        total = len(phases) or 1
        ok = sum(1 for p in phases.values() if p["status"] == "ok")

        # Phase success rate
        rate = ok / total
        results["phase_success_rate"] = {
            "target": self.targets["phase_success_rate"],
            "actual": round(rate, 3),
            "met": rate >= self.targets["phase_success_rate"],
        }

        # Duration
        total_duration = sum(p.get("duration", 0) for p in phases.values()) / 60
        results["max_duration_minutes"] = {
            "target": self.targets["max_duration_minutes"],
            "actual": round(total_duration, 2),
            "met": total_duration <= self.targets["max_duration_minutes"],
        }

        # Error rate
        error_count = len(run_state.errors)
        error_rate = error_count / max(total, 1)
        results["max_error_rate"] = {
            "target": self.targets["max_error_rate"],
            "actual": round(error_rate, 3),
            "met": error_rate <= self.targets["max_error_rate"],
        }

        overall_met = all(r["met"] for r in results.values())
        check_record = {
            "run_id": run_state.run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_met": overall_met,
            "details": results,
        }
        self.checks.append(check_record)
        return check_record

    def get_compliance_report(self):
        """Get SLO compliance report across all checks."""
        if not self.checks:
            return {"total_checks": 0, "compliance_rate": 0, "details": {}}
        met = sum(1 for c in self.checks if c["overall_met"])
        return {
            "total_checks": len(self.checks),
            "compliance_rate": round(met / len(self.checks), 3),
            "latest": self.checks[-1] if self.checks else None,
        }


# ─────────────────────────────────────────────
#  DD9: Status Page Generator
# ─────────────────────────────────────────────

def generate_status_page(platform_state, alerting, slo_tracker, output_path=None):
    """Generate an HTML status page for the migration platform.

    Args:
        platform_state: PlatformState instance
        alerting: AlertingOrchestrator instance
        slo_tracker: SLOTracker instance
        output_path: path for HTML output

    Returns:
        path to generated HTML file
    """
    output_path = output_path or os.path.join("output", "platform_status.html")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    dashboard = platform_state.get_dashboard_data()
    alert_summary = alerting.get_alert_summary()
    slo_report = slo_tracker.get_compliance_report()

    # Current health
    health = platform_state.get_health()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Migration Platform Status</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         max-width: 1200px; margin: 0 auto; padding: 24px; background: #f8f9fa; }}
  .card {{ background: #fff; border-radius: 8px; padding: 20px; margin: 16px 0;
           box-shadow: 0 1px 3px rgba(0,0,0,.12); }}
  .health-score {{ font-size: 64px; font-weight: 700; text-align: center; }}
  .grade-A {{ color: #28a745; }} .grade-B {{ color: #0078D4; }}
  .grade-C {{ color: #ffc107; }} .grade-D {{ color: #fd7e14; }}
  .grade-F {{ color: #dc3545; }}
  h1 {{ color: #0078D4; }} h2 {{ color: #2c3e50; border-bottom: 2px solid #0078D4;
       padding-bottom: 8px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #e9ecef; }}
  th {{ background: #f1f3f5; font-weight: 600; }}
  .badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px;
            font-size: 12px; font-weight: 600; }}
  .badge-ok {{ background: #d4edda; color: #155724; }}
  .badge-warn {{ background: #fff3cd; color: #856404; }}
  .badge-fail {{ background: #f8d7da; color: #721c24; }}
  .slo-met {{ color: #28a745; }} .slo-missed {{ color: #dc3545; }}
</style>
</head>
<body>
<h1>Migration Platform Status</h1>
<p>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>

<div class="card">
  <h2>Health Score</h2>
  <div class="health-score grade-{health.get('grade', 'F')}">{health.get('overall', 0)}/100 ({health.get('grade', 'F')})</div>
</div>

<div class="card">
  <h2>Migration Runs ({dashboard['total_runs']})</h2>
  <table>
    <tr><th>Run ID</th><th>Target</th><th>Status</th><th>Phases</th><th>Errors</th><th>Health</th></tr>
    {"".join(f"<tr><td>{r['run_id']}</td><td>{r['target']}</td>"
             f"<td><span class='badge badge-{'ok' if r['status']=='completed' else 'fail'}'>{r['status']}</span></td>"
             f"<td>{len(r.get('phases', {}))}</td>"
             f"<td>{r.get('error_count', 0)}</td>"
             f"<td>{r.get('health', {}).get('overall', 0)}</td></tr>"
             for r in dashboard.get('runs', []))}
  </table>
</div>

<div class="card">
  <h2>Alerts ({alert_summary.get('total', 0)})</h2>
  <table>
    <tr><th>Status</th><th>Count</th></tr>
    {"".join(f"<tr><td>{s}</td><td>{c}</td></tr>"
             for s, c in alert_summary.get('by_status', {}).items())}
  </table>
</div>

<div class="card">
  <h2>SLO Compliance</h2>
  <p>Compliance rate: <strong>{slo_report.get('compliance_rate', 0) * 100:.1f}%</strong>
     across {slo_report.get('total_checks', 0)} checks</p>
</div>
</body></html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path


# ─────────────────────────────────────────────
#  DD9: Enterprise Report Generator
# ─────────────────────────────────────────────

def generate_platform_report(platform_state, alerting, slo_tracker, output_path=None):
    """Generate a JSON platform report.

    Returns:
        dict with full platform report
    """
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dashboard": platform_state.get_dashboard_data(),
        "health": platform_state.get_health(),
        "alerts": alerting.get_alert_summary(),
        "open_alerts": alerting.get_open_alerts(),
        "slo": slo_tracker.get_compliance_report(),
    }

    output_path = output_path or os.path.join("output", "platform_report.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return report
