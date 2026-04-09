"""
Test Suite — Datadog Integration (DD1–DD3)
==========================================
Tests: DatadogHandler, emit_datadog_metrics, tracing helpers, event submission.

Run:
    pytest tests/test_datadog.py -v
"""

import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from datadog_integration import (
    DatadogHandler,
    _NoOpSpan,
    emit_artifact_metrics,
    emit_conversion_metrics,
    emit_datadog_metrics,
    emit_phase_metrics,
    init_tracer,
    is_datadog_available,
    is_tracing_available,
    load_datadog_config,
    send_datadog_event,
    setup_datadog_logging,
    trace_phase,
)


# ─────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def dd_config():
    return {
        "datadog": {
            "enabled": True,
            "api_key": "test-key-123",
            "site": "datadoghq.eu",
            "service": "test-migration",
            "env": "test",
            "tags": ["team:migration", "project:test"],
            "logs": {"send_level": "WARNING", "source": "test-source"},
            "metrics": {"prefix": "test.migration"},
            "tracing": {"enabled": True, "sample_rate": 1.0},
        }
    }


@pytest.fixture
def dd_config_disabled():
    return {"datadog": {"enabled": False}}


# ─────────────────────────────────────────────
#  DD1: Configuration
# ─────────────────────────────────────────────

class TestLoadDatadogConfig:

    def test_load_defaults(self):
        cfg = load_datadog_config({})
        assert cfg["enabled"] is False
        assert cfg["site"] == "datadoghq.com"
        assert cfg["service"] == "informatica-migration"

    def test_load_from_config(self, dd_config):
        cfg = load_datadog_config(dd_config)
        assert cfg["enabled"] is True
        assert cfg["api_key"] == "test-key-123"
        assert cfg["site"] == "datadoghq.eu"
        assert cfg["service"] == "test-migration"
        assert cfg["env"] == "test"
        assert "team:migration" in cfg["tags"]

    def test_env_var_override(self, dd_config):
        with patch.dict(os.environ, {"DD_API_KEY": "env-key-456", "DD_SITE": "us5.datadoghq.com"}):
            cfg = load_datadog_config(dd_config)
            assert cfg["api_key"] == "env-key-456"
            assert cfg["site"] == "us5.datadoghq.com"

    def test_none_config(self):
        cfg = load_datadog_config(None)
        assert cfg["enabled"] is False

    def test_logs_sub_config(self, dd_config):
        cfg = load_datadog_config(dd_config)
        assert cfg["logs"]["send_level"] == "WARNING"
        assert cfg["logs"]["source"] == "test-source"


# ─────────────────────────────────────────────
#  DD1: DatadogHandler
# ─────────────────────────────────────────────

class TestDatadogHandler:

    def test_create_handler(self):
        handler = DatadogHandler(api_key="test", site="datadoghq.com")
        assert handler.logs_sent == 0
        assert handler.logs_failed == 0
        handler.close()

    def test_handler_buffers_records(self):
        handler = DatadogHandler(api_key="test")
        logger = logging.getLogger("test.dd.buffer")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.info("test message 1")
        logger.info("test message 2")
        assert len(handler._buffer) == 2
        handler.close()
        logger.removeHandler(handler)

    def test_handler_respects_send_level(self):
        handler = DatadogHandler(api_key="test", send_level="ERROR")
        logger = logging.getLogger("test.dd.level")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.info("should be skipped")
        logger.warning("should be skipped too")
        assert len(handler._buffer) == 0
        logger.error("should be buffered")
        assert len(handler._buffer) == 1
        handler.close()
        logger.removeHandler(handler)

    def test_format_record_basic(self):
        handler = DatadogHandler(api_key="test", service="svc", env="dev",
                                  tags=["tag1"])
        record = logging.LogRecord("test", logging.INFO, "", 0, "hello", (), None)
        entry = handler._format_record(record)
        assert entry["message"] == "hello"
        assert entry["service"] == "svc"
        assert "env:dev" in entry["ddtags"]
        assert entry["level"] == "INFO"
        handler.close()

    def test_format_record_migration_attrs(self):
        handler = DatadogHandler(api_key="test")
        record = logging.LogRecord("test", logging.INFO, "", 0, "phase", (), None)
        record.phase = "assessment"
        record.duration = 12.5
        record.mapping_name = "M_LOAD"
        entry = handler._format_record(record)
        assert entry["phase"] == "assessment"
        assert entry["duration"] == 12.5
        assert entry["mapping_name"] == "M_LOAD"
        handler.close()

    def test_flush_writes_local_fallback(self, tmp_path):
        handler = DatadogHandler(api_key="")  # no API key → local fallback
        handler.LOCAL_FALLBACK = str(tmp_path / "fallback.jsonl")
        logger = logging.getLogger("test.dd.fallback")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.info("fallback message")
        handler.flush()
        assert os.path.isfile(handler.LOCAL_FALLBACK)
        with open(handler.LOCAL_FALLBACK) as f:
            line = f.readline()
        data = json.loads(line)
        assert data["message"] == "fallback message"
        handler.close()
        logger.removeHandler(handler)

    def test_close_flushes_and_stops_timer(self):
        handler = DatadogHandler(api_key="test")
        handler.emit(logging.LogRecord("t", logging.INFO, "", 0, "msg", (), None))
        handler.close()
        assert handler._closed is True
        assert handler._timer is None or not handler._timer.is_alive()

    def test_get_stats(self):
        handler = DatadogHandler(api_key="test")
        handler.emit(logging.LogRecord("t", logging.INFO, "", 0, "msg", (), None))
        stats = handler.get_stats()
        assert stats["buffer_size"] == 1
        assert stats["logs_sent"] == 0
        handler.close()

    def test_max_batch_triggers_flush(self):
        handler = DatadogHandler(api_key="")
        handler.MAX_BATCH = 3
        handler.LOCAL_FALLBACK = os.path.join(tempfile.gettempdir(), "dd_batch_test.jsonl")
        for i in range(5):
            handler.emit(logging.LogRecord("t", logging.INFO, "", 0, f"msg{i}", (), None))
        assert handler.flush_count >= 1  # Should have flushed at least once
        handler.close()

    def test_handler_thread_safety(self):
        """Verify handler doesn't crash under concurrent access."""
        handler = DatadogHandler(api_key="test")
        import threading
        def emit_many():
            for i in range(20):
                handler.emit(logging.LogRecord("t", logging.INFO, "", 0, f"t-{i}", (), None))
        threads = [threading.Thread(target=emit_many) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        handler.close()
        # No crash = pass


# ─────────────────────────────────────────────
#  DD2: Metrics Emitter
# ─────────────────────────────────────────────

class TestEmitDatadogMetrics:

    def test_emit_local_only(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        result = emit_datadog_metrics([
            {"name": "phase.duration", "value": 12.5, "tags": ["phase:assessment"]},
            {"name": "phase.status", "value": 1, "tags": ["phase:assessment"]},
        ], config={"tags": ["env:test"]})
        assert result["status"] == "local-only"
        assert result["submitted"] == 0
        assert len(result["metrics"]) == 2

    def test_emit_applies_prefix(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        result = emit_datadog_metrics([
            {"name": "test_metric", "value": 42},
        ], config={"metrics": {"prefix": "custom.prefix"}})
        assert result["metrics"][0]["name"] == "custom.prefix.test_metric"

    def test_emit_preserves_full_name(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        result = emit_datadog_metrics([
            {"name": "informatica.migration.already_prefixed", "value": 1},
        ], config={"metrics": {"prefix": "informatica.migration"}})
        assert result["metrics"][0]["name"] == "informatica.migration.already_prefixed"

    def test_emit_writes_local_file(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        emit_datadog_metrics([{"name": "test", "value": 1}])
        assert os.path.isfile(os.path.join("output", "datadog_metrics.json"))


class TestEmitPhaseMetrics:

    def test_emit_phase_metrics(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        result = emit_phase_metrics(0, "assessment", "ok", 15.3)
        assert len(result["metrics"]) == 2
        names = [m["name"] for m in result["metrics"]]
        assert any("phase.duration" in n for n in names)
        assert any("phase.status" in n for n in names)


class TestEmitArtifactMetrics:

    def test_emit_artifact_metrics(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        result = emit_artifact_metrics({"notebooks": 16, "pipelines": 13, "sql": 8})
        assert any("artifacts.grand_total" in m["name"] for m in result["metrics"])


class TestEmitConversionMetrics:

    def test_emit_conversion_metrics(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        result = emit_conversion_metrics(85.5, 3, {"simple": 5, "complex": 2})
        names = [m["name"] for m in result["metrics"]]
        assert any("conversion_score" in n for n in names)
        assert any("todo_count" in n for n in names)


# ─────────────────────────────────────────────
#  DD3: Tracing
# ─────────────────────────────────────────────

class TestTracing:

    def test_init_tracer_no_ddtrace(self):
        with patch("datadog_integration.HAS_DDTRACE", False):
            result = init_tracer({"tracing": {"enabled": True}})
            assert result is None

    def test_trace_phase_no_ddtrace(self):
        with patch("datadog_integration.HAS_DDTRACE", False):
            span = trace_phase("assessment", phase_id=0)
            assert isinstance(span, _NoOpSpan)

    def test_noop_span_context_manager(self):
        span = _NoOpSpan()
        with span:
            span.set_tag("key", "value")
            span.set_error(Exception("test"))
        span.finish()


class TestTagSpanError:

    def test_tag_span_error(self):
        from datadog_integration import tag_span_error
        span = MagicMock()
        tag_span_error(span, ValueError("bad value"))
        span.set_tag.assert_any_call("error", True)
        span.set_tag.assert_any_call("error.type", "ValueError")

    def test_tag_span_error_none_span(self):
        from datadog_integration import tag_span_error
        tag_span_error(None, ValueError("test"))  # Should not crash


# ─────────────────────────────────────────────
#  DD3: Event Submission
# ─────────────────────────────────────────────

class TestSendDatadogEvent:

    def test_send_event_local_only(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        result = send_datadog_event(
            "Migration Complete",
            "All 8 phases completed successfully",
            alert_type="success",
            tags=["phase:all"],
        )
        assert result["status"] == "local-only"
        assert result["title"] == "Migration Complete"
        events_file = os.path.join("output", "datadog_events.jsonl")
        assert os.path.isfile(events_file)

    def test_send_event_alert_types(self, tmp_path):
        os.chdir(tmp_path)
        os.makedirs("output", exist_ok=True)
        for alert_type in ("info", "warning", "error", "success"):
            result = send_datadog_event("Test", "body", alert_type=alert_type)
            assert result["alert_type"] == alert_type


# ─────────────────────────────────────────────
#  Setup Helper
# ─────────────────────────────────────────────

class TestSetupDatadogLogging:

    def test_setup_disabled(self, dd_config_disabled):
        logger = logging.getLogger("test.dd.setup.disabled")
        result = setup_datadog_logging(logger, dd_config_disabled)
        assert result is None

    def test_setup_no_api_key(self):
        logger = logging.getLogger("test.dd.setup.nokey")
        result = setup_datadog_logging(logger, {"datadog": {"enabled": True}})
        assert result is None

    def test_setup_no_client(self, dd_config):
        with patch("datadog_integration.HAS_DATADOG_CLIENT", False):
            logger = logging.getLogger("test.dd.setup.noclient")
            result = setup_datadog_logging(logger, dd_config)
            assert result is None

    def test_setup_success(self, dd_config):
        with patch("datadog_integration.HAS_DATADOG_CLIENT", True):
            logger = logging.getLogger("test.dd.setup.ok")
            handler = setup_datadog_logging(logger, dd_config)
            if handler:
                assert isinstance(handler, DatadogHandler)
                handler.close()
                logger.removeHandler(handler)


# ─────────────────────────────────────────────
#  Availability Checks
# ─────────────────────────────────────────────

class TestAvailability:

    def test_is_datadog_available(self):
        result = is_datadog_available()
        assert isinstance(result, bool)

    def test_is_tracing_available(self):
        result = is_tracing_available()
        assert isinstance(result, bool)
