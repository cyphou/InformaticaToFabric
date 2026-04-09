"""
Datadog Integration Module (DD1–DD3)
====================================
Provides Datadog observability for the Informatica→Fabric migration tool:
  - DD1: DatadogHandler — custom logging.Handler → Datadog Logs API v2
  - DD2: emit_datadog_metrics() — custom metrics → Datadog Metrics API v2
  - DD3: Tracing helpers, event submission, log-trace correlation

Dependencies (optional):
  pip install datadog-api-client ddtrace

All features degrade gracefully when packages are not installed.
"""

import json
import logging
import os
import socket
import threading
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────────
#  Graceful imports
# ─────────────────────────────────────────────
try:
    from datadog_api_client import ApiClient, Configuration
    from datadog_api_client.v2.api.logs_api import LogsApi
    from datadog_api_client.v2.api.metrics_api import MetricsApi as MetricsApiV2
    from datadog_api_client.v2.model.http_log import HTTPLog
    from datadog_api_client.v2.model.http_log_item import HTTPLogItem
    from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
    from datadog_api_client.v2.model.metric_payload import MetricPayload
    from datadog_api_client.v2.model.metric_point import MetricPoint
    from datadog_api_client.v2.model.metric_resource import MetricResource
    from datadog_api_client.v2.model.metric_series import MetricSeries
    HAS_DATADOG_CLIENT = True
except ImportError:
    HAS_DATADOG_CLIENT = False

try:
    import ddtrace
    from ddtrace import tracer as dd_tracer
    HAS_DDTRACE = True
except ImportError:
    HAS_DDTRACE = False
    dd_tracer = None

_logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Configuration loader
# ─────────────────────────────────────────────

def load_datadog_config(config):
    """Extract Datadog config from migration.yaml dict, with env var overrides."""
    dd = (config or {}).get("datadog", {})
    return {
        "enabled": dd.get("enabled", False),
        "api_key": os.environ.get("DD_API_KEY", dd.get("api_key", "")),
        "site": os.environ.get("DD_SITE", dd.get("site", "datadoghq.com")),
        "service": dd.get("service", "informatica-migration"),
        "env": dd.get("env", "production"),
        "tags": dd.get("tags", []),
        "logs": dd.get("logs", {}),
        "metrics": dd.get("metrics", {}),
        "tracing": dd.get("tracing", {}),
    }


def is_datadog_available():
    """Check if Datadog client library is installed."""
    return HAS_DATADOG_CLIENT


def is_tracing_available():
    """Check if ddtrace is installed."""
    return HAS_DDTRACE


# ─────────────────────────────────────────────
#  DD1: DatadogHandler — Logging
# ─────────────────────────────────────────────

class DatadogHandler(logging.Handler):
    """Custom logging.Handler that batches and sends structured logs to Datadog Logs API v2.

    Features:
      - Batches up to MAX_BATCH records or FLUSH_INTERVAL seconds
      - Thread-safe buffer with lock
      - Exponential backoff on 429 (rate-limited)
      - Injects migration-specific attributes (phase, duration, mapping_name)
      - Graceful fallback: stores to local file if API fails
    """

    MAX_BATCH = 100
    FLUSH_INTERVAL = 5.0
    MAX_RETRIES = 3
    LOCAL_FALLBACK = "output/datadog_logs_fallback.jsonl"

    def __init__(self, api_key, site="datadoghq.com", service="informatica-migration",
                 env="production", source="informatica-migration", tags=None,
                 send_level="INFO"):
        super().__init__()
        self._api_key = api_key
        self._site = site
        self._service = service
        self._env = env
        self._source = source
        self._tags = tags or []
        self._send_level = getattr(logging, send_level.upper(), logging.INFO)
        self._hostname = socket.gethostname()

        self._buffer = []
        self._lock = threading.Lock()
        self._closed = False

        # Metrics
        self.logs_sent = 0
        self.logs_failed = 0
        self.flush_count = 0

        # Start flush timer
        self._timer = None
        self._start_flush_timer()

    def _start_flush_timer(self):
        """Start periodic flush timer."""
        if self._closed:
            return
        self._timer = threading.Timer(self.FLUSH_INTERVAL, self._timed_flush)
        self._timer.daemon = True
        self._timer.start()

    def _timed_flush(self):
        """Flush on timer expiry and restart timer."""
        with self._lock:
            self._do_flush()
        self._start_flush_timer()

    def emit(self, record):
        """Buffer a log record for sending to Datadog."""
        if self._closed or record.levelno < self._send_level:
            return
        entry = self._format_record(record)
        with self._lock:
            self._buffer.append(entry)
            if len(self._buffer) >= self.MAX_BATCH:
                self._do_flush()

    def _format_record(self, record):
        """Format a LogRecord into a Datadog-compatible dict."""
        entry = {
            "ddsource": self._source,
            "ddtags": ",".join(self._tags + [f"env:{self._env}"]),
            "hostname": self._hostname,
            "service": self._service,
            "message": self.format(record) if self.formatter else record.getMessage(),
            "level": record.levelname,
            "logger": record.name,
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
        }
        # Migration-specific attributes
        for attr in ("phase", "duration", "mapping_name", "target", "phase_id",
                      "artifact_type", "conversion_score", "error_type"):
            if hasattr(record, attr):
                entry[attr] = getattr(record, attr)
        # Log-trace correlation (DD3)
        if HAS_DDTRACE:
            span = ddtrace.tracer.current_span()
            if span:
                entry["dd.trace_id"] = str(span.trace_id)
                entry["dd.span_id"] = str(span.span_id)
        return entry

    def _do_flush(self):
        """Send buffered logs to Datadog. Must be called with self._lock held."""
        if not self._buffer:
            return
        batch = self._buffer[:]
        self._buffer.clear()
        self.flush_count += 1

        if not HAS_DATADOG_CLIENT or not self._api_key:
            self._write_local_fallback(batch)
            return

        # Send via Datadog API
        try:
            self._send_batch(batch)
            self.logs_sent += len(batch)
        except Exception:
            self.logs_failed += len(batch)
            self._write_local_fallback(batch)

    def _send_batch(self, batch):
        """POST logs to Datadog Logs API v2 with retry."""
        os.environ["DD_API_KEY"] = self._api_key
        os.environ["DD_SITE"] = self._site
        configuration = Configuration()
        with ApiClient(configuration) as api_client:
            api_instance = LogsApi(api_client)
            body = HTTPLog([HTTPLogItem(**{
                "ddsource": e.get("ddsource", self._source),
                "ddtags": e.get("ddtags", ""),
                "hostname": e.get("hostname", self._hostname),
                "message": json.dumps(e),
                "service": e.get("service", self._service),
            }) for e in batch])

            retries = 0
            while retries < self.MAX_RETRIES:
                try:
                    api_instance.submit_log(body=body)
                    return
                except Exception as exc:
                    if "429" in str(exc):
                        retries += 1
                        time.sleep(2 ** retries)
                    else:
                        raise

    def _write_local_fallback(self, batch):
        """Write logs to local JSONL file as fallback."""
        try:
            os.makedirs(os.path.dirname(self.LOCAL_FALLBACK), exist_ok=True)
            with open(self.LOCAL_FALLBACK, "a", encoding="utf-8") as f:
                for entry in batch:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError:
            pass

    def flush(self):
        """Flush remaining buffer."""
        with self._lock:
            self._do_flush()

    def close(self):
        """Flush and stop timer."""
        self._closed = True
        if self._timer:
            self._timer.cancel()
        self.flush()
        super().close()

    def get_stats(self):
        """Return handler statistics."""
        return {
            "logs_sent": self.logs_sent,
            "logs_failed": self.logs_failed,
            "flush_count": self.flush_count,
            "buffer_size": len(self._buffer),
        }


# ─────────────────────────────────────────────
#  DD2: Metrics Emitter
# ─────────────────────────────────────────────

def emit_datadog_metrics(metrics_list, config=None):
    """Submit custom metrics to Datadog Metrics API v2.

    Args:
        metrics_list: list of dicts with keys: name, value, tags (list), type (gauge|count)
        config: datadog config dict from load_datadog_config()

    Returns:
        dict with submission status
    """
    cfg = config or {}
    api_key = cfg.get("api_key", os.environ.get("DD_API_KEY", ""))
    site = cfg.get("site", os.environ.get("DD_SITE", "datadoghq.com"))
    prefix = cfg.get("metrics", {}).get("prefix", "informatica.migration")
    base_tags = cfg.get("tags", []) + [f"env:{cfg.get('env', 'production')}"]

    result = {
        "submitted": 0,
        "failed": 0,
        "metrics": [],
        "local_path": None,
    }

    # Build series
    now = int(datetime.now(timezone.utc).timestamp())
    series = []
    for m in metrics_list:
        metric_name = f"{prefix}.{m['name']}" if not m["name"].startswith(prefix) else m["name"]
        tags = base_tags + m.get("tags", [])
        series.append({
            "metric": metric_name,
            "type": m.get("type", "gauge"),
            "points": [{"timestamp": now, "value": m["value"]}],
            "tags": tags,
            "resources": [{"name": socket.gethostname(), "type": "host"}],
        })
        result["metrics"].append({"name": metric_name, "value": m["value"]})

    # Try Datadog API
    if HAS_DATADOG_CLIENT and api_key:
        try:
            os.environ["DD_API_KEY"] = api_key
            os.environ["DD_SITE"] = site
            configuration = Configuration()
            with ApiClient(configuration) as api_client:
                api_instance = MetricsApiV2(api_client)
                body = MetricPayload(series=[
                    MetricSeries(
                        metric=s["metric"],
                        type=MetricIntakeType.UNSPECIFIED,
                        points=[MetricPoint(timestamp=s["points"][0]["timestamp"],
                                            value=s["points"][0]["value"])],
                        tags=s["tags"],
                        resources=[MetricResource(name=s["resources"][0]["name"],
                                                   type=s["resources"][0]["type"])],
                    ) for s in series
                ])
                api_instance.submit_metrics(body=body)
                result["submitted"] = len(series)
                result["status"] = "submitted"
        except Exception as exc:
            result["failed"] = len(series)
            result["status"] = f"api_error: {exc}"
    else:
        result["status"] = "local-only"

    # Always write local copy
    local_path = os.path.join("output", "datadog_metrics.json")
    os.makedirs("output", exist_ok=True)
    try:
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump({"timestamp": now, "series": series, "status": result["status"]},
                      f, indent=2, ensure_ascii=False)
        result["local_path"] = local_path
    except OSError:
        pass

    return result


def emit_phase_metrics(phase_id, phase_name, status, duration, config=None):
    """Emit per-phase metrics to Datadog."""
    return emit_datadog_metrics([
        {"name": "phase.duration", "value": round(duration, 2),
         "tags": [f"phase:{phase_name}", f"phase_id:{phase_id}", f"status:{status}"]},
        {"name": "phase.status", "value": 1 if status == "ok" else 0,
         "tags": [f"phase:{phase_name}", f"phase_id:{phase_id}"]},
    ], config=config)


def emit_artifact_metrics(artifact_counts, config=None):
    """Emit artifact count metrics to Datadog.

    Args:
        artifact_counts: dict like {"notebooks": 16, "pipelines": 13, "sql": 8, "dbt": 7}
    """
    metrics = []
    total = 0
    for atype, count in artifact_counts.items():
        metrics.append({
            "name": "artifacts.total",
            "value": count,
            "tags": [f"artifact_type:{atype}"],
        })
        total += count
    metrics.append({"name": "artifacts.grand_total", "value": total, "tags": []})
    return emit_datadog_metrics(metrics, config=config)


def emit_conversion_metrics(conversion_score, todo_count, complexity_dist=None, config=None):
    """Emit conversion quality metrics to Datadog."""
    metrics = [
        {"name": "conversion_score", "value": conversion_score, "tags": []},
        {"name": "todo_count", "value": todo_count, "tags": []},
    ]
    if complexity_dist:
        for level, count in complexity_dist.items():
            metrics.append({"name": "complexity", "value": count, "tags": [f"level:{level}"]})
    return emit_datadog_metrics(metrics, config=config)


# ─────────────────────────────────────────────
#  DD3: Tracing helpers
# ─────────────────────────────────────────────

def init_tracer(config=None):
    """Initialize ddtrace tracer with config."""
    if not HAS_DDTRACE:
        _logger.debug("ddtrace not installed — tracing disabled")
        return None
    cfg = config or {}
    tracing = cfg.get("tracing", {})
    if not tracing.get("enabled", False):
        return None

    ddtrace.config.service = cfg.get("service", "informatica-migration")
    ddtrace.config.env = cfg.get("env", "production")
    if "sample_rate" in tracing:
        dd_tracer.configure(sampler=ddtrace.sampler.DatadogSampler(
            default_sample_rate=tracing["sample_rate"]
        ))
    _logger.info("ddtrace tracer initialized")
    return dd_tracer


def trace_phase(phase_name, phase_id=None):
    """Context manager / decorator for tracing a migration phase.

    Usage:
        with trace_phase("assessment", phase_id=0):
            run_assessment()
    """
    if HAS_DDTRACE:
        span = dd_tracer.trace(
            "informatica.migration.phase",
            service=ddtrace.config.service or "informatica-migration",
            resource=phase_name,
        )
        if phase_id is not None:
            span.set_tag("phase.id", phase_id)
        span.set_tag("phase.name", phase_name)
        return span
    # Return a no-op context manager
    return _NoOpSpan()


class _NoOpSpan:
    """No-op span for when ddtrace is not available."""
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def set_tag(self, key, value):
        pass
    def set_error(self, exc):
        pass
    def finish(self):
        pass


def tag_span_error(span, exc):
    """Tag a span with error information."""
    if span and hasattr(span, "set_tag"):
        span.set_tag("error", True)
        span.set_tag("error.type", type(exc).__name__)
        span.set_tag("error.message", str(exc)[:500])


# ─────────────────────────────────────────────
#  DD3: Event submission
# ─────────────────────────────────────────────

def send_datadog_event(title, text, alert_type="info", tags=None, config=None):
    """Submit an event to Datadog Events API.

    Args:
        title: Event title
        text: Event body (supports Markdown)
        alert_type: info | warning | error | success
        tags: additional tags
        config: datadog config dict

    Returns:
        dict with submission status
    """
    cfg = config or {}
    api_key = cfg.get("api_key", os.environ.get("DD_API_KEY", ""))
    base_tags = cfg.get("tags", []) + (tags or [])

    event = {
        "title": title,
        "text": text,
        "alert_type": alert_type,
        "tags": base_tags,
        "source_type_name": "informatica-migration",
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
    }

    if HAS_DATADOG_CLIENT and api_key:
        try:
            os.environ["DD_API_KEY"] = api_key
            os.environ["DD_SITE"] = cfg.get("site", "datadoghq.com")
            configuration = Configuration()
            with ApiClient(configuration) as api_client:
                from datadog_api_client.v1.api.events_api import EventsApi
                from datadog_api_client.v1.model.event_create_request import EventCreateRequest
                api_instance = EventsApi(api_client)
                body = EventCreateRequest(
                    title=title,
                    text=text,
                    alert_type=alert_type,
                    tags=base_tags,
                )
                api_instance.create_event(body=body)
                event["status"] = "submitted"
        except Exception as exc:
            event["status"] = f"error: {exc}"
    else:
        event["status"] = "local-only"

    # Local fallback
    events_path = os.path.join("output", "datadog_events.jsonl")
    os.makedirs("output", exist_ok=True)
    try:
        with open(events_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except OSError:
        pass

    return event


# ─────────────────────────────────────────────
#  Setup helper (called from run_migration.py)
# ─────────────────────────────────────────────

def setup_datadog_logging(logger, config):
    """Add DatadogHandler to an existing logger if enabled.

    Args:
        logger: Python logging.Logger
        config: full migration config dict

    Returns:
        DatadogHandler instance or None
    """
    dd_cfg = load_datadog_config(config)
    if not dd_cfg["enabled"]:
        return None
    if not dd_cfg["api_key"]:
        _logger.warning("Datadog enabled but no API key — set DD_API_KEY env var")
        return None
    if not HAS_DATADOG_CLIENT:
        _logger.warning("Datadog enabled but datadog-api-client not installed — "
                        "pip install datadog-api-client")
        return None

    logs_cfg = dd_cfg.get("logs", {})
    handler = DatadogHandler(
        api_key=dd_cfg["api_key"],
        site=dd_cfg["site"],
        service=dd_cfg["service"],
        env=dd_cfg["env"],
        source=logs_cfg.get("source", "informatica-migration"),
        tags=dd_cfg["tags"],
        send_level=logs_cfg.get("send_level", "INFO"),
    )
    logger.addHandler(handler)
    _logger.info("DatadogHandler attached to logger")
    return handler
