"""
REST API Server — Sprint 75
FastAPI-compatible REST API for headless migration.

Endpoints:
    POST /migrate    — Start a migration job
    POST /assess     — Run assessment
    GET  /status/{id} — Check job status
    GET  /inventory  — Get current inventory
    GET  /health     — Health check

Usage:
    # With FastAPI installed:
    uvicorn api_server:app --host 0.0.0.0 --port 8000

    # Without FastAPI (uses built-in http.server):
    python api_server.py
"""

import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent

# ─────────────────────────────────────────────
#  Job Tracking
# ─────────────────────────────────────────────

_jobs = {}  # job_id → {status, created, result, ...}


def _create_job(job_type, params=None):
    """Create a new background job entry."""
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "id": job_id,
        "type": job_type,
        "status": "pending",
        "created": datetime.now(timezone.utc).isoformat(),
        "completed": None,
        "params": params or {},
        "result": None,
        "error": None,
    }
    return job_id


def _run_migration_job(job_id, params):
    """Execute a migration job in a background thread."""
    if job_id not in _jobs:
        return
    try:
        _jobs[job_id]["status"] = "running"
        from sdk import MigrationSDK, MigrationConfig

        config = MigrationConfig.from_dict(params)
        sdk = MigrationSDK(config)
        results = sdk.migrate()

        if job_id in _jobs:
            _jobs[job_id]["status"] = "completed"
            _jobs[job_id]["result"] = results
            _jobs[job_id]["completed"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = str(e)
            _jobs[job_id]["completed"] = datetime.now(timezone.utc).isoformat()


def _run_assessment_job(job_id, params):
    """Execute an assessment job in a background thread."""
    if job_id not in _jobs:
        return
    try:
        _jobs[job_id]["status"] = "running"
        from sdk import MigrationSDK, MigrationConfig

        config = MigrationConfig.from_dict(params)
        sdk = MigrationSDK(config)
        inventory = sdk.assess(params.get("source_dir"))

        if job_id in _jobs:
            _jobs[job_id]["status"] = "completed"
            _jobs[job_id]["result"] = {
                "mappings": len(inventory.get("mappings", [])),
                "workflows": len(inventory.get("workflows", [])),
            }
            _jobs[job_id]["completed"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        if job_id in _jobs:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = str(e)
            _jobs[job_id]["completed"] = datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────
#  API Functions (usable with or without FastAPI)
# ─────────────────────────────────────────────

def api_health():
    """GET /health — returns service health status."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def api_start_migration(params=None):
    """POST /migrate — start a migration job.

    Args:
        params: dict with target, input_dir, etc.
    Returns:
        dict with job_id for status polling.
    """
    if params is None:
        params = {}
    job_id = _create_job("migration", params)
    thread = threading.Thread(target=_run_migration_job, args=(job_id, params), daemon=True)
    thread.start()
    return {"job_id": job_id, "status": "pending"}


def api_start_assessment(params=None):
    """POST /assess — start an assessment job.

    Args:
        params: dict with source_dir, target, etc.
    Returns:
        dict with job_id for status polling.
    """
    if params is None:
        params = {}
    job_id = _create_job("assessment", params)
    thread = threading.Thread(target=_run_assessment_job, args=(job_id, params), daemon=True)
    thread.start()
    return {"job_id": job_id, "status": "pending"}


def api_get_status(job_id):
    """GET /status/{job_id} — get job status.

    Returns:
        Job details dict or None if not found.
    """
    return _jobs.get(job_id)


def api_get_inventory():
    """GET /inventory — get the current inventory.

    Returns:
        Inventory dict from the last assessment run.
    """
    inv_path = WORKSPACE / "output" / "inventory" / "inventory.json"
    if inv_path.exists():
        with open(inv_path, encoding="utf-8") as f:
            return json.load(f)
    return {"mappings": [], "workflows": [], "error": "No inventory found — run assessment first"}


def api_list_jobs():
    """GET /jobs — list all jobs."""
    return list(_jobs.values())


# ─────────────────────────────────────────────
#  FastAPI App (conditional import)
# ─────────────────────────────────────────────

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel

    app = FastAPI(
        title="Informatica to Fabric Migration API",
        description="REST API for programmatic migration from Informatica to Microsoft Fabric / Databricks",
        version="1.0.0",
    )

    class MigrateRequest(BaseModel):
        target: str = "fabric"
        input_dir: str = "input"
        output_dir: str = "output"
        databricks_catalog: str = "main"
        dry_run: bool = False
        verbose: bool = False

    class AssessRequest(BaseModel):
        source_dir: str = "input"
        target: str = "fabric"

    @app.get("/health")
    def health():
        return api_health()

    @app.post("/migrate")
    def start_migration(request: MigrateRequest):
        return api_start_migration(request.dict())

    @app.post("/assess")
    def start_assessment(request: AssessRequest):
        return api_start_assessment(request.dict())

    @app.get("/status/{job_id}")
    def get_status(job_id: str):
        result = api_get_status(job_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return result

    @app.get("/inventory")
    def get_inventory():
        return api_get_inventory()

    @app.get("/jobs")
    def list_jobs():
        return api_list_jobs()

except ImportError:
    app = None


# ─────────────────────────────────────────────
#  Standalone HTTP Server (fallback)
# ─────────────────────────────────────────────

def _run_stdlib_server(port=8000):
    """Run a basic HTTP server using stdlib (no FastAPI required)."""
    from http.server import HTTPServer, BaseHTTPRequestHandler

    class MigrationAPIHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self._json_response(api_health())
            elif self.path == "/inventory":
                self._json_response(api_get_inventory())
            elif self.path == "/jobs":
                self._json_response(api_list_jobs())
            elif self.path.startswith("/status/"):
                job_id = self.path.split("/status/")[1]
                result = api_get_status(job_id)
                if result:
                    self._json_response(result)
                else:
                    self._json_response({"error": "Job not found"}, 404)
            else:
                self._json_response({"error": "Not found"}, 404)

        def do_POST(self):
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}

            if self.path == "/migrate":
                self._json_response(api_start_migration(body))
            elif self.path == "/assess":
                self._json_response(api_start_assessment(body))
            else:
                self._json_response({"error": "Not found"}, 404)

        def _json_response(self, data, status=200):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data, indent=2).encode("utf-8"))

        def log_message(self, format, *args):
            logger.info(format, *args)

    server = HTTPServer(("0.0.0.0", port), MigrationAPIHandler)
    print(f"Migration API server running on http://0.0.0.0:{port}")
    print("Endpoints: GET /health, POST /migrate, POST /assess, GET /status/<id>, GET /inventory")
    server.serve_forever()


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    if app:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        _run_stdlib_server(port)
