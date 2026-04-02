"""
Sprint 38 — Web UI & Migration Wizard (Streamlit)

6-Step guided migration wizard:
  1. Upload — XML exports, SQL files
  2. Assess — Run assessment, show complexity breakdown
  3. Configure — Source DB type, Fabric workspace, parameters
  4. Convert — Run SQL + Notebook + Pipeline migration
  5. Review — Browse generated artifacts, view diff
  6. Deploy — Trigger deployment to Fabric

Usage:
    streamlit run web/app.py
    # or: python web/app.py  (fallback to stdlib http.server)
"""

import json
import sys
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORKSPACE))


def _safe_write_upload(target_dir, uploaded_file):
    """Write an uploaded file to target_dir, sanitizing the filename to prevent path traversal."""
    # Strip directory components from filename (prevents ../../ attacks)
    safe_name = Path(uploaded_file.name).name
    # Reject empty or dot-only names
    if not safe_name or safe_name.startswith("."):
        return False
    dest = (target_dir / safe_name).resolve()
    # Verify the resolved path is still inside the target directory
    if not str(dest).startswith(str(target_dir.resolve())):
        return False
    dest.write_bytes(uploaded_file.read())
    return True

# Step definitions
STEPS = [
    {"id": 1, "title": "Upload", "icon": "📂", "desc": "Upload Informatica XML exports and SQL files"},
    {"id": 2, "title": "Assess", "icon": "🔍", "desc": "Run assessment and review complexity"},
    {"id": 3, "title": "Configure", "icon": "⚙️", "desc": "Set source DB type, Fabric workspace, parameters"},
    {"id": 4, "title": "Convert", "icon": "🔄", "desc": "Run SQL, Notebook, and Pipeline migration"},
    {"id": 5, "title": "Review", "icon": "📋", "desc": "Browse generated artifacts"},
    {"id": 6, "title": "Deploy", "icon": "🚀", "desc": "Deploy to Microsoft Fabric"},
]


def _load_inventory():
    """Load inventory.json if available."""
    inv_path = WORKSPACE / "output" / "inventory" / "inventory.json"
    if inv_path.exists():
        with open(inv_path, encoding="utf-8") as f:
            return json.load(f)
    return None


def _list_artifacts(directory):
    """List files in an output directory."""
    path = WORKSPACE / "output" / directory
    if not path.exists():
        return []
    return sorted([f.name for f in path.iterdir() if f.is_file()])


def run_streamlit():
    """Run the Streamlit-based migration wizard."""
    import streamlit as st

    st.set_page_config(
        page_title="Informatica → Fabric Migration",
        page_icon="🔄",
        layout="wide",
    )

    st.title("🔄 Informatica → Microsoft Fabric Migration Wizard")

    # Sidebar: step navigation
    if "step" not in st.session_state:
        st.session_state.step = 1

    st.sidebar.title("Migration Steps")
    for s in STEPS:
        active = "→ " if s["id"] == st.session_state.step else "  "
        if st.sidebar.button(f"{active}{s['icon']} {s['title']}", key=f"nav_{s['id']}"):
            st.session_state.step = s["id"]

    step = st.session_state.step

    # ─── Step 1: Upload ───
    if step == 1:
        st.header("📂 Step 1: Upload Informatica Exports")
        st.markdown("Upload your Informatica XML exports and SQL files.")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Mapping XMLs")
            mapping_files = st.file_uploader(
                "Upload mapping XML files",
                type=["xml"],
                accept_multiple_files=True,
                key="mappings",
            )
            if mapping_files:
                mapping_dir = WORKSPACE / "input" / "mappings"
                mapping_dir.mkdir(parents=True, exist_ok=True)
                for f in mapping_files:
                    _safe_write_upload(mapping_dir, f)
                st.success(f"✅ {len(mapping_files)} mapping files uploaded")

        with col2:
            st.subheader("Workflow XMLs")
            workflow_files = st.file_uploader(
                "Upload workflow XML files",
                type=["xml"],
                accept_multiple_files=True,
                key="workflows",
            )
            if workflow_files:
                workflow_dir = WORKSPACE / "input" / "workflows"
                workflow_dir.mkdir(parents=True, exist_ok=True)
                for f in workflow_files:
                    _safe_write_upload(workflow_dir, f)
                st.success(f"✅ {len(workflow_files)} workflow files uploaded")

        sql_files = st.file_uploader(
            "Upload SQL files (optional)",
            type=["sql"],
            accept_multiple_files=True,
            key="sql",
        )
        if sql_files:
            sql_dir = WORKSPACE / "input" / "sql"
            sql_dir.mkdir(parents=True, exist_ok=True)
            for f in sql_files:
                _safe_write_upload(sql_dir, f)
            st.success(f"✅ {len(sql_files)} SQL files uploaded")

        if st.button("Next → Assess", type="primary"):
            st.session_state.step = 2
            st.rerun()

    # ─── Step 2: Assess ───
    elif step == 2:
        st.header("🔍 Step 2: Assessment")
        if st.button("Run Assessment", type="primary"):
            with st.spinner("Running assessment..."):
                import run_assessment
                try:
                    run_assessment.main()
                    st.success("✅ Assessment complete!")
                except SystemExit as e:
                    if e.code and e.code != 0:
                        st.error(f"❌ Assessment failed with exit code {e.code}")
                    else:
                        st.success("✅ Assessment complete!")

        inv = _load_inventory()
        if inv:
            summary = inv.get("summary", {})
            col1, col2, col3 = st.columns(3)
            col1.metric("Mappings", summary.get("total_mappings", 0))
            col2.metric("Workflows", summary.get("total_workflows", 0))
            col3.metric("SQL Files", summary.get("total_sql_files", 0))

            cb = summary.get("complexity_breakdown", {})
            if cb:
                st.subheader("Complexity Breakdown")
                st.bar_chart(cb)

            score = summary.get("avg_conversion_score", 0)
            st.metric("Conversion Score", f"{score}/100")
        else:
            st.info("Run assessment to see results here.")

        if st.button("Next → Configure", type="primary"):
            st.session_state.step = 3
            st.rerun()

    # ─── Step 3: Configure ───
    elif step == 3:
        st.header("⚙️ Step 3: Configuration")
        target_platform = st.selectbox(
            "Target Platform",
            ["fabric", "databricks"],
            format_func=lambda x: "Microsoft Fabric" if x == "fabric" else "Azure Databricks",
        )
        db_type = st.selectbox(
            "Source Database Type",
            ["oracle", "sqlserver", "teradata", "db2", "mysql", "postgresql"],
        )

        if target_platform == "fabric":
            workspace_id = st.text_input("Fabric Workspace ID", placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
            lakehouse_name = st.text_input("Lakehouse Name", value="migration_lakehouse")
        else:
            workspace_url = st.text_input("Databricks Workspace URL", placeholder="https://adb-xxxx.azuredatabricks.net")
            catalog_name = st.text_input("Unity Catalog Name", value="main")
            secret_scope = st.text_input("Secret Scope", value="migration-secrets")

        if st.button("Save Configuration"):
            import os
            config = {
                "target": target_platform,
                "source": {"db_type": db_type},
            }
            if target_platform == "fabric":
                config["fabric"] = {
                    "workspace_id": workspace_id,
                    "lakehouse_name": lakehouse_name,
                }
            else:
                config["databricks"] = {
                    "workspace_url": workspace_url,
                    "catalog": catalog_name,
                    "secret_scope": secret_scope,
                }
            os.environ["INFORMATICA_MIGRATION_TARGET"] = target_platform
            config_path = WORKSPACE / "migration.yaml"
            try:
                import yaml
                with open(config_path, "w", encoding="utf-8") as f:
                    yaml.dump(config, f, default_flow_style=False)
            except ImportError:
                with open(config_path, "w", encoding="utf-8") as f:
                    json.dump(config, f, indent=2)
            st.success(f"✅ Configuration saved to {config_path.name}")

        if st.button("Next → Convert", type="primary"):
            st.session_state.step = 4
            st.rerun()

    # ─── Step 4: Convert ───
    elif step == 4:
        st.header("🔄 Step 4: Conversion")
        phases = {
            "SQL Migration": "run_sql_migration",
            "Notebook Generation": "run_notebook_migration",
            "Pipeline Generation": "run_pipeline_migration",
            "Schema Generation": "run_schema_generator",
        }
        selected = st.multiselect("Phases to run", list(phases.keys()), default=list(phases.keys()))

        if st.button("Run Conversion", type="primary"):
            import importlib
            for phase_name in selected:
                module_name = phases[phase_name]
                with st.spinner(f"Running {phase_name}..."):
                    try:
                        mod = importlib.import_module(module_name)
                        importlib.reload(mod)
                        mod.main()
                        st.success(f"✅ {phase_name} complete")
                    except SystemExit as e:
                        if e.code and e.code != 0:
                            st.error(f"❌ {phase_name} failed with exit code {e.code}")
                        else:
                            st.success(f"✅ {phase_name} complete")
                    except Exception as e:
                        st.error(f"❌ {phase_name} failed: {e}")

        if st.button("Next → Review", type="primary"):
            st.session_state.step = 5
            st.rerun()

    # ─── Step 5: Review ───
    elif step == 5:
        st.header("📋 Step 5: Review Generated Artifacts")
        tab_nb, tab_pl, tab_sql, tab_val = st.tabs(["Notebooks", "Pipelines", "SQL", "Validation"])

        with tab_nb:
            files = _list_artifacts("notebooks")
            st.write(f"**{len(files)} notebooks generated**")
            for f in files:
                with st.expander(f):
                    content = (WORKSPACE / "output" / "notebooks" / f).read_text(encoding="utf-8", errors="replace")
                    st.code(content, language="python")

        with tab_pl:
            files = _list_artifacts("pipelines")
            st.write(f"**{len(files)} pipelines generated**")
            for f in files:
                with st.expander(f):
                    content = (WORKSPACE / "output" / "pipelines" / f).read_text(encoding="utf-8", errors="replace")
                    st.code(content, language="json")

        with tab_sql:
            files = _list_artifacts("sql")
            st.write(f"**{len(files)} SQL files converted**")
            for f in files:
                with st.expander(f):
                    content = (WORKSPACE / "output" / "sql" / f).read_text(encoding="utf-8", errors="replace")
                    st.code(content, language="sql")

        with tab_val:
            files = _list_artifacts("validation")
            st.write(f"**{len(files)} validation scripts generated**")
            for f in files:
                with st.expander(f):
                    content = (WORKSPACE / "output" / "validation" / f).read_text(encoding="utf-8", errors="replace")
                    st.code(content, language="python")

        if st.button("Next → Deploy", type="primary"):
            st.session_state.step = 6
            st.rerun()

    # ─── Step 6: Deploy ───
    elif step == 6:
        st.header("🚀 Step 6: Deploy")
        import os
        target = os.environ.get("INFORMATICA_MIGRATION_TARGET", "fabric")
        if target == "databricks":
            st.markdown("""
            **Deployment Options for Databricks:**
            1. **Databricks CLI** — import notebooks with `databricks workspace import`
            2. **REST API** — deploy via Databricks Jobs API
            3. **Repos** — link a Git repo to the Databricks workspace
            4. **Manual Upload** — download artifacts and upload via Databricks UI
            """)
        else:
            st.markdown("""
            **Deployment Options for Fabric:**
            1. **Git Integration** — push to linked Fabric Git repo
            2. **REST API** — deploy via Fabric APIs
            3. **Manual Upload** — download artifacts and upload via Fabric portal
            """)

        if st.button("Generate Deployment Manifest"):
            from run_migration import generate_manifest
            config = {"fabric": {"workspace_id": "TODO"}}
            manifest_path = generate_manifest([], config)
            st.success(f"✅ Manifest: {manifest_path}")

        st.subheader("Download Artifacts")
        output_dir = WORKSPACE / "output"
        for subdir in ["notebooks", "pipelines", "sql", "validation", "schema"]:
            path = output_dir / subdir
            if path.exists():
                files = list(path.iterdir())
                if files:
                    st.write(f"**{subdir}/** — {len(files)} files")


def run_fallback_server():
    """Fallback: serve a basic HTML status page via stdlib http.server."""
    import http.server
    import socketserver

    PORT = 8501

    class Handler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()

            inv = _load_inventory()
            inv_status = "✅ Loaded" if inv else "❌ Not found"
            mappings = inv["summary"]["total_mappings"] if inv else 0
            workflows = inv["summary"]["total_workflows"] if inv else 0

            html = f"""<!DOCTYPE html>
<html><head><title>Informatica → Fabric Migration</title>
<style>body{{font-family:system-ui;max-width:800px;margin:40px auto;padding:0 20px}}
h1{{color:#0078D4}}table{{border-collapse:collapse;width:100%}}
td,th{{border:1px solid #ddd;padding:8px;text-align:left}}
th{{background:#0078D4;color:#fff}}.ok{{color:green}}.err{{color:red}}</style></head>
<body>
<h1>🔄 Informatica → Fabric Migration</h1>
<p>Install <code>streamlit</code> for the full wizard UI.</p>
<h2>Status</h2>
<table><tr><th>Item</th><th>Status</th></tr>
<tr><td>Inventory</td><td class="{'ok' if inv else 'err'}">{inv_status}</td></tr>
<tr><td>Mappings</td><td>{mappings}</td></tr>
<tr><td>Workflows</td><td>{workflows}</td></tr>
<tr><td>Notebooks</td><td>{len(_list_artifacts('notebooks'))}</td></tr>
<tr><td>Pipelines</td><td>{len(_list_artifacts('pipelines'))}</td></tr>
<tr><td>SQL Files</td><td>{len(_list_artifacts('sql'))}</td></tr>
</table>
<h2>Usage</h2>
<pre>pip install streamlit
streamlit run web/app.py</pre>
</body></html>"""
            self.wfile.write(html.encode("utf-8"))

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"  Fallback server running at http://localhost:{PORT}")
        print("  Install streamlit for the full wizard: pip install streamlit")
        httpd.serve_forever()


if __name__ == "__main__":
    try:
        import streamlit
        # When run directly, launch Streamlit
        import subprocess
        subprocess.run([sys.executable, "-m", "streamlit", "run", __file__])
    except ImportError:
        print("  Streamlit not installed — using fallback HTTP server.")
        run_fallback_server()
