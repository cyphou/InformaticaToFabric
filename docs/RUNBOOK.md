# Operations Runbook — Informatica to Fabric Migration

## Quick Reference

| Action | Command |
|--------|---------|
| Full migration | `informatica-to-fabric run --config migration.yaml` |
| Full migration (Databricks) | `informatica-to-fabric run --config migration.yaml --target databricks` |
| Assessment only | `informatica-to-fabric run --only 0` |
| Resume failed run | `informatica-to-fabric run --resume` |
| Dry run | `informatica-to-fabric run --dry-run` |
| Batch processing | `informatica-to-fabric run --batch dir1/ dir2/` |
| Web wizard | `streamlit run web/app.py` |

---

## Pre-Flight Checklist

- [ ] Python 3.10+ installed
- [ ] `pip install -e .` completed
- [ ] Informatica XML exports placed in `input/mappings/` and `input/workflows/`
- [ ] SQL files placed in `input/sql/`
- [ ] `migration.yaml` configured with Fabric workspace ID or Databricks workspace URL
- [ ] Network access to Fabric workspace or Databricks workspace (if deploying)

---

## Phase-by-Phase Execution

### Phase 0: Assessment

```bash
informatica-to-fabric run --only 0
```

**Outputs:** `output/inventory/inventory.json`, `complexity_report.md`, `dependency_dag.json`, `wave_plan.json`

**Verify:**
- Mapping count matches Informatica source
- Complexity breakdown looks reasonable
- No ERROR-level issues in console output

### Phase 1: SQL Migration

```bash
informatica-to-fabric run --only 1
```

**Outputs:** `output/sql/SQL_*.sql`, `output/sql/SQL_OVERRIDES_*.sql`

**Verify:**
- All `TODO` comments reviewed
- Oracle/SQL Server constructs converted
- No unhandled DBMS_* or UTL_* calls remain

### Phase 2: Notebook Migration

```bash
informatica-to-fabric run --only 2
```

**Outputs:** `output/notebooks/NB_*.py`

**Verify:**
- One notebook per mapping
- **Fabric:** Parameters wired via `notebookutils.widgets.get()`; Lakehouse references use `bronze.*` / `silver.*` / `gold.*`
- **Databricks:** Parameters wired via `dbutils.widgets.get()`; Unity Catalog references use `catalog.schema.table` (3-level namespace)

### Phase 3: Pipeline Migration

```bash
informatica-to-fabric run --only 3
```

**Outputs:** `output/pipelines/PL_*.json`

**Verify:**
- Dependency chains preserved
- Schedule triggers populated
- Activity references point to correct notebooks
- **Databricks:** Workflow JSON uses `notebook_task` (not `NotebookActivity`)

### Phase 3b: Databricks Workflow Deployment (if `--target databricks`)

```bash
# Import notebooks to Databricks workspace
databricks workspace import_dir output/notebooks/ /Shared/migration --overwrite

# Create workflow jobs from generated JSON
for f in output/pipelines/PL_*.json; do
    databricks jobs create --json-file "$f"
done
```

### Phase 4: Schema Generation

```bash
informatica-to-fabric run --only 4
```

**Outputs:** `output/schema/`

**Verify:**
- Delta Lake DDL correct
- Type mappings reviewed (especially DATE, NUMBER, CLOB)

### Phase 5: Validation

```bash
informatica-to-fabric run --only 5
```

**Outputs:** `output/validation/VAL_*.py`

**Verify:**
- Row count checks present
- Checksum validation for key tables
- JDBC connection placeholders filled in

---

## Troubleshooting

### Assessment fails to parse XML

**Cause:** Malformed XML or encoding issues.

**Fix:**
1. Check XML is well-formed: `xmllint --noout file.xml`
2. If encoding issues, re-export from Informatica with UTF-8 encoding
3. Check `output/inventory/parse_issues.json` for specific errors

### SQL conversion produces incorrect output

**Cause:** Complex SQL constructs not covered by regex rules.

**Fix:**
1. Review `TODO` comments in `output/sql/`
2. Check if the construct is in `ORACLE_REPLACEMENTS` / `SQLSERVER_REPLACEMENTS`
3. Add custom rules to `run_sql_migration.py` if needed
4. For PL/SQL packages, consider manual rewrite as Spark notebooks

### Notebooks fail in Fabric

**Cause:** Missing tables, incorrect column names, or unsupported functions.

**Fix:**
1. Ensure Bronze lakehouse tables exist (run schema DDL first)
2. Search for `TODO` in notebook and fill in actual column names
3. Check Spark SQL function compatibility: `broadcast()`, `MERGE INTO`
4. Verify Delta Lake table paths

### Pipeline dependency errors

**Cause:** Circular dependencies or missing notebook references.

**Fix:**
1. Check `output/inventory/dependency_dag.json` for cycles
2. Verify notebook names in pipeline JSON match actual files
3. Check `wave_plan.json` for wave ordering

---

## Monitoring & Observability

### Audit Log
Every migration run generates `output/audit_log.json`:
```json
{
  "migration_run": "2024-01-15T10:30:00Z",
  "phases": [...],
  "summary": {
    "total_phases": 6,
    "succeeded": 6,
    "failed": 0,
    "total_duration_seconds": 12.5
  }
}
```

### Profiling
Enable with `--profile` flag:
```bash
informatica-to-fabric run --profile
```
Adds per-phase memory tracking (`memory_before_mb`, `memory_after_mb`, `memory_delta_mb`) to audit log.

### Dashboard
```bash
python dashboard.py
# Opens output/dashboard.html
```

---

## Multi-Tenant Operations

### Key Vault Integration
Use `{{KV:secret-name}}` placeholders in `migration.yaml`:
```yaml
# Fabric target
fabric:
  workspace_id: "{{KV:workspace-id}}"
source:
  jdbc_url: "{{KV:oracle-jdbc-url}}"
```

**Databricks alternative:** Use `dbutils.secrets.get(scope="my-scope", key="secret-key")` for credential retrieval in generated notebooks.

Run with tenant:
```bash
informatica-to-fabric run --tenant my-keyvault-name
```

### Batch Processing
```bash
informatica-to-fabric run --batch tenant_a/input/ tenant_b/input/
```

### Deployment Manifest
```bash
informatica-to-fabric run --manifest
# Generates output/manifest.json for CI/CD pipelines
```

---

## Recovery Procedures

### Resuming a Failed Migration
```bash
# Resume from last successful phase
informatica-to-fabric run --resume

# Check what was completed
cat output/.checkpoint.json
```

### Starting Fresh
```bash
# Clear checkpoint and re-run
informatica-to-fabric run --reset

# Or manually remove outputs
rm -rf output/notebooks/ output/pipelines/ output/sql/ output/validation/
```

### Partial Re-Run
```bash
# Re-run only notebook generation (Phase 2)
informatica-to-fabric run --only 2

# Skip assessment, run everything else
informatica-to-fabric run --skip 0
```

---

## Emergency Contacts

| Role | Contact |
|------|---------|
| Migration Lead | [Fill in] |
| Fabric Admin | [Fill in] |
| Source DBA | [Fill in] |
| Network/Security | [Fill in] |
