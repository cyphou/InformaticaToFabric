# Migration Summary — Target: Fabric

**Generated:** 2026-04-07 16:45 UTC
**Target Platform:** Fabric

## Phase Results

| Phase | Name | Status | Duration |
|-------|------|--------|----------|
| 0 | Assessment | ⏭️ Skipped | — |
| 1 | SQL Migration | ❌ None | — |
| 2 | Notebook Migration | ❌ None | — |
| 3 | DBT Migration | ❌ None | — |
| 4 | Pipeline Migration | ❌ None | — |
| 5 | AutoSys Migration | ❌ None | — |
| 6 | Schema Generation | ❌ None | — |
| 7 | Validation | ❌ None | — |

**Phases completed:** 0/8
**Total duration:** 0.0s

## Output Directories

| Directory | Contents |
|-----------|----------|
| `output/inventory/` | Assessment inventory, complexity report, DAG, HTML report |
| `output/sql/` | Converted SQL files (Oracle/SQL Server → Spark SQL) |
| `output/notebooks/` | PySpark notebooks (one per mapping) |
| `output/pipelines/` | Pipeline definitions (one per workflow) |
| `output/schema/` | DDL + workspace setup notebook |
| `output/validation/` | Validation notebooks + test matrix |
| `output/audit_log.json` | Structured audit log (JSON) |

## Next Steps

1. Review generated artifacts in `output/`
2. Fill in TODO placeholders in notebooks and SQL files
3. Configure JDBC connections in validation notebooks
4. Deploy to Fabric via Git integration or REST API
5. Run validation notebooks against live data
