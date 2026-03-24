# Migration Summary

**Generated:** 2026-03-24 07:57 UTC

## Phase Results

| Phase | Name | Status | Duration |
|-------|------|--------|----------|
| 0 | Assessment | ⏭️ Skipped | — |
| 1 | SQL Migration | ❌ None | — |
| 2 | Notebook Migration | ❌ None | — |
| 3 | Pipeline Migration | ❌ None | — |
| 4 | Validation | ❌ None | — |

**Phases completed:** 0/5
**Total duration:** 0.0s

## Output Directories

| Directory | Contents |
|-----------|----------|
| `output/inventory/` | Assessment inventory, complexity report, DAG, HTML report |
| `output/sql/` | Converted SQL files (Oracle/SQL Server → Spark SQL) |
| `output/notebooks/` | PySpark notebooks (one per mapping) |
| `output/pipelines/` | Fabric Pipeline JSON (one per workflow) |
| `output/validation/` | Validation notebooks + test matrix |

## Next Steps

1. Review generated artifacts in `output/`
2. Fill in TODO placeholders in notebooks and SQL files
3. Configure JDBC connections in validation notebooks
4. Deploy to Fabric via Git integration or REST API
5. Run validation notebooks against live data
