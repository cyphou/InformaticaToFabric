# ADR-004: Azure Databricks as Second Target Platform

## Status
Accepted

## Context
Microsoft Fabric was the original target platform. Many enterprise customers also use Azure Databricks with Unity Catalog, requiring a second deployment target.

## Decision
Add Databricks as a first-class target platform controlled by `--target databricks` and the `INFORMATICA_MIGRATION_TARGET` environment variable. Use the same assessment and conversion pipeline, with target-aware code generation at the notebook, pipeline, and schema layers.

## Key Differences

| Aspect | Fabric | Databricks |
|--------|--------|------------|
| Namespace | 2-level (`lakehouse.table`) | 3-level (`catalog.schema.table`) |
| Widgets | `notebookutils.widgets.get()` | `dbutils.widgets.get()` |
| Secrets | `notebookutils.credentials.getSecret()` | `dbutils.secrets.get()` |
| Pipelines | Fabric Data Pipeline JSON | Databricks Workflow (Jobs API) JSON |
| DDL | Delta on Lakehouse | Delta on Unity Catalog |

## Rationale
- Reuse 100% of assessment and SQL conversion logic across both targets
- Target differentiation only at generation time (notebooks, pipelines, schemas)
- Dedicated templates (`notebook_template_databricks.py`, `pipeline_template_databricks.json`) prevent cross-contamination
- Unity Catalog 3-level namespace ensures data governance compliance

## Consequences
- Every code-generation function checks `_get_target()` for conditional output
- Templates are duplicated (Fabric vs Databricks) but share the same structure
- Deployment scripts are separate (`deploy_to_fabric.py` vs `deploy_to_databricks.py`)
- Test suite includes 50+ Databricks-specific tests
