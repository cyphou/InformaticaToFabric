# ADR-005: DBT as Third Target Platform

## Status
Accepted

## Context
For SQL-expressible transformations, dbt (data build tool) offers a declarative, version-controlled alternative to PySpark notebooks. Customers with dbt expertise want native dbt model output.

## Decision
Add `--target dbt` and `--target auto` modes. SQL-expressible mappings (Simple/Medium complexity) generate dbt SQL models; Complex/Custom mappings are forced to PySpark notebooks.

## Routing Logic (`auto` mode)
```
if complexity in ("Simple", "Medium") and no_stored_procs and no_java_transforms:
    → dbt SQL model
else:
    → PySpark notebook
```

## Generated Artifacts
- `dbt_project.yml` — project configuration
- `profiles.yml` — Databricks connection profile
- `models/staging/stg_*.sql` — source staging models
- `models/intermediate/int_*.sql` — transformation models
- `models/marts/mart_*.sql` — business-layer models
- `macros/` — reusable Jinja macros (NVL, DECODE, IIF)
- `tests/` — dbt schema tests (not_null, unique, relationships)
- `seeds/` — static reference data

## Rationale
- dbt is industry-standard for SQL-first analytics engineering
- Jinja templating replaces Informatica expression syntax naturally
- Incremental models (`is_incremental()`) replace Informatica update strategies
- Snapshots replace SCD Type 2 patterns
- dbt test framework provides built-in validation

## Consequences
- Mixed-mode workflows combine dbt tasks + notebook tasks in Databricks Workflows
- Informatica functions (IIF, DECODE, NVL) are wrapped as dbt Jinja macros
- Complex PL/SQL and Java transforms cannot be expressed in dbt and fall back to PySpark
- CI/CD uses `dbt build --select` for incremental deployment
