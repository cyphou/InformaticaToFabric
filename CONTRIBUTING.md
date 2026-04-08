# Contributing

## Development Setup

```bash
# Clone
git clone https://github.com/cyphou/InformaticaToFabric.git
cd InformaticaToFabric

# Install in development mode with test dependencies
pip install -e ".[dev]"

# Verify
py -m pytest --tb=short
```

## Running Tests

```bash
# Full suite
py -m pytest

# With coverage
py -m pytest --cov=. --cov-report=term-missing

# Specific test file
py -m pytest tests/test_gaps.py -v

# Specific test class
py -m pytest tests/test_coverage.py::TestSqlConversion -v
```

**Current status:** 1,489 tests, ~98% coverage. Multi-target support: Microsoft Fabric, Azure Databricks, and DBT.

## Test Files

| File | Purpose | Tests |
|------|---------|-------|
| `tests/test_migration.py` | Core unit tests | ~30 |
| `tests/test_extended.py` | Extended cases | ~50 |
| `tests/test_coverage.py` | Coverage push (Sprint 17) | ~112 |
| `tests/test_e2e.py` | End-to-end integration | 19 |
| `tests/test_iics.py` | IICS format support | 23 |
| `tests/test_gaps.py` | Gap remediation (Sprint 20) | 52 |
| `tests/test_sprint22_24.py` | Session config, scheduler, GTT/MV | ~57 |
| `tests/test_sprint25.py` | Lineage, scoring, multi-DB | ~35 |
| `tests/test_sprint26_30.py` | Templates, schema, waves, validation | ~112 |
| `tests/test_sprint31_40.py` | Phase 2: PL/SQL, DQ, multi-tenant, PII | ~109 |
| `tests/test_databricks_target.py` | Azure Databricks target (Unity Catalog, dbutils) | 83 |
| `tests/test_dbt_target.py` | DBT target router, model generation, E2E | 75 |
| `tests/test_autosys.py` | AutoSys JIL parsing, DAG, cron, pipeline gen | 63 |
| `tests/test_phase3_5.py` | Sprints 47–65: DLT, UC lineage, cluster policies, DBT advanced, AutoSys enhanced | 117 |
| `tests/test_artifact_validation.py` | Artifact validation: pipeline JSON, DBT SQL, notebook structure | 76 |
| `tests/test_sprint45.py` | Sprint 45: additional coverage | ~30 |
| `tests/test_sprint66.py` | Gap closure & lineage reports (Sprint 66) | ~40 |
| `tests/test_dbt_enhancements.py` | DBT enhancements (Sprint 67) | ~35 |
| `tests/test_sprint68_70.py` | Phase 7: DevOps, Platform-Native, Observability | ~60 |
| `tests/test_sprint71_73.py` | Phase 8: Query Optimization, PL/SQL, Dynamic SQL | ~55 |
| `tests/test_sprint74_76.py` | Phase 9: Plugins, SDK, Rule Engine | ~50 |
| `tests/test_sprint77_79.py` | Phase 10: Validation, Catalog Integration | ~50 |

## Code Style

We use **ruff** for linting and formatting:

```bash
# Lint
ruff check .

# Auto-fix
ruff check --fix .
```

Configuration is in `pyproject.toml`.

## Project Structure

```
├── run_assessment.py          # Phase 0: XML parsing & inventory
├── run_sql_migration.py       # Phase 1: Oracle/SQL Server → Spark SQL
├── run_notebook_migration.py  # Phase 2: Mappings → PySpark notebooks
├── run_pipeline_migration.py  # Phase 3: Workflows → Pipeline JSON
├── run_schema_generator.py    # Phase 4: Delta Lake schema generation
├── run_validation.py          # Phase 5: Validation script generation
├── run_dbt_migration.py        # Phase 3: DBT model generation
├── run_autosys_migration.py    # Phase 5: AutoSys JIL migration
├── run_migration.py           # Orchestrator (runs all phases)
├── run_artifact_validation.py  # Artifact validation
├── run_target_comparison.py    # Target comparison reports
├── generate_html_reports.py   # HTML report generation
├── generate_pptx.py           # PowerPoint deck generation
├── dashboard.py               # Interactive dashboard
├── deploy_to_fabric.py        # Fabric deployment
├── deploy_to_databricks.py    # Databricks deployment
├── deploy_dbt_project.py      # DBT project deployment
├── plugins.py                 # Plugin system (custom transforms)
├── sdk.py                     # Python SDK
├── api_server.py              # REST API server
├── rule_engine.py             # Configurable rule engine
├── catalog_integration.py     # Data catalog integration
├── tests/                     # 1,489 tests (22 test files)
├── input/                     # Informatica XML exports
│   └── autosys/               # AutoSys JIL files (.jil)
├── output/                    # Generated artifacts
│   └── dbt/                   # Generated DBT project
├── templates/                 # Notebook/pipeline templates (Fabric + Databricks + DBT)
└── docs/                      # Documentation
```

## Adding a New Transformation Type

1. Add the transformation abbreviation to `TRANSFORMATION_TYPE_MAP` in `run_assessment.py`
2. Add PySpark code generation in `run_notebook_migration.py` `_generate_cell()`
3. Add a test case in `tests/test_coverage.py`
4. Update `informatica-patterns.instructions.md` if relevant

## Adding a New SQL Conversion Rule

1. Add the regex pattern to `ORACLE_REPLACEMENTS` or `SQLSERVER_REPLACEMENTS` in `run_sql_migration.py`
2. Add test cases in `tests/test_gaps.py::TestSqlConversionGapRules` or `tests/test_coverage.py::TestSqlConversion`
3. Run the full suite to verify no regressions

## Target Platform Considerations

The tool supports **four targets**: Microsoft Fabric, Azure Databricks, DBT, and PySpark (with `--target auto` for auto-routing).

- When adding notebook-related features, ensure both `notebookutils` (Fabric) and `dbutils` (Databricks) paths work
- Pipeline generation must produce Fabric Pipeline JSON **or** Databricks Workflow JSON depending on `--target`
- Databricks uses Unity Catalog 3-level namespace (`catalog.schema.table`) vs Fabric 2-level (`schema.table`)
- DBT target generates Jinja SQL models under `output/dbt/` — test with `py -m pytest tests/test_dbt_target.py -v`
- AutoSys JIL integration converts BOX/CMD/FW jobs to Pipeline/Workflow JSON — test with `py -m pytest tests/test_autosys.py -v`
- Test Databricks paths with `py -m pytest tests/test_databricks_target.py -v`

## Pull Request Checklist

- [ ] Tests pass: `py -m pytest`
- [ ] Coverage doesn't regress: `py -m pytest --cov=.`
- [ ] Linting passes: `ruff check .`
- [ ] New features have tests
- [ ] TODO comments explain any manual steps needed
