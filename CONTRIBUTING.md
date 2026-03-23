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

**Current status:** 333 tests, 88% coverage.

## Test Files

| File | Purpose | Tests |
|------|---------|-------|
| `tests/test_migration.py` | Core unit tests | ~30 |
| `tests/test_extended.py` | Extended cases | ~50 |
| `tests/test_coverage.py` | Coverage push (Sprint 17) | ~112 |
| `tests/test_e2e.py` | End-to-end integration | 19 |
| `tests/test_iics.py` | IICS format support | 23 |
| `tests/test_gaps.py` | Gap remediation (Sprint 20) | 52 |

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
├── run_validation.py          # Phase 4: Validation script generation
├── run_migration.py           # Orchestrator (runs all phases)
├── generate_html_reports.py   # HTML report generation
├── dashboard.py               # Interactive dashboard
├── deploy_to_fabric.py        # Fabric deployment
├── tests/                     # Test suite
├── input/                     # Informatica XML exports
├── output/                    # Generated artifacts
├── templates/                 # Notebook/pipeline templates
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

## Pull Request Checklist

- [ ] Tests pass: `py -m pytest`
- [ ] Coverage doesn't regress: `py -m pytest --cov=.`
- [ ] Linting passes: `ruff check .`
- [ ] New features have tests
- [ ] TODO comments explain any manual steps needed
