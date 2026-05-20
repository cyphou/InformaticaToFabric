<!-- Copilot instructions for the Informatica to Databricks + Fabric migration project -->

# Project: Informatica to Databricks + Fabric Migration

Automated migration of Informatica artifacts to Databricks + Fabric format.

## Architecture — Pipeline

```
Informatica → Databricks + Fabric
```

## Project Structure

- **Source / Extraction**: `src/`
- **Target / Generation**: `informatica_to_fabric.egg-info/`, `output/`
- **Tests**: `tests/` (37 test files)
- **Docs**: `docs/`

## Key Modules

- **Generation**:
  - `benchmarks\generate_mappings.py`
  - `cicd_generator.py`
  - `diff_generator.py`
  - `generate_html_reports.py`
  - `generate_pptx.py`
  - `iac_generator.py`
  - `run_blueprint_generator.py`
  - `run_schema_generator.py`
- **Conversion**:
  - `ai_converter.py`
- **Assessment**:
  - `output\databricks_bundle\src\NB_DQ_VALIDATE_EMAILS.py`
  - `output\notebooks\NB_DQ_VALIDATE_EMAILS.py`
  - `output\validation\VAL_AGG_ORDERS_BY_CUSTOMER.py`
  - `output\validation\VAL_DIM_CUSTOMER.py`
  - `output\validation\VAL_DIM_EMPLOYEE.py`
  - `output\validation\VAL_DIM_INVENTORY.py`
  - `output\validation\VAL_FACT_ORDERS.py`
  - `output\validation\VAL_FACT_TXN_HIGH.py`
  - `output\validation\VAL_FACT_TXN_LOW.py`
  - `output\validation\VAL_FACT_TXN_TAGS.py`
  - `output\validation\VAL_LAKEHOUSE_BRONZE.py`
  - `output\validation\VAL_LAKEHOUSE_SILVER.py`
  - `output\validation\VAL_PIPELINE_EXECUTION.py`
  - `output\validation\VAL_TGT_ACCOUNTS.py`
  - `output\validation\VAL_TGT_ALERT_QUEUE.py`
  - ... and 14 more
- **Deployment**:
  - `deploy_dbt_project.py`
  - `deploy_to_databricks.py`
  - `deploy_to_fabric.py`
  - `idmc_client.py`
- **Utilities**:
  - `agentic_alerting.py`
  - `api_server.py`
  - `assistant.py`
  - `benchmarks\__init__.py`
  - `benchmarks\run_benchmark.py`
  - `catalog_integration.py`
  - `certification.py`
  - `compliance.py`
  - `cost_advisor.py`
  - `dashboard.py`
  - `datadog_integration.py`
  - `examples\plugins\example_custom_udf.py`
  - `examples\plugins\example_header_injector.py`
  - `examples\plugins\example_naming_enforcer.py`
  - `migration_review.py`
  - ... and 55 more

## Hard Constraints

1. **Read before write** — never assume file contents from memory
2. **Test after every change** — run `pytest tests/ --tb=short -q`
3. **No duplicate functions** — always search for an existing name before creating one
4. **Git hygiene** — commit only when tests pass, conventional messages (`feat:`, `fix:`, `test:`, `docs:`)

## Multi-Agent Architecture

This project uses a specialized agent architecture. See `docs/AGENTS.md` for the full
architecture diagram and `.github/agents/` for per-agent definitions.

## Workflow Rules

### 1. Plan Before Build
- For multi-step work, create a plan before starting
- If something goes sideways, STOP and re-plan

### 2. Read Before Write
- **Always read target code before editing**
- Read `copilot-instructions.md` at session start for project rules

### 3. Testing Contract
- Run `pytest tests/ --tb=short -q` after EVERY implementation change
- If tests fail → fix them before reporting completion
- New features **require** new tests
- Never weaken test assertions to make tests pass

### 4. Scope Discipline
- Only modify files directly related to the task
- No drive-by refactors
- Prefer the smallest change that solves the problem
