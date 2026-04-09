# User Guide — Informatica to Microsoft Fabric / Azure Databricks Migration

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [Preparing Input Files](#3-preparing-input-files)
4. [Running the Migration](#4-running-the-migration)
5. [Understanding the Output](#5-understanding-the-output)
6. [Using AI Agents](#6-using-ai-agents)
7. [Deploying to Fabric](#7-deploying-to-fabric)
8. [Deploying to Azure Databricks](#8-deploying-to-azure-databricks)
9. [Configuration Options](#9-configuration-options)

---

## 1. Prerequisites

- **Python 3.10+** (tested with 3.14)
- **VS Code** with GitHub Copilot (for agent-driven workflow)
- **Informatica XML exports** — workflow, mapping, and session XML files
- **Microsoft Fabric workspace** (for Fabric target) **or Azure Databricks workspace** (for Databricks target)

## 2. Installation

```bash
# Clone the repository
git clone https://github.com/cyphou/InformaticaToFabric.git
cd InformaticaToFabric

# Install in development mode
pip install -e ".[dev]"

# Verify installation
informatica-to-fabric --help
```

## 3. Preparing Input Files

Place your Informatica export files in the `input/` directory:

```
input/
├── workflows/     # Workflow XML exports (.xml)
├── mappings/      # Mapping XML exports (.xml)
├── sessions/      # Session XML exports (.xml)
├── sql/           # Oracle/SQL Server SQL files (.sql)
└── autosys/       # AutoSys JIL files (.jil) — optional
```

### Exporting from Informatica PowerCenter

1. Open **Informatica Repository Manager**
2. Select the folder containing your workflows
3. Right-click → **Export Objects…**
4. Choose **XML** format
5. Save workflow XMLs to `input/workflows/`
6. Repeat for mappings → `input/mappings/`

### Exporting from IICS (Cloud)

1. Open **Informatica Intelligent Cloud Services**
2. Navigate to **Data Integration → Explore**
3. Select taskflows/mappings → **Export**
4. Save to `input/workflows/` (taskflows) or `input/mappings/`

> **Tip:** The tool auto-detects whether files are PowerCenter or IICS format.

### Exporting from AutoSys

If your Informatica workflows are scheduled via CA AutoSys:

1. Use `autorep -J ALL -q` to export job definitions, or
2. Export individual JIL blocks: `autorep -J BOX_NAME -q > box_name.jil`
3. Save `.jil` files to `input/autosys/`

The tool will:
- Parse BOX, CMD, FW, and FT job types
- Extract `condition` dependency chains (`s(job)`, `f(job)`, `n(job)`, `d(job)`)
- Convert `days_of_week` + `start_times` to cron schedules
- **Link** `pmcmd startworkflow` commands to Informatica workflows in the inventory
- Generate Fabric Pipeline or Databricks Workflow JSON per BOX/chain

## 4. Running the Migration

### Full Pipeline (Recommended)

```bash
# Run all 8 phases (default target: Fabric)
informatica-to-fabric

# Target Azure Databricks instead
informatica-to-fabric --target databricks

# Target DBT models on Databricks (SQL-expressible mappings → dbt, complex → PySpark)
informatica-to-fabric --target dbt

# Auto-route: simple/medium → dbt, complex → PySpark notebooks
informatica-to-fabric --target auto

# PySpark-only on Databricks (no dbt)
informatica-to-fabric --target pyspark

# Include AutoSys JIL files from a custom directory
informatica-to-fabric --autosys-dir /path/to/jil/files

# Or equivalently
python run_migration.py
```

### Phase by Phase

```bash
# Phase 0: Assessment only
python run_assessment.py

# Phase 1: SQL conversion
python run_sql_migration.py

# Phase 2: Notebook generation
python run_notebook_migration.py

# Phase 3: DBT model generation (Databricks target only)
python run_dbt_migration.py

# Phase 4: Pipeline generation
python run_pipeline_migration.py

# Phase 5: AutoSys JIL migration
python run_autosys_migration.py
# Or from a custom directory:
python run_autosys_migration.py /path/to/jil/files

# Phase 6: Schema generation
python run_schema_generator.py

# Phase 7: Validation script generation
python run_validation.py
```

### Common Options

```bash
# Skip assessment (already done)
informatica-to-fabric --skip 0

# Run only SQL + notebooks
informatica-to-fabric --only 1 2

# Dry run (preview without writing files)
informatica-to-fabric --dry-run --verbose

# Resume from last completed phase
informatica-to-fabric --resume
```

### Observability & Monitoring Options

```bash
# Enable Datadog integration (logs, metrics, APM tracing)
informatica-to-fabric --datadog

# Enable agentic alerting (auto-remediation)
informatica-to-fabric --agent monitor       # Observe-only mode
informatica-to-fabric --agent auto_fix      # Auto-fix with approval
informatica-to-fabric --agent full_auto     # Fully autonomous

# Check global monitoring platform status
informatica-to-fabric --platform-status

# Generate monitoring platform report
informatica-to-fabric --platform-report

# Run migration review workflow (merge, optimize, rework)
informatica-to-fabric --review
```

## 5. Understanding the Output

After a full run, `output/` contains:

```
output/
├── inventory/
│   ├── inventory.json          # Full inventory of all parsed objects
│   ├── complexity_report.md    # Complexity classification
│   ├── dependency_dag.json     # Workflow dependency graph
│   ├── assessment_report.html  # Interactive HTML report
│   └── migration_report.html   # Migration readiness report
├── sql/
│   ├── SQL_*.sql               # Converted SQL files
│   └── SQL_OVERRIDES_*.sql     # Converted SQL overrides
├── notebooks/
│   └── NB_*.py                 # PySpark notebooks (one per mapping)
├── dbt/                        # DBT project (if --target dbt|auto)
│   ├── dbt_project.yml         # dbt project configuration
│   ├── profiles.yml            # Databricks SQL Warehouse connection
│   ├── packages.yml            # dbt package dependencies
│   └── models/
│       ├── sources.yml         # Source table definitions
│       ├── schema.yml          # Column tests & docs
│       ├── staging/stg_*.sql   # 1:1 source views
│       ├── intermediate/int_*.sql  # Transformation logic (CTE chains)
│       └── marts/mart_*.sql    # Business-ready tables
├── pipelines/
│   └── PL_*.json               # Pipeline JSON (one per workflow)
├── autosys/                    # AutoSys migration output
│   ├── PL_AUTOSYS_*.json       # Pipeline/Workflow JSON (one per BOX/chain)
│   └── autosys_summary.json    # Linkage report, job counts, unlinked warnings
├── validation/
│   ├── VAL_*.py                # Validation notebooks
│   └── test_matrix.md          # Test coverage matrix
├── migration_summary.md        # Overall summary
└── migration_issues.md         # Issues requiring attention
```

### Key Files to Review

| File | What to Check |
|------|--------------|
| `inventory.json` | Verify all mappings/workflows were parsed |
| `complexity_report.md` | Review complexity classifications |
| `NB_*.py` notebooks | Check transformation logic, especially TODO comments |
| `PL_*.json` pipelines | Verify activity dependencies and parameters |
| `migration_issues.md` | Address any flagged issues before deployment |

### Understanding TODO Comments

Generated code includes `-- TODO:` markers for items needing manual review:

- **`TODO: Replace Oracle DBMS package call`** — Oracle-specific packages with no direct Spark equivalent
- **`TODO: Convert to LEFT JOIN`** — Oracle `(+)` outer join syntax
- **`TODO: Replace @<link> DB link with spark.read.jdbc()`** — Database links
- **`TODO: Replace with Delta table + scheduled refresh`** — Materialized views

## 6. Using AI Agents

In VS Code with Copilot, invoke specialized agents in the chat:

| Task | Command |
|------|---------|
| Full migration | `@migration-orchestrator start migration` |
| Parse & inventory | `@assessment parse input/workflows/` |
| Convert a mapping | `@notebook-migration convert mapping M_LOAD_CUSTOMERS` |
| Convert SQL | `@sql-migration convert Oracle SQL overrides` |
| Generate a pipeline | `@pipeline-migration convert workflow WF_DAILY_LOAD` |
| Generate tests | `@validation generate tests for Silver tables` |

See [AGENTS.md](../AGENTS.md) for full agent documentation.

## 7. Deploying to Fabric

### Using the Deploy Script

```bash
# Preview what would be deployed
python deploy_to_fabric.py --workspace-id <GUID> --dry-run

# Deploy all artifacts
python deploy_to_fabric.py --workspace-id <GUID>
```

### Manual Deployment

1. **Notebooks:** Upload `.py` files to your Fabric workspace → Notebooks
2. **Pipelines:** Import `.json` files via Fabric Data Factory → Import
3. **SQL:** Execute converted SQL in Fabric SQL endpoints

### Using Fabric Git Integration

1. Connect your Fabric workspace to a Git repository
2. Push the `output/` contents to the repo
3. Fabric will auto-sync notebooks and pipelines

## 8. Deploying to Azure Databricks

### Import Notebooks

```bash
# Import notebooks via Databricks CLI
databricks workspace import_dir output/notebooks/ /Shared/migration --overwrite
```

### Create Workflow Jobs

```bash
# Create jobs from generated workflow JSON
for f in output/pipelines/PL_*.json; do
    databricks jobs create --json-file "$f"
done
```

### Using Databricks Repos

1. Link your Git repository to Databricks Repos
2. Push the `output/` contents to the repo
3. Reference notebooks from workflow job definitions

> **Note:** Databricks notebooks use `dbutils.widgets.get()` for parameters and Unity Catalog 3-level namespace (`catalog.schema.table`).

## 9. Configuration Options

### CLI Arguments

| Flag | Description |
|------|------------|
| `--skip N [N...]` | Skip specific phases (0=assessment, 1=SQL, 2=notebooks, 3=dbt, 4=pipelines, 5=autosys, 6=schema, 7=validation) |
| `--only N [N...]` | Run only specific phases |
| `--dry-run` | Preview without writing files |
| `--verbose` | Enable detailed logging |
| `--resume` | Resume from last checkpoint |
| `--config FILE` | Load configuration from YAML file |
| `--target TARGET` | Target platform: `fabric` (default), `databricks`, `dbt`, `pyspark`, or `auto` |
| `--autosys-dir DIR` | Path to directory containing AutoSys JIL files (default: `input/autosys/`) |
| `--log-format json` | Use JSON-formatted logging |
| `--batch DIR [DIR...]` | Run migration for multiple input directories |
| `--manifest` | Generate deployment manifest after migration |
| `--tenant ID` | Tenant ID for Key Vault secret substitution |
| `--parallel-waves N` | Max parallel wave executions |
| `--profile` | Enable per-phase memory and timing profiling |
| `--reset` | Clear checkpoint and start fresh |
| `--datadog` | Enable Datadog observability (logs, metrics, APM tracing) |
| `--agent MODE` | Agentic alerting mode: `monitor`, `auto_fix`, or `full_auto` |
| `--platform-status` | Show global monitoring platform status |
| `--platform-report` | Generate monitoring platform report |
| `--review` | Run migration review workflow (merge, optimize, rework) |

### Interactive Dashboard

```bash
# Generate and open the migration dashboard
python dashboard.py --open
```

The dashboard provides a visual overview of migration progress, complexity distribution, and test results.

## 10. Datadog Observability

### Setup

1. Install Datadog dependencies:
   ```bash
   pip install informatica-to-fabric[datadog]
   ```

2. Set your Datadog API key:
   ```bash
   export DD_API_KEY="your-api-key"
   ```

3. Configure in `migration.yaml`:
   ```yaml
   datadog:
     enabled: true
     site: "datadoghq.com"        # or datadoghq.eu, us3.datadoghq.com
     service: "informatica-migration"
     env: "production"
     logs:
       enabled: true
       source: "informatica-migration"
     metrics:
       enabled: true
       prefix: "informatica.migration"
     tracing:
       enabled: false              # Requires ddtrace
       sample_rate: 1.0
   ```

4. Run with Datadog enabled:
   ```bash
   informatica-to-fabric --datadog
   ```

### What Gets Sent

- **Logs:** Structured migration logs to Datadog Log Explorer (phase, mapping name, duration)
- **Metrics:** Per-phase duration, artifact counts, conversion scores, deployment status
- **APM Traces:** Distributed traces per phase for performance profiling (when tracing enabled)
- **Monitors:** Alert definitions for migration failures and SLA breaches

### Graceful Degradation

If `datadog-api-client` is not installed, the tool logs a warning and continues without Datadog integration — no crash.

## 11. Agentic Alerting & Auto-Remediation

The agentic alerting system processes migration signals and can automatically remediate common issues:

```bash
# Monitor mode — observe and alert but don't auto-fix
informatica-to-fabric --agent monitor

# Auto-fix mode — fix known issues with approval
informatica-to-fabric --agent auto_fix

# Full auto — autonomous remediation with learning loop
informatica-to-fabric --agent full_auto
```

The agent builds a learning database (`output/agent_learning.db`) that improves remediation confidence over time.

## 12. IDMC Full Component Assessment

For environments with Informatica Intelligent Data Management Cloud (IDMC):

```bash
# Assess all IDMC components via REST API
informatica-to-fabric --review
```

Covers: CDI, CDGC, CDQ, MDM, DI, B2B, API Manager, Connector, EDC, Axon, Marketplace, and Test Data Management.
