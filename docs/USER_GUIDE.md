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
└── sql/           # Oracle/SQL Server SQL files (.sql)
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

## 4. Running the Migration

### Full Pipeline (Recommended)

```bash
# Run all 5 phases (default target: Fabric)
informatica-to-fabric

# Target Azure Databricks instead
informatica-to-fabric --target databricks

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

# Phase 3: Pipeline generation
python run_pipeline_migration.py

# Phase 4: Validation script generation
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
├── pipelines/
│   └── PL_*.json               # Pipeline JSON (one per workflow)
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
| `--skip N [N...]` | Skip specific phases (0=assessment, 1=SQL, 2=notebooks, 3=pipelines, 4=validation) |
| `--only N [N...]` | Run only specific phases |
| `--dry-run` | Preview without writing files |
| `--verbose` | Enable detailed logging |
| `--resume` | Resume from last checkpoint |
| `--config FILE` | Load configuration from YAML file |
| `--target TARGET` | Target platform: `fabric` (default) or `databricks` |
| `--log-format json` | Use JSON-formatted logging |

### Interactive Dashboard

```bash
# Generate and open the migration dashboard
python dashboard.py --open
```

The dashboard provides a visual overview of migration progress, complexity distribution, and test results.
