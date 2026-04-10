# Multi-Agent Architecture — Informatica to Fabric / Databricks Migration

<p align="center">
  <img src="https://img.shields.io/badge/agents-9-0078D4?style=for-the-badge" alt="9 Agents"/>
  <img src="https://img.shields.io/badge/Informatica-FF4500?style=for-the-badge&logo=informatica&logoColor=white" alt="Informatica"/>
  <img src="https://img.shields.io/badge/%E2%86%92-gray?style=for-the-badge" alt="arrow"/>
  <img src="https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white" alt="Fabric"/>
  <img src="https://img.shields.io/badge/Azure%20Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks"/>
</p>

## Overview

This project uses a **9-agent specialization model** to automate and guide the migration from **Informatica PowerCenter and IICS** to **Microsoft Fabric** or **Azure Databricks**. Each agent is a VS Code Copilot agent (`.agent.md`) with scoped domain knowledge, file ownership, and clear boundaries.

**Current state:** 100 sprints complete (Phase 1–17 + DD1–DD12) — 2,143 tests, dual-target support (Microsoft Fabric + Azure Databricks), DBT model generation (`--target dbt|auto`), AutoSys JIL migration (BOX/CMD/FW → Pipeline/Workflow), full PowerCenter + IICS support, CLI tool (`informatica-to-fabric --target fabric|databricks|dbt|pyspark|auto --autosys-dir <path>`), Unity Catalog 3-level namespace, Databricks Workflows (Jobs API), Databricks deployment script (`deploy_to_databricks.py`), Unity Catalog lineage & permissions generator, cluster config & policy recommender, DLT notebook generation, Databricks SQL dashboards, DBU cost estimator, advanced Workflows (job clusters, health rules), DBT macros/incremental/snapshots/CI/CD/mixed workflows, DECODE→CASE SQL expansion, SCD2 snapshot detection, enriched CTEs from field lineage, Router→separate dbt models, standalone dbt deploy script, SVG lineage flow diagrams, HTML lineage reports, Event Wait/Raise pipeline activities, AutoSys condition conversion/alarms/calendars/machine mapping/coverage reports, session config mapping, schedule trigger conversion, GTT/MV/DB link detection, multi-DB support (Oracle, SQL Server, Teradata, DB2, MySQL, PostgreSQL), Delta Lake schema generation, migration wave planner, 5-level validation framework, credential sanitization, audit logging, PII detection, DQ rules, multi-tenant Key Vault integration, web UI wizard, enterprise runbook, advanced PL/SQL conversion, DevOps CI/CD (env configs, deployment pipelines, DAB bundles, promotion), platform-native features (Lakehouse vs Warehouse advisor, T-SQL DDL, SQL Warehouse DDL, OneLake shortcuts, Delta Sharing, Mirroring), observability (Fabric CU cost estimator, Azure Monitor metrics, Teams/Slack webhook alerting, Datadog logs/metrics/APM tracing), Datadog observability integration (structured logs, custom metrics, APM distributed tracing, monitor definitions), agentic alerting (signal processing, auto-remediation actions, learning loop, confidence scoring), global monitoring platform (unified control plane, 4-tier escalation chains, SLO tracking, enterprise dashboards), IDMC full component assessment (CDI/CDGC/CDQ/MDM/DI/B2B/API Manager/Connector/EDC/Axon/Market/Test Data parsing via REST API), migration review workflow (merge, optimize, rework phases, quality scoring, readiness gate), query optimization (partition strategy, Spark config tuning, broadcast join detection, materialization advisor), advanced PL/SQL engine (cursors, BULK COLLECT, FORALL, exception blocks, package state), dynamic SQL (EXECUTE IMMEDIATE, CONNECT BY→CTE, PIVOT/UNPIVOT, correlated subquery rewrite, temporal tables), plugin system (custom transforms, SQL rewrites, post-processing hooks), Python SDK & REST API, configurable rule engine (YAML/JSON rulesets), statistical validation (distribution comparison, SCD2 verification, null distribution, RI checks, A/B testing, business rules), data catalog integration (Purview entities, Unity Catalog lineage, column-level lineage, impact analysis), IaC generation (Terraform HCL + Azure Bicep for Fabric/Databricks provisioning), container & Kubernetes deployment (Dockerfile, Docker Compose, K8s manifests, Helm chart), CI/CD pipeline generation (GitHub Actions + Azure DevOps with environment gates), synthetic mapping benchmark suite (configurable scale/complexity), parallel generation & memory optimization (ProcessPoolExecutor, streaming XML, SQL cache), regression suite with golden dataset snapshots, Structured Streaming templates (Kafka/Event Hub/Auto Loader → Delta sink), CDC pattern detection & MERGE INTO generation (full_cdc/upsert_only/soft_delete), Change Data Feed reader, Fabric Eventstream definition generator, CDC validation (operation balance, orphan delete, merge key uniqueness), watermark/late-arrival/idempotency patterns, Azure Functions migration (7 trigger types: Service Bus, Event Hub, SQL trigger, Cosmos DB change feed, HTTP, Timer, Blob — with auto-detection of CDC/ESB/event-driven patterns, AzureFunctionActivity pipeline integration, and complete function project scaffolding), CDC/RT deployment blueprints (Event Hub + APIM + Azure Functions Bicep/Terraform/deployment scripts), governance & compliance (Fabric/Databricks RLS policies, column masking, GDPR/CCPA PII classification, retention policies, right-to-erasure templates, data residency validation, 6-gate certification workflow, evidence packages, sign-off templates), AI-assisted migration (LLM-powered SQL conversion, confidence scoring, pattern learning, gap resolution, chat-based assistant), visual lineage explorer (Cytoscape.js interactive graphs, impact analysis, side-by-side diff review, batch review reports), and ML pipeline templates (Feature Store notebooks, MLflow experiment tracking, batch scoring pipelines, TCO comparison, reserved capacity planner, cost allocation tags, cost optimization dashboard).

---

## Architecture Diagram

```mermaid
flowchart TB
    USER["👤 User"] --> ORCH
    ORCH["🎯 migration-orchestrator\nCoordinator Agent"]
    ORCH --> ASS["🔍 assessment\nDiscovery & Inventory\n(PowerCenter + IICS)"]
    ORCH --> SQL["🗄️ sql-migration\nOracle/SQL Server → Spark SQL"]
    ORCH --> NB["📓 notebook-migration\nMappings → PySpark"]
    ORCH --> PL["⚡ pipeline-migration\nWorkflows → Pipelines"]
    ORCH --> STR["🌊 streaming-migration\nCDC / RT / Functions"]
    ORCH --> GOV["🔒 governance\nSecurity / Compliance"]
    ORCH --> INF["🏗️ infrastructure\nIaC / CI-CD / Observability"]
    ORCH --> VAL["✅ validation\nTesting & QA"]

    ASS -.->|inventory.json\ncomplexity_report.md\ndependency_dag.json| SQL
    ASS -.->|mapping metadata| NB
    ASS -.->|workflow metadata| PL
    ASS -.->|CDC/streaming indicators| STR
    SQL -.->|converted SQL| NB
    NB -.->|notebook references| PL
    NB -.->|target tables| VAL
    PL -.->|pipeline references| VAL
    STR -.->|Functions/Eventstream| PL
    GOV -.->|certification gate| INF
    INF -.->|deploy status| ORCH

    style ORCH fill:#0078D4,color:#fff,stroke:#005A9E,stroke-width:2px
    style ASS fill:#E67E22,color:#fff,stroke:#CA6F1E,stroke-width:2px
    style SQL fill:#8E44AD,color:#fff,stroke:#7D3C98,stroke-width:2px
    style NB fill:#27AE60,color:#fff,stroke:#1E8449,stroke-width:2px
    style PL fill:#2980B9,color:#fff,stroke:#2471A3,stroke-width:2px
    style STR fill:#E74C3C,color:#fff,stroke:#C0392B,stroke-width:2px
    style GOV fill:#F39C12,color:#fff,stroke:#D68910,stroke-width:2px
    style INF fill:#1ABC9C,color:#fff,stroke:#16A085,stroke-width:2px
    style VAL fill:#C0392B,color:#fff,stroke:#A93226,stroke-width:2px
    style USER fill:#34495E,color:#fff,stroke:#2C3E50,stroke-width:2px
```

<details>
<summary><b>ASCII fallback diagram</b> (for environments without Mermaid)</summary>

```
                    ┌──────────────────────────┐
                    │  migration-orchestrator   │  ← Coordinator
                    │  (plans, delegates,       │
                    │   tracks progress)        │
                    └────────┬─────────────────┘
                             │ delegates to
          ┌──────────────────┼──────────────────────┐
          │                  │                       │
   ┌──────▼──────┐   ┌──────▼──────┐   ┌────────────▼────────┐
   │ assessment  │   │  notebook   │   │  pipeline           │
   │ (XML parse, │   │  migration  │   │  migration          │
   │  inventory) │   │  (PySpark)  │   │  (JSON pipelines)   │
   └─────────────┘   └──────┬──────┘   └─────────────────────┘
                             │
                      ┌──────▼──────┐
                      │    sql      │
                      │  migration  │
                      │ (Oracle→    │
                      │  SparkSQL)  │
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │ validation  │
                      │ (testing &  │
                      │  QA)        │
                      └─────────────┘
```

</details>

---

## Quick Reference

| Agent | Invoke With | Owns | Outputs |
|-------|-------------|------|---------|
| **🎯 @migration-orchestrator** | `@migration-orchestrator start migration` | Migration plan, wave scheduling, progress | `output/migration_summary.md` |
| **🔍 @assessment** | `@assessment parse input/workflows/` | XML parsing (PowerCenter + IICS), AutoSys JIL parsing, IDMC 12-service assessment (via REST API), inventory, complexity, DAG, session config, scheduler | `output/inventory/`, `output/autosys/` |
| **📓 @notebook-migration** | `@notebook-migration convert mapping M_X` | Mapping → PySpark notebook or DBT model generation (Fabric `notebookutils` or Databricks `dbutils` or `dbt`) | `output/notebooks/NB_*.py`, `output/dbt/` |
| **⚡ @pipeline-migration** | `@pipeline-migration convert workflow WF_X` | Workflow/Taskflow → Fabric Pipeline JSON or Databricks Workflow JSON, AutoSys BOX/CMD → Pipeline JSON | `output/pipelines/PL_*.json`, `output/autosys/PL_AUTOSYS_*.json` |
| **🗄️ @sql-migration** | `@sql-migration convert Oracle SQL overrides` | Oracle/SQL Server → Spark SQL / T-SQL (+ GTT, MV, DB link detection) | `output/sql/SQL_*.sql` |
| **🌊 @streaming-migration** | `@streaming-migration analyze CDC patterns` | CDC detection, Structured Streaming, Azure Functions, Eventstreams, deployment blueprints | `output/functions/`, `output/blueprints/` |
| **🔒 @governance** | `@governance apply security policies` | RLS/CLS policies, PII classification, GDPR/CCPA compliance, 6-gate certification | `output/catalog/`, `output/security/`, `output/compliance/` |
| **🏗️ @infrastructure** | `@infrastructure generate terraform` | IaC (Terraform/Bicep), CI/CD, containers, observability, deployment | `output/environments/`, `output/scripts/` |
| **✅ @validation** | `@validation generate tests for Silver tables` | Test scripts, row counts, checksums, diffs | `output/validation/VAL_*.py` |

---

## Agent Descriptions

### 1. 🎯 `migration-orchestrator` (Coordinator)

| | |
|---|---|
| **Role** | Top-level coordinator that plans the migration, delegates to specialized agents, and tracks overall progress |
| **Inputs** | User request (e.g., "migrate workflow X") or full migration scope |
| **Outputs** | Migration plan, progress tracking, delegation instructions, summary report |
| **File** | [.github/agents/migration-orchestrator.agent.md](.github/agents/migration-orchestrator.agent.md) |

### 2. 🔍 `assessment` (Discovery & Inventory)

| | |
|---|---|
| **Role** | Parses Informatica XML exports (PowerCenter + IICS) and IDMC REST API (12 cloud services: CDI, CDGC, CDQ, MDM, DI, B2B, API Manager, Connector, EDC, Axon, Market, Test Data), builds inventories, classifies complexity, maps dependencies, extracts session configs and scheduler definitions |
| **Inputs** | Informatica XML export files (workflows, mappings, sessions, IICS taskflows), IDMC REST API credentials (optional) |
| **Outputs** | `inventory.json`, `complexity_report.md`, `dependency_dag.json`, `idmc_inventory.json` |
| **File** | [.github/agents/assessment.agent.md](.github/agents/assessment.agent.md) |

### 3. 📓 `notebook-migration` (Transformation Conversion)

| | |
|---|---|
| **Role** | Converts Informatica mappings into Fabric Notebooks or Databricks Notebooks (PySpark) — target-aware via `--target` flag |
| **Inputs** | Mapping metadata from assessment, transformation rules, converted SQL |
| **Outputs** | Fabric Notebook `.py` files with PySpark transformation logic |
| **File** | [.github/agents/notebook-migration.agent.md](.github/agents/notebook-migration.agent.md) |

### 4. ⚡ `pipeline-migration` (Orchestration Conversion)

| | |
|---|---|
| **Role** | Converts Informatica workflows and IICS taskflows into Fabric Data Pipeline JSON or Databricks Workflow (Jobs API) JSON definitions (including schedule triggers) |
| **Inputs** | Workflow metadata from assessment, notebook references |
| **Outputs** | Fabric Data Pipeline JSON definitions with dependency chains |
| **File** | [.github/agents/pipeline-migration.agent.md](.github/agents/pipeline-migration.agent.md) |

### 5. 🗄️ `sql-migration` (SQL Conversion)

| | |
|---|---|
| **Role** | Converts Oracle/SQL Server SQL (overrides, stored procs) to Fabric-compatible SQL (Spark SQL / T-SQL), detects GTT, Materialized Views, and DB links |
| **Inputs** | SQL overrides from mappings, stored procedure files, Oracle and SQL Server sources |
| **Outputs** | Converted SQL files, Notebook `%%sql` cells |
| **File** | [.github/agents/sql-migration.agent.md](.github/agents/sql-migration.agent.md) |

### 6. ✅ `validation` (Testing & QA)

| | |
|---|---|
| **Role** | Generates validation notebooks, compares row counts, checksums, and data quality |
| **Inputs** | Source/target table pairs, migration metadata |
| **Outputs** | Validation notebooks, test matrix, pass/fail summaries |
| **File** | [.github/agents/validation.agent.md](.github/agents/validation.agent.md) |

### 7. 🌊 `streaming-migration` (CDC / Real-Time / Functions)

| | |
|---|---|
| **Role** | Detects CDC and streaming patterns, generates Structured Streaming notebooks, Azure Functions (7 triggers), Eventstream definitions, and CDC/RT deployment blueprints (Bicep/Terraform) |
| **Inputs** | Mapping metadata with streaming/CDC indicators from assessment, inventory.json |
| **Outputs** | Azure Functions projects (`output/functions/`), deployment blueprints (`output/blueprints/`), CDC MERGE cells in notebooks, Eventstream JSON in pipelines |
| **File** | [.github/agents/streaming-migration.agent.md](.github/agents/streaming-migration.agent.md) |

### 8. 🔒 `governance` (Security / Compliance / Certification)

| | |
|---|---|
| **Role** | Generates RLS/CLS security policies, PII classification, GDPR/CCPA compliance templates, 6-gate certification workflow, and data catalog integration (Purview/Unity Catalog) |
| **Inputs** | Inventory with column metadata, PII detection results, validation test results |
| **Outputs** | Security policies (`output/security/`), compliance reports (`output/compliance/`), catalog entities (`output/catalog/`) |
| **File** | [.github/agents/governance.agent.md](.github/agents/governance.agent.md) |

### 9. 🏗️ `infrastructure` (IaC / CI-CD / Observability / Deployment)

| | |
|---|---|
| **Role** | Generates Terraform/Bicep IaC, CI/CD pipelines (GitHub Actions/Azure DevOps), container configs (Docker/K8s/Helm), configures observability (Datadog/Azure Monitor), and manages environment promotion |
| **Inputs** | Generated artifacts from all agents, configuration from `migration.yaml` |
| **Outputs** | IaC templates (`output/environments/`), deployment scripts (`output/scripts/`), DAB bundles (`output/databricks_bundle/`) |
| **File** | [.github/agents/infrastructure.agent.md](.github/agents/infrastructure.agent.md) |

---

## Data Flow

```mermaid
flowchart LR
    subgraph "📂 Input"
        XML["Informatica\nXML Exports\n(PowerCenter + IICS)"]
        SQL_IN["Oracle/SQL Server\nStored Procs"]
    end

    subgraph "🔍 Phase 1 — Assessment"
        ASS["assessment agent\nParse → Classify → DAG"]
    end

    subgraph "⚙️ Phase 2 — Conversion"
        SQL_MIG["sql-migration\nOracle/SQL Server → Spark SQL"]
        NB_MIG["notebook-migration\nMapping → PySpark"]
        PL_MIG["pipeline-migration\nWorkflow → JSON"]
    end

    subgraph "✅ Phase 3 — Validation"
        VAL["validation agent\nRow counts + checksums"]
    end

    subgraph "📤 Output"
        INV["inventory.json"]
        NB_OUT["NB_*.py\nNotebooks"]
        PL_OUT["PL_*.json\nPipelines"]
        SQL_OUT["SQL_*.sql\nConverted SQL"]
        VAL_OUT["VAL_*.py\nTest scripts"]
    end

    XML --> ASS
    SQL_IN --> ASS
    ASS --> INV
    ASS --> SQL_MIG
    ASS --> NB_MIG
    ASS --> PL_MIG
    SQL_MIG --> SQL_OUT
    SQL_MIG --> NB_MIG
    NB_MIG --> NB_OUT
    NB_MIG --> PL_MIG
    PL_MIG --> PL_OUT
    NB_OUT --> VAL
    PL_OUT --> VAL
    VAL --> VAL_OUT

    style XML fill:#FF4500,color:#fff
    style SQL_IN fill:#FF4500,color:#fff
    style ASS fill:#E67E22,color:#fff
    style SQL_MIG fill:#8E44AD,color:#fff
    style NB_MIG fill:#27AE60,color:#fff
    style PL_MIG fill:#2980B9,color:#fff
    style VAL fill:#C0392B,color:#fff
    style INV fill:#ECF0F1,color:#2C3E50
    style NB_OUT fill:#ECF0F1,color:#2C3E50
    style PL_OUT fill:#ECF0F1,color:#2C3E50
    style SQL_OUT fill:#ECF0F1,color:#2C3E50
    style VAL_OUT fill:#ECF0F1,color:#2C3E50
```

---

## Agent Interaction — Full Migration

```mermaid
sequenceDiagram
    actor User
    participant Orch as 🎯 Orchestrator
    participant Ass as 🔍 Assessment
    participant SQL as 🗄️ SQL Migration
    participant NB as 📓 Notebook Migration
    participant PL as ⚡ Pipeline Migration
    participant Val as ✅ Validation

    User->>Orch: "Migrate all Informatica workflows to Fabric"

    rect rgb(230, 126, 34, 0.1)
        Note over Orch,Ass: Phase 1 — Discovery
        Orch->>Ass: Parse & inventory all XML exports
        Ass-->>Orch: inventory.json + complexity_report + DAG
    end

    rect rgb(142, 68, 173, 0.1)
        Note over Orch,SQL: Phase 2 — SQL Conversion
        Orch->>SQL: Convert Oracle SQL overrides from inventory
        SQL-->>Orch: Converted SQL files (output/sql/)
    end

    rect rgb(39, 174, 96, 0.1)
        Note over Orch,NB: Phase 3 — Notebook Generation
        Orch->>NB: Generate notebooks for all mappings
        NB-->>Orch: NB_*.py files (output/notebooks/)
    end

    rect rgb(41, 128, 185, 0.1)
        Note over Orch,PL: Phase 4 — Pipeline Generation
        Orch->>PL: Generate pipelines for all workflows
        PL-->>Orch: PL_*.json files (output/pipelines/)
    end

    rect rgb(192, 57, 43, 0.1)
        Note over Orch,Val: Phase 5 — Validation
        Orch->>Val: Generate validation for all migrated objects
        Val-->>Orch: VAL_*.py + test_matrix.md
    end

    Orch-->>User: ✅ Migration complete — review output/
```

### Single Mapping Flow

```mermaid
sequenceDiagram
    actor User
    participant NB as 📓 Notebook Migration

    User->>NB: "Convert mapping M_LOAD_CUSTOMERS"
    NB->>NB: Read mapping XML/metadata
    NB->>NB: Identify: SQ → EXP → FIL → LKP → TGT
    NB->>NB: Map each transformation → PySpark
    NB->>NB: Generate NB_M_LOAD_CUSTOMERS.py
    NB-->>User: ✅ output/notebooks/NB_M_LOAD_CUSTOMERS.py
```

---

## Handoff Protocol

When an agent encounters work outside its domain:

1. **Complete your part** — finish everything within your scope
2. **State the handoff** — clearly describe what needs to happen next
3. **Name the target agent** — e.g., "Hand off to @sql-migration for SQL override conversion"
4. **List artifacts** — specify files and data structures involved
5. **Include context** — provide intermediate results the next agent needs

---

## File Ownership Rules

- **One owner per output directory** — each agent writes only to its designated output folder
- **Read access is universal** — any agent can read any file for context
- **Write access is restricted** — only the owning agent writes to its output folder
- **Validation is cross-cutting** — reads outputs from all agents, writes only to `output/validation/`

| Agent | Write Access | Read Access |
|-------|-------------|-------------|
| 🎯 Orchestrator | `output/migration_summary.md`, `output/migration_issues.md` | Everything |
| 🔍 Assessment | `output/inventory/` | `input/` |
| 🗄️ SQL Migration | `output/sql/` | `output/inventory/`, `input/sql/` |
| 📓 Notebook Migration | `output/notebooks/`, `output/dbt/` | `output/inventory/`, `output/sql/`, `templates/` |
| ⚡ Pipeline Migration | `output/pipelines/`, `output/autosys/` | `output/inventory/`, `output/notebooks/`, `templates/` |
| 🌊 Streaming Migration | `output/functions/`, `output/blueprints/` | `output/inventory/`, `templates/streaming_*` |
| 🔒 Governance | `output/security/`, `output/compliance/`, `output/catalog/` | `output/inventory/`, `output/validation/` |
| 🏗️ Infrastructure | `output/environments/`, `output/scripts/`, `output/databricks_bundle/` | Everything (for deployment) |
| ✅ Validation | `output/validation/` | `output/notebooks/`, `output/pipelines/`, `output/sql/` |

---

## Directory Structure

```
InformaticaToDBFabric/
├── .github/
│   └── agents/                          # 🤖 Agent definitions (9 agents)
│       ├── migration-orchestrator.agent.md
│       ├── assessment.agent.md
│       ├── notebook-migration.agent.md
│       ├── pipeline-migration.agent.md
│       ├── sql-migration.agent.md
│       ├── validation.agent.md
│       ├── streaming-migration.agent.md
│       ├── governance.agent.md
│       └── infrastructure.agent.md
├── .vscode/
│   └── instructions/                    # 📘 Shared rules
│       └── informatica-patterns.instructions.md
├── docs/                                # 📖 User documentation
│   ├── USER_GUIDE.md                    #   Usage guide & examples
│   ├── TROUBLESHOOTING.md               #   Common issues & fixes
│   ├── ENTERPRISE_PLAYBOOK.md           #   Enterprise migration playbook
│   ├── RUNBOOK.md                       #   Operations runbook
│   └── ADR/                             #   Architecture Decision Records
├── input/                               # 📂 Informatica exports
│   ├── workflows/                       #   Workflow XML
│   ├── mappings/                        #   Mapping XML
│   ├── sessions/                        #   Session XML
│   ├── sql/                             #   Oracle SQL files
│   └── autosys/                         #   AutoSys JIL files (.jil)
├── output/                              # 📤 Generated artifacts
│   ├── inventory/                       #   🔍 Assessment results
│   ├── notebooks/                       #   📓 Fabric Notebooks
│   ├── dbt/                             #   🏗️ DBT project (models, config)
│   ├── pipelines/                       #   ⚡ Pipeline JSON
│   ├── autosys/                         #   ⏰ AutoSys → Pipeline/Workflow JSON
│   ├── sql/                             #   🗄️ Converted SQL
│   ├── schema/                          #   📐 Delta Lake schema DDL
│   ├── scripts/                         #   📜 Deployment scripts
│   └── validation/                      #   ✅ Test scripts
├── templates/                           # 📋 Reusable templates
│   ├── notebook_template.py             #   Fabric notebook template
│   ├── notebook_template_databricks.py  #   Databricks notebook template
│   ├── dbt_template.sql                 #   DBT model template (Jinja + SQL)
│   ├── pipeline_template.json           #   Fabric pipeline template
│   ├── pipeline_template_databricks.json #  Databricks workflow template
│   └── validation_template.py
├── tests/                               # 🧪 2,143 tests
│   ├── test_migration.py                #   Core conversion tests
│   ├── test_extended.py                 #   Extended transformation tests
│   ├── test_coverage.py                 #   Coverage gap tests
│   ├── test_e2e.py                      #   End-to-end integration tests
│   ├── test_iics.py                     #   IICS-specific tests
│   ├── test_gaps.py                     #   Gap remediation tests
│   ├── test_sprint22_24.py              #   Session config, scheduler, GTT/MV tests
│   ├── test_sprint25.py                 #   Lineage & scoring tests
│   ├── test_sprint26_30.py              #   Templates, schema, waves, validation, production
│   ├── test_sprint31_40.py              #   Phase 2 tests (object gaps, PL/SQL, multi-tenant, DQ)
│   ├── test_databricks_target.py        #   Azure Databricks target tests
│   ├── test_dbt_target.py              #   DBT target tests (Sprint 51)
│   ├── test_autosys.py                 #   AutoSys JIL tests (Sprint 61)
│   ├── test_sprint45.py               #   Sprint 45 coverage tests
│   ├── test_phase3_5.py               #   Phase 3-5 tests (Sprints 47–65)
│   ├── test_artifact_validation.py   #   Artifact validation (pipeline/DBT/notebook)
│   ├── test_sprint66.py              #   Gap closure & lineage reports (Sprint 66)
│   ├── test_dbt_enhancements.py      #   DBT enhancements (Sprint 67)
│   ├── test_sprint68_70.py           #   Phase 7 tests (DevOps, Platform-Native, Observability)
│   ├── test_sprint71_73.py           #   Phase 8 tests (Query Opt, PL/SQL, Dynamic SQL)
│   ├── test_sprint74_76.py           #   Phase 9 tests (Plugins, SDK, Rule Engine)
│   ├── test_sprint77_79.py           #   Phase 10 tests (Validation, Catalog)
│   ├── test_datadog.py               #   DD1–DD3 tests (Datadog logging, metrics, APM)
│   ├── test_agentic_alerting.py      #   DD4–DD6 tests (Agentic alerting & learning)
│   ├── test_monitoring_platform.py   #   DD7–DD9 tests (Global monitoring platform)
│   ├── test_idmc_review.py           #   DD10–DD12 tests (IDMC & migration review)
│   ├── test_sprint92_94.py           #   Phase 15 tests (IaC, Container, CI/CD)
│   ├── test_sprint95_97.py           #   Phase 16 tests (Benchmark, Parallel, Regression)
│   ├── test_functions_migration.py   #   Azure Functions migration tests (45 tests)
│   └── test_sprint83_85.py           #   Phase 12 tests (Security, Compliance, Certification)
│   ├── test_sprint86_88.py           #   Phase 13 tests (AI-Assisted SQL & Assistant)
│   ├── test_sprint89_91.py           #   Phase 14 tests (Lineage Explorer, Diff & Review)
│   └── test_sprint98_100.py          #   Phase 17 tests (ML Pipelines, Cost Advisor)
├── examples/                            # 📋 Example files
│   └── README.md
├── web/                                 # 🌐 Web UI wizard
│   └── app.py
├── run_migration.py                     # 🚀 Orchestrator (runs all phases)
├── run_assessment.py                    # XML parsing & inventory
├── run_sql_migration.py                 # Oracle/SQL → Spark SQL
├── run_notebook_migration.py            # Mappings → PySpark notebooks
├── run_pipeline_migration.py            # Workflows → Pipeline JSON
├── run_dbt_migration.py                 # DBT model generation
├── run_autosys_migration.py             # AutoSys JIL migration
├── run_schema_generator.py              # Delta Lake schema generation
├── run_functions_migration.py           # Azure Functions generation (CDC/ESB/event-driven)├── run_blueprint_generator.py           # 🏗️ CDC/RT deployment blueprint (Event Hub + APIM + Functions)├── security_migration.py                # 🔒 RLS/CLS policies & column masking (Phase 12)
├── compliance.py                        # 📋 GDPR/CCPA compliance & data residency (Phase 12)
├── certification.py                     # ✅ 6-gate certification & evidence packages (Phase 12)
├── run_validation.py                    # Validation script generation
├── run_artifact_validation.py           # Artifact validation
├── run_target_comparison.py             # Target comparison reports
├── generate_html_reports.py             # HTML report generation
├── generate_pptx.py                     # PowerPoint deck generation
├── dashboard.py                         # Interactive dashboard
├── deploy_to_fabric.py                  # Fabric deployment
├── deploy_to_databricks.py              # Databricks deployment
├── deploy_dbt_project.py                # DBT project deployment
├── plugins.py                           # Plugin system (custom transforms)
├── sdk.py                               # Python SDK
├── api_server.py                        # REST API server
├── rule_engine.py                       # Configurable rule engine
├── catalog_integration.py               # Data catalog integration (Purview/Unity)
├── datadog_integration.py               # 📊 Datadog observability (logs, metrics, APM)
├── agentic_alerting.py                  # 🤖 Agentic auto-remediation & learning loop
├── monitoring_platform.py               # 🌐 Global monitoring platform & escalation
├── idmc_client.py                       # 🔗 IDMC full component assessment
├── migration_review.py                  # 📋 Migration review workflow
├── iac_generator.py                     # 🏗️ Terraform & Bicep IaC generation
├── cicd_generator.py                    # 🔄 CI/CD pipeline generation (GitHub Actions & Azure DevOps)
├── ai_converter.py                      # 🤖 LLM-powered SQL conversion & pattern learning (Phase 13)
├── assistant.py                         # 💬 Chat-based migration assistant (Phase 13)
├── diff_generator.py                    # 🔍 Side-by-side diff, lineage explorer, batch review (Phase 14)
├── ml_pipeline.py                       # 🧠 ML pipeline templates & Feature Store (Phase 17)
├── cost_advisor.py                      # 💰 Cost optimization advisor (Phase 17)
├── Dockerfile                           # 🐳 Production container
├── docker-compose.yml                   # 🐳 Docker Compose (API + Web + Redis)
├── k8s/                                 # ☸️ Kubernetes manifests
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   └── secret.yaml
├── charts/                              # ⎈ Helm chart
│   └── informatica-migration/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
├── benchmarks/                          # 📊 Benchmark suite
│   ├── generate_mappings.py             #   Synthetic mapping generator
│   ├── run_benchmark.py                 #   Benchmark harness with profiling
│   └── SCALABILITY.md                   #   Scalability report
├── AGENTS.md                            # 🤖 This file
├── CONTRIBUTING.md                      # 🤝 Contributing guidelines
├── DEVELOPMENT_PLAN.md                  # 📝 100-sprint dev plan (Phase 1-17)
├── GAP_ANALYSIS.md                      # 📊 Gap analysis
├── MIGRATION_PLAN.md                    # 📝 Migration strategy
├── README.md                            # 📖 Project overview
├── pyproject.toml                       # 📦 Package config & CLI
├── migration.yaml                       # ⚙️ Runtime configuration
└── requirements.txt                     # 📦 Dependencies
```

---

## How to Use

### Full Migration (Orchestrated)

```
@migration-orchestrator start migration
```

### CLI Usage

```bash
pip install -e .
informatica-to-fabric --target fabric                    # Fabric (default)
informatica-to-fabric --target databricks                # Databricks
informatica-to-fabric --target dbt                       # DBT models on Databricks
informatica-to-fabric --target auto                      # Auto-route: dbt + PySpark
informatica-to-fabric --autosys-dir /path/to/jil/files   # Include AutoSys JIL
informatica-to-fabric --config migration.yaml            # Custom config
```

### Individual Tasks

| Task | Command |
|------|---------|
| Parse & inventory | `@assessment parse the workflow XML in input/workflows/` |
| Convert a mapping | `@notebook-migration convert mapping M_LOAD_CUSTOMERS` |
| Convert SQL | `@sql-migration convert the Oracle SQL overrides` |
| Generate a pipeline | `@pipeline-migration convert workflow WF_DAILY_LOAD` |
| Generate DBT models | `@notebook-migration convert mapping M_LOAD_CUSTOMERS --target dbt` |
| Migrate AutoSys JIL | `python run_autosys_migration.py input/autosys/` |
| Generate tests | `@validation generate tests for the Silver lakehouse tables` |

### Deploy to Fabric

1. **Review outputs** in the `output/` folder
2. **Deploy** using Fabric Git integration, manual upload, or Fabric REST API

### Deploy to Azure Databricks

1. **Review outputs** in the `output/` folder
2. **Import notebooks** via Databricks CLI: `databricks workspace import`
3. **Create jobs** from workflow JSON via Databricks Jobs API
4. **Or** use Databricks Repos to link a Git repo with the generated artifacts

---

## When NOT to Use Specialized Agents

Use the **default agent** (or `@migration-orchestrator`) for:
- Quick questions about the project
- Multi-domain tasks that touch 3+ agents
- Documentation updates
- General project planning
