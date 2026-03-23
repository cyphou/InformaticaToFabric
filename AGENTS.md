# Multi-Agent Architecture — Informatica to Fabric Migration

<p align="center">
  <img src="https://img.shields.io/badge/agents-6-0078D4?style=for-the-badge" alt="6 Agents"/>
  <img src="https://img.shields.io/badge/Informatica-FF4500?style=for-the-badge&logo=informatica&logoColor=white" alt="Informatica"/>
  <img src="https://img.shields.io/badge/%E2%86%92-gray?style=for-the-badge" alt="arrow"/>
  <img src="https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white" alt="Fabric"/>
</p>

## Overview

This project uses a **6-agent specialization model** to automate and guide the migration from Informatica to Microsoft Fabric. Each agent is a VS Code Copilot agent (`.agent.md`) with scoped domain knowledge, file ownership, and clear boundaries.

---

## Architecture Diagram

```mermaid
flowchart TB
    USER["👤 User"] --> ORCH
    ORCH["🎯 migration-orchestrator\nCoordinator Agent"]
    ORCH --> ASS["🔍 assessment\nDiscovery & Inventory"]
    ORCH --> SQL["🗄️ sql-migration\nOracle → Spark SQL"]
    ORCH --> NB["📓 notebook-migration\nMappings → PySpark"]
    ORCH --> PL["⚡ pipeline-migration\nWorkflows → Pipelines"]
    ORCH --> VAL["✅ validation\nTesting & QA"]

    ASS -.->|inventory.json\ncomplexity_report.md\ndependency_dag.json| SQL
    ASS -.->|mapping metadata| NB
    ASS -.->|workflow metadata| PL
    SQL -.->|converted SQL| NB
    NB -.->|notebook references| PL
    NB -.->|target tables| VAL
    PL -.->|pipeline references| VAL

    style ORCH fill:#0078D4,color:#fff,stroke:#005A9E,stroke-width:2px
    style ASS fill:#E67E22,color:#fff,stroke:#CA6F1E,stroke-width:2px
    style SQL fill:#8E44AD,color:#fff,stroke:#7D3C98,stroke-width:2px
    style NB fill:#27AE60,color:#fff,stroke:#1E8449,stroke-width:2px
    style PL fill:#2980B9,color:#fff,stroke:#2471A3,stroke-width:2px
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
| **🔍 @assessment** | `@assessment parse input/workflows/` | XML parsing, inventory, complexity, DAG | `output/inventory/` |
| **📓 @notebook-migration** | `@notebook-migration convert mapping M_X` | Mapping → PySpark notebook generation | `output/notebooks/NB_*.py` |
| **⚡ @pipeline-migration** | `@pipeline-migration convert workflow WF_X` | Workflow → Pipeline JSON generation | `output/pipelines/PL_*.json` |
| **🗄️ @sql-migration** | `@sql-migration convert Oracle SQL overrides` | Oracle → Spark SQL / T-SQL conversion | `output/sql/SQL_*.sql` |
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
| **Role** | Parses Informatica XML exports, builds inventories, classifies complexity, maps dependencies |
| **Inputs** | Informatica XML export files (workflows, mappings, sessions) |
| **Outputs** | `inventory.json`, `complexity_report.md`, `dependency_dag.json` |
| **File** | [.github/agents/assessment.agent.md](.github/agents/assessment.agent.md) |

### 3. 📓 `notebook-migration` (Transformation Conversion)

| | |
|---|---|
| **Role** | Converts Informatica mappings into Fabric Notebooks (PySpark) |
| **Inputs** | Mapping metadata from assessment, transformation rules, converted SQL |
| **Outputs** | Fabric Notebook `.py` files with PySpark transformation logic |
| **File** | [.github/agents/notebook-migration.agent.md](.github/agents/notebook-migration.agent.md) |

### 4. ⚡ `pipeline-migration` (Orchestration Conversion)

| | |
|---|---|
| **Role** | Converts Informatica workflows into Fabric Data Pipeline JSON definitions |
| **Inputs** | Workflow metadata from assessment, notebook references |
| **Outputs** | Fabric Data Pipeline JSON definitions with dependency chains |
| **File** | [.github/agents/pipeline-migration.agent.md](.github/agents/pipeline-migration.agent.md) |

### 5. 🗄️ `sql-migration` (SQL Conversion)

| | |
|---|---|
| **Role** | Converts Oracle SQL (overrides, stored procs) to Fabric-compatible SQL (Spark SQL / T-SQL) |
| **Inputs** | SQL overrides from mappings, stored procedure files |
| **Outputs** | Converted SQL files, Notebook `%%sql` cells |
| **File** | [.github/agents/sql-migration.agent.md](.github/agents/sql-migration.agent.md) |

### 6. ✅ `validation` (Testing & QA)

| | |
|---|---|
| **Role** | Generates validation notebooks, compares row counts, checksums, and data quality |
| **Inputs** | Source/target table pairs, migration metadata |
| **Outputs** | Validation notebooks, test matrix, pass/fail summaries |
| **File** | [.github/agents/validation.agent.md](.github/agents/validation.agent.md) |

---

## Data Flow

```mermaid
flowchart LR
    subgraph "📂 Input"
        XML["Informatica\nXML Exports"]
        SQL_IN["Oracle SQL\nStored Procs"]
    end

    subgraph "🔍 Phase 1 — Assessment"
        ASS["assessment agent\nParse → Classify → DAG"]
    end

    subgraph "⚙️ Phase 2 — Conversion"
        SQL_MIG["sql-migration\nOracle → Spark SQL"]
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
| 📓 Notebook Migration | `output/notebooks/` | `output/inventory/`, `output/sql/`, `templates/` |
| ⚡ Pipeline Migration | `output/pipelines/` | `output/inventory/`, `output/notebooks/`, `templates/` |
| ✅ Validation | `output/validation/` | `output/notebooks/`, `output/pipelines/`, `output/sql/` |

---

## Directory Structure

```
InformaticaToDBFabric/
├── .github/
│   └── agents/                          # 🤖 Agent definitions (6 agents)
│       ├── migration-orchestrator.agent.md
│       ├── assessment.agent.md
│       ├── notebook-migration.agent.md
│       ├── pipeline-migration.agent.md
│       ├── sql-migration.agent.md
│       └── validation.agent.md
├── .vscode/
│   └── instructions/                    # 📘 Shared rules
│       └── informatica-patterns.instructions.md
├── input/                               # 📂 Informatica exports
│   ├── workflows/                       #   Workflow XML
│   ├── mappings/                        #   Mapping XML
│   ├── sessions/                        #   Session XML
│   └── sql/                             #   Oracle SQL files
├── output/                              # 📤 Generated artifacts
│   ├── inventory/                       #   🔍 Assessment results
│   ├── notebooks/                       #   📓 Fabric Notebooks
│   ├── pipelines/                       #   ⚡ Pipeline JSON
│   ├── sql/                             #   🗄️ Converted SQL
│   └── validation/                      #   ✅ Test scripts
├── templates/                           # 📋 Reusable templates
│   ├── notebook_template.py
│   ├── pipeline_template.json
│   └── validation_template.py
├── AGENTS.md                            # 🤖 This file
├── MIGRATION_PLAN.md                    # 📝 Migration strategy
└── README.md                            # 📖 Project overview
```

---

## How to Use

### Full Migration (Orchestrated)

```
@migration-orchestrator start migration
```

### Individual Tasks

| Task | Command |
|------|---------|
| Parse & inventory | `@assessment parse the workflow XML in input/workflows/` |
| Convert a mapping | `@notebook-migration convert mapping M_LOAD_CUSTOMERS` |
| Convert SQL | `@sql-migration convert the Oracle SQL overrides` |
| Generate a pipeline | `@pipeline-migration convert workflow WF_DAILY_LOAD` |
| Generate tests | `@validation generate tests for the Silver lakehouse tables` |

### Deploy to Fabric

1. **Review outputs** in the `output/` folder
2. **Deploy** using Fabric Git integration, manual upload, or Fabric REST API

---

## When NOT to Use Specialized Agents

Use the **default agent** (or `@migration-orchestrator`) for:
- Quick questions about the project
- Multi-domain tasks that touch 3+ agents
- Documentation updates
- General project planning
