<p align="center">
  <img src="https://img.shields.io/badge/Informatica-FF4500?style=for-the-badge&logo=informatica&logoColor=white" alt="Informatica"/>
  <img src="https://img.shields.io/badge/%E2%86%92-gray?style=for-the-badge" alt="arrow"/>
  <img src="https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white" alt="Fabric"/>
  <img src="https://img.shields.io/badge/Azure%20Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks"/>
</p>

<h1 align="center">Migration Plan — Informatica PowerCenter / IDMC to Microsoft Fabric / Azure Databricks</h1>

<p align="center">
  <strong>An 8-phase strategy to migrate Informatica PowerCenter/IICS/IDMC (12 cloud services) workloads to Microsoft Fabric, Azure Databricks, or DBT — including AutoSys JIL scheduler migration and full IDMC platform assessment.</strong>
</p>

---

## Executive Summary

This plan outlines the migration strategy from **Informatica PowerCenter/IICS/IDMC** (including all 12 IDMC cloud services) to **Microsoft Fabric** or **Azure Databricks**, replacing ETL/ELT workloads with a combination of:

| Informatica Component | Fabric Target | Databricks Target | DBT Target | Agent |
|---|---|---|---|---|
| Mappings (transformations) | **Fabric Notebooks** (PySpark / Spark SQL) | **Databricks Notebooks** (PySpark / Spark SQL) | **DBT Models** (SQL + Jinja) | `@notebook-migration` |
| Workflows / Taskflows | **Fabric Data Pipelines** (ADF-based orchestration) | **Databricks Workflows** (Jobs API) | **dbt build** commands | `@pipeline-migration` |
| SQL overrides / Stored Procs | **Fabric SQL / Warehouse SQL** | **Databricks SQL** | **DBT macros** | `@sql-migration` |
| Sessions / connections | **Fabric Lakehouses + Shortcuts** | **Unity Catalog + Volumes** | **profiles.yml** | `@assessment` |
| Scheduler | **Pipeline Triggers + Fabric Scheduler** | **Job Scheduler (cron)** | **dbt Cloud / Airflow** | `@pipeline-migration` |
| **AutoSys JIL** (BOX/CMD/FW) | **Fabric Data Pipelines** (activities + triggers) | **Databricks Workflows** (multi-task jobs) | — | `@pipeline-migration` |
| **IDMC Platform** (CDI/CDGC/CDQ/MDM/DI/B2B/API/Connector/EDC/Axon/Market/Test Data) | Assessed via REST API → inventory + complexity scoring | Assessed via REST API → inventory + complexity scoring | — | `@assessment` |

### Migration Overview

```mermaid
flowchart LR
    subgraph "Source (Informatica)"
        WF["📋 Workflows"]
        MAP["🔄 Mappings"]
        SQL_S["🗄️ SQL Overrides"]
        SESS["⚙️ Sessions"]
    end

    subgraph "Migration Engine (6 Agents)"
        ASS["🔍 Assess"]
        CONV["⚙️ Convert"]
        VAL["✅ Validate"]
    end

    subgraph "Target (Microsoft Fabric / Azure Databricks)"
        NB["📓 Notebooks\nPySpark"]
        PL["⚡ Pipelines / Workflows\nJSON"]
        SQL_T["🗄️ SQL\nSpark SQL"]
        LH["🏠 Lakehouses / Unity Catalog\nBronze/Silver/Gold"]
    end

    WF --> ASS
    MAP --> ASS
    SQL_S --> ASS
    SESS --> ASS
    ASS --> CONV
    CONV --> NB
    CONV --> PL
    CONV --> SQL_T
    CONV --> VAL
    NB --> LH
    SQL_T --> LH
    PL --> NB

    style WF fill:#FF4500,color:#fff
    style MAP fill:#FF4500,color:#fff
    style SQL_S fill:#FF4500,color:#fff
    style SESS fill:#FF4500,color:#fff
    style ASS fill:#4B8BBE,color:#fff
    style CONV fill:#4B8BBE,color:#fff
    style VAL fill:#27AE60,color:#fff
    style NB fill:#0078D4,color:#fff
    style PL fill:#0078D4,color:#fff
    style SQL_T fill:#0078D4,color:#fff
    style LH fill:#0078D4,color:#fff
```

---

## Phase 0 — Discovery & Assessment

### 0.1 Inventory Extraction
- Export all Informatica **workflows**, **mappings**, **sessions**, and **connections** as XML.
- Parse XML metadata to build a structured inventory:
  - Source/target tables and systems (Oracle, flat files, etc.)
  - Transformation types (SQ, EXP, LKP, AGG, RTR, FIL, JNR, UPD, etc.)
  - SQL overrides and stored procedure calls
  - Workflow dependencies and scheduling info
  - Parameter files and variable usage

### 0.2 Complexity Classification
Each mapping is classified by migration complexity:

```mermaid
pie title Expected Complexity Distribution
    "🟢 Simple — Auto-generate" : 40
    "🟡 Medium — Semi-automated" : 35
    "🟠 Complex — Manual assist" : 20
    "🔴 Custom — Redesign" : 5
```

| Complexity | Criteria | Migration Path |
|---|---|---|
| 🟢 **Simple** | Source → Filter/Expression → Target, no SQL override | Auto-generate Notebook |
| 🟡 **Medium** | Lookups, Aggregators, Joiners, simple SQL overrides | Semi-automated Notebook |
| 🟠 **Complex** | Routers, Update Strategy, stored procs, multi-pipeline | Manual Notebook + SQL |
| 🔴 **Custom** | Java/custom transformations, SDK calls | Redesign in Fabric |

### 0.3 Dependency Mapping
- Build a DAG of workflow/mapping dependencies
- Identify shared reusable transformations (mapplets)
- Map Oracle DB connections → Fabric Lakehouse/Warehouse connections

---

## Phase 1 — Foundation Setup

### 1.1 Workspace Architecture

**Microsoft Fabric:**

```mermaid
flowchart TB
    subgraph WS["Fabric Workspace: DataPlatform-Migration"]
        direction TB
        subgraph Storage["🏠 Storage Layer (Lakehouses)"]
            BRONZE["🥉 Bronze\nRaw ingestion\nSchema-on-read"]
            SILVER["🥈 Silver\nCleansed & typed\nBusiness keys applied"]
            GOLD["🥇 Gold\nBusiness-ready\nAggregated & curated"]
        end
        subgraph Compute["⚙️ Compute Layer"]
            NOTEBOOKS["📓 Notebooks\nPySpark transformations"]
            WAREHOUSE["🗄️ SQL Warehouse\nStored procedures\nSQL-heavy workloads"]
        end
        subgraph Orchestration["⚡ Orchestration Layer"]
            PIPELINES["📋 Data Pipelines\nWorkflow orchestration"]
            TRIGGERS["⏰ Triggers\nSchedule & event-based"]
            ENV["🌐 Environments\nSpark config + libraries"]
        end
    end

    BRONZE --> NOTEBOOKS
    NOTEBOOKS --> SILVER
    SILVER --> NOTEBOOKS
    NOTEBOOKS --> GOLD
    WAREHOUSE --> SILVER
    PIPELINES --> NOTEBOOKS
    PIPELINES --> WAREHOUSE
    TRIGGERS --> PIPELINES

    style BRONZE fill:#CD7F32,color:#fff
    style SILVER fill:#C0C0C0,color:#000
    style GOLD fill:#FFD700,color:#000
    style NOTEBOOKS fill:#0078D4,color:#fff
    style WAREHOUSE fill:#0078D4,color:#fff
    style PIPELINES fill:#0078D4,color:#fff
    style TRIGGERS fill:#0078D4,color:#fff
    style ENV fill:#0078D4,color:#fff
```

### 1.2 Connection Setup
- Configure **Shortcuts** or **Pipelines** for Oracle source ingestion (Fabric) or **External Locations** (Databricks)
- Set up **OneLake** (Fabric) or **Unity Catalog Volumes** (Databricks) for storage
- Configure **Fabric SQL Warehouse** or **Databricks SQL Warehouse** for SQL-heavy transformations
- Create **Fabric Environment** or **Databricks Cluster Policy** with required Python/Spark libraries

> **Databricks alternative:** Replace Fabric Lakehouses with Unity Catalog 3-level namespace (`catalog.schema.table`). Use `--target databricks` during migration.

---

## Phase 2 — Transformation Migration (Mappings → Notebooks)

### 2.1 Mapping-to-Notebook Conversion Rules

| Informatica Transformation | Fabric Notebook (PySpark) |
|---|---|
| Source Qualifier (SQ) | `spark.read.format("delta").load()` or JDBC read |
| Expression (EXP) | `.withColumn()` / `.select(expr(...))` |
| Filter (FIL) | `.filter()` / `.where()` |
| Aggregator (AGG) | `.groupBy().agg()` |
| Joiner (JNR) | `.join()` |
| Lookup (LKP) | `.join()` (broadcast for small tables) |
| Router (RTR) | Multiple `.filter()` branches |
| Update Strategy (UPD) | Delta merge (`MERGE INTO`) |
| Sorter (SRT) | `.orderBy()` |
| Rank (RNK) | `Window` functions with `row_number()` |
| Union (UNI) | `.union()` / `.unionByName()` |
| Normalizer | `.explode()` |
| Stored Procedure (SP) | Fabric SQL endpoint or Notebook `%%sql` |
| Sequence Generator | `monotonically_increasing_id()` or Delta identity |

### 2.2 Notebook Template
Each migrated mapping produces a notebook. The template varies by target platform:

**Fabric (default):**
```python
# Cell 1: Parameters (mapped from Informatica parameter file)
source_table = "schema.table_name"
target_lakehouse = "Silver"
load_date = notebookutils.widgets.get("load_date")
```

**Azure Databricks (`--target databricks`):**
```python
# Cell 1: Parameters
source_table = "catalog.schema.table_name"
target_catalog = "silver"
load_date = dbutils.widgets.get("load_date")
```

Common cells (both targets):

# Cell 2: Read Source
df_source = spark.read.format("jdbc").options(**oracle_config).load()

# Cell 3: Transformations (mapped from EXP, FIL, AGG, etc.)
df_transformed = (
    df_source
    .filter(col("status") == "ACTIVE")
    .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    .groupBy("department").agg(count("*").alias("emp_count"))
)

# Cell 4: Write Target
df_transformed.write.format("delta").mode("overwrite").saveAsTable(f"{target_lakehouse}.target_table")
```

---

## Phase 3 — Orchestration Migration (Workflows → Data Pipelines)

### 3.1 Workflow-to-Pipeline Mapping

**Fabric Data Pipelines:**

| Informatica Workflow Element | Fabric Data Pipeline |
|---|---|
| Workflow | Data Pipeline |
| Session | Notebook Activity |
| Command Task | Notebook Activity or Script Activity |
| Timer | Wait Activity or Schedule Trigger |
| Decision | If Condition Activity |
| Event Wait/Raise | Pipeline dependency / Get Metadata |
| Assignment | Set Variable Activity |
| Email Task | Web Activity (Logic App / webhook) |
| Link conditions | Activity dependency conditions (Success/Failure/Completion) |
| Worklet | Child Pipeline (Invoke Pipeline Activity) |

**Azure Databricks Workflows:**

| Informatica Workflow Element | Databricks Workflow (Jobs API) |
|---|---|
| Workflow | Multi-task Job |
| Session | Notebook Task |
| Command Task | Spark Submit Task or Python Wheel Task |
| Timer | Job Schedule (cron expression) |
| Decision | `if/else` task with `task_key` conditions |
| Event Wait/Raise | Task dependency with `depends_on` |
| Assignment | Job Parameters |
| Email Task | Webhook notification |
| Link conditions | Task dependency conditions (success/failure/all) |
| Worklet | Run Job Task |

### 3.2 Pipeline Template Structure
```json
{
  "name": "PL_<workflow_name>",
  "activities": [
    {
      "type": "NotebookActivity",
      "name": "NB_<mapping_name>",
      "notebook": { "referenceName": "NB_<mapping_name>" },
      "parameters": { "load_date": "@pipeline().parameters.load_date" }
    }
  ],
  "parameters": {
    "load_date": { "type": "string", "defaultValue": "@utcnow()" }
  }
}
```

---

## Phase 4 — SQL Migration (Oracle → Fabric SQL)

### 4.1 Scope
- Informatica SQL overrides → Fabric Notebook `%%sql` cells or Warehouse stored procedures
- Oracle stored procedures called by Informatica → Fabric Warehouse SQL or Notebook logic
- Oracle-specific syntax → T-SQL/Spark SQL equivalent

### 4.2 Common Oracle → Fabric SQL Conversions

| Oracle SQL | Fabric SQL (T-SQL / Spark SQL) |
|---|---|
| `NVL(a, b)` | `COALESCE(a, b)` |
| `DECODE(...)` | `CASE WHEN ... END` |
| `SYSDATE` | `CURRENT_TIMESTAMP` |
| `ROWNUM` | `ROW_NUMBER() OVER(...)` |
| `TO_DATE(str, fmt)` | `CAST(str AS DATE)` / `to_date()` |
| `TO_CHAR(date, fmt)` | `FORMAT(date, fmt)` / `date_format()` |
| `NVL2(a, b, c)` | `CASE WHEN a IS NOT NULL THEN b ELSE c END` |
| `(+)` outer join | `LEFT JOIN` / `RIGHT JOIN` |
| `CONNECT BY` | Recursive CTE |
| `MERGE INTO` | Delta `MERGE INTO` (Spark SQL) |

---

## Phase 5 — Testing & Validation

### 5.1 Validation Strategy

```mermaid
flowchart LR
    L1["🔢 Level 1\nRow Count"] --> L2["🔐 Level 2\nChecksum"] --> L3["📊 Level 3\nAggregates"] --> L4["🔍 Level 4\nSample Records"] --> L5["🚀 Level 5\nEnd-to-End"]

    style L1 fill:#27AE60,color:#fff
    style L2 fill:#2980B9,color:#fff
    style L3 fill:#8E44AD,color:#fff
    style L4 fill:#E67E22,color:#fff
    style L5 fill:#C0392B,color:#fff
```

| Level | Method | What It Checks |
|---|---|---|
| **L1** | Row Count | Source vs. target row counts match |
| **L2** | Checksum | Hash-based comparison of key columns |
| **L3** | Aggregates | SUM, AVG, MIN, MAX of metric columns |
| **L4** | Sample Records | Field-by-field comparison of random sample |
| **L5** | End-to-End | Full pipeline run + downstream report validation |

### 5.2 Automated Validation Notebook
A dedicated validation notebook runs post-migration:
```python
# Compare row counts
oracle_count = spark.read.jdbc(url, "source_table").count()
fabric_count = spark.table("silver.target_table").count()
assert oracle_count == fabric_count, f"Row mismatch: {oracle_count} vs {fabric_count}"
```

> [!TIP]
> The `@validation` agent auto-generates validation notebooks for every migrated mapping. See [templates/validation_template.py](templates/validation_template.py).

---

## Phase 6 — DBT Model Generation

For teams adopting the **DBT** approach (`--target dbt` or `--target auto`):

### 6.1 DBT Target Router
- Mappings are classified as **SQL-expressible** or **PySpark-required**
- SQL-expressible mappings → DBT models (Jinja + SQL)
- Complex mappings (custom Java, heavy UDF) → PySpark notebooks
- `--target auto` uses the router to decide per-mapping

### 6.2 DBT Project Generation
| Artifact | Output |
|---|---|
| Model `.sql` files | `output/dbt/models/` |
| `dbt_project.yml` | `output/dbt/` |
| `profiles.yml` | `output/dbt/` |
| Source YAML | `output/dbt/models/sources.yml` |
| Schema tests | `output/dbt/models/schema.yml` |

---

## Phase 7 — AutoSys JIL Migration

For environments using **CA AutoSys** as the enterprise job scheduler:

### 7.1 JIL Parsing
- Parse `.jil` files to extract **BOX**, **CMD**, **FW** (File Watcher), and **FT** (File Transfer) jobs
- Extract job attributes: `condition`, `days_of_week`, `start_times`, `start_mins`, `run_calendar`, `command`, `box_name`, `machine`
- Parse condition expressions: `s()` (success), `f()` (failure), `n()` (notrunning), `d()` (done)
- Build cross-job dependency graph from conditions

### 7.2 Informatica Linkage
- Detect `pmcmd startworkflow` patterns in CMD job commands
- Link AutoSys jobs to their Informatica workflow counterparts
- Cross-reference against `output/inventory/inventory.json` for matched workflows

### 7.3 AutoSys → Pipeline Mapping

| AutoSys Element | Fabric Pipeline | Databricks Workflow |
|---|---|---|
| BOX job | Pipeline (container) | Multi-task Job |
| CMD job (pmcmd) | Notebook Activity | Notebook Task |
| CMD job (generic) | Script Activity | Python Wheel Task |
| FW (File Watcher) | Get Metadata Activity | File Arrival Sensor |
| FT (File Transfer) | Copy Activity | DBFS Copy Task |
| `condition: s(A)` | `dependsOn: Succeeded` | `depends_on: [{task_key}]` |
| `condition: f(A)` | `dependsOn: Failed` | Failure notification |
| `days_of_week` | Schedule Trigger (cron) | Quartz cron expression |
| `run_calendar` | Schedule Trigger | Calendar-based schedule |
| Standalone chains | Flattened Pipeline | Multi-task Job |

### 7.4 Output
- `output/autosys/PL_AUTOSYS_*.json` — Generated pipeline/workflow definitions
- `output/autosys/autosys_summary.json` — Migration summary with linkage report

---

## Phase 8 — Cutover & Decommission

```mermaid
flowchart LR
    A["🔄 Parallel Run\n2-4 weeks"] --> B["📦 Incremental\nCutover"] --> C["📊 Monitoring\nFabric Monitor"] --> D["🗑️ Decommission\nInformatica"]

    style A fill:#3498DB,color:#fff
    style B fill:#2980B9,color:#fff
    style C fill:#27AE60,color:#fff
    style D fill:#E74C3C,color:#fff
```

1. **Parallel Run** — Run Informatica and target platform (Fabric/Databricks) side-by-side for 2-4 weeks
2. **Incremental Cutover** — Migrate workflows in batches by priority and dependency order
3. **Monitoring** — Fabric Monitor / Databricks Jobs UI + pipeline run history + alerting
4. **Decommission** — Disable Informatica workflows and AutoSys JIL schedules after validation sign-off

---

## Migration Timeline

```mermaid
gantt
    title Informatica → Fabric / Databricks Migration Phases
    dateFormat  YYYY-MM-DD
    axisFormat %b %Y

    section Discovery
    Phase 0 - Assessment          :a0, 2026-04-01, 14d

    section Foundation
    Phase 1 - Fabric Setup        :a1, after a0, 7d

    section Migration
    Phase 2 - Notebooks           :a2, after a1, 30d
    Phase 3 - Pipelines           :a3, after a1, 30d
    Phase 4 - SQL Conversion      :a4, after a1, 21d

    section Testing
    Phase 5 - Validation          :a5, after a2, 14d

    section DBT & AutoSys
    Phase 6 - DBT Models          :a6, after a1, 14d
    Phase 7 - AutoSys JIL         :a7, after a1, 14d

    section Cutover
    Phase 8 - Parallel Run        :a8, after a5, 28d
    Phase 8 - Decommission        :milestone, after a8, 0d
```

| Phase | Description | Key Outputs |
|---|---|---|
| **Phase 0** | Discovery & Assessment | `inventory.json`, `complexity_report.md`, `dependency_dag.json` |
| **Phase 1** | Fabric Foundation Setup | Workspace, Lakehouses, Warehouse, Environment |
| **Phase 2** | Transformation Migration | `NB_*.py` notebooks in `output/notebooks/` |
| **Phase 3** | Orchestration Migration | `PL_*.json` pipelines in `output/pipelines/` |
| **Phase 4** | SQL Migration | `SQL_*.sql` files in `output/sql/` |
| **Phase 5** | Testing & Validation | `VAL_*.py` scripts + `test_matrix.md` |
| **Phase 6** | DBT Model Generation | `output/dbt/models/`, `dbt_project.yml`, `profiles.yml` |
| **Phase 7** | AutoSys JIL Migration | `output/autosys/PL_AUTOSYS_*.json`, `autosys_summary.json` |
| **Phase 8** | Cutover & Decommission | Sign-off, Informatica + AutoSys shutdown |

---

## Multi-Agent Architecture

The migration is powered by a **6-agent system** defined in the `.github/agents/` directory.

> **Full details:** [AGENTS.md](AGENTS.md) — Architecture diagrams, interaction flows, handoff protocol, file ownership rules.

---

<p align="center">
  <sub>See <a href="README.md">README.md</a> for quick start and usage instructions.</sub>
</p>
