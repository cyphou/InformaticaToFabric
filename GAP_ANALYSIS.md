# Informatica to Fabric — Object Inventory & Gap Analysis

<p align="center">
  <img src="https://img.shields.io/badge/objects_covered-62%25-27AE60?style=for-the-badge" alt="62% covered"/>
  <img src="https://img.shields.io/badge/gaps_identified-18-E74C3C?style=for-the-badge" alt="18 gaps"/>
  <img src="https://img.shields.io/badge/status-living_document-0078D4?style=for-the-badge" alt="Living doc"/>
</p>

**Generated:** 2026-03-23  
**Scope:** Informatica PowerCenter 9.x/10.x + IICS → Microsoft Fabric  
**Purpose:** Comprehensive inventory of all Informatica object types with migration readiness assessment and gap identification.

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [1. Transformation Objects](#1-transformation-objects)
- [2. Workflow & Orchestration Objects](#2-workflow--orchestration-objects)
- [3. Repository & Metadata Objects](#3-repository--metadata-objects)
- [4. SQL & Stored Procedure Constructs](#4-sql--stored-procedure-constructs)
- [5. IICS (Cloud) Objects](#5-iics-cloud-objects)
- [6. Infrastructure & Configuration Objects](#6-infrastructure--configuration-objects)
- [7. Gap Summary & Prioritization](#7-gap-summary--prioritization)
- [8. Remediation Roadmap](#8-remediation-roadmap)

---

## Executive Summary

```mermaid
pie title Informatica Object Migration Coverage
    "Fully Covered" : 26
    "Partially Covered" : 10
    "Placeholder Only" : 6
    "Not Covered (Gap)" : 18
```

| Category | Total Objects | Covered | Partial | Placeholder | Gap |
|----------|:------------:|:-------:|:-------:|:-----------:|:---:|
| **Transformations** | 30 | 10 | 4 | 6 | 10 |
| **Workflow Elements** | 14 | 9 | 3 | 0 | 2 |
| **Repository Objects** | 8 | 2 | 3 | 0 | 3 |
| **SQL Constructs** | 30 | 24 | 0 | 0 | 6 |
| **IICS Objects** | 8 | 0 | 1 | 0 | 7 |
| **Infrastructure** | 6 | 0 | 0 | 0 | 6 |
| **Total** | **96** | **45** | **11** | **6** | **34** |

**Coverage legend:**
- ✅ **Covered** — Full conversion logic documented and implemented
- 🟡 **Partial** — Detected/parsed but incomplete conversion
- 🔶 **Placeholder** — Generates TODO cell; requires manual conversion
- ❌ **Gap** — Not handled at all; invisible to the migration tooling

---

## 1. Transformation Objects

### 1.1 Fully Covered Transformations (10)

These have complete PySpark conversion rules in the notebook-migration agent and shared instructions.

| # | Informatica Type | Abbrev | Fabric Equivalent | Conversion Quality |
|---|---|---|---|---|
| 1 | Source Qualifier | SQ | `spark.read.format("jdbc").load()` / `spark.table()` | ⭐⭐⭐ Full |
| 2 | Expression | EXP | `.withColumn()` + `expr()` / `when().otherwise()` | ⭐⭐⭐ Full (IIF, DECODE, IS_SPACES, LTRIM, RTRIM, TO_DATE, LPAD, REG_EXTRACT, ERROR) |
| 3 | Filter | FIL | `.filter()` / `.where()` | ⭐⭐⭐ Full |
| 4 | Aggregator | AGG | `.groupBy().agg()` | ⭐⭐⭐ Full (GROUP BY ports, sorted input, SUM/COUNT/AVG/MIN/MAX/FIRST/LAST) |
| 5 | Joiner | JNR | `.join()` | ⭐⭐⭐ Full (Inner/Left/Right/Full, master-detail) |
| 6 | Lookup (Connected) | LKP | `broadcast()` + `.join()` | ⭐⭐⭐ Full (condition mapping, default on miss, first value, broadcast < 100MB) |
| 7 | Router | RTR | Multiple `.filter()` branches | ⭐⭐½ Good (single output groups; multi-group routers need manual review) |
| 8 | Update Strategy | UPD | Delta `MERGE INTO` | ⭐⭐⭐ Full (DD_INSERT/UPDATE/DELETE/REJECT) |
| 9 | Rank | RNK | `Window` + `row_number()` / `rank()` | ⭐⭐⭐ Full |
| 10 | Stored Procedure | SP | `%%sql` cell / PySpark logic | ⭐⭐½ Good (simple SP covered; complex SP handed to sql-migration) |

### 1.2 Partially Covered Transformations (4)

Detected and abbreviated, but missing complete PySpark conversion templates.

| # | Informatica Type | Abbrev | What's Missing | Fabric Approach |
|---|---|---|---|---|
| 11 | Sorter | SRT | No PySpark template in agent docs | `.orderBy()` — straightforward |
| 12 | Union | UNI | No PySpark template in agent docs | `.union()` / `.unionByName()` — straightforward |
| 13 | Unconnected Lookup | ULKP | Only mentioned; no detailed conversion | Wrap in function or `when().otherwise()` with join |
| 14 | Sequence Generator | SEQ | Abbreviated; basic guidance only | `monotonically_increasing_id()` or Delta identity |

### 1.3 Placeholder-Only Transformations (6)

Detected and classified as "Custom" complexity. A TODO placeholder cell is generated in notebooks.

| # | Informatica Type | Abbrev | Why No Auto-Conversion | Manual Effort |
|---|---|---|---|---|
| 15 | Java Transformation | JTX | Embedded Java code | 🔴 High — rewrite in PySpark or Python UDF |
| 16 | Custom Transformation | CT | Custom C/C++ logic | 🔴 High — full redesign |
| 17 | HTTP Transformation | HTTP | External API call | 🟡 Medium — use `requests` UDF or pipeline Web Activity |
| 18 | XML Generator | XMLG | XML construction | 🟡 Medium — use `to_xml()` or string templates |
| 19 | XML Parser | XMLP | XML parsing | 🟡 Medium — use `spark.read.format("xml")` |
| 20 | Transaction Control | TC | Transaction scope management | 🟡 Medium — Delta ACID handles most cases; explicit control needs review |

### 1.4 Gap — Not Covered Transformations (10)

These Informatica transformation types are **not recognized** by the project. If present in a mapping, they will appear as unknown types in the assessment warnings.

| # | Informatica Type | Usage Frequency | Fabric Equivalent | Priority |
|---|---|---|---|---|
| 21 | **Mapplet** | 🔥 Very Common | Reusable notebook / function library | **P0** |
| 22 | **SQL Transformation** | 🔥 Common | `%%sql` cell / `spark.sql()` | **P0** |
| 23 | **Normalizer** | 🟡 Moderate | `.explode()` / `.select(explode())` | **P1** |
| 24 | **Web Service Consumer** | 🟡 Moderate | Pipeline Web Activity / `requests` UDF | **P1** |
| 25 | **Data Masking** | 🟡 Moderate | Fabric Data Masking / PySpark UDF | **P1** |
| 26 | **External Procedure** | 🟢 Rare | Python UDF / subprocess | **P2** |
| 27 | **Advanced External Procedure** | 🟢 Rare | Python UDF / subprocess | **P2** |
| 28 | **Association** | 🟢 Rare (DQ) | PySpark window functions | **P2** |
| 29 | **Key Generator** | 🟢 Rare (DQ) | `monotonically_increasing_id()` / hash | **P2** |
| 30 | **Address Validator** | 🟢 Rare (DQ) | Azure Maps API / third-party | **P3** |

> **Impact note:** Mapplets (item 21) are **critical** — they appear inside mappings as reusable fragments. Without Mapplet expansion, any mapping that references a Mapplet will have missing transformation logic.

---

## 2. Workflow & Orchestration Objects

### 2.1 Fully Covered (9)

| # | Informatica Element | Fabric Activity | Parsing | Pipeline JSON |
|---|---|---|---|---|
| 1 | Session | Notebook Activity | ✅ | ✅ |
| 2 | Command Task | Notebook / Script Activity | ✅ | ✅ |
| 3 | Timer | Wait Activity | ✅ | ✅ |
| 4 | Decision | IfCondition Activity | ✅ | ✅ |
| 5 | Assignment | SetVariable Activity | ✅ | ✅ |
| 6 | Email Task | Web Activity (webhook) | ✅ | ✅ |
| 7 | Worklet | ExecutePipeline (child) | ✅ | ✅ (depth-2 nesting) |
| 8 | Workflow Link (unconditional) | `dependsOn: Succeeded` | ✅ | ✅ |
| 9 | Workflow Link (conditional) | IfCondition wrapping | ✅ | ✅ |

### 2.2 Partially Covered (3)

| # | Informatica Element | What's Covered | What's Missing |
|---|---|---|---|
| 10 | Scheduler | Name extracted | Cron expression / repeat interval parsing |
| 11 | Event Wait | Documented mapping | No XML parsing for event conditions |
| 12 | Event Raise | Documented mapping | No XML parsing for event definitions |

### 2.3 Gap — Not Covered (2)

| # | Informatica Element | Usage | Fabric Equivalent | Priority |
|---|---|---|---|---|
| 13 | **Control Task** (Abort/Fail) | 🟡 Moderate | Pipeline `Fail Activity` | **P1** |
| 14 | **Session Config Objects** | 🟡 Moderate | Spark pool settings / notebook config | **P1** |

---

## 3. Repository & Metadata Objects

### 3.1 Coverage Status

| # | Informatica Object | Current Status | Detail |
|---|---|---|---|
| 1 | **Mappings** | ✅ Covered | Full XML parsing → inventory → notebook generation |
| 2 | **Workflows** | ✅ Covered | Full XML parsing → inventory → pipeline generation |
| 3 | **Mapplets** | ❌ **Gap** | Not parsed, not expanded. Mapplet references inside mappings are invisible. |
| 4 | **Sessions** | 🟡 Partial | Session attributes extracted from workflow XML; standalone session XML noted but no dedicated parser |
| 5 | **Parameter Files (.prm)** | 🟡 Partial | `$$PARAM` regex extraction from mapping XML; `.prm` file contents not parsed |
| 6 | **Connection Objects** | 🟡 Partial | Inferred from source/target metadata; connection XML not parsed |
| 7 | **Source/Target Definitions** | ✅ Covered | Extracted from mapping XML (name, owner, database type) |
| 8 | **Deployment Groups** | ❌ **Gap** | Not parsed or tracked |

### 3.2 Critical Gap Detail: Mapplets

```mermaid
flowchart LR
    M["Mapping XML"] --> TX["TRANSFORMATION\nTYPE='Mapplet'"]
    TX --> REF["MAPPLETNAME='MPLT_COMMON_DERIVE'"]
    REF -->|"must resolve"| MPLT["Mapplet XML\n(separate file)"]
    MPLT --> INNER["Inner transformations\nEXP + FIL + LKP"]
    
    style TX fill:#E74C3C,color:#fff
    style REF fill:#E74C3C,color:#fff
    style MPLT fill:#F39C12,color:#fff
```

**Problem:** When a mapping references a Mapplet, the mapping XML contains a `TRANSFORMATION` element with `TYPE="Mapplet"` and a `MAPPLETNAME` attribute pointing to a separate Mapplet definition. The current parser does not:
1. Recognize Mapplet-type transformations
2. Resolve Mapplet references to their definitions
3. Expand inner Mapplet transformations into the mapping's transformation chain

**Impact:** Any mapping using Mapplets will have an incomplete transformation list, potentially wrong complexity classification, and a generated notebook with missing logic.

### 3.3 Critical Gap Detail: Parameter Files

```
# Example .prm file (Informatica)
[Global]
$$DB_CONNECTION=ORACLE_PROD
$$SCHEMA_NAME=SALES
$$LOAD_DATE=2026-03-23
$$BATCH_SIZE=10000

[WF_DAILY_SALES_LOAD.s_M_LOAD_ORDERS]
$$TARGET_SCHEMA=SILVER
$$TRUNCATE_FLAG=Y
```

**Current handling:** The assessment parser extracts `$$PARAM_NAME` references from mapping XML via regex, but the `.prm` file contents are never read. This means:
- Parameter default values are unknown
- Session-level parameter overrides are invisible
- Connection parameters embedded in `.prm` files are missed

---

## 4. SQL & Stored Procedure Constructs

### 4.1 Oracle Functions — Coverage Matrix

| Category | Covered | Gap |
|----------|---------|-----|
| **Null handling** | `NVL`, `NVL2` | ✅ Complete |
| **Conditional** | `DECODE`, `CASE WHEN` | ✅ Complete |
| **Date/time** | `SYSDATE`, `SYSTIMESTAMP`, `TO_DATE`, `TO_CHAR`, `TRUNC`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `LAST_DAY` | ✅ Complete |
| **String** | `SUBSTR`, `INSTR`, `LENGTH`, `LPAD`, `RPAD`, `REPLACE`, `REGEXP_LIKE`, `REGEXP_REPLACE` | ✅ Complete |
| **Type conversion** | `TO_NUMBER`, `TO_CHAR(number)`, `VARCHAR2`, `NUMBER` | ✅ Complete |
| **Aggregation** | `LISTAGG`, `WM_CONCAT` | ✅ Complete |
| **Joins** | `(+)` outer join syntax | ✅ Complete |
| **DML** | `MERGE INTO` | ✅ Complete (→ Delta MERGE) |
| **Hierarchy** | `CONNECT BY`, `START WITH`, `LEVEL` | 🟡 Documented but flagged as limited |
| **Sequences** | `SEQ.NEXTVAL` | ✅ Complete |
| **PL/SQL** | `CURSOR`, `BULK COLLECT`, `FORALL`, `EXCEPTION WHEN`, `PRAGMA` | 🔶 Detected, flagged as non-convertible TODO |

### 4.2 Oracle Constructs — Gaps

| # | Oracle Construct | Usage | Why It's a Gap | Priority |
|---|---|---|---|---|
| 1 | **Analytic functions** (`LEAD`, `LAG`, `NTILE`, `DENSE_RANK`, `FIRST_VALUE`, `LAST_VALUE`) | 🔥 Very Common | Not in ORACLE_PATTERNS detection; Spark SQL has equivalents but no auto-conversion | **P0** |
| 2 | **Global Temp Tables** (`CREATE GLOBAL TEMPORARY TABLE`) | 🟡 Moderate | Used in complex SPs; no Spark equivalent pattern documented | **P1** |
| 3 | **PL/SQL Packages** (full package body conversion) | 🟡 Moderate | `PACKAGE BODY` detected; no strategy to split into individual notebooks | **P1** |
| 4 | **Materialized Views** | 🟡 Moderate | Not detected; Fabric uses Delta tables with scheduled refresh | **P2** |
| 5 | **Database Links** (`@dblink`) | 🟢 Rare | Cross-database references not detected | **P2** |
| 6 | **Object Types** (`CREATE TYPE`) | 🟢 Rare | Custom Oracle types not detected | **P3** |

### 4.3 Non-Oracle SQL Sources

The project assumes **Oracle** as the source database. Other common Informatica source databases are not addressed:

| Source DB | Detection | Conversion Rules | Gap Severity |
|-----------|-----------|-----------------|:------------:|
| Oracle | ✅ Full | ✅ 30+ patterns | — |
| SQL Server | ❌ None | ❌ None | **P1** if customer uses MSSQL |
| Teradata | ❌ None | ❌ None | **P2** |
| DB2 | ❌ None | ❌ None | **P2** |
| MySQL/PostgreSQL | ❌ None | ❌ None | **P2** |
| Flat files (CSV/fixed-width) | ❌ None | ❌ None | **P1** |

> **Note:** Many Informatica deployments use **flat file sources** (CSV, fixed-width, XML) alongside database sources. The current project only handles JDBC-based source reads.

---

## 5. IICS (Cloud) Objects

The project detects IICS format XML but **cannot parse it**. This is a significant gap for customers migrating from Informatica Cloud.

### 5.1 IICS Object Types

| # | IICS Object | PowerCenter Equivalent | Covered | Priority |
|---|---|---|---|---|
| 1 | **Cloud Mapping** | Mapping | ❌ Different XML schema | **P0** |
| 2 | **Taskflow** | Workflow | ❌ Different orchestration model | **P0** |
| 3 | **Synchronization Task** | Session (simple) | ❌ Not parsed | **P1** |
| 4 | **Mass Ingestion Task** | Bulk load session | ❌ Not parsed | **P1** |
| 5 | **Mapping Task** | Session | ❌ Not parsed | **P1** |
| 6 | **Data Quality Task** | DQ workflow | ❌ Not parsed | **P2** |
| 7 | **Application Integration** | — (IICS-only) | ❌ Not parsed | **P2** |
| 8 | **IICS Connections** | Connection objects | ❌ Not parsed | **P1** |

### 5.2 IICS XML Structure Differences

```xml
<!-- PowerCenter XML (currently supported) -->
<POWERMART>
  <REPOSITORY>
    <FOLDER NAME="MY_FOLDER">
      <MAPPING NAME="M_LOAD_CUSTOMERS" ...>
        <TRANSFORMATION NAME="SQ_CUSTOMERS" TYPE="Source Qualifier" .../>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>

<!-- IICS XML (NOT supported) -->
<exportMetadata>
  <weightedCSPackage>
    <dTemplate name="m_load_customers" objectType="com.infa.deployment.mapping">
      <field name="transformations">
        <dTemplate objectType="com.infa.adapter.source">
          <!-- completely different structure -->
        </dTemplate>
      </field>
    </dTemplate>
  </weightedCSPackage>
</exportMetadata>
```

---

## 6. Infrastructure & Configuration Objects

These Informatica objects relate to runtime configuration and security — typically not migrated as code but require manual Fabric setup.

| # | Informatica Object | Fabric Equivalent | Auto-Migratable? | Priority |
|---|---|---|---|---|
| 1 | **Domain/Node configuration** | Fabric Capacity settings | No — manual | **P3** |
| 2 | **Integration Service** | Spark Pool / Fabric Runtime | No — manual | **P3** |
| 3 | **Repository Service** | Fabric Workspace + Git | No — manual | **P3** |
| 4 | **Roles & Permissions** | Fabric Workspace Roles + Row-Level Security | No — but could generate scripts | **P2** |
| 5 | **Deployment Groups** | Fabric Deployment Pipelines (CI/CD) | No — manual | **P2** |
| 6 | **Grid/High Availability config** | Fabric auto-scaling | No — inherent in Fabric | **P3** |

---

## 7. Gap Summary & Prioritization

### 7.1 Priority Matrix

```mermaid
quadrantChart
    title Gap Priority vs Effort
    x-axis Low Effort --> High Effort
    y-axis Low Priority --> High Priority
    quadrant-1 Do Now
    quadrant-2 Plan Carefully
    quadrant-3 Nice to Have
    quadrant-4 Quick Wins
    Mapplet Support: [0.7, 0.95]
    SQL Transformation: [0.3, 0.9]
    IICS Parsing: [0.9, 0.85]
    Flat File Sources: [0.4, 0.75]
    Analytic Functions: [0.2, 0.85]
    Parameter File Parser: [0.3, 0.7]
    Normalizer Template: [0.2, 0.6]
    Control Task: [0.15, 0.55]
    Session Config: [0.3, 0.55]
    Web Service Consumer: [0.5, 0.5]
    Data Masking: [0.5, 0.45]
    Non-Oracle DBs: [0.6, 0.4]
    PL/SQL Packages: [0.7, 0.4]
    Global Temp Tables: [0.3, 0.35]
    Connection XML Parser: [0.4, 0.35]
    Scheduler Cron Parser: [0.2, 0.3]
    Database Links: [0.3, 0.2]
    Address Validator: [0.8, 0.1]
```

### 7.2 Prioritized Gap Table

| Priority | Gap | Impact | Effort | Remediation |
|:--------:|-----|--------|--------|-------------|
| **P0** | Mapplet expansion | Mappings referencing Mapplets have incomplete logic | High | Parse Mapplet XML → expand into mapping transformation chain |
| **P0** | SQL Transformation type | Common transformation completely invisible | Low | Add to `TRANSFORMATION_ABBREV`, generate `%%sql` / `spark.sql()` cell |
| **P0** | Oracle analytic functions | `LEAD/LAG/RANK/DENSE_RANK` very common in SQL overrides | Low | Add detection patterns + Spark SQL equivalents (mostly 1:1) |
| **P1** | IICS XML parsing | Blocks all cloud migrations | Very High | Build separate IICS XML parser for `exportMetadata` schema |
| **P1** | Flat file source handling | Many pipelines read CSV/fixed-width files | Medium | Add `spark.read.csv()` / `.text()` patterns to notebook agent |
| **P1** | Parameter file (.prm) parser | Parameter defaults and overrides are unknown | Low | Parse `.prm` files → inject into notebook parameters |
| **P1** | Normalizer template | `.explode()` pattern not documented | Low | Add PySpark template to notebook agent |
| **P1** | Control Task (Abort/Fail) | Missing error path in pipelines | Low | Map to Fabric `Fail Activity` |
| **P1** | Session Config objects | Spark pool/memory settings not migrated | Medium | Extract DTM buffer size, sort order → Spark config |
| **P1** | Web Service Consumer | API-calling transformations invisible | Medium | Map to pipeline Web Activity or `requests` UDF |
| **P1** | Data Masking | Compliance-critical transformation | Medium | Map to Fabric Dynamic Data Masking or PySpark UDF |
| **P1** | Non-Oracle SQL sources (MSSQL) | Common in multi-source pipelines | Medium | Add SQL Server → Spark SQL pattern set |
| **P2** | Connection XML parser | Connection details are only inferred | Medium | Parse PowerCenter connection XML objects |
| **P2** | PL/SQL Package splitting | `PACKAGE BODY` flagged but no split strategy | High | Split into individual notebooks/functions |
| **P2** | Global Temp Tables | No Spark equivalent documented | Low | Map to `createOrReplaceTempView()` |
| **P2** | Scheduler cron parser | Only schedule name captured | Low | Parse repeat interval → Fabric trigger cron |
| **P2** | Roles & permissions | Manual Fabric setup | Medium | Generate workspace role assignment scripts |
| **P3** | Database links (`@dblink`) | Not detected | Low | Flag for manual JDBC config |
| **P3** | Object Types (`CREATE TYPE`) | Rare usage | Low | Flatten to struct/columns |
| **P3** | Address Validator | Rare, third-party dependent | High | Azure Maps API integration |

---

## 8. Remediation Roadmap

### Sprint 6 — Critical Gaps (Recommended)

| # | Task | Owner | Deliverable | Est. Complexity |
|---|------|-------|-------------|-----------------|
| 6.1 | Add Mapplet parsing + expansion | Assessment + Notebook | Parse `<MAPPLET>` XML, resolve references, expand into mapping transformation chain | High |
| 6.2 | Add SQL Transformation type | Assessment + Notebook | Abbreviation `SQLT`, generate `spark.sql()` cell from embedded SQL | Low |
| 6.3 | Add Oracle analytic function detection | Assessment + SQL | Add `LEAD`, `LAG`, `DENSE_RANK`, `NTILE`, `FIRST_VALUE`, `LAST_VALUE` to `ORACLE_PATTERNS` + conversion rules (mostly 1:1) | Low |
| 6.4 | Add parameter file (.prm) parser | Assessment | Read `.prm` files, parse `[section]` + key-value pairs, inject into inventory | Low |
| 6.5 | Add Normalizer/Sorter/Union PySpark templates | Notebook | Complete conversion patterns for 3 partially-covered types | Low |
| 6.6 | Add flat file source handling | Notebook | `spark.read.csv()` / `.option("header", True)` patterns | Low |
| 6.7 | Add Control Task → Fail Activity | Pipeline | Map abort/fail tasks to Fabric Fail Activity | Low |

### Sprint 7 — Extended Coverage (Future)

| # | Task | Owner | Deliverable |
|---|------|-------|-------------|
| 7.1 | IICS XML parser (Cloud mappings) | Assessment | Separate parser for `exportMetadata` schema |
| 7.2 | IICS Taskflow → Fabric Pipeline | Pipeline | Cloud task orchestration conversion |
| 7.3 | SQL Server → Spark SQL patterns | SQL | Parallel pattern set for T-SQL sources |
| 7.4 | Web Service Consumer conversion | Notebook + Pipeline | `requests` UDF + Web Activity patterns |
| 7.5 | Data Masking support | Notebook | Dynamic Data Masking / PySpark UDF patterns |
| 7.6 | Connection XML parser | Assessment | Full connection object extraction |
| 7.7 | PL/SQL Package splitter | SQL | Split PACKAGE BODY into individual notebooks |

---

## Appendix A: Full Informatica Transformation Type Reference

Complete list of all PowerCenter transformation types and their project status.

| # | Transformation | Type Code | Project Status | Agent |
|---|---|---|---|---|
| 1 | Source Qualifier | SQ | ✅ Covered | Notebook |
| 2 | Expression | EXP | ✅ Covered | Notebook |
| 3 | Filter | FIL | ✅ Covered | Notebook |
| 4 | Aggregator | AGG | ✅ Covered | Notebook |
| 5 | Joiner | JNR | ✅ Covered | Notebook |
| 6 | Lookup (Connected) | LKP | ✅ Covered | Notebook |
| 7 | Lookup (Unconnected) | ULKP | 🟡 Partial | Notebook |
| 8 | Router | RTR | ✅ Covered | Notebook |
| 9 | Update Strategy | UPD | ✅ Covered | Notebook |
| 10 | Sorter | SRT | 🟡 Partial | Notebook |
| 11 | Rank | RNK | ✅ Covered | Notebook |
| 12 | Union | UNI | 🟡 Partial | Notebook |
| 13 | Normalizer | NRM | 🟡 Partial | Notebook |
| 14 | Sequence Generator | SEQ | ✅ Covered | Notebook |
| 15 | Stored Procedure | SP | ✅ Covered | SQL + Notebook |
| 16 | Transaction Control | TC | 🔶 Placeholder | Notebook |
| 17 | Java | JTX | 🔶 Placeholder | Notebook |
| 18 | Custom | CT | 🔶 Placeholder | Notebook |
| 19 | HTTP | HTTP | 🔶 Placeholder | Notebook |
| 20 | XML Generator | XMLG | 🔶 Placeholder | Notebook |
| 21 | XML Parser | XMLP | 🔶 Placeholder | Notebook |
| 22 | **Mapplet** | MPLT | ❌ Gap | Assessment + Notebook |
| 23 | **SQL** | SQLT | ❌ Gap | Assessment + Notebook |
| 24 | **Data Masking** | DM | ❌ Gap | Notebook |
| 25 | **External Procedure** | EP | ❌ Gap | Notebook |
| 26 | **Advanced External Procedure** | AEP | ❌ Gap | Notebook |
| 27 | **Web Service Consumer** | WSC | ❌ Gap | Notebook + Pipeline |
| 28 | **Association** | ASSOC | ❌ Gap | Notebook |
| 29 | **Key Generator** | KEYGEN | ❌ Gap | Notebook |
| 30 | **Address Validator** | ADDRVAL | ❌ Gap | Notebook |

---

## Appendix B: How to Use This Document

1. **Before migration:** Review this gap analysis against your Informatica inventory. If your mappings use gap items (Mapplets, SQL Transformations, etc.), plan manual effort accordingly.
2. **During assessment:** Run `@assessment` and check the warnings for "Unknown transformation type" — these are gap items the parser doesn't recognize.
3. **Estimating effort:** Use the Priority column to estimate the manual conversion effort for each gap item in your specific deployment.
4. **Contributing:** When you implement a gap fix (e.g., add Mapplet support), update this document's status accordingly.

---

<p align="center">
  <sub>This is a <b>living document</b>. Update it as gaps are addressed and new object types are encountered.</sub>
</p>
