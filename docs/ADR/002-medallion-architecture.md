# ADR-002: Medallion Architecture Target

## Status
Accepted

## Context
Migrated Informatica workloads need a target data architecture in Microsoft Fabric. The original Informatica setup uses source-to-staging-to-target patterns.

## Decision
Target a **Medallion architecture** (Bronze → Silver → Gold) on Delta Lake / OneLake:
- **Bronze:** Raw ingestion (mirrors source tables)
- **Silver:** Cleansed and conformed data (transformations applied)
- **Gold:** Business-ready aggregates and curated datasets

## Consequences
- Generated notebooks follow Bronze/Silver/Gold naming conventions
- Table names in `infer_target_table()` use Silver as default layer
- Validation scripts compare source against Silver/Gold tables
- Pipeline annotations include layer information
