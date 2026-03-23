# ADR-001: Multi-Agent Architecture

## Status
Accepted

## Context
The Informatica-to-Fabric migration tool needs to handle multiple distinct technical domains: XML parsing, SQL conversion, PySpark notebook generation, pipeline JSON generation, and validation. Each domain has specialized knowledge and distinct output formats.

## Decision
Use a 6-agent specialization model where each agent owns a specific domain:
1. **migration-orchestrator** — Coordination and planning
2. **assessment** — XML parsing and inventory
3. **sql-migration** — Oracle/SQL Server → Spark SQL conversion
4. **notebook-migration** — Mapping → PySpark notebook generation
5. **pipeline-migration** — Workflow → Pipeline JSON generation
6. **validation** — Test script generation

## Consequences
- Clear separation of concerns — each agent has defined input/output boundaries
- Agents can be invoked independently for targeted tasks
- File ownership rules prevent conflicts (one writer per output directory)
- The orchestrator handles cross-cutting concerns and sequencing
