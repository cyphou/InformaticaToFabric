# ADR-003: Regex-Based SQL Conversion

## Status
Accepted

## Context
Converting Oracle and SQL Server SQL to Spark SQL requires handling dozens of syntactic and semantic differences. Options considered: AST-based parser (e.g., sqlglot), regex-based pattern matching, or manual rules.

## Decision
Use **regex-based pattern matching** with ordered replacement rules. Complex cases (DECODE with variable arguments) use custom parsers.

## Rationale
- Regex covers 90%+ of real-world Oracle/SQL Server patterns
- Easier to maintain and extend than a full AST approach
- TODO markers flag edge cases for manual review
- DECODE uses a dedicated recursive parser due to variable argument counts

## Consequences
- Rules are order-dependent (more specific patterns first)
- Some complex nested SQL may need manual adjustment
- Every converted file includes a "Review all TODO comments" header
- New rules can be added by appending to `ORACLE_REPLACEMENTS` or `SQLSERVER_REPLACEMENTS`
