# Informatica-to-Fabric Migration -- Complexity Report

**Generated:** 2026-03-23
**Source Platform:** Informatica PowerCenter
**Target Platform:** Microsoft Fabric

---

## Summary

| Metric | Count |
|--------|-------|
| Total Mappings | 3 |
| Total Workflows | 1 |
| Total Sessions | 3 |
| SQL Files Analyzed | 1 |

---

## Complexity Breakdown

| Complexity | Count | % | Migration Approach |
|------------|-------|---|-------------------|
| **Simple** | 1 | 33% | Auto-generate PySpark notebooks |
| **Medium** | 0 | 0% | Semi-automated with manual review |
| **Complex** | 2 | 67% | Manual assist required |
| **Custom** | 0 | 0% | Redesign required |

---

## Mapping Details

| Mapping | Complexity | Sources | Targets | Transformations | SQL Override | Stored Proc |
|---------|------------|---------|---------|-----------------|-------------|-------------|
| M_LOAD_CUSTOMERS | **Simple** | Oracle.SALES.CUSTOMERS | DIM_CUSTOMER | SQ -> EXP -> FIL | No | No |
| M_LOAD_ORDERS | **Complex** | Oracle.SALES.ORDERS, Oracle.SALES.PRODUCTS | FACT_ORDERS, AGG_ORDERS_BY_CUSTOMER | SQ -> LKP -> EXP -> AGG | Yes | No |
| M_UPSERT_INVENTORY | **Complex** | Oracle.SALES.STG_INVENTORY | DIM_INVENTORY | SQ -> EXP -> UPD | No | No |

---

## SQL Overrides Found

### M_LOAD_ORDERS
- **Sql Query:**
  ```sql
  SELECT ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, NVL(DISCOUNT_PCT, 0) AS DISCOUNT_PCT, ORDER_DATE, ORDER_STATUS, CHANNEL FROM SALES.ORDERS WHERE ORDER_DATE >= TO_DATE('$$LOAD_DATE', 'YYYY-MM-DD') AND ORDER_STATUS != 'CANCELLED'
  ```
- **Lookup Sql Override:**
  ```sql
  SELECT PRODUCT_ID, NVL(PRODUCT_NAME, 'Unknown') AS PRODUCT_NAME, NVL(CATEGORY, 'Uncategorized') AS CATEGORY FROM SALES.PRODUCTS
  ```


---

## Oracle SQL Files Analysis

### SP_UPDATE_ORDER_STATS.sql
- **Path:** `input\sql\SP_UPDATE_ORDER_STATS.sql`
- **Total lines:** 83
- **Oracle-specific constructs:**

| Construct | Occurrences | Lines |
|-----------|-------------|-------|
| `MERGE` | 1 | 15 |
| `DECODE` | 1 | 24 |
| `NVL` | 1 | 54 |
| `SYSDATE` | 4 | 10, 42, 48, 77 |
| `TO_CHAR` | 1 | 69 |
| `DBMS_` | 1 | 69 |
| `EXCEPTION WHEN` | 1 | 71 |
| `NUMBER` | 1 | 12 |
| `CREATE OR REPLACE` | 1 | 8 |


---

## Migration Recommendations

1. **Simple mappings** -- Auto-generate using the notebook-migration agent. Expect minimal manual intervention.
2. **Medium mappings** -- Use notebook-migration with manual review of LKP/AGG/JNR logic. Validate join conditions.
3. **Complex mappings** -- Hand off SQL overrides to sql-migration agent first, then generate notebooks.
4. **Custom mappings** -- Require full redesign. Java transformations must be rewritten in PySpark.
5. **Oracle SQL files** -- Hand off to sql-migration for MERGE, DECODE, NVL, etc.
6. **Workflow orchestration** -- Hand off to pipeline-migration for Fabric Pipeline JSON generation.
