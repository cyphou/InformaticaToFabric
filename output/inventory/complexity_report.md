# Informatica-to-Fabric Migration -- Complexity Report

**Generated:** 2026-03-23
**Source Platform:** Informatica PowerCenter
**Target Platform:** Microsoft Fabric

---

## Summary

| Metric | Count |
|--------|-------|
| Total Mappings | 6 |
| Total Workflows | 1 |
| Total Sessions | 3 |
| SQL Files Analyzed | 3 |

---

## Complexity Breakdown

| Complexity | Count | % | Migration Approach |
|------------|-------|---|-------------------|
| **Simple** | 2 | 33% | Auto-generate PySpark notebooks |
| **Medium** | 1 | 17% | Semi-automated with manual review |
| **Complex** | 3 | 50% | Manual assist required |
| **Custom** | 0 | 0% | Redesign required |

---

## Mapping Details

| Mapping | Complexity | Sources | Targets | Transformations | SQL Override | Stored Proc |
|---------|------------|---------|---------|-----------------|-------------|-------------|
| m_load_contacts | **Medium** | src_sf_contacts | tgt_lh_contacts | EXP -> port -> FIL -> LKP -> AGG -> DM | No | No |
| m_sync_accounts | **Simple** | src_accounts | tgt_accounts | EXP | No | No |
| M_LOAD_CUSTOMERS | **Simple** | Oracle.SALES.CUSTOMERS | DIM_CUSTOMER | SQ -> EXP -> FIL | No | No |
| M_LOAD_EMPLOYEES | **Complex** | Oracle.HR.EMPLOYEES | DIM_EMPLOYEE | SQ -> EXP -> FIL -> SRT -> LKP -> SQLT | Yes | No |
| M_LOAD_ORDERS | **Complex** | Oracle.SALES.ORDERS, Oracle.SALES.PRODUCTS | FACT_ORDERS, AGG_ORDERS_BY_CUSTOMER | SQ -> LKP -> EXP -> AGG | Yes | No |
| M_UPSERT_INVENTORY | **Complex** | Oracle.SALES.STG_INVENTORY | DIM_INVENTORY | SQ -> EXP -> UPD | No | No |

---

## SQL Overrides Found

### M_LOAD_EMPLOYEES
- **Sql Query:**
  ```sql
  SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, MONTHLY_SALARY, HIRE_DATE, STATUS FROM HR.EMPLOYEES WHERE STATUS = 'ACTIVE' AND HIRE_DATE >= ADD_MONTHS(SYSDATE, -120)
  ```
- **Lookup Sql Override:**
  ```sql
  SELECT DEPT_NAME, MANAGER_NAME FROM HR.DEPARTMENTS WHERE ACTIVE = 'Y'
  ```
- **Sql Query:**
  ```sql
  SELECT EMPLOYEE_ID, DENSE_RANK() OVER (PARTITION BY DEPARTMENT ORDER BY ANNUAL_SALARY DESC) AS DEPT_SALARY_RANK FROM HR.EMPLOYEES
  ```

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

### SP_CALC_RANKINGS.sql
- **Path:** `input\sql\SP_CALC_RANKINGS.sql`
- **Total lines:** 96
- **Oracle-specific constructs:**

| Construct | Occurrences | Lines |
|-----------|-------------|-------|
| `MERGE` | 1 | 13 |
| `DECODE` | 1 | 70 |
| `NVL` | 1 | 35 |
| `SYSDATE` | 5 | 10, 50, 56, 82, 86 |
| `TO_CHAR` | 3 | 79, 83, 86 |
| `DBMS_` | 2 | 86, 92 |
| `EXCEPTION WHEN` | 1 | 89 |
| `CREATE OR REPLACE` | 1 | 8 |
| `LEAD` | 2 | 30, 69 |
| `LAG` | 4 | 29, 31, 68, 71 |
| `DENSE_RANK` | 1 | 18 |
| `NTILE` | 1 | 19 |
| `FIRST_VALUE` | 1 | 21 |
| `LAST_VALUE` | 1 | 25 |
| `ROW_NUMBER` | 1 | 20 |
| `OVER` | 11 | 18, 19, 20, 21, 25, 29, 30, 31, 68, 69... |
| `PARTITION BY` | 3 | 68, 69, 71 |

### SP_REFRESH_DASHBOARD.sql
- **Path:** `input\sql\SP_REFRESH_DASHBOARD.sql`
- **Total lines:** 126
- **Oracle-specific constructs:**

| Construct | Occurrences | Lines |
|-----------|-------------|-------|
| `ROWNUM` | 1 | 101 |
| `LEAD` | 1 | 100 |
| `LAG` | 1 | 99 |
| `ROW_NUMBER` | 1 | 101 |
| `OVER` | 4 | 97, 99, 100, 101 |
| `PARTITION BY` | 4 | 97, 99, 100, 101 |
| `DB_LINK` | 12 | 13, 14, 15, 30, 51, 61, 61, 83, 87, 87... |

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
