# Migration Issues Report

**Generated:** 2026-03-23  
**Source Platform:** Informatica PowerCenter  
**Target Platform:** Microsoft Fabric  

---

## Summary

| Severity | Count | Description |
|----------|-------|-------------|
| **P0** — Blocking | 2 | Must fix before any testing |
| **P1** — Important | 3 | Must fix before production |
| **P2** — Low | 1 | Can address post-deployment |

**Total Issues:** 6

---

## P0 — Blocking Issues

### ISSUE-001: JDBC connection strings are placeholder values

| Field | Value |
|-------|-------|
| **Severity** | P0 |
| **Category** | Configuration |
| **Affected Files** | `output/validation/VAL_DIM_CUSTOMER.py`, `VAL_FACT_ORDERS.py`, `VAL_AGG_ORDERS_BY_CUSTOMER.py`, `VAL_DIM_INVENTORY.py` |
| **Description** | Validation notebooks contain placeholder `<host>`, `<username>`, `<password>` in JDBC connection strings. |
| **Action Required** | Replace with actual Oracle JDBC URL and credentials. Use Fabric Key Vault–linked service or `notebookutils.credentials.getSecret()`. |

### ISSUE-002: Lakehouse tables not yet created

| Field | Value |
|-------|-------|
| **Severity** | P0 |
| **Category** | Infrastructure |
| **Affected Files** | All notebooks in `output/notebooks/` |
| **Description** | Target tables (`silver.dim_customer`, `silver.fact_orders`, `gold.agg_orders_by_customer`, `silver.dim_inventory`) do not yet exist in the Fabric Lakehouse. |
| **Action Required** | Tables will auto-create on first Delta write, but verify schemas match expectations. Run notebooks in order: CUSTOMERS → ORDERS → INVENTORY. |

---

## P1 — Important Issues

### ISSUE-003: Post-SQL `EXEC SP_UPDATE_ORDER_STATS` not converted

| Field | Value |
|-------|-------|
| **Severity** | P1 |
| **Category** | SQL Conversion |
| **Affected Files** | `output/sql/SQL_OVERRIDES_M_LOAD_ORDERS.sql` (line 83) |
| **Description** | The original workflow's M_LOAD_ORDERS session had a Post-SQL command: `EXEC SP_UPDATE_ORDER_STATS`. This is marked as `-- TODO` in the SQL overrides file. The stored procedure has been converted to Spark SQL in `SQL_SP_UPDATE_ORDER_STATS.sql`, but it is not wired into the notebook or pipeline. |
| **Action Required** | Either: (a) add the converted SP SQL as a final `%%sql` cell in `NB_M_LOAD_ORDERS.py`, or (b) add a separate Notebook Activity in the pipeline after `NB_M_LOAD_ORDERS` that executes the SP logic. |

### ISSUE-004: Pipeline `alert_webhook_url` parameter needs configuration

| Field | Value |
|-------|-------|
| **Severity** | P1 |
| **Category** | Configuration |
| **Affected Files** | `output/pipelines/PL_WF_DAILY_SALES_LOAD.json` |
| **Description** | The pipeline has an `alert_webhook_url` parameter used by EMAIL_FAILURE activities (converted to Web Activity). The default value is a placeholder. |
| **Action Required** | Set the `alert_webhook_url` to an Azure Logic App HTTP trigger, Teams Incoming Webhook, or Power Automate flow URL. |

### ISSUE-005: Verify `DISCOUNT_PCT` COALESCE default value

| Field | Value |
|-------|-------|
| **Severity** | P1 |
| **Category** | Data Semantics |
| **Affected Files** | `output/notebooks/NB_M_LOAD_ORDERS.py` |
| **Description** | The Expression transformation converts a DISCOUNT_PCT lookup result with `COALESCE(col, 0.0)`. Need to verify this matches the original Informatica mapping's default value on lookup miss. |
| **Action Required** | Check the original `LKP_PRODUCTS` lookup default value in the Informatica mapping. If it differs from `0.0`, update the COALESCE. |

---

## P2 — Low Priority Issues

### ISSUE-006: Fabric Git integration not configured

| Field | Value |
|-------|-------|
| **Severity** | P2 |
| **Category** | Deployment |
| **Affected Files** | All output files |
| **Description** | No Git sync or REST API deployment has been configured for pushing artifacts to a Fabric workspace. |
| **Action Required** | Set up Fabric Git integration or use the Fabric REST API to deploy notebooks and pipeline JSON from the `output/` directory. |

---

## Informational Notes

These are not issues but items to be aware of:

| Note | Detail |
|------|--------|
| **UPD weight = 2** | Update Strategy transformations are treated as inherently complex (weight 2) because they require Delta MERGE. This affects M_UPSERT_INVENTORY classification. |
| **Source/Target at FOLDER level** | Some Informatica XML exports place SOURCE/TARGET elements as siblings of MAPPING at the FOLDER level, not nested inside MAPPING. The assessment parser handles this with fallback logic. |
| **Validation connection handling** | All validation notebooks include try/except around Oracle JDBC reads. If the Oracle source is unavailable, validation will report "Connection failure" but won't crash. |
| **Oracle constructs converted** | 9 Oracle-specific constructs were found and converted: MERGE, DECODE ×2, NVL ×2, SYSDATE, TRUNC, TO_DATE, DBMS_OUTPUT. |

---

## Resolution Tracking

| Issue | Status | Resolved By | Date |
|-------|--------|-------------|------|
| ISSUE-001 | Open | — | — |
| ISSUE-002 | Open | — | — |
| ISSUE-003 | Open | — | — |
| ISSUE-004 | Open | — | — |
| ISSUE-005 | Open | — | — |
| ISSUE-006 | Open | — | — |
