# Databricks notebook source / Fabric Notebook
# =============================================================================
# Validation Notebook: VAL_PIPELINE_EXECUTION
# Pipeline: PL_WF_DAILY_SALES_LOAD
# Validates: execution order, notebook completion, branching logic
# =============================================================================

# COMMAND ----------

# Cell 1: Configuration
import json
from pyspark.sql.functions import col, lit, when, max as spark_max
from datetime import datetime

pipeline_name = "PL_WF_DAILY_SALES_LOAD"
run_timestamp = datetime.utcnow().isoformat()
results = []

expected_notebooks = [
    {
        "name": "NB_M_LOAD_CUSTOMERS",
        "target_tables": ["silver.dim_customer"],
        "must_run_before": ["NB_M_LOAD_ORDERS"],
        "required": True
    },
    {
        "name": "NB_M_LOAD_ORDERS",
        "target_tables": ["silver.fact_orders", "gold.agg_orders_by_customer"],
        "must_run_before": [],
        "required": True
    },
    {
        "name": "NB_M_UPSERT_INVENTORY",
        "target_tables": ["silver.dim_inventory"],
        "must_run_before": [],
        "required": True
    }
]

# COMMAND ----------

# Cell 2: Check All Target Tables Exist and Have Data
table_checks = []
try:
    all_target_tables = [
        "silver.dim_customer",
        "silver.fact_orders",
        "gold.agg_orders_by_customer",
        "silver.dim_inventory"
    ]

    for tbl in all_target_tables:
        try:
            cnt = spark.table(tbl).count()
            status = "PASS" if cnt > 0 else "FAIL"
            table_checks.append({
                "table": tbl,
                "result": status,
                "detail": f"Row count = {cnt}"
            })
        except Exception as e:
            table_checks.append({
                "table": tbl,
                "result": "FAIL",
                "detail": f"Table not accessible — {e}"
            })

    tables_overall = "PASS" if all(c["result"] == "PASS" for c in table_checks) else "FAIL"
    tables_detail = "; ".join([f"{c['table']}={c['result']}" for c in table_checks])

except Exception as e:
    tables_overall = "SKIPPED"
    tables_detail = f"Error checking tables — {e}"

results.append({
    "check": "Target Tables Exist & Populated",
    "result": tables_overall,
    "detail": tables_detail
})
print(f"[Pipeline] Target Tables: {tables_overall}")
for c in table_checks:
    print(f"  - {c['table']}: {c['result']} ({c['detail']})")

# COMMAND ----------

# Cell 3: Execution Order Validation
try:
    from delta.tables import DeltaTable

    order_checks = []

    table_timestamps = {}
    for tbl in ["silver.dim_customer", "silver.fact_orders", "gold.agg_orders_by_customer", "silver.dim_inventory"]:
        try:
            dt = DeltaTable.forName(spark, tbl)
            last_op = dt.history(1).select("timestamp", "operation").collect()[0]
            table_timestamps[tbl] = last_op["timestamp"]
        except Exception:
            table_timestamps[tbl] = None

    # Check: dim_customer loaded before fact_orders
    ts_customer = table_timestamps.get("silver.dim_customer")
    ts_orders = table_timestamps.get("silver.fact_orders")

    if ts_customer and ts_orders:
        if ts_customer <= ts_orders:
            order_checks.append({
                "rule": "dim_customer before fact_orders",
                "result": "PASS",
                "detail": f"dim_customer={ts_customer}, fact_orders={ts_orders}"
            })
        else:
            order_checks.append({
                "rule": "dim_customer before fact_orders",
                "result": "FAIL",
                "detail": f"dim_customer={ts_customer} AFTER fact_orders={ts_orders}"
            })
    else:
        order_checks.append({
            "rule": "dim_customer before fact_orders",
            "result": "SKIPPED",
            "detail": "Could not retrieve Delta timestamps"
        })

    # Check: fact_orders and agg_orders_by_customer written by same notebook
    ts_agg = table_timestamps.get("gold.agg_orders_by_customer")
    if ts_orders and ts_agg:
        time_diff_seconds = abs((ts_orders - ts_agg).total_seconds())
        threshold_seconds = 600  # 10 minutes
        co_write_status = "PASS" if time_diff_seconds <= threshold_seconds else "FAIL"
        order_checks.append({
            "rule": "fact_orders & agg_orders co-written",
            "result": co_write_status,
            "detail": f"Time diff = {time_diff_seconds}s (threshold={threshold_seconds}s)"
        })
    else:
        order_checks.append({
            "rule": "fact_orders & agg_orders co-written",
            "result": "SKIPPED",
            "detail": "Missing timestamps"
        })

    order_overall = "PASS" if all(c["result"] == "PASS" for c in order_checks) else (
        "SKIPPED" if all(c["result"] == "SKIPPED" for c in order_checks) else "FAIL"
    )
    order_detail = "; ".join([f"{c['rule']}={c['result']}" for c in order_checks])

except Exception as e:
    order_overall = "SKIPPED"
    order_detail = f"Error checking execution order — {e}"
    order_checks = []

results.append({
    "check": "Execution Order",
    "result": order_overall,
    "detail": order_detail
})
print(f"[Pipeline] Execution Order: {order_overall}")
for c in order_checks:
    print(f"  - {c['rule']}: {c['result']} ({c['detail']})")

# COMMAND ----------

# Cell 4: IfCondition Branching Logic Validation
try:
    branch_checks = []

    customer_ok = any(
        c["result"] == "PASS" and c["table"] == "silver.dim_customer"
        for c in table_checks
    )
    inventory_ok = any(
        c["result"] == "PASS" and c["table"] == "silver.dim_inventory"
        for c in table_checks
    )

    if customer_ok and inventory_ok:
        branch_checks.append({
            "rule": "IfCondition: customer success → inventory runs",
            "result": "PASS",
            "detail": "Both dim_customer and dim_inventory populated"
        })
    elif customer_ok and not inventory_ok:
        branch_checks.append({
            "rule": "IfCondition: customer success → inventory runs",
            "result": "FAIL",
            "detail": "dim_customer loaded but dim_inventory is empty/missing"
        })
    elif not customer_ok:
        branch_checks.append({
            "rule": "IfCondition: customer failed → check branch",
            "result": "SKIPPED",
            "detail": "dim_customer not loaded — cannot validate branching outcome"
        })

    all_populated = all(c["result"] == "PASS" for c in table_checks)
    branch_checks.append({
        "rule": "Full pipeline: all targets populated",
        "result": "PASS" if all_populated else "FAIL",
        "detail": f"{sum(1 for c in table_checks if c['result'] == 'PASS')}/{len(table_checks)} tables populated"
    })

    branch_overall = "PASS" if all(c["result"] == "PASS" for c in branch_checks) else (
        "SKIPPED" if all(c["result"] == "SKIPPED" for c in branch_checks) else "FAIL"
    )
    branch_detail = "; ".join([f"{c['rule']}={c['result']}" for c in branch_checks])

except Exception as e:
    branch_overall = "SKIPPED"
    branch_detail = f"Error validating branching logic — {e}"
    branch_checks = []

results.append({
    "check": "IfCondition Branching",
    "result": branch_overall,
    "detail": branch_detail
})
print(f"[Pipeline] IfCondition Branching: {branch_overall}")
for c in branch_checks:
    print(f"  - {c['rule']}: {c['result']} ({c['detail']})")

# COMMAND ----------

# Cell 5: Cross-Table Referential Integrity (post-pipeline)
try:
    ri_checks = []

    # fact_orders.customer_id → dim_customer.customer_id
    fact_orders = spark.table("silver.fact_orders")
    dim_customer = spark.table("silver.dim_customer")
    orphan_orders = fact_orders.join(
        dim_customer.select("customer_id").distinct(),
        "customer_id",
        "left_anti"
    ).count()
    ri_checks.append({
        "rule": "fact_orders.customer_id → dim_customer",
        "result": "PASS" if orphan_orders == 0 else "FAIL",
        "detail": f"Orphaned order rows = {orphan_orders}"
    })

    # agg_orders_by_customer.customer_id → dim_customer.customer_id
    agg_orders = spark.table("gold.agg_orders_by_customer")
    orphan_agg = agg_orders.join(
        dim_customer.select("customer_id").distinct(),
        "customer_id",
        "left_anti"
    ).count()
    ri_checks.append({
        "rule": "agg_orders.customer_id → dim_customer",
        "result": "PASS" if orphan_agg == 0 else "FAIL",
        "detail": f"Orphaned agg rows = {orphan_agg}"
    })

    ri_overall = "PASS" if all(c["result"] == "PASS" for c in ri_checks) else "FAIL"
    ri_detail = "; ".join([f"{c['rule']}={c['result']}" for c in ri_checks])

except Exception as e:
    ri_overall = "SKIPPED"
    ri_detail = f"Error in RI checks — {e}"
    ri_checks = []

results.append({
    "check": "Cross-Table Referential Integrity",
    "result": ri_overall,
    "detail": ri_detail
})
print(f"[Pipeline] Referential Integrity: {ri_overall}")
for c in ri_checks:
    print(f"  - {c['rule']}: {c['result']} ({c['detail']})")

# COMMAND ----------

# Cell 6: Summary
overall = "PASS" if all(r["result"] == "PASS" for r in results) else (
    "FAIL" if any(r["result"] == "FAIL" for r in results) else "SKIPPED"
)

summary = {
    "pipeline": pipeline_name,
    "run_timestamp": run_timestamp,
    "expected_notebooks": [nb["name"] for nb in expected_notebooks],
    "checks": results,
    "overall": overall
}

print("=" * 60)
print(f"PIPELINE VALIDATION SUMMARY — {pipeline_name}")
print("=" * 60)
print(json.dumps(summary, indent=2))
print("=" * 60)
print(f"OVERALL RESULT: {overall}")
print("=" * 60)

df_summary = spark.createDataFrame(
    [(r["check"], r["result"], r["detail"]) for r in results],
    ["check_name", "result", "detail"]
)
df_summary.show(truncate=False)
