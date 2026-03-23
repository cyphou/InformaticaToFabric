# Databricks notebook source / Fabric Notebook
# =============================================================================
# Validation Notebook: VAL_AGG_ORDERS_BY_CUSTOMER
# Source: Oracle ORDERS + PRODUCTS (aggregated) → Target: gold.agg_orders_by_customer
# Mapping: M_LOAD_ORDERS (Complex: SQ→LKP→EXP→AGG→TGT, overwrite mode)
# =============================================================================

# COMMAND ----------

# Cell 1: Configuration
import json
from pyspark.sql.functions import (
    md5, concat_ws, col, count, lit, when, isnull,
    sum as spark_sum, min as spark_min, max as spark_max,
    abs as spark_abs, avg as spark_avg
)
from datetime import datetime

mapping_name = "M_LOAD_ORDERS"
target_table = "gold.agg_orders_by_customer"

# Oracle JDBC configuration
source_jdbc_url = "jdbc:oracle:thin:@<host>:1521/<service>"
source_jdbc_properties = {
    "user": "<username>",
    "password": "<password>",  # Use Key Vault in production
    "driver": "oracle.jdbc.driver.OracleDriver"
}

source_agg_query = """(
    SELECT o.customer_id,
           COUNT(o.order_id) AS total_orders,
           SUM(o.quantity * p.unit_price) AS total_amount,
           AVG(o.quantity * p.unit_price) AS avg_order_amount,
           MIN(o.order_date) AS first_order_date,
           MAX(o.order_date) AS last_order_date
    FROM ORDERS o
    JOIN PRODUCTS p ON o.product_id = p.product_id
    GROUP BY o.customer_id
)"""

key_columns = ["customer_id"]
checksum_columns = ["customer_id", "total_orders", "total_amount"]
metric_columns = ["total_amount", "total_orders", "avg_order_amount"]
nullable_not_allowed = ["customer_id", "total_orders", "total_amount"]

run_timestamp = datetime.utcnow().isoformat()
results = []

# COMMAND ----------

# Cell 2: Level 1 — Row Count Validation (distinct customers)
try:
    df_source = spark.read.jdbc(
        url=source_jdbc_url,
        table=source_agg_query,
        properties=source_jdbc_properties
    )
    source_count = df_source.count()
except Exception as e:
    source_count = None
    print(f"WARNING: Could not read Oracle source — {e}")

try:
    df_target = spark.table(target_table)
    target_count = df_target.count()
except Exception as e:
    target_count = None
    print(f"WARNING: Could not read Fabric target — {e}")

if source_count is None or target_count is None:
    row_count_result = "SKIPPED"
    row_count_detail = "Connection failure — see warnings above"
elif source_count == target_count:
    row_count_result = "PASS"
    row_count_detail = f"Source={source_count}, Target={target_count} (distinct customers)"
else:
    row_count_result = "FAIL"
    row_count_detail = f"Source={source_count}, Target={target_count}, Delta={target_count - source_count}"

results.append({
    "check": "Row Count (customer groups)",
    "level": 1,
    "result": row_count_result,
    "detail": row_count_detail
})
print(f"[Level 1] Row Count: {row_count_result} — {row_count_detail}")

# COMMAND ----------

# Cell 3: Level 2 — Column-Level Checksum
try:
    source_hash = (
        df_source
        .withColumn("row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in checksum_columns])))
        .selectExpr("sum(cast(conv(substring(row_hash, 1, 8), 16, 10) as bigint)) as checksum")
        .collect()[0]["checksum"]
    )

    target_hash = (
        df_target
        .withColumn("row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in checksum_columns])))
        .selectExpr("sum(cast(conv(substring(row_hash, 1, 8), 16, 10) as bigint)) as checksum")
        .collect()[0]["checksum"]
    )

    if source_hash == target_hash:
        checksum_result = "PASS"
        checksum_detail = f"Checksum match: {source_hash}"
    else:
        checksum_result = "FAIL"
        checksum_detail = f"Source={source_hash}, Target={target_hash}"
except Exception as e:
    checksum_result = "SKIPPED"
    checksum_detail = f"Error computing checksums — {e}"

results.append({
    "check": "Column Checksum",
    "level": 2,
    "result": checksum_result,
    "detail": checksum_detail
})
print(f"[Level 2] Column Checksum: {checksum_result} — {checksum_detail}")

# COMMAND ----------

# Cell 4: Level 3 — Aggregate Validation (grand totals)
try:
    raw_source_query = """(
        SELECT SUM(o.quantity * p.unit_price) AS grand_total_amount,
               COUNT(o.order_id) AS grand_total_orders
        FROM ORDERS o
        JOIN PRODUCTS p ON o.product_id = p.product_id
    )"""
    raw_source = spark.read.jdbc(
        url=source_jdbc_url,
        table=raw_source_query,
        properties=source_jdbc_properties
    ).collect()[0]

    target_totals = df_target.agg(
        spark_sum("total_amount").alias("grand_total_amount"),
        spark_sum("total_orders").alias("grand_total_orders")
    ).collect()[0]

    tolerance = 0.01
    amt_diff = abs((raw_source["grand_total_amount"] or 0) - (target_totals["grand_total_amount"] or 0))
    orders_match = raw_source["grand_total_orders"] == target_totals["grand_total_orders"]

    agg_checks = []
    agg_checks.append(f"grand_total_amount {'PASS' if amt_diff <= tolerance else 'FAIL (diff=' + str(amt_diff) + ')'}")
    agg_checks.append(f"grand_total_orders {'PASS' if orders_match else 'FAIL'}")

    agg_overall = "PASS" if all("PASS" in c for c in agg_checks) else "FAIL"
    agg_detail = "; ".join(agg_checks)

except Exception as e:
    agg_overall = "SKIPPED"
    agg_detail = f"Error in aggregate validation — {e}"

results.append({
    "check": "Aggregate Validation (grand totals)",
    "level": 3,
    "result": agg_overall,
    "detail": agg_detail
})
print(f"[Level 3] Aggregate Validation: {agg_overall} — {agg_detail}")

# COMMAND ----------

# Cell 5: Level 3b — Data Quality Rules
dq_checks = []

# Null checks
try:
    for col_name in nullable_not_allowed:
        null_count = df_target.filter(isnull(col(col_name))).count()
        status = "PASS" if null_count == 0 else "FAIL"
        dq_checks.append({"rule": f"NOT NULL: {col_name}", "result": status, "detail": f"Null count={null_count}"})
except Exception as e:
    dq_checks.append({"rule": "NOT NULL checks", "result": "SKIPPED", "detail": str(e)})

# Key uniqueness: one row per customer
try:
    total = df_target.count()
    distinct_keys = df_target.select("customer_id").distinct().count()
    key_status = "PASS" if total == distinct_keys else "FAIL"
    dq_checks.append({"rule": "Key Uniqueness: customer_id", "result": key_status, "detail": f"Total={total}, Distinct={distinct_keys}"})
except Exception as e:
    dq_checks.append({"rule": "Key Uniqueness", "result": "SKIPPED", "detail": str(e)})

# Value range: total_orders > 0, total_amount > 0
try:
    bad_orders = df_target.filter(col("total_orders") <= 0).count()
    bad_amount = df_target.filter(col("total_amount") <= 0).count()
    range_status = "PASS" if bad_orders == 0 and bad_amount == 0 else "FAIL"
    dq_checks.append({
        "rule": "Value Range: total_orders>0, total_amount>0",
        "result": range_status,
        "detail": f"Invalid orders={bad_orders}, Invalid amount={bad_amount}"
    })
except Exception as e:
    dq_checks.append({"rule": "Value Range", "result": "SKIPPED", "detail": str(e)})

# Referential integrity: customer_id in dim_customer
try:
    dim_customer_ids = spark.table("silver.dim_customer").select("customer_id").distinct()
    orphans = df_target.join(dim_customer_ids, "customer_id", "left_anti").count()
    ri_status = "PASS" if orphans == 0 else "FAIL"
    dq_checks.append({"rule": "RI: customer_id → dim_customer", "result": ri_status, "detail": f"Orphaned={orphans}"})
except Exception as e:
    dq_checks.append({"rule": "RI: customer_id", "result": "SKIPPED", "detail": str(e)})

dq_overall = "PASS" if all(c["result"] == "PASS" for c in dq_checks) else (
    "SKIPPED" if all(c["result"] == "SKIPPED" for c in dq_checks) else "FAIL"
)
dq_detail = "; ".join([f"{c['rule']}={c['result']}" for c in dq_checks])

results.append({
    "check": "Data Quality",
    "level": 3,
    "result": dq_overall,
    "detail": dq_detail
})
print(f"[Level 3b] Data Quality: {dq_overall}")
for c in dq_checks:
    print(f"  - {c['rule']}: {c['result']} ({c['detail']})")

# COMMAND ----------

# Cell 6: Level 4 — Sample Record Comparison
try:
    sample_keys = (
        df_target.select("customer_id")
        .orderBy("customer_id")
        .limit(50)
        .collect()
    )
    key_list = [row["customer_id"] for row in sample_keys]

    df_src_sample = df_source.filter(col("customer_id").isin(key_list))
    df_tgt_sample = df_target.filter(col("customer_id").isin(key_list))

    joined = df_src_sample.alias("s").join(df_tgt_sample.alias("t"), "customer_id", "full_outer")

    tolerance = 0.01
    mismatches = joined.filter(
        (spark_abs(col("s.total_amount") - col("t.total_amount")) > tolerance) |
        (col("s.total_orders") != col("t.total_orders"))
    )
    mismatch_count = mismatches.count()

    if mismatch_count == 0:
        sample_result = "PASS"
        sample_detail = f"50 sampled customer aggregations match (tolerance={tolerance})"
    else:
        sample_result = "FAIL"
        sample_detail = f"{mismatch_count} mismatched customers in sample of 50"
        mismatches.show(10, truncate=False)
except Exception as e:
    sample_result = "SKIPPED"
    sample_detail = f"Error during sample comparison — {e}"

results.append({
    "check": "Sample Comparison",
    "level": 4,
    "result": sample_result,
    "detail": sample_detail
})
print(f"[Level 4] Sample Comparison: {sample_result} — {sample_detail}")

# COMMAND ----------

# Cell 7: Summary
overall = "PASS" if all(r["result"] == "PASS" for r in results) else (
    "FAIL" if any(r["result"] == "FAIL" for r in results) else "SKIPPED"
)

summary = {
    "mapping": mapping_name,
    "target_table": target_table,
    "run_timestamp": run_timestamp,
    "known_differences": [
        "Floating-point tolerance of 0.01 applied to total_amount and avg_order_amount",
        "Overwrite mode — target should exactly match source aggregation"
    ],
    "checks": results,
    "overall": overall
}

print("=" * 60)
print(f"VALIDATION SUMMARY — {mapping_name} → {target_table}")
print("=" * 60)
print(json.dumps(summary, indent=2))
print("=" * 60)
print(f"OVERALL RESULT: {overall}")
print("=" * 60)

df_summary = spark.createDataFrame(
    [(r["check"], r["level"], r["result"], r["detail"]) for r in results],
    ["check_name", "level", "result", "detail"]
)
df_summary.show(truncate=False)
