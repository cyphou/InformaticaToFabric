# Databricks notebook source / Fabric Notebook
# =============================================================================
# Validation Notebook: VAL_FACT_ORDERS
# Source: Oracle ORDERS + PRODUCTS → Target: silver.fact_orders
# Mapping: M_LOAD_ORDERS (Complex: SQ→LKP→EXP→AGG→TGT, append mode)
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
target_table = "silver.fact_orders"

# Oracle JDBC configuration
source_jdbc_url = "jdbc:oracle:thin:@<host>:1521/<service>"
source_jdbc_properties = {
    "user": "<username>",
    "password": "<password>",  # Use Key Vault in production
    "driver": "oracle.jdbc.driver.OracleDriver"
}
source_query = """(
    SELECT o.order_id, o.customer_id, o.order_date, o.quantity,
           p.product_id, p.product_name, p.unit_price,
           o.quantity * p.unit_price AS order_amount,
           o.order_status
    FROM ORDERS o
    JOIN PRODUCTS p ON o.product_id = p.product_id
)"""

key_columns = ["order_id"]
checksum_columns = ["order_id", "customer_id", "product_id", "order_date", "quantity", "order_amount", "order_status"]
metric_columns = ["order_amount", "quantity"]
nullable_not_allowed = ["order_id", "customer_id", "product_id", "order_date", "quantity"]

run_timestamp = datetime.utcnow().isoformat()
results = []

# COMMAND ----------

# Cell 2: Level 1 — Row Count Validation
try:
    df_source = spark.read.jdbc(
        url=source_jdbc_url,
        table=source_query,
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
elif target_count >= source_count:
    row_count_result = "PASS"
    row_count_detail = f"Source={source_count}, Target={target_count} (append mode — target >= source expected)"
else:
    row_count_result = "FAIL"
    row_count_detail = f"Source={source_count}, Target={target_count} — target has fewer rows than source"

results.append({
    "check": "Row Count",
    "level": 1,
    "result": row_count_result,
    "detail": row_count_detail
})
print(f"[Level 1] Row Count: {row_count_result} — {row_count_detail}")

# COMMAND ----------

# Cell 3: Level 2 — Column-Level Checksum Validation
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
        checksum_detail = f"Source={source_hash}, Target={target_hash} (may differ in append mode if prior batches exist)"
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

# Cell 4: Level 3 — Aggregate Validation (Business Metrics)
try:
    source_agg = df_source.agg(
        spark_sum("order_amount").alias("total_amount"),
        spark_sum("quantity").alias("total_quantity"),
        spark_avg("order_amount").alias("avg_amount"),
        spark_min("order_date").alias("min_order_date"),
        spark_max("order_date").alias("max_order_date"),
        count("*").alias("row_count")
    ).collect()[0]

    target_agg = df_target.agg(
        spark_sum("order_amount").alias("total_amount"),
        spark_sum("quantity").alias("total_quantity"),
        spark_avg("order_amount").alias("avg_amount"),
        spark_min("order_date").alias("min_order_date"),
        spark_max("order_date").alias("max_order_date"),
        count("*").alias("row_count")
    ).collect()[0]

    agg_checks = []
    tolerance = 0.01
    amt_diff = abs((source_agg["total_amount"] or 0) - (target_agg["total_amount"] or 0))
    agg_checks.append("total_amount " + ("PASS" if amt_diff <= tolerance else f"FAIL (diff={amt_diff})"))

    qty_match = source_agg["total_quantity"] == target_agg["total_quantity"]
    agg_checks.append("total_quantity " + ("PASS" if qty_match else "FAIL"))

    agg_overall = "PASS" if all("PASS" in c for c in agg_checks) else "FAIL"
    agg_detail = "; ".join(agg_checks) + f" | Source total_amount={source_agg['total_amount']}, Target total_amount={target_agg['total_amount']}"

except Exception as e:
    agg_overall = "SKIPPED"
    agg_detail = f"Error in aggregate validation — {e}"

results.append({
    "check": "Aggregate Validation",
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

# Key uniqueness
try:
    total = df_target.count()
    distinct_keys = df_target.select("order_id").distinct().count()
    key_status = "PASS" if total == distinct_keys else "FAIL"
    dq_checks.append({"rule": "Key Uniqueness: order_id", "result": key_status, "detail": f"Total={total}, Distinct={distinct_keys}"})
except Exception as e:
    dq_checks.append({"rule": "Key Uniqueness", "result": "SKIPPED", "detail": str(e)})

# Referential integrity: customer_id exists in dim_customer
try:
    dim_customer_ids = spark.table("silver.dim_customer").select("customer_id").distinct()
    orphan_count = df_target.join(dim_customer_ids, "customer_id", "left_anti").count()
    ri_status = "PASS" if orphan_count == 0 else "FAIL"
    dq_checks.append({"rule": "RI: customer_id → dim_customer", "result": ri_status, "detail": f"Orphaned rows={orphan_count}"})
except Exception as e:
    dq_checks.append({"rule": "RI: customer_id", "result": "SKIPPED", "detail": str(e)})

# Value range: order_amount > 0, quantity > 0
try:
    neg_amount = df_target.filter(col("order_amount") <= 0).count()
    neg_qty = df_target.filter(col("quantity") <= 0).count()
    range_status = "PASS" if neg_amount == 0 and neg_qty == 0 else "FAIL"
    dq_checks.append({
        "rule": "Value Range: amount>0, qty>0",
        "result": range_status,
        "detail": f"Negative amounts={neg_amount}, Negative qty={neg_qty}"
    })
except Exception as e:
    dq_checks.append({"rule": "Value Range", "result": "SKIPPED", "detail": str(e)})

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
        df_target.select("order_id")
        .orderBy("order_id")
        .limit(100)
        .collect()
    )
    key_list = [row["order_id"] for row in sample_keys]

    common_cols = [c for c in checksum_columns if c in df_source.columns and c in df_target.columns]
    df_source_sample = df_source.filter(col("order_id").isin(key_list)).select(common_cols)
    df_target_sample = df_target.filter(col("order_id").isin(key_list)).select(common_cols)

    diff = df_source_sample.subtract(df_target_sample)
    diff_count = diff.count()

    if diff_count == 0:
        sample_result = "PASS"
        sample_detail = "100 sampled records match"
    else:
        sample_result = "FAIL"
        sample_detail = f"{diff_count} mismatched records in sample of 100"
        diff.show(10, truncate=False)
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
        "Append mode: target row count may exceed source if prior batches exist",
        "order_amount floating-point tolerance: 0.01"
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
