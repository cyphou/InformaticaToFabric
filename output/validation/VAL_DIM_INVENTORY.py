# Databricks notebook source / Fabric Notebook
# =============================================================================
# Validation Notebook: VAL_DIM_INVENTORY
# Source: Oracle INVENTORY → Target: silver.dim_inventory
# Mapping: M_UPSERT_INVENTORY (Complex: Update Strategy → Delta MERGE)
# Mode: MERGE (insert / update / delete)
# =============================================================================

# COMMAND ----------

# Cell 1: Configuration
import json
from pyspark.sql.functions import (
    md5, concat_ws, col, count, lit, when, isnull,
    sum as spark_sum, min as spark_min, max as spark_max,
    abs as spark_abs
)
from datetime import datetime
from delta.tables import DeltaTable

mapping_name = "M_UPSERT_INVENTORY"
target_table = "silver.dim_inventory"

# Oracle JDBC configuration
source_jdbc_url = "jdbc:oracle:thin:@<host>:1521/<service>"
source_jdbc_properties = {
    "user": "<username>",
    "password": "<password>",  # Use Key Vault in production
    "driver": "oracle.jdbc.driver.OracleDriver"
}
source_table = "INVENTORY"

key_columns = ["inventory_id"]
checksum_columns = ["inventory_id", "product_id", "warehouse_id", "quantity_on_hand", "reorder_point", "last_updated"]
nullable_not_allowed = ["inventory_id", "product_id", "warehouse_id", "quantity_on_hand"]

run_timestamp = datetime.utcnow().isoformat()
results = []

# COMMAND ----------

# Cell 2: Level 1 — Row Count Validation (post-MERGE)
try:
    df_source = spark.read.jdbc(
        url=source_jdbc_url,
        table=f"({source_table})",
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
    row_count_detail = f"Source={source_count}, Target={target_count} (MERGE: post-merge counts match)"
else:
    row_count_result = "FAIL"
    row_count_detail = f"Source={source_count}, Target={target_count}, Delta={target_count - source_count}"

results.append({
    "check": "Row Count (post-MERGE)",
    "level": 1,
    "result": row_count_result,
    "detail": row_count_detail
})
print(f"[Level 1] Row Count: {row_count_result} — {row_count_detail}")

# COMMAND ----------

# Cell 3: MERGE Operation Verification (INSERT / UPDATE / DELETE counts)
try:
    delta_table = DeltaTable.forName(spark, target_table)
    history = delta_table.history(5).filter(col("operation") == "MERGE")

    if history.count() > 0:
        last_merge = history.orderBy(col("timestamp").desc()).first()
        metrics = last_merge["operationMetrics"]

        inserts = int(metrics.get("numTargetRowsInserted", 0))
        updates = int(metrics.get("numTargetRowsUpdated", 0))
        deletes = int(metrics.get("numTargetRowsDeleted", 0))

        merge_detail = f"INSERTs={inserts}, UPDATEs={updates}, DELETEs={deletes}"
        print(f"[MERGE Metrics] {merge_detail}")
        print(f"  Last MERGE timestamp: {last_merge['timestamp']}")

        if source_count is not None and target_count is not None:
            expected_count = source_count
            merge_result = "PASS" if target_count == expected_count else "FAIL"
            merge_detail += f" | Expected post-merge count={expected_count}, Actual={target_count}"
        else:
            merge_result = "PASS"
    else:
        merge_result = "FAIL"
        merge_detail = "No MERGE operation found in Delta history (last 5 operations)"

except Exception as e:
    merge_result = "SKIPPED"
    merge_detail = f"Error reading Delta history — {e}"

results.append({
    "check": "MERGE Verification",
    "level": "MERGE",
    "result": merge_result,
    "detail": merge_detail
})
print(f"[MERGE] Verification: {merge_result} — {merge_detail}")

# COMMAND ----------

# Cell 4: MERGE Correctness — Key Synchronization
try:
    source_keys = set(df_source.select("inventory_id").rdd.flatMap(lambda x: x).collect())
    target_keys = set(df_target.select("inventory_id").rdd.flatMap(lambda x: x).collect())

    extra_in_target = target_keys - source_keys
    missing_from_target = source_keys - target_keys

    if len(extra_in_target) == 0 and len(missing_from_target) == 0:
        key_sync_result = "PASS"
        key_sync_detail = "All source keys present in target, no extra keys in target"
    else:
        key_sync_result = "FAIL"
        key_sync_detail = f"Extra in target: {len(extra_in_target)}, Missing from target: {len(missing_from_target)}"
        if len(extra_in_target) > 0:
            print(f"  Extra keys in target (should have been deleted): {list(extra_in_target)[:10]}")
        if len(missing_from_target) > 0:
            print(f"  Missing keys from target (should have been inserted): {list(missing_from_target)[:10]}")

except Exception as e:
    key_sync_result = "SKIPPED"
    key_sync_detail = f"Error in key synchronization check — {e}"

results.append({
    "check": "Key Synchronization (post-MERGE)",
    "level": "MERGE",
    "result": key_sync_result,
    "detail": key_sync_detail
})
print(f"[MERGE] Key Sync: {key_sync_result} — {key_sync_detail}")

# COMMAND ----------

# Cell 5: Level 2 — Column-Level Checksum
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

# Cell 6: Level 3 — Data Quality Rules
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
    distinct_keys = df_target.select("inventory_id").distinct().count()
    key_status = "PASS" if total == distinct_keys else "FAIL"
    dq_checks.append({"rule": "Key Uniqueness: inventory_id", "result": key_status, "detail": f"Total={total}, Distinct={distinct_keys}"})
except Exception as e:
    dq_checks.append({"rule": "Key Uniqueness", "result": "SKIPPED", "detail": str(e)})

# Value range: quantity_on_hand >= 0, reorder_point >= 0
try:
    neg_qty = df_target.filter(col("quantity_on_hand") < 0).count()
    neg_reorder = df_target.filter(col("reorder_point") < 0).count()
    range_status = "PASS" if neg_qty == 0 and neg_reorder == 0 else "FAIL"
    dq_checks.append({
        "rule": "Value Range: qty>=0, reorder_point>=0",
        "result": range_status,
        "detail": f"Negative qty={neg_qty}, Negative reorder_point={neg_reorder}"
    })
except Exception as e:
    dq_checks.append({"rule": "Value Range", "result": "SKIPPED", "detail": str(e)})

# Delta table format check
try:
    is_delta = DeltaTable.isDeltaTable(spark, target_table)
    delta_status = "PASS" if is_delta else "FAIL"
    dq_checks.append({"rule": "Delta format", "result": delta_status, "detail": f"isDeltaTable={is_delta}"})
except Exception as e:
    dq_checks.append({"rule": "Delta format", "result": "SKIPPED", "detail": str(e)})

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
print(f"[Level 3] Data Quality: {dq_overall}")
for c in dq_checks:
    print(f"  - {c['rule']}: {c['result']} ({c['detail']})")

# COMMAND ----------

# Cell 7: Level 4 — Sample Record Comparison (with timestamp tolerance)
try:
    sample_keys = (
        df_target.select("inventory_id")
        .orderBy("inventory_id")
        .limit(100)
        .collect()
    )
    key_list = [row["inventory_id"] for row in sample_keys]

    # Exclude last_updated from strict comparison (timestamp differences expected)
    compare_cols = [c for c in checksum_columns if c != "last_updated"]

    df_source_sample = df_source.filter(col("inventory_id").isin(key_list)).select(compare_cols)
    df_target_sample = df_target.filter(col("inventory_id").isin(key_list)).select(compare_cols)

    diff = df_source_sample.subtract(df_target_sample)
    diff_count = diff.count()

    if diff_count == 0:
        sample_result = "PASS"
        sample_detail = "100 sampled records match (excluding last_updated timestamp)"
    else:
        sample_result = "FAIL"
        sample_detail = f"{diff_count} mismatched records (excluding last_updated)"
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

# Cell 8: Summary
overall = "PASS" if all(r["result"] == "PASS" for r in results) else (
    "FAIL" if any(r["result"] == "FAIL" for r in results) else "SKIPPED"
)

summary = {
    "mapping": mapping_name,
    "target_table": target_table,
    "write_mode": "MERGE (insert/update/delete)",
    "run_timestamp": run_timestamp,
    "known_differences": [
        "last_updated timestamp excluded from sample comparison — processing time differs",
        "Oracle treats '' as NULL; Spark does not — may cause checksum diffs on string columns",
        "MERGE metrics depend on Delta history retention"
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
    [(r["check"], str(r["level"]), r["result"], r["detail"]) for r in results],
    ["check_name", "level", "result", "detail"]
)
df_summary.show(truncate=False)
