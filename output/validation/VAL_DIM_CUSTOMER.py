# Databricks notebook source / Fabric Notebook
# =============================================================================
# Validation Notebook: VAL_DIM_CUSTOMER
# Source: Oracle CUSTOMERS → Target: silver.dim_customer
# Mapping: M_LOAD_CUSTOMERS (Simple: SQ→EXP→FIL→TGT, overwrite mode)
# =============================================================================

# COMMAND ----------

# Cell 1: Configuration
import json
from pyspark.sql.functions import (
    md5, concat_ws, col, count, lit, when, isnull,
    sum as spark_sum, min as spark_min, max as spark_max,
    abs as spark_abs
)
from pyspark.sql.types import StructType, StringType, IntegerType
from datetime import datetime

mapping_name = "M_LOAD_CUSTOMERS"
target_table = "silver.dim_customer"

# Oracle JDBC configuration — update these for your environment
source_jdbc_url = "jdbc:oracle:thin:@<host>:1521/<service>"
source_jdbc_properties = {
    "user": "<username>",
    "password": "<password>",  # In production, use Key Vault: dbutils.secrets.get(scope, key)
    "driver": "oracle.jdbc.driver.OracleDriver"
}
source_table = "CUSTOMERS"

key_columns = ["customer_id"]
checksum_columns = ["customer_id", "first_name", "last_name", "email", "phone", "status"]
nullable_not_allowed = ["customer_id", "first_name", "last_name"]

run_timestamp = datetime.utcnow().isoformat()
results = []

# COMMAND ----------

# Cell 2: Level 1 — Row Count Validation
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
    row_count_detail = f"Source={source_count}, Target={target_count}"
else:
    row_count_result = "FAIL"
    row_count_detail = f"Source={source_count}, Target={target_count}, Delta={target_count - source_count}"

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
        checksum_detail = f"Source checksum={source_hash}, Target checksum={target_hash}"
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

# Cell 4: Level 3 — Data Quality Rules
dq_checks = []

# 4a: Null checks on mandatory columns
try:
    for col_name in nullable_not_allowed:
        null_count = df_target.filter(isnull(col(col_name))).count()
        status = "PASS" if null_count == 0 else "FAIL"
        dq_checks.append({
            "rule": f"NOT NULL: {col_name}",
            "result": status,
            "detail": f"Null count = {null_count}"
        })
except Exception as e:
    dq_checks.append({"rule": "NOT NULL checks", "result": "SKIPPED", "detail": str(e)})

# 4b: Key uniqueness
try:
    total = df_target.count()
    distinct_keys = df_target.select("customer_id").distinct().count()
    key_status = "PASS" if total == distinct_keys else "FAIL"
    dq_checks.append({
        "rule": "Key Uniqueness: customer_id",
        "result": key_status,
        "detail": f"Total={total}, Distinct keys={distinct_keys}"
    })
except Exception as e:
    dq_checks.append({"rule": "Key Uniqueness", "result": "SKIPPED", "detail": str(e)})

# 4c: Valid status values (business rule)
try:
    valid_statuses = ["ACTIVE", "INACTIVE", "SUSPENDED", "CLOSED"]
    invalid_count = df_target.filter(~col("status").isin(valid_statuses)).count()
    status_check = "PASS" if invalid_count == 0 else "FAIL"
    dq_checks.append({
        "rule": "Valid status values",
        "result": status_check,
        "detail": f"Invalid status count = {invalid_count}"
    })
except Exception as e:
    dq_checks.append({"rule": "Valid status values", "result": "SKIPPED", "detail": str(e)})

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

# Cell 5: Level 4 — Sample Record Comparison
try:
    sample_keys = (
        df_target.select("customer_id")
        .orderBy("customer_id")
        .limit(100)
        .collect()
    )
    key_list = [row["customer_id"] for row in sample_keys]

    df_source_sample = df_source.filter(col("customer_id").isin(key_list)).select(checksum_columns)
    df_target_sample = df_target.filter(col("customer_id").isin(key_list)).select(checksum_columns)

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

# Cell 6: Summary
overall = "PASS" if all(r["result"] == "PASS" for r in results) else (
    "FAIL" if any(r["result"] == "FAIL" for r in results) else "SKIPPED"
)

summary = {
    "mapping": mapping_name,
    "target_table": target_table,
    "run_timestamp": run_timestamp,
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
