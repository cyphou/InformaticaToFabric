# Databricks notebook source / Fabric Notebook
# =============================================================================
# Validation Notebook: VAL_FACT_ORDERS
# Source: Oracle.SALES.ORDERS, Oracle.SALES.PRODUCTS → Target: silver.fact_orders
# Mapping: M_LOAD_ORDERS
# =============================================================================

# COMMAND ----------

# Cell 1: Configuration
import json
from pyspark.sql.functions import (
    md5, concat_ws, col, count, lit, when, isnull,
    sum as spark_sum, min as spark_min, max as spark_max
)
from datetime import datetime

mapping_name = "M_LOAD_ORDERS"
target_table = "silver.fact_orders"

# Oracle JDBC — update for your environment
source_jdbc_url = "jdbc:oracle:thin:@<host>:1521/<service>"
source_jdbc_properties = {
    "user": "<username>",
    "password": "<password>",  # Use Key Vault in production
    "driver": "oracle.jdbc.driver.OracleDriver"
}
source_table = "SALES.ORDERS"

key_columns = ['order_id']
checksum_columns = key_columns  # TODO: expand with all validated columns
nullable_not_allowed = key_columns  # TODO: add mandatory columns

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
    print(f"WARNING: Could not read source — {e}")

try:
    df_target = spark.table(target_table)
    target_count = df_target.count()
except Exception as e:
    target_count = None
    print(f"WARNING: Could not read target — {e}")

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
    "check": "Row Count", "level": 1,
    "result": row_count_result, "detail": row_count_detail
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
        checksum_detail = f"Source={source_hash}, Target={target_hash}"
except Exception as e:
    checksum_result = "SKIPPED"
    checksum_detail = f"Error computing checksums — {e}"

results.append({
    "check": "Column Checksum", "level": 2,
    "result": checksum_result, "detail": checksum_detail
})
print(f"[Level 2] Column Checksum: {checksum_result} — {checksum_detail}")
# COMMAND ----------

# Cell 4: Level 3 — Data Quality Rules
dq_checks = []

# NULL checks on mandatory columns
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

# Key uniqueness
try:
    total = df_target.count()
    distinct_keys = df_target.select("order_id").distinct().count()
    key_status = "PASS" if total == distinct_keys else "FAIL"
    dq_checks.append({
        "rule": "Key Uniqueness: order_id",
        "result": key_status,
        "detail": f"Total={total}, Distinct={distinct_keys}"
    })
except Exception as e:
    dq_checks.append({"rule": "Key Uniqueness", "result": "SKIPPED", "detail": str(e)})

dq_overall = "PASS" if all(c["result"] == "PASS" for c in dq_checks) else (
    "SKIPPED" if all(c["result"] == "SKIPPED" for c in dq_checks) else "FAIL"
)
dq_detail = "; ".join([f'{c["rule"]}={c["result"]}' for c in dq_checks])

results.append({
    "check": "Data Quality", "level": 3,
    "result": dq_overall, "detail": dq_detail
})
print(f"[Level 3] Data Quality: {dq_overall}")
for c in dq_checks:
    print(f'  - {c["rule"]}: {c["result"]} ({c["detail"]})')
# COMMAND ----------

# Cell 5: Summary Report
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
print(json.dumps(summary, indent=2, default=str))
print("=" * 60)
print(f"OVERALL RESULT: {overall}")
print("=" * 60)

df_summary = spark.createDataFrame(
    [(r["check"], r["level"], r["result"], r["detail"]) for r in results],
    ["check_name", "level", "result", "detail"]
)
df_summary.show(truncate=False)
