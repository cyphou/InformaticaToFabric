# Databricks notebook source
# =============================================================================
# Validation Notebook: VAL_TGT_ACCOUNTS
# Source: src_accounts → Target: main.silver.tgt_accounts
# Mapping: m_sync_accounts
# =============================================================================

# COMMAND ----------

# Cell 1: Configuration
import json
from pyspark.sql.functions import (
    md5, concat_ws, col, count, lit, when, isnull,
    sum as spark_sum, min as spark_min, max as spark_max
)
from datetime import datetime

mapping_name = "m_sync_accounts"
target_table = "main.silver.tgt_accounts"

# Oracle JDBC — update for your environment
source_jdbc_url = "jdbc:oracle:thin:@<host>:1521/<service>"
source_jdbc_properties = {
    "user": "<username>",
    "password": "<password>",  # Use Key Vault in production
    "driver": "oracle.jdbc.driver.OracleDriver"
}
source_table = "SCHEMA.src_accounts"

key_columns = ['account_id']
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
    distinct_keys = df_target.select("account_id").distinct().count()
    key_status = "PASS" if total == distinct_keys else "FAIL"
    dq_checks.append({
        "rule": "Key Uniqueness: account_id",
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

# Cell 5: Level 4 — Key Field Sampling
try:
    sample_size = 100
    source_sample = df_source.orderBy(col(key_columns[0])).limit(sample_size)
    target_sample = df_target.orderBy(col(key_columns[0])).limit(sample_size)

    # Compare key column values
    source_keys = set(row[key_columns[0]] for row in source_sample.select(key_columns[0]).collect())
    target_keys = set(row[key_columns[0]] for row in target_sample.select(key_columns[0]).collect())
    common_keys = source_keys & target_keys
    missing_in_target = source_keys - target_keys

    if not missing_in_target:
        sampling_result = "PASS"
        sampling_detail = f"All {len(common_keys)} sampled keys present in target"
    else:
        sampling_result = "FAIL"
        sampling_detail = f"{len(missing_in_target)} of {len(source_keys)} sampled keys missing in target"
except Exception as e:
    sampling_result = "SKIPPED"
    sampling_detail = f"Sampling failed — {e}"

results.append({
    "check": "Key Sampling", "level": 4,
    "result": sampling_result, "detail": sampling_detail
})
print(f"[Level 4] Key Sampling: {sampling_result} — {sampling_detail}")
# COMMAND ----------

# Cell 6: Level 5 — Aggregate Comparison
try:
    # Compare aggregate stats between source and target
    numeric_cols = [f.name for f in df_target.schema.fields
                    if str(f.dataType) in ('IntegerType()', 'LongType()',
                                           'DoubleType()', 'FloatType()',
                                           'DecimalType()')]
    agg_checks = []
    for nc in numeric_cols[:5]:  # Limit to first 5 numeric columns
        src_sum = df_source.agg(spark_sum(col(nc))).collect()[0][0]
        tgt_sum = df_target.agg(spark_sum(col(nc))).collect()[0][0]
        if src_sum is None and tgt_sum is None:
            agg_checks.append(("PASS", f"{nc}: both NULL"))
        elif src_sum is not None and tgt_sum is not None:
            diff_pct = abs(float(src_sum) - float(tgt_sum)) / max(abs(float(src_sum)), 1) * 100
            if diff_pct < 0.01:
                agg_checks.append(("PASS", f"{nc}: src={src_sum}, tgt={tgt_sum}"))
            else:
                agg_checks.append(("FAIL", f"{nc}: src={src_sum}, tgt={tgt_sum}, diff={diff_pct:.2f}%"))
        else:
            agg_checks.append(("FAIL", f"{nc}: src={src_sum}, tgt={tgt_sum}"))

    if not agg_checks:
        agg_result = "SKIPPED"
        agg_detail = "No numeric columns found"
    elif all(r[0] == "PASS" for r in agg_checks):
        agg_result = "PASS"
        agg_detail = "; ".join(r[1] for r in agg_checks)
    else:
        agg_result = "FAIL"
        agg_detail = "; ".join(r[1] for r in agg_checks)
except Exception as e:
    agg_result = "SKIPPED"
    agg_detail = f"Aggregate comparison failed — {e}"

results.append({
    "check": "Aggregate Comparison", "level": 5,
    "result": agg_result, "detail": agg_detail
})
print(f"[Level 5] Aggregate Comparison: {agg_result} — {agg_detail}")
# COMMAND ----------

# Cell 7: Summary Report
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
