# Validation Template — Migrated Mapping Verification
# Notebook: VAL_<TARGET_TABLE>
# Validates: NB_<MAPPING_NAME>
# Source: <oracle_schema.source_table>
# Target: <lakehouse.target_table>

# %% Cell 1: Configuration
import json
from pyspark.sql.functions import *

mapping_name = "<MAPPING_NAME>"
source_table = "<oracle_schema.source_table>"
target_table = "<lakehouse.target_table>"
key_columns = []       # Primary/business key columns for comparison
metric_columns = []    # Numeric columns to validate aggregates
tolerance = 0.01       # Floating point tolerance

# JDBC config for Oracle source comparison
# jdbc_url = "jdbc:oracle:thin:@<host>:<port>/<service>"
# jdbc_properties = {"user": "<user>", "password": "<password>", "driver": "oracle.jdbc.driver.OracleDriver"}

results = {}

# %% Cell 2: Row Count Validation
# df_source = spark.read.jdbc(url=jdbc_url, table=source_table, properties=jdbc_properties)
# source_count = df_source.count()

df_target = spark.table(target_table)
target_count = df_target.count()

# row_count_pass = source_count == target_count
# results["row_count"] = {"status": "PASS" if row_count_pass else "FAIL", "source": source_count, "target": target_count}
results["row_count"] = {"status": "CHECK", "target": target_count}
print(f"Row Count — Target: {target_count}")

# %% Cell 3: Column Checksum Validation
if key_columns:
    checksum_df = (
        df_target
        .withColumn("row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in key_columns])))
        .selectExpr("sum(cast(conv(substring(row_hash, 1, 8), 16, 10) as bigint)) as checksum")
    )
    target_checksum = checksum_df.collect()[0]["checksum"]
    results["checksum"] = {"status": "CHECK", "target_checksum": target_checksum}
    print(f"Target Checksum: {target_checksum}")

# %% Cell 4: Aggregate Validation
if metric_columns:
    agg_exprs = []
    for mc in metric_columns:
        agg_exprs.extend([
            sum(mc).alias(f"sum_{mc}"),
            avg(mc).alias(f"avg_{mc}"),
            min(mc).alias(f"min_{mc}"),
            max(mc).alias(f"max_{mc}")
        ])
    target_agg = df_target.agg(*agg_exprs).collect()[0]
    results["aggregates"] = {col_name: target_agg[col_name] for col_name in target_agg.asDict()}
    print("Aggregate Results:")
    for k, v in results["aggregates"].items():
        print(f"  {k}: {v}")

# %% Cell 5: NULL / Duplicate Checks
if key_columns:
    # Check for unexpected NULLs in key columns
    null_counts = {}
    for kc in key_columns:
        nc = df_target.filter(col(kc).isNull()).count()
        null_counts[kc] = nc
    results["null_check"] = null_counts
    print(f"NULL counts in key columns: {null_counts}")

    # Check for duplicate keys
    dup_count = df_target.groupBy(key_columns).count().filter(col("count") > 1).count()
    results["duplicate_check"] = {"status": "PASS" if dup_count == 0 else "FAIL", "duplicate_keys": dup_count}
    print(f"Duplicate key rows: {dup_count}")

# %% Cell 6: Summary Report
overall = all(
    v.get("status") == "PASS"
    for v in results.values()
    if isinstance(v, dict) and "status" in v
)

results["mapping"] = mapping_name
results["overall"] = "PASS" if overall else "REVIEW"

print("\n=== VALIDATION SUMMARY ===")
print(json.dumps(results, indent=2, default=str))
