# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}
# METADATA_END

# CELL 1 - Metadata & Parameters
# Notebook: NB_M_UPSERT_INVENTORY
# Migrated from: Informatica mapping M_UPSERT_INVENTORY
# Complexity: Complex
# Source: STG_INVENTORY (bronze.stg_inventory)
# Target: silver.dim_inventory (Delta MERGE)
# Flow: SQ_STG_INVENTORY -> EXP_CALC_AVAILABLE -> UPD_INVENTORY -> DIM_INVENTORY
# Update Strategy: DD_INSERT / DD_UPDATE / DD_DELETE / DD_REJECT via Delta MERGE

from pyspark.sql.functions import col, coalesce, lit, current_timestamp
from delta.tables import DeltaTable

# CELL 2 - Source Read: SQ_STG_INVENTORY
# --- Source Qualifier: SQ_STG_INVENTORY (from Informatica Source Qualifier) ---

df_source = spark.table("bronze.stg_inventory")

# CELL 3 - Transformation: EXP_CALC_AVAILABLE (from Informatica Expression)
# --- Expression: EXP_CALC_AVAILABLE ---
# QTY_AVAILABLE = QTY_ON_HAND - NVL(QTY_RESERVED, 0)
#   Original (Oracle): NVL(QTY_RESERVED, 0)
#   Converted (Spark): coalesce(col("QTY_RESERVED"), lit(0))

df_calc = (
    df_source
    .withColumn(
        "QTY_AVAILABLE",
        col("QTY_ON_HAND") - coalesce(col("QTY_RESERVED"), lit(0))
    )
    .withColumn("ETL_LOAD_DATE", current_timestamp())
)

# CELL 4 - Transformation: UPD_INVENTORY (from Informatica Update Strategy)
# --- Update Strategy: UPD_INVENTORY ---
# IIF(CHANGE_FLAG = 'I', DD_INSERT,
#   IIF(CHANGE_FLAG = 'U', DD_UPDATE,
#     IIF(CHANGE_FLAG = 'D', DD_DELETE, DD_REJECT)))
#
# Split into streams by CHANGE_FLAG, plus rejected rows for logging.

df_insert = df_calc.filter(col("CHANGE_FLAG") == "I")
df_update = df_calc.filter(col("CHANGE_FLAG") == "U")
df_delete = df_calc.filter(col("CHANGE_FLAG") == "D")
df_reject = df_calc.filter(~col("CHANGE_FLAG").isin("I", "U", "D"))

# Log rejected rows (DD_REJECT)
reject_count = df_reject.count()
if reject_count > 0:
    print(f"WARNING: {reject_count} rows rejected (CHANGE_FLAG not in I/U/D)")
    df_reject.show(20, truncate=False)

# CELL 5 - Target Write: DIM_INVENTORY -> silver.dim_inventory (Delta MERGE)
# --- Target: DIM_INVENTORY -> silver.dim_inventory ---
# MERGE keys: PRODUCT_ID, WAREHOUSE_ID

df_merge_source = df_calc.filter(col("CHANGE_FLAG").isin("I", "U", "D"))

if spark.catalog.tableExists("silver.dim_inventory"):
    delta_target = DeltaTable.forName(spark, "silver.dim_inventory")

    (
        delta_target.alias("t")
        .merge(
            df_merge_source.alias("s"),
            "t.PRODUCT_ID = s.PRODUCT_ID AND t.WAREHOUSE_ID = s.WAREHOUSE_ID"
        )
        # DD_DELETE: matched rows flagged for delete
        .whenMatchedDelete(
            condition="s.CHANGE_FLAG = 'D'"
        )
        # DD_UPDATE: matched rows flagged for update
        .whenMatchedUpdate(
            condition="s.CHANGE_FLAG = 'U'",
            set={
                "QTY_ON_HAND": "s.QTY_ON_HAND",
                "QTY_RESERVED": "s.QTY_RESERVED",
                "QTY_AVAILABLE": "s.QTY_AVAILABLE",
                "LAST_UPDATED": "s.ETL_LOAD_DATE",
            }
        )
        # DD_INSERT: unmatched rows flagged for insert
        .whenNotMatchedInsert(
            condition="s.CHANGE_FLAG = 'I'",
            values={
                "PRODUCT_ID": "s.PRODUCT_ID",
                "WAREHOUSE_ID": "s.WAREHOUSE_ID",
                "QTY_ON_HAND": "s.QTY_ON_HAND",
                "QTY_RESERVED": "s.QTY_RESERVED",
                "QTY_AVAILABLE": "s.QTY_AVAILABLE",
                "LAST_UPDATED": "s.ETL_LOAD_DATE",
            }
        )
        .execute()
    )
else:
    # First run — create the target table from inserts only
    df_insert.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.dim_inventory")
    print("Target table silver.dim_inventory created (first run)")

# CELL 6 - Audit Log
insert_count = df_insert.count()
update_count = df_update.count()
delete_count = df_delete.count()
print(f"Notebook NB_M_UPSERT_INVENTORY completed successfully")
print(f"Rows inserted: {insert_count}")
print(f"Rows updated: {update_count}")
print(f"Rows deleted: {delete_count}")
print(f"Rows rejected: {reject_count}")
