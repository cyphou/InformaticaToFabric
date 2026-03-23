# Fabric notebook source

# METADATA_START
# {"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}}
# METADATA_END

# CELL 1 - Metadata & Parameters
# Notebook: NB_M_LOAD_ORDERS
# Migrated from: Informatica mapping M_LOAD_ORDERS
# Complexity: Complex
# Source: SALES.ORDERS (bronze.orders)
# Targets: silver.fact_orders, gold.agg_orders_by_customer
# Flow: SQ_ORDERS -> LKP_PRODUCTS -> EXP_CALC_TOTAL -> [FACT_ORDERS + AGG_BY_CUSTOMER -> AGG_ORDERS_BY_CUSTOMER]
# SQL Override: NVL->COALESCE, TO_DATE->to_date, $$LOAD_DATE parameter

from pyspark.sql.functions import (
    col, coalesce, lit, to_date, broadcast,
    count, sum as _sum, avg as _avg, current_timestamp
)

# Parameters (from Informatica $$LOAD_DATE / pipeline parameter)
load_date = notebookutils.widgets.get("load_date")

# CELL 2 - Source Read: SQ_ORDERS (with SQL Override)
# --- Source Qualifier: SQ_ORDERS (from Informatica Source Qualifier) ---
# SQL Override applied:
#   Original (Oracle): NVL(DISCOUNT_PCT, 0), TO_DATE($$LOAD_DATE, 'YYYY-MM-DD'), ORDER_STATUS != 'CANCELLED'
#   Converted (Spark): COALESCE(DISCOUNT_PCT, 0), to_date(load_date), ORDER_STATUS != 'CANCELLED'

df_orders = (
    spark.table("bronze.orders")
    .withColumn("DISCOUNT_PCT", coalesce(col("DISCOUNT_PCT"), lit(0)))
    .filter(col("ORDER_DATE") >= to_date(lit(load_date), "yyyy-MM-dd"))
    .filter(col("ORDER_STATUS") != "CANCELLED")
)

# CELL 3 - Transformation: LKP_PRODUCTS (from Informatica Lookup - Connected)
# --- Lookup: LKP_PRODUCTS ---
# Join on PRODUCT_ID, broadcast for performance (small dimension)
# Default values on no match: COALESCE for PRODUCT_NAME and CATEGORY

df_products = spark.table("bronze.products")

df_with_products = (
    df_orders.join(
        broadcast(df_products),
        on="PRODUCT_ID",
        how="left"
    )
    .withColumn("PRODUCT_NAME", coalesce(col("PRODUCT_NAME"), lit("Unknown")))
    .withColumn("CATEGORY", coalesce(col("CATEGORY"), lit("Uncategorized")))
)

# CELL 4 - Transformation: EXP_CALC_TOTAL (from Informatica Expression)
# --- Expression: EXP_CALC_TOTAL ---
# LINE_TOTAL = QUANTITY * UNIT_PRICE * (1 - DISCOUNT_PCT / 100)

df_calc = df_with_products.withColumn(
    "LINE_TOTAL",
    col("QUANTITY") * col("UNIT_PRICE") * (lit(1) - col("DISCOUNT_PCT") / lit(100))
)

# CELL 5 - Target Write: FACT_ORDERS -> silver.fact_orders
# --- Target: FACT_ORDERS -> silver.fact_orders (overwrite) ---

df_fact_orders = df_calc.select(
    "ORDER_ID", "CUSTOMER_ID", "PRODUCT_NAME", "CATEGORY",
    "QUANTITY", "UNIT_PRICE", "LINE_TOTAL",
    "ORDER_DATE", "ORDER_STATUS", "CHANNEL"
)

df_fact_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.fact_orders")

# CELL 6 - Transformation: AGG_BY_CUSTOMER (from Informatica Aggregator)
# --- Aggregator: AGG_BY_CUSTOMER ---
# GROUP BY: CUSTOMER_ID
# Aggregations: COUNT(ORDER_ID), SUM(LINE_TOTAL), AVG(LINE_TOTAL)

df_agg = (
    df_calc
    .groupBy("CUSTOMER_ID")
    .agg(
        count("ORDER_ID").alias("TOTAL_ORDERS"),
        _sum("LINE_TOTAL").alias("TOTAL_REVENUE"),
        _avg("LINE_TOTAL").alias("AVG_ORDER_VALUE")
    )
)

# CELL 7 - Target Write: AGG_ORDERS_BY_CUSTOMER -> gold.agg_orders_by_customer
# --- Target: AGG_ORDERS_BY_CUSTOMER -> gold.agg_orders_by_customer (overwrite) ---

df_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.agg_orders_by_customer")

# CELL 8 - Audit Log
fact_count = df_fact_orders.count()
agg_count = df_agg.count()
print(f"Notebook NB_M_LOAD_ORDERS completed successfully (load_date={load_date})")
print(f"Rows written to silver.fact_orders: {fact_count}")
print(f"Rows written to gold.agg_orders_by_customer: {agg_count}")
