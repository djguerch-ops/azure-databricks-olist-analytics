# Databricks notebook source
from pyspark.sql.functions import col, upper, trim

# ============================================================
# OLIST PROJECT
# SILVER LAYER - order_items
# Source : olist.bronze.order_items
# Target : olist.silver.order_items
# ============================================================

# Read Bronze order_items table
df_order_items = spark.table("olist.bronze.order_items")

# Clean and standardize order_items data
df_order_items_silver = (
    df_order_items

    # Keep only valid product identifiers
    .filter(col("order_item_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("seller_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("shipping_limit_date").isNotNull())

    # delete price < 0
    .filter(col("price") >= 0)

    # delete freight_value < 0
    .filter(col("freight_value") >= 0)

    # Remove duplicate order_items records
    .dropDuplicates(["order_id", "order_item_id"])
)

# Write Silver order_items table
(
    df_order_items_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.order_items")
)

print("Silver order_items table created successfully.")
print("Rows:", df_order_items_silver.count())

# COMMAND ----------

spark.table("olist.bronze.order_items").printSchema()