# Databricks notebook source
from pyspark.sql.functions import col, upper, trim

# ============================================================
# OLIST PROJECT
# SILVER LAYER - PRODUCTS
# Source : olist.bronze.products
# Target : olist.silver.products
# ============================================================

# Read Bronze products table
df_products = spark.table("olist.bronze.products")

# Clean and standardize products data
df_products_silver = (
    df_products

    # Keep only valid product identifiers
    .filter(col("product_id").isNotNull())

    # Standardize product category name
    .withColumn(
        "product_category_name",
        upper(trim(col("product_category_name")))
    )

    # Remove duplicate product records
    .dropDuplicates(["product_id"])
)

# Write Silver products table
(
    df_products_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.products_silver")
)

print("Silver products table created successfully.")
print("Rows:", df_products_silver.count())