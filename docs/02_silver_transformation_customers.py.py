# Databricks notebook source
from pyspark.sql.functions import col, upper, trim

# ============================================================
# OLIST PROJECT
# SILVER LAYER - CUSTOMERS
# Source : olist.bronze.customers
# Target : olist.silver.customers
# ============================================================

# Read Bronze customers table
df_customers = spark.read.table("olist.bronze.customers")

# Clean and standardize customers data
df_customers_silver = (
    df_customers

    # Keep only valid customer identifiers
    .filter(col("customer_id").isNotNull())
    .filter(col("customer_unique_id").isNotNull())

    # Standardize text fields
    .withColumn("customer_city", trim(upper(col("customer_city"))))
    .withColumn("customer_state", trim(upper(col("customer_state"))))

    # Remove duplicate customer records
    .dropDuplicates(["customer_id"])
)

# Write Silver customers table
(
    df_customers_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.customers")
)

print("Silver customers table created successfully.")
print("Rows:", df_customers_silver.count())