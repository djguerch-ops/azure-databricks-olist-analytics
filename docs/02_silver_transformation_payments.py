# Databricks notebook source
from pyspark.sql.functions import col, upper, trim

# ============================================================
# OLIST PROJECT
# SILVER LAYER - payments
# Source : olist.bronze.payments
# Target : olist.silver.payments
# ============================================================

# Read Bronze payments table
df_payments = spark.table("olist.bronze.payments")

# Clean and standardize payments data
df_payments_silver = (
    df_payments

    # Keep only valid payments identifiers
    .filter(col("order_id").isNotNull())
   

    # delete price < 0
    .filter(col("payment_value") >= 0)

    # Remove duplicate payments records
    .dropDuplicates(["order_id", "payment_sequential"])
    # trim col 
    .withColumn(
    "payment_type",
    upper(trim(col("payment_type")))
)
)

# Write Silver payments table
(
    df_payments_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.payments")
)

print("Silver payments table created successfully.")
print("Rows:", df_payments_silver.count())