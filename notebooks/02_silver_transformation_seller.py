# Databricks notebook source
from pyspark.sql.functions import col, upper, trim

# ============================================================
# OLIST PROJECT
# SILVER LAYER - SELLERS
# Source : olist.bronze.sellers
# Target : olist.silver.sellers
# ============================================================

df_sellers = spark.table("olist.bronze.sellers")

df_sellers_silver = (
    df_sellers

    .filter(col("seller_id").isNotNull())

    .withColumn(
        "seller_city",
        upper(trim(col("seller_city")))
    )

    .withColumn(
        "seller_state",
        upper(trim(col("seller_state")))
    )

    .dropDuplicates(["seller_id"])
)

(
    df_sellers_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.sellers")
)

print("Silver sellers table created successfully.")
print("Rows:", df_sellers_silver.count())