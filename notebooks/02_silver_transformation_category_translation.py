# Databricks notebook source
from pyspark.sql.functions import col, upper, trim

# ============================================================
# OLIST PROJECT
# SILVER LAYER - CATEGORY_TRANSLATION
# Source : olist.bronze.category_translation
# Target : olist.silver.category_translation
# ============================================================

df_category = spark.table("olist.bronze.category_translation")

df_category_silver = (
    df_category

    .filter(col("product_category_name").isNotNull())

    .withColumn(
        "product_category_name",
        upper(trim(col("product_category_name")))
    )

    .withColumn(
        "product_category_name_english",
        upper(trim(col("product_category_name_english")))
    )

    .dropDuplicates(["product_category_name"])
)

(
    df_category_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.category_translation")
)

print("Silver category_translation table created successfully.")
print("Rows:", df_category_silver.count())