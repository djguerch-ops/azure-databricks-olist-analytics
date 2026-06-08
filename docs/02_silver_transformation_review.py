# Databricks notebook source
from pyspark.sql.functions import col

# ============================================================
# OLIST PROJECT
# SILVER LAYER - REVIEWS
# Source : olist.bronze.reviews
# Target : olist.silver.reviews
# ============================================================

# Read Bronze reviews table
df_reviews = spark.table("olist.bronze.reviews")

# Clean and validate reviews data
df_reviews_silver = (
    df_reviews

    # Keep only valid review and order identifiers
    .filter(col("review_id").isNotNull())
    .filter(col("order_id").isNotNull())

    # Keep only valid review scores
    .filter(col("review_score").between(1, 5))

    # Keep valid review dates
    .filter(col("review_creation_date").isNotNull())
    .filter(col("review_answer_timestamp").isNotNull())

    # Remove duplicate reviews
    .dropDuplicates(["review_id"])
)

# Write Silver reviews table
(
    df_reviews_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.silver.reviews")
)

print("Silver reviews table created successfully.")
print("Rows:", df_reviews_silver.count())