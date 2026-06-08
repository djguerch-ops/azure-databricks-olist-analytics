# Databricks notebook source
from pyspark.sql.functions import col, year, date_format, datediff
from pyspark.sql.functions import current_timestamp

spark.sql(f"use catalog olist")
spark.sql(f"use schema bronze")

# Lecture des tables bronze

#df_orders= spark.read.table("orders")
#df_product= spark.read.table("products")
#df_order_items= spark.read.table("order_items")
#df_order_payment= spark.read.table("order_payments")
#df_order_review= spark.read.table("order_reviews")
#df_sellers= spark.read.table("sellers")
#df_geolocation= spark.read.table("geolocation")
#df_customers= spark.read.table("customers")
#df_category_translation= spark.read.table("category_translation")
#


df_orders_silver = (
    spark.table("olist.bronze.orders")

    # Keep only valid order records
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())

    # Remove duplicate orders
    .dropDuplicates(["order_id"])

    # Add business date fields
    .withColumn("purchase_year", year(col("order_purchase_timestamp")))
    .withColumn("purchase_month", date_format(col("order_purchase_timestamp"), "yyyy-MM"))

    # Calculate delivery delay in days
    .withColumn(
        "delivery_delay_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
    )
)

df_orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("olist.silver.orders")

print("Silver orders table created successfully.")
print("Rows:", df_orders_silver.count())



