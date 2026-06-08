# Databricks notebook source
from pyspark.sql.functions import col, countDistinct, sum, avg, round

# ============================================================
# OLIST PROJECT
# GOLD LAYER - SALES ANALYTICS
# Source : olist.silver.*
# Target : olist.gold.sales_analytics
# ============================================================

orders = spark.table("olist.silver.orders")
order_items = spark.table("olist.silver.order_items")
products = spark.table("olist.silver.products")
category = spark.table("olist.silver.category_translation")

# Join business tables
df_sales = (
    order_items
    .join(orders, "order_id", "inner")
    .join(products, "product_id", "left")
    .join(category, "product_category_name", "left")
)

# Aggregate monthly sales KPIs
df_gold_sales = (
    df_sales
    .groupBy(
        "purchase_month",
        "product_category_name_english"
    )
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("product_id").alias("total_products"),
        round(sum("price"), 2).alias("total_revenue"),
        round(sum("freight_value"), 2).alias("total_freight"),
        round(avg("price"), 2).alias("avg_item_price")
    )
    .orderBy("purchase_month", "total_revenue", ascending=False)
)

# Write Gold table
(
    df_gold_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist.gold.sales_analytics")
)

print("Gold sales analytics table created successfully.")
print("Rows:", df_gold_sales.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM olist.gold.sales_analytics
# MAGIC ORDER BY purchase_month, total_revenue DESC
# MAGIC LIMIT 20;