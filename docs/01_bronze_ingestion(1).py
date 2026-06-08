# Databricks notebook source
# ============================================================
# OLIST PROJECT
# BRONZE LAYER - MULTI-TABLE INGESTION
# Source : ADLS Gen2 raw container
# Target : Unity Catalog Bronze tables
# ============================================================

from pyspark.sql.functions import current_timestamp

# Base path in ADLS Gen2
base_path = "abfss://raw@stinstacartdatalake.dfs.core.windows.net/olist"

# Mapping between source folders and target Bronze tables
tables = {
    "orders": "orders",
    "customers": "customers",
    "products": "products",
    "order_items": "order_items",
    "payments": "payments",
    "reviews": "reviews",
    "sellers": "sellers",
    "geolocation": "geolocation",
    "category_translation": "category_translation"
}


# Loop through all source folders and create Bronze Delta tables
for folder_name, table_name in tables.items():

    source_path = f"{base_path}/{folder_name}/*.csv"
    target_table = f"olist.bronze.{table_name}"

    print(f"Processing {source_path} -> {target_table}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"{target_table} created with {df.count()} rows")