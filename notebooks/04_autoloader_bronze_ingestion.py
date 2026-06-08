from pyspark.sql.functions import current_timestamp, col

source_path = (
    "abfss://raw@stinstacartdatalake.dfs.core.windows.net/"
    "olist_autoloader/orders/"
)

schema_path = (
    "abfss://raw@stinstacartdatalake.dfs.core.windows.net/"
    "_schemas/orders/"
)

checkpoint_path = (
    "abfss://raw@stinstacartdatalake.dfs.core.windows.net/"
    "_checkpoints/orders/"
)

target_table = "olist.bronze.orders_autoloader"

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    .option("inferColumnTypes", "true")
    .load(source_path)
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

query = (
    df_stream.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
