# Databricks notebook source
from pyspark.sql import functions as F

BUCKET_PATH = "s3://ecommerce-lakehouse-1482"

TOPICS = ["orders", "payments", "clickstream"]

for topic in TOPICS:

    source_path = f"{BUCKET_PATH}/raw/streaming/{topic}/"
    schema_path = f"{BUCKET_PATH}/schemas/streaming/{topic}/"
    checkpoint_path = f"{BUCKET_PATH}/checkpoints/streaming/{topic}/"

    table_name = f"ecommerce_lakehouse.bronze.streaming_{topic}"

    print(f"Processing: {topic}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.useIncrementalListing", "false")
        .load(source_path)
        .withColumn("_processed_at", F.current_timestamp())
    )

    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )

    query.awaitTermination()

    print(f"Completed: {table_name}")

print("STREAMING BRONZE INGESTION COMPLETED")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS topic, COUNT(*) AS total
# MAGIC FROM ecommerce_lakehouse.bronze.streaming_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'payments', COUNT(*)
# MAGIC FROM ecommerce_lakehouse.bronze.streaming_payments
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'clickstream', COUNT(*)
# MAGIC FROM ecommerce_lakehouse.bronze.streaming_clickstream;

# COMMAND ----------

