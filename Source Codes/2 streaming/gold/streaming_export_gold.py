# Databricks notebook source
CATALOG = "ecommerce_lakehouse"
BASE = "s3://ecommerce-lakehouse-1482/exports/gold/streaming"

tables = [
    "streaming_daily_sales",
    "streaming_daily_payments",
    "streaming_daily_clickstream"
]

for name in tables:
    (
        spark.table(f"{CATALOG}.gold.{name}")
        .write.format("parquet")
        .mode("overwrite")
        .save(f"{BASE}/{name}/")
    )
    print(f"Exported {name}")

print("STREAMING GOLD EXPORT COMPLETED")

# COMMAND ----------

