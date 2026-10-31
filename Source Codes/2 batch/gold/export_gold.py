# Databricks notebook source
# MAGIC %md
# MAGIC Export Gold Delta tables to S3 as Parquet
# MAGIC ==

# COMMAND ----------

CATALOG = "ecommerce_lakehouse"
BUCKET = "s3://ecommerce-lakehouse-1482"

exports = [
    "daily_sales",
    "product_performance",
    "customer_value",
    "dim_customer_scd2",
]

for name in exports:
    (
        spark.table(f"{CATALOG}.gold.{name}")
        .write
        .mode("overwrite")
        .parquet(f"{BUCKET}/exports/gold/{name}")
    )
    print(f"Exported: {name}")


# COMMAND ----------

