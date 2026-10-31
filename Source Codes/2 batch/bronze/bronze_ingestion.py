# Databricks notebook source
# MAGIC %md
# MAGIC Use Auto Loader to ingest raw batch
# MAGIC files into Bronze Delta tables.
# MAGIC ==

# COMMAND ----------

CATALOG = "ecommerce_lakehouse" 
BUCKET = "s3://ecommerce-lakehouse-1482"

sources = { "customers": "csv", "products": "json", "orders": "parquet","payments": "csv" }

for name, fmt in sources.items():
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{BUCKET}/schemas/{name}")
        .option("rescuedDataColumn", "_rescued_data")
    )

    if fmt == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
        )

    stream = reader.load(f"{BUCKET}/raw/batch/{name}")

    (
        stream.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            f"{BUCKET}/checkpoints/bronze/{name}"
        )
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.bronze.{name}")
    )

    print(f"Bronze ingestion completed: {name}")


# COMMAND ----------

