# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"
topics = ["orders", "payments", "clickstream"]
failures = []

for topic in topics:
    table = f"{CATALOG}.silver.streaming_{topic}"
    quarantine = f"{CATALOG}.silver.streaming_quarantine_{topic}"

    df = spark.table(table)
    total = df.count()
    null_ids = df.filter(F.col("event_id").isNull()).count()
    duplicate_ids = (
        df.groupBy("event_id").count()
          .filter(F.col("count") > 1).count()
    )
    bad_count = spark.table(quarantine).count()

    print(
        f"{topic}: silver={total}, quarantine={bad_count}, "
        f"null_event_ids={null_ids}, duplicate_event_ids={duplicate_ids}"
    )

    if null_ids or duplicate_ids:
        failures.append(table)

gold_tables = [
    f"{CATALOG}.gold.streaming_daily_sales",
    f"{CATALOG}.gold.streaming_daily_payments",
    f"{CATALOG}.gold.streaming_daily_clickstream"
]

for table in gold_tables:
    print(f"{table}: {spark.table(table).count()} rows")

if failures:
    raise Exception(f"STREAMING QUALITY FAILED: {failures}")

print("STREAMING DATA QUALITY PASSED")


# COMMAND ----------

