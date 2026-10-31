# Databricks notebook source
CATALOG = "ecommerce_lakehouse"
topics = ["orders", "payments", "clickstream"]

for topic in topics:
    bronze = spark.table(f"{CATALOG}.bronze.streaming_{topic}").count()
    silver = spark.table(f"{CATALOG}.silver.streaming_{topic}").count()
    quarantine = spark.table(
        f"{CATALOG}.silver.streaming_quarantine_{topic}"
    ).count()

    print(
        f"{topic}: Bronze={bronze}, Silver={silver}, "
        f"Quarantine={quarantine}"
    )

    if silver + quarantine > bronze:
        raise Exception(f"Unexpected excess rows for {topic}")

for name in [
    "streaming_daily_sales",
    "streaming_daily_payments",
    "streaming_daily_clickstream"
]:
    table = f"{CATALOG}.gold.{name}"
    print(f"{table}: {spark.table(table).count()} rows")

print("STREAMING PIPELINE VALIDATION COMPLETED")


# COMMAND ----------

