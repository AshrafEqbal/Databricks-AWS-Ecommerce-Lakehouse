# Databricks notebook source
# MAGIC %md
# MAGIC Final validation of required
# MAGIC tables, S3 exports, and latest quality run.
# MAGIC ==

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"
BUCKET = "s3://ecommerce-lakehouse-1482"

required_tables = {
    "bronze": ["customers", "products", "orders", "payments"],
    "silver": [
        "customers", "products", "orders", "payments",
        "quarantine_customers", "quarantine_products",
        "quarantine_orders", "quarantine_payments"
    ],
    "gold": [
        "daily_sales", "product_performance",
        "customer_value", "dim_customer_scd2",
        "data_quality_results"
    ]
}

failures = []

# COMMAND ----------

# Verify required tables and row counts
for schema, tables in required_tables.items():
    for table in tables:
        full_name = f"{CATALOG}.{schema}.{table}"

        if not spark.catalog.tableExists(full_name):
            failures.append(f"Missing table: {full_name}")
            continue

        count = spark.table(full_name).count()
        print(f"OK | {full_name} | {count} rows")

        if count == 0 and not table.startswith("quarantine_"):
            failures.append(f"Unexpected empty table: {full_name}")

# COMMAND ----------

# Verify S3 exports
for name in [
    "daily_sales",
    "product_performance",
    "customer_value",
    "dim_customer_scd2"
]:
    path = f"{BUCKET}/exports/gold/{name}"

    try:
        count = spark.read.parquet(path).count()
        print(f"OK | S3 export {name} | {count} rows")

        if count == 0:
            failures.append(f"Empty S3 export: {name}")

    except Exception as e:
        failures.append(f"S3 export failed: {name}: {str(e)[:300]}")

# COMMAND ----------

# Check only the latest data-quality run
quality = spark.table(f"{CATALOG}.gold.data_quality_results")

latest_run = quality.agg(F.max("run_timestamp")).first()[0]

latest_quality = quality.filter(
    F.col("run_timestamp") == F.lit(latest_run)
)

failed_checks = latest_quality.filter(
    F.col("status") == "FAIL"
).count()

if failed_checks:
    failures.append(f"Latest data quality failures: {failed_checks}")

# COMMAND ----------

# Final validation
if failures:
    raise RuntimeError("\n".join(failures))

print("PIPELINE VALIDATION PASSED")

# COMMAND ----------

