# Databricks notebook source
# MAGIC %md
# MAGIC Run quality checks and append results to a
# MAGIC Delta audit table.
# MAGIC ==

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone

CATALOG = "ecommerce_lakehouse"

checks = []


# COMMAND ----------

# Silver row counts and quarantine counts
for name in ["customers", "products", "orders", "payments"]:
    valid_count = spark.table(f"{CATALOG}.silver.{name}").count()
    quarantine_count = spark.table(f"{CATALOG}.silver.quarantine_{name}").count()

    checks.append((f"silver_{name}", "row_count", valid_count))
    checks.append((f"silver_{name}", "quarantine_count", quarantine_count))

# Referential integrity
orders = spark.table(f"{CATALOG}.silver.orders")
customers = spark.table(f"{CATALOG}.silver.customers")
products = spark.table(f"{CATALOG}.silver.products")
payments = spark.table(f"{CATALOG}.silver.payments")

checks.extend([
    (
        "orders",
        "missing_customer",
        orders.join(customers, "customer_id", "left_anti").count()
    ),
    (
        "orders",
        "missing_product",
        orders.join(products, "product_id", "left_anti").count()
    ),
    (
        "payments",
        "missing_order",
        payments.join(orders, "order_id", "left_anti").count()
    )
])

# SCD2 current-version integrity
scd = spark.table(f"{CATALOG}.gold.dim_customer_scd2")

bad_scd = (
    scd.groupBy("customer_id")
    .agg(F.sum(F.col("is_current").cast("int")).alias("current_versions"))
    .filter(F.col("current_versions") != 1)
    .count()
)

checks.append(("dim_customer_scd2", "invalid_current_versions", bad_scd))

# COMMAND ----------

# Build audit result rows
run_time = datetime.now(timezone.utc).replace(tzinfo=None)

result_rows = []

for table_name, check_name, observed_value in checks:
    if check_name == "row_count":
        status = "PASS" if observed_value > 0 else "FAIL"
    else:
        status = "PASS" if observed_value == 0 else "FAIL"

    result_rows.append((
        run_time,
        table_name,
        check_name,
        int(observed_value),
        status
    ))

result_df = spark.createDataFrame(
    result_rows,
    ["run_timestamp", "table_name", "check_name", "observed_value", "status"]
)

TARGET = f"{CATALOG}.gold.data_quality_results"

if not spark.catalog.tableExists(TARGET):
    result_df.write.mode("overwrite").format("delta").saveAsTable(TARGET)
else:
    result_df.write.mode("append").format("delta").saveAsTable(TARGET)

display(result_df.orderBy("status", "table_name"))

# Fail the Workflow task if this run contains failures
failed = result_df.filter(F.col("status") == "FAIL").count()

if failed > 0:
    raise RuntimeError(f"Data quality checks failed: {failed}")

print("All data quality checks passed.")

# COMMAND ----------

