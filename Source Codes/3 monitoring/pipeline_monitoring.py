# Databricks notebook source
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, LongType
)
import uuid

CATALOG = "ecommerce_lakehouse"
MONITOR_TABLE = f"{CATALOG}.gold.pipeline_monitoring"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

run_id = str(uuid.uuid4())
recorded_at = datetime.utcnow()

schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
    StructField("layer", StringType(), False),
    StructField("entity", StringType(), False),
    StructField("row_count", LongType(), True),
    StructField("null_event_ids", LongType(), True),
    StructField("duplicate_event_ids", LongType(), True),
    StructField("quarantine_count", LongType(), True),
    StructField("latest_event_timestamp", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("details", StringType(), True)
])

records = []
failures = []

def exists(table):
    return spark.catalog.tableExists(table)

def record(
    layer,
    entity,
    row_count=None,
    null_ids=None,
    duplicate_ids=None,
    quarantine_count=None,
    latest_timestamp=None,
    status="PASS",
    details=""
):
    records.append((
        run_id,
        recorded_at,
        layer,
        entity,
        row_count,
        null_ids,
        duplicate_ids,
        quarantine_count,
        latest_timestamp,
        status,
        details
    ))

# Batch tables
batch_tables = [
    ("bronze", "customers"),
    ("bronze", "products"),
    ("bronze", "orders"),
    ("bronze", "payments"),
    ("silver", "customers"),
    ("silver", "products"),
    ("silver", "orders"),
    ("silver", "payments"),
    ("gold", "daily_sales"),
    ("gold", "product_performance"),
    ("gold", "customer_value"),
    ("gold", "dim_customer_scd2")
]

for layer, name in batch_tables:

    table = f"{CATALOG}.{layer}.{name}"

    if exists(table):
        count = spark.table(table).count()
        record(layer, name, row_count=count)
    else:
        record(
            layer, name,
            status="FAIL",
            details=f"Missing table: {table}"
        )
        failures.append(table)

# Streaming tables
topics = ["orders", "payments", "clickstream"]

for topic in topics:

    bronze_table = (
        f"{CATALOG}.bronze.streaming_{topic}"
    )

    silver_table = (
        f"{CATALOG}.silver.streaming_{topic}"
    )

    quarantine_table = (
        f"{CATALOG}.silver.streaming_quarantine_{topic}"
    )

    if not exists(bronze_table):
        record(
            "bronze_streaming", topic,
            status="FAIL",
            details="Bronze table missing"
        )
        failures.append(bronze_table)
        continue

    bronze_count = spark.table(bronze_table).count()

    record(
        "bronze_streaming",
        topic,
        row_count=bronze_count
    )

    if not exists(silver_table):
        record(
            "silver_streaming", topic,
            status="FAIL",
            details="Silver table missing"
        )
        failures.append(silver_table)
        continue

    silver = spark.table(silver_table)

    silver_count = silver.count()

    null_ids = silver.filter(
        F.col("event_id").isNull()
    ).count()

    duplicate_ids = (
        silver.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    latest_timestamp = silver.agg(
        F.max("event_timestamp").alias("latest")
    ).first()["latest"]

    quarantine_count = (
        spark.table(quarantine_table).count()
        if exists(quarantine_table)
        else None
    )

    status = "PASS"
    details = "Streaming quality checks passed"

    if null_ids > 0 or duplicate_ids > 0:
        status = "FAIL"
        details = "Null or duplicate event IDs detected"
        failures.append(silver_table)

    record(
        "silver_streaming",
        topic,
        row_count=silver_count,
        null_ids=null_ids,
        duplicate_ids=duplicate_ids,
        quarantine_count=quarantine_count,
        latest_timestamp=latest_timestamp,
        status=status,
        details=details
    )

# Streaming Gold tables
gold_tables = [
    "streaming_daily_sales",
    "streaming_daily_payments",
    "streaming_daily_clickstream"
]

for name in gold_tables:

    table = f"{CATALOG}.gold.{name}"

    if exists(table):
        record(
            "gold_streaming",
            name,
            row_count=spark.table(table).count()
        )
    else:
        record(
            "gold_streaming",
            name,
            status="FAIL",
            details=f"Missing table: {table}"
        )
        failures.append(table)

# Persist metrics
metrics_df = spark.createDataFrame(
    records,
    schema=schema
)

(
    metrics_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(MONITOR_TABLE)
)

print("Monitoring run:", run_id)

display(
    spark.table(MONITOR_TABLE)
    .filter(F.col("run_id") == run_id)
    .orderBy("layer", "entity")
)

if failures:
    raise Exception(
        "PIPELINE MONITORING FAILED: "
        + ", ".join(failures)
    )

print("PIPELINE MONITORING PASSED")

# COMMAND ----------

# DBTITLE 1,Latest monitoring run
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.gold.pipeline_monitoring
# MAGIC WHERE run_id = (
# MAGIC     SELECT run_id
# MAGIC     FROM ecommerce_lakehouse.gold.pipeline_monitoring
# MAGIC     ORDER BY recorded_at DESC
# MAGIC     LIMIT 1
# MAGIC )
# MAGIC ORDER BY layer, entity;

# COMMAND ----------

# DBTITLE 1,Failed checks
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce_lakehouse.gold.pipeline_monitoring
# MAGIC WHERE status = 'FAIL'
# MAGIC ORDER BY recorded_at DESC;

# COMMAND ----------

# DBTITLE 1,Silver streaming quality
# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC     recorded_at,
# MAGIC     entity,
# MAGIC     row_count,
# MAGIC     null_event_ids,
# MAGIC     duplicate_event_ids,
# MAGIC     quarantine_count,
# MAGIC     latest_event_timestamp,
# MAGIC     status
# MAGIC FROM ecommerce_lakehouse.gold.pipeline_monitoring
# MAGIC WHERE layer = 'silver_streaming'
# MAGIC ORDER BY recorded_at DESC;

# COMMAND ----------

# DBTITLE 1,Row counts by layer
# MAGIC %sql
# MAGIC SELECT
# MAGIC     recorded_at,
# MAGIC     layer,
# MAGIC     entity,
# MAGIC     row_count
# MAGIC FROM ecommerce_lakehouse.gold.pipeline_monitoring
# MAGIC ORDER BY recorded_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _kafka_topic, COUNT(*) AS total_rows,
# MAGIC        MAX(_kafka_offset) AS latest_offset,
# MAGIC        COUNT_IF(_kafka_offset >= 10) AS new_test_rows
# MAGIC FROM ecommerce_lakehouse.bronze.streaming_clickstream
# MAGIC GROUP BY _kafka_topic;

# COMMAND ----------

