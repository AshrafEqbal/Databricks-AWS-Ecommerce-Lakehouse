# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"
CHECKPOINT = "s3://ecommerce-lakehouse-1482/checkpoints/streaming/silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

def run_topic(topic, required, condition, casts):
    source = f"{CATALOG}.bronze.streaming_{topic}"
    valid_table = f"{CATALOG}.silver.streaming_{topic}"
    bad_table = f"{CATALOG}.silver.streaming_quarantine_{topic}"

    df = spark.readStream.table(source)
    df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    for col_name, dtype in casts.items():
        df = df.withColumn(col_name, F.expr(f"try_cast(`{col_name}` AS {dtype})"))

    df = (
        df.withColumn("_silver_processed_at", F.current_timestamp())
          .withWatermark("event_timestamp", "2 days")
          .dropDuplicates(["event_id"])
    )

    required_condition = F.lit(True)
    for col_name in required:
        required_condition = required_condition & F.col(col_name).isNotNull()

    valid_condition = required_condition & condition

    valid = df.filter(valid_condition)
    bad = (
        df.filter(~valid_condition)
          .withColumn("_quarantine_reason", F.lit("Failed validation"))
    )

    q_valid = (
        valid.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/{topic}/valid")
        .trigger(availableNow=True)
        .toTable(valid_table)
    )

    q_bad = (
        bad.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/{topic}/quarantine")
        .trigger(availableNow=True)
        .toTable(bad_table)
    )

    q_valid.awaitTermination()
    q_bad.awaitTermination()
    print(f"{topic}: Silver and quarantine completed")


run_topic(
    "orders",
    ["event_id", "order_id", "customer_id", "product_id",
     "quantity", "amount", "event_timestamp"],
    (F.col("quantity") > 0) & (F.col("amount") >= 0),
    {"order_id": "BIGINT", "customer_id": "BIGINT",
     "product_id": "BIGINT", "quantity": "INT", "amount": "DOUBLE"}
)

run_topic(
    "payments",
    ["event_id", "payment_id", "order_id", "amount",
     "status", "event_timestamp"],
    F.col("amount") >= 0,
    {"payment_id": "BIGINT", "order_id": "BIGINT", "amount": "DOUBLE"}
)

run_topic(
    "clickstream",
    ["event_id", "customer_id", "session_id", "page", "event_timestamp"],
    F.length(F.trim(F.col("page"))) > 0,
    {"customer_id": "BIGINT"}
)

print("STREAMING SILVER COMPLETED")


# COMMAND ----------

