# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
CHECKPOINT = "s3://ecommerce-lakehouse-1482/checkpoints/streaming/gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# Daily order sales
orders = (
    spark.readStream.table(f"{SILVER}.streaming_orders")
    .withWatermark("event_timestamp", "2 days")
)

sales = (
    orders.groupBy(F.window("event_timestamp", "1 day").alias("w"))
    .agg(
        F.approx_count_distinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("amount").alias("total_sales")
    )
    .select(
        F.to_date("w.start").alias("sales_date"),
        "total_orders", "total_quantity", "total_sales"
    )
)

q_sales = (
    sales.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT}/daily_sales")
    .trigger(availableNow=True)
    .toTable(f"{GOLD}.streaming_daily_sales")
)

# Daily payment totals by status
payments = (
    spark.readStream.table(f"{SILVER}.streaming_payments")
    .withWatermark("event_timestamp", "2 days")
)

payment_stats = (
    payments.groupBy(
        F.window("event_timestamp", "1 day").alias("w"), "status"
    )
    .agg(
        F.approx_count_distinct("payment_id").alias("total_payments"),
        F.sum("amount").alias("total_payment_amount")
    )
    .select(
        F.to_date("w.start").alias("payment_date"),
        "status", "total_payments", "total_payment_amount"
    )
)

q_payments = (
    payment_stats.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT}/daily_payments")
    .trigger(availableNow=True)
    .toTable(f"{GOLD}.streaming_daily_payments")
)

# Daily page views
clicks = (
    spark.readStream.table(f"{SILVER}.streaming_clickstream")
    .withWatermark("event_timestamp", "2 days")
)

click_stats = (
    clicks.groupBy(
        F.window("event_timestamp", "1 day").alias("w"), "page"
    )
    .agg(
        F.count("*").alias("page_views"),
        F.approx_count_distinct("session_id").alias("unique_sessions"),
        F.approx_count_distinct("customer_id").alias("unique_customers")
    )
    .select(
        F.to_date("w.start").alias("activity_date"),
        "page", "page_views", "unique_sessions", "unique_customers"
    )
)

q_clicks = (
    click_stats.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT}/daily_clickstream")
    .trigger(availableNow=True)
    .toTable(f"{GOLD}.streaming_daily_clickstream")
)

q_sales.awaitTermination()
q_payments.awaitTermination()
q_clicks.awaitTermination()

print("STREAMING GOLD COMPLETED")


# COMMAND ----------

