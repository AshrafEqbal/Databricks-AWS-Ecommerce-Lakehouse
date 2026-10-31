# Databricks notebook source
# MAGIC %md
# MAGIC Build Gold business aggregates from Silver tables.
# MAGIC ==

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"

orders = spark.table(f"{CATALOG}.silver.orders")


# COMMAND ----------

# DAILY SALES

gold_sales = (
    orders
    .filter(F.col("status") == "COMPLETED")
    .withColumn("order_date", F.to_date("order_timestamp"))
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("amount").alias("total_sales")
    )
)

gold_sales.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.gold.daily_sales"
)


# COMMAND ----------

# PRODUCT PERFORMANCE

products = spark.table(f"{CATALOG}.silver.products")

gold_products = (
    orders
    .filter(F.col("status") == "COMPLETED")
    .groupBy("product_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("units_sold"),
        F.sum("amount").alias("total_revenue")
    )
    .join(products, "product_id", "left")
    .select(
        "product_id",
        "product_name",
        "category",
        "total_orders",
        "units_sold",
        "total_revenue"
    )
)

gold_products.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.gold.product_performance"
)


# COMMAND ----------

# CUSTOMER VALUE

customers = spark.table(f"{CATALOG}.silver.customers")

gold_customers = (
    orders
    .filter(F.col("status") == "COMPLETED")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("amount").alias("lifetime_value")
    )
    .join(customers, "customer_id", "left")
)

gold_customers.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.gold.customer_value"
)

print("Gold analytics tables created.")

# COMMAND ----------

