# Databricks notebook source
# MAGIC %md
# MAGIC Create S3 folders and write the initial
# MAGIC sample source datasets. Run once for initial setup only.
# MAGIC ==

# COMMAND ----------

from pyspark.sql import functions as F

BUCKET = "s3://ecommerce-lakehouse-1482"


# COMMAND ----------

folders = [ "raw/streaming/orders/", "raw/streaming/payments/",
"raw/streaming/clickstream/", "raw/batch/customers/",
"raw/batch/products/", "raw/batch/orders/", "raw/batch/payments/",
"bronze/", "silver/", "gold/", "checkpoints/", "schemas/",
"exports/gold/"]

for folder in folders: dbutils.fs.mkdirs(f"{BUCKET}/{folder}")


# COMMAND ----------

customers = [ (101, "Aarav", "Delhi", "2025-01-10", "Gold"), (102,
"Priya", "Mumbai", "2025-02-15", "Silver"), (103, "Rahul", "Bengaluru",
"2025-03-20", "Gold"), (104, "Ananya", "Hyderabad", "2025-04-05",
"Bronze"), (105, "Kabir", "Pune", "2025-05-12", "Silver")]

products = [ (201, "Laptop", "Electronics", 65000.0, 20), (202,
"Headphones", "Electronics", 2500.0, 100), (203, "Chair", "Furniture",
4500.0, 35), (204, "Desk", "Furniture", 8000.0, 15), (205, "Phone",
"Electronics", 35000.0, 50)]

orders = [ (1001, 101, 201, 1, 65000.0, "COMPLETED",
"2025-06-01T10:15:00"), (1002, 102, 202, 2, 5000.0, "COMPLETED",
"2025-06-01T11:30:00"), (1003, 103, 205, 1, 35000.0, "PENDING",
"2025-06-02T09:00:00"), (1004, 104, 203, 2, 9000.0, "COMPLETED",
"2025-06-02T12:45:00"), (1005, 105, 204, 1, 8000.0, "CANCELLED",
"2025-06-03T14:20:00")]

payments = [ (5001, 1001, 65000.0, "CARD", "SUCCESS"), (5002, 1002,
5000.0, "UPI", "SUCCESS"), (5003, 1003, 35000.0, "UPI", "PENDING"),
(5004, 1004, 9000.0, "CARD", "SUCCESS"), (5005, 1005, 8000.0,
"NET_BANKING", "REFUNDED")]


# COMMAND ----------

spark.createDataFrame( customers, ["customer_id", "customer_name",
"city", "signup_date", "segment"]
).write.mode("overwrite").option("header", True).csv(
f"{BUCKET}/raw/batch/customers" )

spark.createDataFrame( payments, ["payment_id", "order_id", "amount",
"payment_method", "payment_status"]
).write.mode("overwrite").option("header", True).csv(
f"{BUCKET}/raw/batch/payments" )


# COMMAND ----------

spark.createDataFrame( products, ["product_id", "product_name",
"category", "price", "stock"] ).write.mode("overwrite").json(
f"{BUCKET}/raw/batch/products" )


# COMMAND ----------

spark.createDataFrame( orders, ["order_id", "customer_id", "product_id",
"quantity", "amount", "status", "order_timestamp"]
).write.mode("overwrite").parquet( f"{BUCKET}/raw/batch/orders" )

print("S3 folders and sample source datasets created.")


# COMMAND ----------

