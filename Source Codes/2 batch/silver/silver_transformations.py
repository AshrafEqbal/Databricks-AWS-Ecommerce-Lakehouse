# Databricks notebook source
# MAGIC %md
# MAGIC Cast types, remove duplicate
# MAGIC business keys, validate records, and write valid and quarantined records
# MAGIC to Silver Delta tables.
# MAGIC ==

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"

def safe_cast(df, mapping):
    for column_name, data_type in mapping.items():
        df = df.withColumn(
            column_name, F.expr(f"try_cast({column_name} AS {data_type})")
        )
    return df

def write_silver_and_quarantine(df, valid_condition, key, entity):
    # Keep one record per business key.
    deduplicated = df.dropDuplicates([key])

    valid = deduplicated.filter(valid_condition)
    invalid = deduplicated.filter(~valid_condition)

    valid.write.mode("overwrite").format("delta").saveAsTable(
        f"{CATALOG}.silver.{entity}"
    )
    invalid.write.mode("overwrite").format("delta").saveAsTable(
        f"{CATALOG}.silver.quarantine_{entity}"
    )

    print(
        f"{entity}: valid={valid.count()}, "
        f"quarantined={invalid.count()}"
    )


# COMMAND ----------

# CUSTOMERS

df = spark.table(f"{CATALOG}.bronze.customers")
df = safe_cast(df, {"customer_id": "INT"})

write_silver_and_quarantine(
    df,
    F.col("customer_id").isNotNull()
    & F.col("customer_name").isNotNull()
    & F.col("city").isNotNull(),
    "customer_id",
    "customers",
)

# PRODUCTS

df = spark.table(f"{CATALOG}.bronze.products")
df = safe_cast(df, {"product_id": "INT", "price": "DOUBLE", "stock": "INT"})

write_silver_and_quarantine(
    df,
    F.col("product_id").isNotNull()
    & F.col("product_name").isNotNull()
    & (F.col("price") > 0)
    & (F.col("stock") >= 0),
    "product_id",
    "products",
)

# ORDERS

df = spark.table(f"{CATALOG}.bronze.orders")
df = safe_cast(
    df,
    {
        "order_id": "INT",
        "customer_id": "INT",
        "product_id": "INT",
        "quantity": "INT",
        "amount": "DOUBLE",
    },
).withColumn("order_timestamp", F.to_timestamp("order_timestamp"))

write_silver_and_quarantine(
    df,
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("amount") >= 0)
    & F.col("order_timestamp").isNotNull(),
    "order_id",
    "orders",
)

# PAYMENTS

df = spark.table(f"{CATALOG}.bronze.payments")
df = safe_cast(df, {"payment_id": "INT", "order_id": "INT", "amount": "DOUBLE"})

write_silver_and_quarantine(
    df,
    F.col("payment_id").isNotNull()
    & F.col("order_id").isNotNull()
    & (F.col("amount") >= 0)
    & F.col("payment_status").isNotNull(),
    "payment_id",
    "payments",
)


# COMMAND ----------

