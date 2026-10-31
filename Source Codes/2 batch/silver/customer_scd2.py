# Databricks notebook source
# MAGIC %md
# MAGIC Maintain customer history using Slowly Changing Dimension Type 2. 
# MAGIC The current version is marked is_current=true; previous versions are closed.
# MAGIC ==

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "ecommerce_lakehouse"
TARGET = f"{CATALOG}.gold.dim_customer_scd2"

# Build the source DataFrame with a hash of the tracked attributes
# so we can detect whether a customer's dimension values changed.
source = (
    spark.table(f"{CATALOG}.silver.customers")
    .withColumn(
        "customer_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("customer_name"), F.lit("")),
                F.coalesce(F.col("city"), F.lit("")),
                F.coalesce(F.col("segment"), F.lit("")),
            ),
            256,
        ),
    )
)

# COMMAND ----------

#Initial dimension load
if not spark.catalog.tableExists(TARGET):
    initial = (
        source
        .withColumn("effective_from", F.current_timestamp())
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    initial.write.mode("overwrite").format("delta").saveAsTable(TARGET)

else:
    # Identify changed current records by comparing hashes.
    current = (
        spark.table(TARGET)
        .filter(F.col("is_current") == True)
        .select("customer_id", "customer_hash")
    )

    changed = (
        source.alias("s")
        .join(current.alias("t"), "customer_id", "inner")
        .filter(F.col("s.customer_hash") != F.col("t.customer_hash"))
        .select("s.customer_id")
        .distinct()
    )

    changed_ids = [r["customer_id"] for r in changed.collect()]

    # Close previous versions.
    if changed_ids:
        ids = ",".join(str(int(x)) for x in changed_ids)
        DeltaTable.forName(spark, TARGET).update(
            condition=f"customer_id IN ({ids}) AND is_current = true",
            set={
                "effective_to": "current_timestamp()",
                "is_current": "false",
            },
        )

    # Insert new versions for changed and previously unseen customers.
    current_after_close = (
        spark.table(TARGET)
        .filter(F.col("is_current") == True)
        .select("customer_id")
    )

    to_insert = source.join(
        current_after_close,
        "customer_id",
        "left_anti",
    )

    new_versions = (
        to_insert
        .withColumn("effective_from", F.current_timestamp())
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if new_versions.limit(1).count() > 0:
        new_versions.write.mode("append").format("delta").saveAsTable(TARGET)



# COMMAND ----------

#SCD2 validation
scd = spark.table(TARGET)

# Each customer must have exactly one current version.
invalid_customers = (
    scd.groupBy("customer_id")
    .agg(F.sum(F.col("is_current").cast("int")).alias("current_versions"))
    .filter(F.col("current_versions") != 1)
    .count()
)

assert invalid_customers == 0, "SCD2 validation failed"

print("SCD2 rows:", scd.count())
print("Current:", scd.filter("is_current = true").count())
print("Historical:", scd.filter("is_current = false").count())


# COMMAND ----------

