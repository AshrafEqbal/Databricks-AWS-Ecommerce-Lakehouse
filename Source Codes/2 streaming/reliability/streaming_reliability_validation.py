# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "ecommerce_lakehouse"
TOPICS = ["orders", "payments", "clickstream"]

failures = []

for topic in TOPICS:

    bronze = spark.table(
        f"{CATALOG}.bronze.streaming_{topic}"
    )

    silver = spark.table(
        f"{CATALOG}.silver.streaming_{topic}"
    )

    quarantine = spark.table(
        f"{CATALOG}.silver.streaming_quarantine_{topic}"
    )

    print(f"\n========== {topic.upper()} ==========")

    # 1. Bronze must contain all three test event types.
    test_bronze = bronze.filter(
        F.col("event_id").startswith("RELTEST-")
    )

    test_count = test_bronze.count()

    duplicate_bronze = (
        test_bronze.groupBy("event_id")
        .count()
        .filter(F.col("count") == 2)
        .count()
    )

    late_bronze = test_bronze.filter(
        F.col("event_id").contains("-LATE-")
    ).count()

    recovery_bronze = test_bronze.filter(
        F.col("event_id").contains("-RECOVERY-")
    ).count()

    print("Reliability events in Bronze:", test_count)
    print("Duplicate event groups in Bronze:", duplicate_bronze)
    print("Late events in Bronze:", late_bronze)
    print("Recovery events in Bronze:", recovery_bronze)

    if test_count < 4:
        failures.append(f"{topic}: missing Bronze test events")

    if duplicate_bronze < 1:
        failures.append(f"{topic}: duplicate pair missing in Bronze")

    if late_bronze < 1:
        failures.append(f"{topic}: late event missing in Bronze")

    if recovery_bronze < 1:
        failures.append(f"{topic}: recovery event missing in Bronze")

    # 2. Silver should not contain duplicate event IDs.
    duplicate_silver = (
        silver.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_ids = silver.filter(
        F.col("event_id").isNull()
    ).count()

    print("Duplicate Silver event groups:", duplicate_silver)
    print("Null Silver event IDs:", null_ids)

    if duplicate_silver > 0:
        failures.append(f"{topic}: duplicate Silver event IDs")

    if null_ids > 0:
        failures.append(f"{topic}: null Silver event IDs")

    # 3. Check duplicate test event was deduplicated.
    duplicate_id = (
        test_bronze
        .filter(F.col("event_id").contains("-DUP-"))
        .select("event_id")
        .first()
    )

    if duplicate_id:
        duplicate_silver_count = silver.filter(
            F.col("event_id") == duplicate_id["event_id"]
        ).count()

        print("Duplicate test event count in Silver:",
              duplicate_silver_count)

        if duplicate_silver_count > 1:
            failures.append(f"{topic}: duplicate test event not deduplicated")

    # 4. Check recovery event reached Silver.
    recovery_count = silver.filter(
        F.col("event_id").contains("-RECOVERY-")
    ).count()

    print("Recovery events in Silver:", recovery_count)

    if recovery_count < 1:
        failures.append(f"{topic}: recovery event missing from Silver")

    # 5. Report late event behavior.
    late_silver = silver.filter(
        F.col("event_id").contains("-LATE-")
    ).count()

    print("Late events in Silver:", late_silver)
    print("Quarantine rows:", quarantine.count())

    test_bronze.select(
        "event_id", "event_timestamp"
    ).orderBy("event_id").show(20, truncate=False)

if failures:
    print("\nRELIABILITY TEST FAILED")
    for failure in failures:
        print(failure)
    raise Exception("Reliability validation failed")

print("\nALL RELIABILITY TESTS PASSED")

# COMMAND ----------

