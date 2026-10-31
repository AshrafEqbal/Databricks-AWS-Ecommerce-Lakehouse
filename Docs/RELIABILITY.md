# Reliability, Data Quality, and Pipeline Validation

## 1. Overview

Reliability is an important part of the e-commerce lakehouse implementation.

The project includes data-quality checks, validation scripts, deterministic S3 object naming, Kafka offset management, streaming deduplication, late-arriving event scenarios, recovery testing, and monitoring.

The objective is to make data processing more predictable and to detect failures or unexpected data conditions.

---

## 2. Idempotent Event Delivery

The Kafka consumer uses deterministic S3 object keys based on:

- Kafka topic
- Kafka partition
- Kafka offset

The object path follows this pattern:

```text
raw/streaming/
    {topic}/
        partition={partition}/
            offset={offset}.json
```

This means the same Kafka event offset maps to the same S3 object key.

### Kafka Offset Management

The consumer commits Kafka offsets after a successful S3 upload.

The processing sequence is:

1. Read an event from Kafka.
2. Identify its topic, partition, and offset.
3. Generate the deterministic S3 object key.
4. Upload the event to S3.
5. Commit the Kafka offset after successful upload.

This reduces the risk of losing an event when an upload fails.

Deterministic object naming also supports retry-safe writes for the same event offset.

**Important:** This does not guarantee end-to-end exactly-once processing across Kafka, S3, Auto Loader, Delta, and downstream transformations.

---

## 3. Data Quality and Quarantine

The platform includes separate data-quality processing for batch and streaming datasets.

### Batch Data Quality

The batch implementation checks incoming records and separates valid and invalid records.

The checks include:
- Data validation.
- Data-type conversion.
- Required-field validation.
- Invalid-record identification.
- Quarantine processing.

### Streaming Data Quality

Streaming data-quality processing validates incoming event records and supports invalid-record isolation.

The purpose is to prevent invalid data from silently entering trusted analytical outputs.

### Quarantine

Invalid records are separated from valid records so they can be examined independently.

This provides better visibility into source-data issues without mixing invalid records into business-facing analytics.

---

## 4. Customer SCD Type 2 Validation

The project implements Slowly Changing Dimension Type 2 for customer history.

### Test Scenario

Customer 101 originally had the location:

`Delhi`

The customer location was subsequently changed to:

`Jaipur`

The SCD Type 2 process preserved the previous customer version and created a new current version.

### Expected Behaviour

- Historical customer record remains available.
- Updated customer record is inserted.
- Previous version is no longer marked current.
- New version is marked current.

### Test Result

The implementation produced six records in the customer SCD2 dimension, including the historical version.

This demonstrates historical attribute tracking rather than overwriting the previous customer record.

---

## 5. Streaming Reliability Testing

File:

`Source Codes/streaming/ingestion/reliability_test_producer.py`

This producer generates controlled test events to validate streaming behaviour.

The implemented scenarios include:

### A. Duplicate Event Scenario

Tests how repeated event deliveries are handled by the downstream streaming processing and deduplication logic.

### B. Late-Arriving Event Scenario

Tests the behaviour of events that arrive later than their event-time ordering.

This helps evaluate the configured watermark and deduplication logic.

### C. Recovery Event Scenario

Generates recovery test events to verify that the pipeline can continue processing after the test conditions.

### Test Execution

The development reliability test generated four test events per topic.

The tested topics were:

- Orders
- Payments
- Clickstream

The consumer uploaded the test events to S3, after which the streaming processing and validation steps were executed.

The recorded reliability validation completed with:

`ALL RELIABILITY TESTS PASSED`

This is the result of the implemented development test scenarios, not a guarantee of every possible failure condition.

---

## 6. Pipeline Validation

The project includes separate validation components.

### Batch Validation

File:

`Source Codes/shared/pipeline_validation.py`

Validates the batch processing outputs and data-integrity conditions.

### Streaming Validation

File:

`Source Codes/shared/streaming_pipeline_validation.py`

Validates the streaming processing outputs and expected conditions.

### Reliability Validation

File:

`Source Codes/streaming/reliability/streaming_reliability_validation.py`

Validates the tested streaming reliability scenarios.

---

## 7. Monitoring

### Databricks Pipeline Monitoring

File:

`Source Codes/monitoring/pipeline_monitoring.py`

The monitoring notebook records pipeline metrics in:

`ecommerce_lakehouse.gold.pipeline_monitoring`

### Infrastructure Health Monitoring

File:

`Source Codes/monitoring/health_monitor.py`

The local health-monitoring script checks:

- Kafka broker availability.
- Kafka topic availability.
- Local consumer process status.
- S3 streaming data freshness.
- Databricks job status when the API configuration is available.

This provides visibility into both infrastructure and pipeline execution.

---

## 8. Operational Limitations

The following implementation details are important when interpreting the reliability results.

### Kafka and S3 Delivery

Kafka offsets are committed after successful S3 uploads. Deterministic object keys make retries safer, but this is not a universal exactly-once guarantee.

### Watermark-Based Deduplication

Streaming Silver uses watermark-based processing. Events arriving beyond the configured watermark may not be retained by the deduplication path.

### Auto Loader Execution

The tested Auto Loader workflow uses `availableNow=True`, which processes available files in triggered micro-batches. It is not a continuously running streaming query.

### Quarantine and Valid Outputs

Valid-record and quarantine outputs are separate writes. They are not an atomic transaction across both destinations.

### Recovery

Recovery tests validate the specific scenarios implemented in the project. They should not be interpreted as exhaustive disaster-recovery or production failover testing.

---

## 9. Reliability Summary

The project demonstrates practical reliability engineering concepts:

- Deterministic event object naming.
- Kafka offset management.
- Retry-safe S3 writes for repeated offsets.
- Data validation.
- Invalid-record quarantine.
- Customer history preservation.
- Streaming deduplication.
- Watermark-based event-time processing.
- Late-event testing.
- Recovery-event testing.
- Pipeline validation.
- Infrastructure monitoring.

Together, these components provide a foundation for a more reliable batch and streaming e-commerce data platform.