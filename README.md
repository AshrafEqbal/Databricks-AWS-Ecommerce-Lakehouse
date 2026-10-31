# Hybrid Batch & Real-Time E-Commerce Lakehouse Data Platform

An end-to-end enterprise-style data engineering project implementing **Batch Processing and Real-Time Streaming** using AWS, Apache Kafka, Databricks, PySpark, Delta Lake, and the Medallion Architecture.

The platform processes e-commerce data from multiple sources through two independent but complementary pipelines, transforms it into analytics-ready datasets, and makes the results available for business intelligence and reporting.

---

## 1. Project Overview

Modern e-commerce platforms generate data through different channels. Customer information, product catalogues, orders, and payments may arrive as periodic batch files, while customer interactions and transactional events are generated continuously.

This project demonstrates how to build a unified data platform capable of handling both processing patterns.

The platform consists of two major pipelines:

**Batch Data Pipeline**
- Processes periodically uploaded e-commerce datasets.
- Uses Amazon S3 as the batch data landing zone.
- Uses Databricks Auto Loader and Delta Lake for ingestion.
- Transforms data through Bronze, Silver, and Gold layers.
- Implements data quality checks, quarantine handling, and SCD Type 2.
- Produces business-oriented analytical datasets.

**Real-Time Streaming Data Pipeline**
- Generates events using Python producers.
- Uses Apache Kafka for event streaming.
- Uses a Python Kafka consumer to deliver events to Amazon S3.
- Uses Databricks Auto Loader to ingest streaming files.
- Processes data through Bronze, Silver, and Gold layers.
- Implements deduplication, watermark-based processing, late-event testing, and recovery validation.
- Produces streaming analytical datasets.

Both pipelines integrate with shared validation and monitoring capabilities and support downstream analytics through Power BI.

---

## 2. System Architecture

The project follows a hybrid architecture with two separate data ingestion paths and a common Medallion processing approach.

### A. Batch Architecture

**Data Sources → Amazon S3 → Databricks → Bronze → Silver → Gold → S3 Exports → Power BI**

1. E-commerce batch datasets are uploaded to Amazon S3.
2. Databricks Auto Loader ingests the source files.
3. Raw records are stored in Bronze Delta tables.
4. Silver transformations clean, standardise, validate, and transform the data.
5. Invalid records are separated into quarantine datasets.
6. Customer history is maintained using SCD Type 2.
7. Gold transformations generate business-level analytical datasets.
8. Gold outputs are exported to Amazon S3.
9. The resulting datasets are available for downstream reporting.

**Batch source domains:**
- Customers
- Orders
- Payments
- Products

### B. Real-Time Streaming Architecture

**Python Producers → Apache Kafka → Python Consumer → Amazon S3 → Databricks Auto Loader → Bronze → Silver → Gold → S3 Exports**

1. Python producers generate e-commerce events.
2. Events are published to Apache Kafka topics.
3. A Python Kafka consumer reads events from Kafka.
4. The consumer uploads event records to Amazon S3.
5. S3 objects are organised using topic, partition, and offset information.
6. Databricks Auto Loader detects and ingests new files.
7. Streaming records are stored in Bronze Delta tables.
8. Silver transformations perform data cleaning, deduplication, and quality processing.
9. Gold transformations generate streaming analytics.
10. Gold datasets are exported to Amazon S3 for downstream consumption.

**Streaming event domains:**
- Orders
- Payments
- Clickstream

**Important implementation detail:** This project implements Kafka → Python Consumer → Amazon S3 → Databricks Auto Loader. It does not use a direct Kafka-to-Databricks streaming connection.

---

## 3. Medallion Architecture

Both pipelines follow the Bronze, Silver, and Gold Medallion Architecture.

### Bronze Layer — Raw Data Ingestion

Purpose:
- Ingest data from source systems.
- Preserve the incoming records.
- Maintain a structured raw-data layer.
- Support downstream transformations.

Batch:
- Customer, order, payment, and product ingestion.
- Raw data stored in Delta tables.

Streaming:
- Kafka events delivered to S3.
- Auto Loader ingests the raw streaming files.
- Separate Bronze tables for orders, payments, and clickstream.

### Silver Layer — Data Cleaning and Transformation

Purpose:
- Clean and standardise records.
- Apply schema and data-type transformations.
- Validate incoming data.
- Handle duplicates.
- Separate invalid records.
- Prepare trusted datasets for analytics.

Batch processing includes:
- Customer transformations.
- Order transformations.
- Payment transformations.
- Product transformations.
- Customer SCD Type 2 history tracking.
- Data-quality checks and quarantine.

Streaming processing includes:
- Streaming data transformations.
- Deduplication.
- Watermark-based processing.
- Data-quality checks.
- Quarantine handling.
- Late-arriving event scenarios.

### Gold Layer — Business Analytics

Purpose:
- Generate business-ready analytical datasets.
- Aggregate data for reporting.
- Support business intelligence.
- Prepare data for downstream consumption.

**Batch Gold datasets:**

| Dataset | Purpose |
|---|---|
| `daily_sales` | Daily order, quantity, and sales metrics |
| `product_performance` | Product-level performance analytics |
| `customer_value` | Customer-level value analytics |

**Streaming Gold datasets:**

| Dataset | Purpose |
|---|---|
| `streaming_daily_sales` | Sales analytics from streaming orders |
| `streaming_daily_payments` | Payment analytics from streaming events |
| `streaming_daily_clickstream` | Clickstream activity analytics |

---

## 4. Data Engineering Features

### 4.1 Batch Processing

- Amazon S3 batch ingestion.
- Databricks Auto Loader.
- Delta Lake storage.
- PySpark transformations.
- Bronze-to-Silver-to-Gold processing.
- Incremental processing support.
- Data validation.
- Invalid-record quarantine.
- Gold analytics generation.
- S3 Gold exports.

### 4.2 Real-Time Streaming

- Python-based event producers.
- Apache Kafka topics.
- Python Kafka consumer.
- Kafka-to-S3 event delivery.
- Partition- and offset-based S3 object organisation.
- Kafka offset commits after successful S3 uploads.
- Databricks Auto Loader ingestion.
- Streaming Bronze, Silver, and Gold processing.
- Streaming data-quality validation.

### 4.3 Slowly Changing Dimension Type 2

Customer SCD Type 2 processing preserves historical customer information when tracked attributes change.

The implementation was tested by changing customer 101's location from Delhi to Jaipur.

The resulting dimension retained:
- The historical Delhi record.
- The new Jaipur record.
- Historical and current versions of the customer.

The test produced six records in the SCD2 dimension, including the historical version.

### 4.4 Data Quality and Quarantine

The project includes data-quality processing to identify invalid records.

Implemented capabilities include:
- Data validation.
- Data-type conversion.
- Invalid-record detection.
- Valid-record separation.
- Quarantine datasets.
- Pipeline validation.

This prevents invalid records from silently entering trusted analytical datasets.

### 4.5 Idempotency and Reliability

The Kafka consumer uses deterministic S3 object keys based on:

- Kafka topic.
- Kafka partition.
- Kafka offset.

The consumer commits Kafka offsets after a successful S3 upload.

This design supports safe retries through deterministic object naming and reduces duplicate object creation for the same event offset.

### 4.6 Late-Arriving Events

The streaming pipeline includes watermark-based processing and late-arriving event test scenarios.

This demonstrates how event-time processing and deduplication behave when events arrive after their original event time.

### 4.7 Recovery Testing

The reliability testing producer generates test events covering:
- Duplicate events.
- Late-arriving events.
- Recovery events.

The pipeline is subsequently validated to check whether the expected records and transformations are handled correctly.

---

## 5. Databricks Workflow and Orchestration

Databricks is the central data processing and orchestration platform.

The project includes a Databricks multi-task workflow named:

`ecommerce_lakehouse_pipeline`

The workflow contains 13 tasks.

The pipeline is organised into separate ingestion, transformation, quality, validation, export, and monitoring activities.

The implementation uses:
- Databricks Workflows.
- Databricks notebooks exported as Python source files.
- Unity Catalog.
- Delta Lake.
- Auto Loader.
- PySpark.
- S3 external storage access.

The project uses the Unity Catalog:

`ecommerce_lakehouse`

With the following schemas:

- `bronze`
- `silver`
- `gold`

The configured S3 storage integration uses an external location and an IAM-backed storage credential.

---

## 6. Monitoring and Validation

The project includes both Databricks-level pipeline monitoring and a local infrastructure health-monitoring component.

### Databricks Pipeline Monitoring

The monitoring implementation records pipeline metrics in:

`ecommerce_lakehouse.gold.pipeline_monitoring`

It supports monitoring of pipeline execution and validation outcomes.

### Infrastructure Health Monitoring

The Python health-monitoring script checks:

- Kafka broker availability.
- Kafka topic availability.
- Local consumer process status.
- S3 streaming data freshness.
- Databricks job status when the API configuration is provided.

### Validation Components

The project includes:
- Batch pipeline validation.
- Streaming pipeline validation.
- Streaming reliability validation.
- Data-quality validation.
- Pipeline monitoring.

---

## 7. Technology Stack

| Technology | Role |
|---|---|
| Python | Producers, consumers, monitoring, and supporting scripts |
| Apache Kafka | Real-time event streaming |
| Amazon S3 | Raw data storage and Gold exports |
| AWS IAM | Secure access to AWS resources |
| Databricks | Data engineering and orchestration |
| PySpark | Distributed data processing |
| Delta Lake | Reliable table storage |
| Databricks Auto Loader | Incremental file ingestion |
| Unity Catalog | Data governance and organisation |
| Docker Compose | Kafka infrastructure |
| Power BI | Analytics and visualisation |

---

## 8. Repository Structure

The repository is organised to clearly separate the batch and streaming implementations.

```text
kafka_s3_pipeline/
│
├── architecture/
│
├── Dashboard/
│   └── screenshots/
│
├── Source Codes/
│   │
│   ├── batch/
│   │   ├── bronze/
│   │   │   └── bronze_ingestion.py
│   │   │
│   │   ├── silver/
│   │   │   ├── customer_scd2.py
│   │   │   └── silver_transformations.py
│   │   │
│   │   ├── gold/
│   │   │   ├── export_gold.py
│   │   │   └── gold_analytics.py
│   │   │
│   │   └── data_quality/
│   │       └── data_quality.py
│   │
│   ├── data source setup/
│   │   └── source_setup.py
│   │
│   ├── monitoring/
│   │   ├── health_monitor.py
│   │   └── pipeline_monitoring.py
│   │
│   ├── shared/
│   │   ├── pipeline_validation.py
│   │   └── streaming_pipeline_validation.py
│   │
│   └── streaming/
│       │
│       ├── ingestion/
│       │   ├── consumer.py
│       │   ├── event_producer.py
│       │   ├── reliability_test_producer.py
│       │   └── streaming_bronze_ingestion.py
│       │
│       ├── silver/
│       │   └── streaming_silver_transformations.py
│       │
│       ├── gold/
│       │   ├── streaming_export_gold.py
│       │   └── streaming_gold_analytics.py
│       │
│       ├── data_quality/
│       │   └── streaming_data_quality.py
│       │
│       └── reliability/
│           └── streaming_reliability_validation.py
│
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

---

## 9. Implementation Results

The platform was implemented and tested using sample e-commerce data.

### Batch Pipeline Results

| Component | Tested result |
|---|---|
| Bronze customers | 5 records |
| Bronze orders | 5 records |
| Bronze payments | 5 records |
| Bronze products | 5 records |
| Silver valid records | 5 per source |
| Quarantined records | 0 in the successful sample run |
| SCD Type 2 | Historical customer version retained |
| Daily sales | 2 output rows |
| Product performance | 3 output rows |
| Customer value | 3 output rows |

### Streaming Pipeline Results

The reliability test producer generated four test events per topic, covering duplicate, late-arriving, and recovery scenarios.

The consumer uploaded the tested events to S3.

The resulting Bronze streaming tables contained 14 records per topic in the tested run.

The streaming Silver and Gold processing, data-quality checks, pipeline validation, and reliability validation were subsequently executed.

<code>ALL RELIABILITY TESTS PASSED</code>

These figures represent the development test dataset and execution, not production-scale throughput or performance benchmarks.

---

## 10. Setup and Execution

### Prerequisites

- AWS account.
- Amazon S3 bucket.
- Appropriate AWS IAM permissions.
- Databricks workspace.
- Unity Catalog access.
- Docker Desktop.
- Python environment.
- Power BI Desktop (for dashboard development).

### Step 1 — Start Kafka Infrastructure

Start Docker Desktop and execute:

```bash
docker compose up -d
```

Verify the containers:

```bash
docker compose ps
```

### Step 2 — Configure AWS

Configure AWS credentials through an approved credential provider.

Create or configure the required S3 bucket and permissions.

The development implementation used:

- S3 bucket: `ecommerce-lakehouse-1482`
- IAM role: `ecommerce-lakehouse-s3-role`

### Step 3 — Install Python Dependencies

```bash
python -m venv .venv
```

Windows CMD:

```cmd
.venv\Scripts\activate
pip install -r requirements.txt
```

### Step 4 — Configure Databricks

Configure:
- Unity Catalog.
- Storage credentials.
- External location.
- Bronze, Silver, and Gold schemas.
- Required notebook permissions.
- Databricks Workflow tasks and dependencies.

### Step 5 — Execute Batch Pipeline

Run the batch pipeline components in the order configured by the Databricks Workflow:

1. Source setup.
2. Bronze ingestion.
3. Silver transformations.
4. Customer SCD Type 2.
5. Data-quality processing.
6. Gold analytics.
7. Pipeline validation.
8. Gold export.

### Step 6 — Execute Streaming Pipeline

1. Start Kafka infrastructure.
2. Run the event producers.
3. Start the Kafka consumer.
4. Verify event delivery to S3.
5. Execute streaming Bronze ingestion.
6. Execute streaming Silver transformations.
7. Run streaming data-quality processing.
8. Execute streaming Gold analytics.
9. Run streaming validation and reliability tests.
10. Export streaming Gold datasets to S3.

### Step 7 — Monitoring and Reporting

Run the monitoring components and verify the Gold datasets.

Connect Power BI to the prepared analytical outputs using the configured data-access method.

---

## 11. Important Engineering Considerations

The implementation uses deterministic S3 keys and commits Kafka offsets after successful S3 uploads. This improves retry safety, but it should not be described as a guarantee of end-to-end exactly-once processing.

Streaming Silver deduplication uses watermark-based logic. Events arriving beyond the configured watermark may not be retained by that deduplication path.

The Auto Loader workflow uses triggered processing with `availableNow=True` in the tested implementation. This processes available files in micro-batches; it is not a continuously running streaming query.

Valid-record and quarantine outputs are separate writes and are not an atomic transaction across both destinations.

The implementation should therefore be described as a reliability-focused data engineering platform with tested retry, validation, and recovery scenarios, rather than as a platform with universal production-level delivery guarantees.

---

## 12. Security

Credentials must never be committed to GitHub.

Do not publish:
- AWS access keys.
- Databricks personal access tokens.
- Private keys.
- `.env` files containing secrets.
- Account passwords.
- Sensitive production data.

Use IAM roles, environment variables, and approved secrets-management mechanisms.

Any credential exposed during development should be revoked and rotated before publishing the repository.

---

## 13. Future Enhancements

Potential extensions include:

- CI/CD automation for Databricks deployments.
- Infrastructure as Code using Terraform.
- Automated pipeline testing.
- Advanced observability and alerting.
- Schema evolution testing.
- More extensive late-event and replay scenarios.
- Performance benchmarking with larger datasets.
- Expanded Power BI dashboards.
- Role-based data access and governance.

---

## 14. Project Summary

This project demonstrates the design and implementation of a hybrid e-commerce data lakehouse supporting both batch and real-time event processing.

It brings together Apache Kafka, Amazon S3, Databricks, PySpark, Delta Lake, and Power BI within a structured Medallion Architecture.

The platform demonstrates practical data engineering concepts including distributed data processing, incremental ingestion, data-quality management, historical dimension tracking, event deduplication, reliability testing, monitoring, and analytical data delivery.

**Core outcome:** Two complementary data pipelines — Batch and Streaming — following the same Bronze, Silver, and Gold architecture to transform raw e-commerce data into trusted, business-ready analytics."# Databricks-AWS-Ecommerce-Lakehouse-" 
