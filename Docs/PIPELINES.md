# Data Pipeline Architecture and Execution

## 1. Introduction

The project implements two independent data processing pipelines using the Medallion Architecture:

1. Batch Data Pipeline
2. Real-Time Streaming Data Pipeline

Both pipelines use Bronze, Silver, and Gold layers, but differ in their ingestion mechanisms and processing characteristics.

---

## 2. Batch Data Pipeline

### Architecture

```text
Batch Data Sources
        |
        v
    Amazon S3
        |
        v
Databricks Auto Loader
        |
        v
      BRONZE
        |
        v
      SILVER
        |
        +---- Data Cleaning
        +---- Validation
        +---- Customer SCD Type 2
        +---- Quarantine
        |
        v
       GOLD
        |
        +---- Daily Sales
        +---- Product Performance
        +---- Customer Value
        |
        v
    Amazon S3
        |
        v
     Power BI
```

### Source Data

The batch pipeline processes four e-commerce datasets:

- Customers
- Orders
- Payments
- Products

The datasets are stored under the respective S3 batch prefixes.

### Bronze Layer

File:

`Source Codes/batch/bronze/bronze_ingestion.py`

Responsibilities:

- Ingest batch source files.
- Detect and process source data.
- Store raw records in Delta tables.
- Maintain the Bronze layer.

### Silver Layer

Files:

- `Source Codes/batch/silver/silver_transformations.py`
- `Source Codes/batch/silver/customer_scd2.py`

Responsibilities:

- Clean and standardise records.
- Apply required transformations.
- Validate records.
- Handle invalid data.
- Maintain customer history using SCD Type 2.

### Data Quality

File:

`Source Codes/batch/data_quality/data_quality.py`

Responsibilities:

- Validate incoming data.
- Detect invalid records.
- Separate valid and invalid records.
- Support quarantine processing.

### Gold Layer

Files:

- `Source Codes/batch/gold/gold_analytics.py`
- `Source Codes/batch/gold/export_gold.py`

The Gold layer creates:

| Table | Description |
|---|---|
| `daily_sales` | Daily sales and order metrics |
| `product_performance` | Product-level analytics |
| `customer_value` | Customer-level value analytics |

The prepared outputs are exported to Amazon S3.

---

## 3. Real-Time Streaming Pipeline

### Architecture

```text
Python Event Producers
          |
          v
    Apache Kafka
          |
          v
    Kafka Consumer
          |
          v
       Amazon S3
          |
          v
   Databricks Auto Loader
          |
          v
        BRONZE
          |
          v
        SILVER
          |
          +---- Transformations
          +---- Deduplication
          +---- Watermark Processing
          +---- Data Quality
          |
          v
         GOLD
          |
          +---- Streaming Daily Sales
          +---- Streaming Daily Payments
          +---- Streaming Daily Clickstream
          |
          v
      Amazon S3
          |
          v
       Power BI
```

### Event Producers

File:

`Source Codes/streaming/ingestion/event_producer.py`

Generates e-commerce events and publishes them to Kafka.

Topics:

- `orders`
- `payments`
- `clickstream`

### Kafka Consumer

File:

`Source Codes/streaming/ingestion/consumer.py`

Responsibilities:

- Subscribe to Kafka topics.
- Consume incoming events.
- Upload event records to S3.
- Organise objects using topic, partition, and offset.
- Commit Kafka offsets after successful S3 upload.

### Streaming Bronze

File:

`Source Codes/streaming/ingestion/streaming_bronze_ingestion.py`

Responsibilities:

- Detect new streaming files in S3.
- Ingest files using Databricks Auto Loader.
- Store records in Bronze Delta tables.

### Streaming Silver

File:

`Source Codes/streaming/silver/streaming_silver_transformations.py`

Responsibilities:

- Clean and transform incoming records.
- Apply deduplication.
- Use watermark-based processing.
- Prepare validated streaming datasets.

### Streaming Data Quality

File:

`Source Codes/streaming/data_quality/streaming_data_quality.py`

Responsibilities:

- Validate streaming records.
- Identify invalid records.
- Support quarantine handling.

### Streaming Gold

Files:

- `Source Codes/streaming/gold/streaming_gold_analytics.py`
- `Source Codes/streaming/gold/streaming_export_gold.py`

Gold outputs:

- `streaming_daily_sales`
- `streaming_daily_payments`
- `streaming_daily_clickstream`

These outputs are exported to Amazon S3.

---

## 4. Shared Validation

Files:

- `Source Codes/shared/pipeline_validation.py`
- `Source Codes/shared/streaming_pipeline_validation.py`

These scripts provide validation for the batch and streaming processing paths.

Validation includes checking the expected pipeline outputs and data integrity conditions.

---

## 5. Data Source Setup

File:

`Source Codes/data source setup/source_setup.py`

This component supports the initial data source and pipeline setup.

---

## 6. Execution and Orchestration

The Databricks Workflow:

`ecommerce_lakehouse_pipeline`

contains 13 configured tasks.

The workflow coordinates the required pipeline activities and their dependencies.

The local Kafka infrastructure is managed through Docker Compose.

The Python producers and consumer operate as the streaming event delivery components.

---

## 7. Processing Characteristics

| Feature | Batch | Streaming |
|---|---|---|
| Data ingestion | S3 batch files | Kafka events delivered to S3 |
| Ingestion engine | Auto Loader | Auto Loader |
| Bronze storage | Delta | Delta |
| Silver processing | PySpark | PySpark |
| Gold analytics | Business aggregates | Event-based aggregates |
| Data quality | Yes | Yes |
| Quarantine | Yes | Yes |
| Validation | Yes | Yes |
| Export | S3 | S3 |

Both pipelines share the same architectural principles while supporting different data arrival patterns.