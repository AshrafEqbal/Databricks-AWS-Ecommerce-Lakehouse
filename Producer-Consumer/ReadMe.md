# Kafka Producer–Consumer (Real-Time Data Ingestion)

## Overview
This component implements real-time event ingestion for the **Enterprise Real-Time & Batch Lakehouse Data Platform** using Apache Kafka, Python, and AWS S3.

It simulates events from two sources:
- **SondaHub Store** – E-commerce orders and payment events.
- **SondaHub Social** – Customer clickstream events.

## Architecture

```text
Event Sources
     |
     v
Python Kafka Producer
     |
     v
Apache Kafka
     |
     v
Python Kafka Consumer
     |
     v
AWS S3 (Raw Layer)
     |
     v
Databricks Auto Loader
     |
     v
Bronze → Silver → Gold
```

## Kafka Topics

| Topic | Description |
|---|---|
| `orders` | Order-related events |
| `payments` | Payment transactions |
| `clickstream` | Customer browsing activity |

## Producer
The Python producer generates simulated real-time e-commerce events and publishes them to their respective Kafka topics.

Events contain common metadata such as:
- `event_id`
- `event_type`
- `event_timestamp`
- `customer_id`

Additional fields depend on the event type.

## Consumer
The Python consumer subscribes to Kafka topics and processes incoming messages.

Responsibilities:
- Consume events from Kafka.
- Deserialize JSON messages.
- Maintain Kafka consumer offsets.
- Organize events by topic and date.
- Upload JSON event files to AWS S3.

## AWS S3 Integration
Consumed events are stored in the raw streaming layer:

```text
s3://ecommerce-lakehouse-1482/raw/streaming/
    orders/
    payments/
    clickstream/
```

Events are organized using date-based partitions.

## Databricks Integration
Databricks Auto Loader ingests the raw streaming data from S3 into Bronze Delta tables.

The downstream pipeline performs:
- Bronze ingestion and metadata tracking.
- Silver transformations and deduplication.
- Data quality checks and quarantine.
- Gold-level analytics.

## Technologies Used
- Python
- Apache Kafka
- Kafka-Python
- AWS S3
- Boto3
- JSON
- Databricks Auto Loader
- PySpark
- Delta Lake

## Purpose
This component demonstrates an end-to-end real-time data ingestion workflow, connecting event producers, Kafka messaging, cloud object storage, and a Databricks Lakehouse architecture.
