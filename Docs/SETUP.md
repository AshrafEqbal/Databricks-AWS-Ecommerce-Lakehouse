# Environment Setup and Installation Guide

## 1. Overview

This guide explains how to configure the development environment and infrastructure required to run the Hybrid Batch and Streaming E-Commerce Lakehouse.

The platform uses:

- Python
- Docker and Docker Compose
- Apache Kafka
- Amazon S3
- AWS IAM
- Databricks
- Unity Catalog
- Delta Lake
- Power BI

## 2. Prerequisites

Before starting, ensure the following are available:

| Requirement | Purpose |
|---|---|
| Python | Producers, consumers and monitoring |
| Docker Desktop | Running Kafka infrastructure |
| AWS Account | Cloud storage and permissions |
| Amazon S3 | Raw data and Gold exports |
| Databricks Workspace | Data processing and orchestration |
| Unity Catalog | Data organisation and governance |
| Power BI Desktop | Analytics and dashboards |

## 3. Repository Setup

Clone the repository:

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd kafka_s3_pipeline
```

Create a Python virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows CMD:

```cmd
.venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## 4. AWS Configuration

The development implementation used:

- S3 bucket: `ecommerce-lakehouse-1482`
- IAM role: `ecommerce-lakehouse-s3-role`

Configure AWS credentials through an approved credential provider.

The S3 storage structure used in the implementation included:

```text
ecommerce-lakehouse-1482/
│
├── raw/
│   ├── batch/
│   │   ├── customers/
│   │   ├── orders/
│   │   ├── payments/
│   │   └── products/
│   │
│   └── streaming/
│       ├── orders/
│       ├── payments/
│       └── clickstream/
│
├── bronze/
├── silver/
├── gold/
├── checkpoints/
├── schemas/
│
└── exports/
    └── gold/
        ├── customer_value/
        ├── daily_sales/
        └── product_performance/
```

The IAM permissions must allow the required S3 operations for the relevant prefixes.

Never store AWS credentials directly in Python files.

## 5. Docker and Kafka Setup

Start Docker Desktop.

From the repository root, run:

```bash
docker compose up -d
```

Check running containers:

```bash
docker compose ps
```

The development environment used an Apache Kafka broker exposed on port `9092`.

The Kafka topics used were:

- `orders`
- `payments`
- `clickstream`

Verify the broker address, topic configuration, and advertised listeners against `docker-compose.yml` before running producers or consumers.

## 6. Databricks Configuration

The development implementation used the Unity Catalog:

`ecommerce_lakehouse`

Schemas:

- `bronze`
- `silver`
- `gold`

The storage integration included:

- Storage credential: `ecommerce_s3_credential`
- External location: `ecommerce_lakehouse_location`
- IAM role: `ecommerce-lakehouse-s3-role`

The external location referenced the project S3 bucket.

Configure the required permissions and ensure the Databricks workspace can access the relevant S3 prefixes.

## 7. Databricks Workflow

The project uses a Databricks Workflow named:

`ecommerce_lakehouse_pipeline`

The implemented workflow contains 13 tasks.

Import the Python source files into Databricks and configure the task dependencies according to the existing working workflow.

Do not assume that running files alphabetically will reproduce the workflow.

## 8. Power BI Setup

Power BI is used to visualise the prepared Gold datasets.

The available analytical outputs include:

- Daily Sales
- Product Performance
- Customer Value
- Streaming Daily Sales
- Streaming Daily Payments
- Streaming Daily Clickstream

Configure a supported connection to the Gold data, such as Databricks SQL or an AWS analytics connection.

Store dashboard screenshots in:

`Dashboard/screenshots/`

## 9. Security Checklist

Before running or publishing:

- Do not commit AWS credentials.
- Do not commit Databricks access tokens.
- Do not commit `.env` files containing secrets.
- Use IAM roles and approved secrets-management methods.
- Keep generated logs and temporary data out of Git.
- Rotate credentials that were exposed during development.

## 10. Environment Verification

Before executing the pipelines, verify:

1. Python environment is active.
2. Required Python packages are installed.
3. Docker containers are running.
4. Kafka topics are available.
5. AWS credentials and S3 access work.
6. Databricks external location is accessible.
7. Unity Catalog schemas exist.
8. Databricks Workflow dependencies are configured.

Once these checks pass, the batch and streaming pipelines can be executed.