# Multi-Store Order Streaming Pipeline

This project simulates orders from three stores and moves the data through Kafka, PostgreSQL and Databricks.

The goal is to practice streaming data, inventory checks and preparing order data for analysis.

## Architecture

![Multi-Store Order Streaming Pipeline](docs/architecture.png)

Each store has its own PostgreSQL database and Kafka request and result topics.

1. Generate random orders and send them to the store's request topic.
2. Check the order, simulated payment status and available inventory.
3. Publish the result and save the order items in PostgreSQL.
4. Send saved records to the shared Kafka `orders` topic.
5. Land the events in Bronze, then clean them in Silver.

## Data layers

- **Bronze:** reads Kafka events and stores them as JSON landing files, using checkpoints to track progress.
- **Silver:** standardizes prices and currencies, converts dates and approval flags, and writes cleaned order data to Delta tables.

The diagram shows the intended flow. The analytics filenames currently need aligning: `silver_ingestion.py` contains Kafka-to-JSON ingestion, while `bronze_ingestion.py` contains the Silver transformations.

## Tools

Python, Apache Kafka, PostgreSQL, PySpark and Databricks.

## Run locally

Prepare the `store_1`, `store_2` and `store_3` PostgreSQL databases with `products` and `orders` tables. Create the configured Kafka topics, then copy `.env.example` to `.env` and add your Kafka and PostgreSQL settings.

Private settings are ignored by Git. Existing local settings are kept in `.env.local`, which takes precedence over `.env`. Environment variables take precedence over both files.

From the project root:

```bash
python -m pip install -r requirement.txt
cp .env .env
python src/store_simulation.py
```

Run the analytics stages separately in Databricks with access to Kafka and the configured volumes and tables. Set the same `KAFKA_*` variables in the Databricks runtime, using [Databricks secrets](https://docs.databricks.com/aws/en/security/secrets/secrets-spark-conf-env-var) for credentials.

Before publishing, rotate the old Kafka credentials and database password. Previous Git commits still contain them.
