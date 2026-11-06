# Multi-Store Order Streaming Pipeline

A Python data engineering project that simulates orders from three stores, processes them through Apache Kafka, updates inventory in PostgreSQL, and publishes saved order records for downstream Spark analytics.

The project explores event-driven processing, producer/consumer messaging, inventory validation, relational storage, and Bronze/Silver/Gold data transformations. Payment outcomes and customer orders are simulated.

## Current capabilities

- Generate orders containing customer, store, product, quantity, price, and simulated payment information.
- Use separate Kafka request and result topics for each store.
- Validate order fields and product quantities, check inventory, and record approval or rejection reasons.
- Save processed orders as item-level rows in each store's PostgreSQL database.
- Publish pending database rows to a shared Kafka `orders` topic and mark successfully delivered rows as sent.
- Provide Spark scripts for ingestion, transformation, and store-specific analytical outputs.

## Architecture

<img width="7603" height="3319" alt="Order pipeline architecture" src="https://github.com/user-attachments/assets/a3d5271d-2e2c-419d-990e-6a4d8afc8301" />

The operational flow is:

1. The simulator generates orders and publishes them to a store's request topic.
2. The inventory consumer validates each order and attempts the requested stock deductions.
3. The consumer publishes approval details to the store's result topic.
4. The database sender consumes those results and inserts the order's items into the store database.
5. The database-to-Kafka publisher reads rows where `sent_to_kafka = FALSE` and sends them to the shared `orders` topic.
6. The analytics scripts consume or transform the resulting data in an external Spark environment.

Each store uses its own database and topics:

| Store / database | Price prefix | Request topic | Result topic |
| --- | --- | --- | --- |
| `store_1` | `$` | `order_request_s1` | `orders_result_s1` |
| `store_2` | `SAR` | `order_request_s2` | `orders_result_s2` |
| `store_3` | `€` | `order_request_s3` | `orders_result_s3` |

## Technology

| Component | Technology |
| --- | --- |
| Simulation and processing | Python; the Dockerfile targets Python 3.14 |
| Event transport | Apache Kafka through `confluent-kafka` |
| Operational storage | PostgreSQL |
| Database access | SQLAlchemy and Psycopg |
| Analytics | PySpark Structured Streaming scripts using Databricks-style tables and volumes |
| Container definition | Dockerfile; its startup command still needs updating for the current source layout |

## Repository structure

```text
project_x/
├── src/
│   ├── store_simulation.py
│   ├── order_generation/
│   │   ├── generate_orders.py
│   │   ├── order_producer.py
│   │   └── products_details/product_catalog.py
│   ├── inventory/
│   │   └── order_consumer.py
│   ├── db/
│   │   ├── db_connector.py
│   │   └── db_sender.py
│   ├── messaging/
│   │   ├── kafka_client.py
│   │   └── db_to_kafka.py
│   └── analytics/
│       ├── bronze_ingestion.py
│       ├── silver_ingestion.py
│       └── gold_ingestion.py
├── requirement.txt
├── Dockerfile
└── README.md
```

## Local setup

### 1. Install the Python dependencies

From the repository root on Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirement.txt
```

`requirement.txt` currently contains the operational dependencies. The Spark analytics runtime is configured separately.

### 2. Prepare PostgreSQL

Create the three databases listed above and configure a database user with the required read/write permissions.

Each database needs:

- A `products` table with product IDs matching the generator's catalog and a `quantity` column containing initial stock.
- An `orders` table matching the fields inserted by [the database sender](src/db/db_sender.py) and selected by [the publisher](src/messaging/db_to_kafka.py).
- A generated row `id` and a `sent_to_kafka` flag that defaults to `FALSE`, because inserts omit both fields.

The `orders` table stores one row per item, so several rows can share an `order_id`. Database creation, schema migrations, and inventory seed scripts are not yet included in this repository.

### 3. Configure Kafka and database access

Prepare the six request/result topics listed above and the shared `orders` topic. Configure broker access, SASL authentication, and permissions for the required topics and consumer group.

Connection settings are currently defined in these files:

- [Kafka client configuration](src/messaging/kafka_client.py)
- [Store and PostgreSQL configuration](src/store_simulation.py)
- [Analytics Kafka and storage configuration](src/analytics/bronze_ingestion.py)

Update those settings for your environment. Loading configuration from environment variables is a planned improvement.

### 4. Start the simulator

With the virtual environment activated, run this from the repository root:

```bash
PYTHONPATH=src python -m src.store_simulation
```

The temporary `PYTHONPATH` setting supports the current mixture of top-level imports and `src.*` imports. Import resolution has been checked; a full run also requires the configured databases, schema, inventory, and Kafka connection.

The simulator runs continuously, cycles through the three stores, and pauses between cycles. Press `Ctrl+C` to stop it.

## Analytics setup and status

The analytics scripts expect an external Spark environment with an available `spark` session, Kafka integration, access to the configured `/Volumes` locations, and permission to read/write the configured tables.

| Stage | Current behavior |
| --- | --- |
| Bronze | Consumes the shared `orders` topic, parses its records, and writes JSON files to a landing directory. |
| Silver | Reads `orders.bronze.bronz_order`, parses a JSON `value` column, converts prices/timestamps, and separates approved and rejected orders. |
| Gold | Reads approved Silver records and writes separate outputs for each store. |

The loader connecting Bronze's JSON output to Silver's expected input table is not included. That boundary must be implemented or configured before running the stages together. Gold currently creates store-specific outputs; business metric aggregation is a future step.

## Next steps

- Standardize imports and configure an installable application package.
- Update the Docker startup command and service networking for the `src/` layout.
- Add database migrations and reproducible inventory seeding.
- Make order processing atomic and safe to retry, with explicit Kafka acknowledgment and a transactional outbox.
- Define whole-order versus partial-item fulfillment; current inventory updates commit separately for each item.
- Connect the Bronze and Silver stages and add schema validation and persistent duplicate handling.
- Preserve currency explicitly and use item approval when building sales metrics.
- Add automated tests for rejection, retries, publication failures, and duplicate events.
