# Real-time CDC Pipeline

A production-style **Change Data Capture (CDC)** pipeline that streams every INSERT, UPDATE, and DELETE from PostgreSQL to Kafka in real time — no polling, no delay. Today it runs end-to-end locally; the roadmap extends it into a **streaming lakehouse** (Flink → Iceberg → MinIO/S3) with a clear migration path to AWS.

---

## Architecture (today)

```
┌─────────────────────┐   WAL (logical replication)   ┌─────────────────┐
│   PostgreSQL 15     │ ─────────────────────────────► │    Debezium     │
│  schema: ecommerce  │                                │  Kafka Connect  │
└─────────────────────┘                                └────────┬────────┘
                                                                │ JSON events
                                                       ┌────────▼────────┐
                                                       │      Kafka      │
                                                       │   (KRaft mode)  │
                                                       └────────┬────────┘
                                          ┌────────────────────┼────────────────────┐
                                          │                    │                    │
                               ┌──────────▼──────┐  ┌─────────▼──────┐  ┌─────────▼──────────┐
                               │   Kafka UI      │  │   Dashboard    │  │  Flink → Iceberg   │
                               │  (monitoring)   │  │  (Streamlit)   │  │  (lakehouse, next) │
                               └─────────────────┘  └────────────────┘  └────────────────────┘
```

**How it works:**
1. PostgreSQL writes all changes to its **WAL (Write-Ahead Log)** in `logical` mode.
2. Debezium reads the WAL via a **replication slot** and **publication**, packaging each change as a JSON event.
3. Events land on **Kafka topics** (`cdc.ecommerce.<table>`), partitioned and retained for downstream consumers.
4. The Streamlit dashboard consumes topics directly and renders live metrics.

> The fourth box — **Flink → Iceberg** — is the next milestone. See [Roadmap](#roadmap--from-cdc-demo-to-streaming-lakehouse) for the full target architecture.

---

## Tech Stack

| Service | Image / Tool | Port | Role |
|---------|-------------|------|------|
| PostgreSQL 15 | `postgres:15` | `5432` | Source database — logical replication enabled |
| Kafka | `confluentinc/cp-kafka:latest` | `9092` / `29092` | Message broker — KRaft mode (no Zookeeper) |
| Debezium | `debezium/connect:2.7.3.Final` | `8083` | CDC engine — bridges Postgres WAL to Kafka |
| Kafka UI | `provectuslabs/kafka-ui:latest` | `8080` | Web UI to inspect topics and messages |
| Adminer | `adminer:latest` | `8081` | Lightweight PostgreSQL web UI |
| Dashboard | `./dashboard` (Streamlit) | `8501` | Real-time CDC event dashboard from Kafka |

*Coming in the roadmap:* **Apache Flink** (stream processor), **Apache Iceberg** (table format), **MinIO** (S3-compatible object store).

---

## Data Model

Schema `ecommerce` in database `enterprise_db` — simulates a computer hardware e-commerce store.

```
ecommerce
├── customers        — buyers with loyalty tiers: BRONZE / SILVER / GOLD / PLATINUM
├── products         — catalog (CPU, GPU, RAM, SSD, PSU, Case, Cooler, Monitor, Peripheral)
├── orders           — orders with auto-generated number (ORD-YYYY-NNNNNN)
├── order_items      — line items per order (FK → orders, products)
├── inventory        — stock levels per product across warehouses A/B/C
├── audit_logs       — manual change tracking log
├── cdc_heartbeat    — Debezium heartbeat table (prevents WAL bloat)
├── debezium_signals — send signals to Debezium (incremental snapshot, etc.)
└── cdc_metrics      — trigger-based counter: INSERT/UPDATE/DELETE per table
```

Debezium tracks 6 tables via `cdc_publication`:
`customers`, `products`, `orders`, `order_items`, `inventory`, `debezium_signals`

---

## Project Structure

```
Real-time CDC/
├── docker-compose.yml
├── requirements.txt              # host-side deps for generate_data.py
├── postgres/
│   ├── init.sql                  # schema, seed data, triggers, functions
│   ├── postgresql.conf           # logical replication settings
│   └── pg_hba.conf
├── debezium/
│   └── postgres-connector.json   # connector config
├── scripts/
│   └── generate_data.py          # continuous data generator
└── dashboard/
    ├── app.py                    # Streamlit real-time dashboard
    ├── Dockerfile
    └── requirements.txt
```

A [proposed structure](#proposed-file-structure) for the Flink/lakehouse work is described in the roadmap.

---

## Quick Start

### 1. Start the stack

```bash
docker compose up -d
```

Wait ~30 seconds for all services to become healthy:

```bash
docker compose ps
```

### 2. Register the Debezium connector

This step is **required** — without it Kafka receives nothing.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-connector.json
```

Verify it is running:

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

### 3. Generate data

```bash
pip install -r requirements.txt

# Default: 2 orders every 5 seconds
python scripts/generate_data.py

# Faster for demo
python scripts/generate_data.py --interval 2 --orders-per-tick 5
```

| Flag | Default | Description |
|------|---------|-------------|
| `--interval` | `5.0` | Seconds between ticks |
| `--orders-per-tick` | `2` | Orders created per tick |
| `--new-customers` | `1` | New customers per tick (40% chance) |
| `--once` | off | Run one tick then exit |

### 4. Open the UIs

| UI | URL | Credentials |
|----|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Adminer (DB) | http://localhost:8081 | Server: `postgres` · User: `admin` · Password: `admin` · DB: `enterprise_db` |
| CDC Dashboard | http://localhost:8501 | — |
| Debezium API | http://localhost:8083 | — |

---

## Kafka Topics

| Topic | Source table |
|-------|-------------|
| `cdc.ecommerce.customers` | customers |
| `cdc.ecommerce.products` | products |
| `cdc.ecommerce.orders` | orders |
| `cdc.ecommerce.order_items` | order_items |
| `cdc.ecommerce.inventory` | inventory |
| `debezium-config` | Debezium internal |
| `debezium-offsets` | Debezium offset tracking |
| `debezium-status` | Debezium connector state |

Each event payload:

```json
{
  "before": { "id": "...", "status": "PROCESSING" },
  "after":  { "id": "...", "status": "SHIPPED" },
  "op":     "u",
  "ts_ms":  1782662915000,
  "source": { "table": "orders", "lsn": 12345678 }
}
```

`op` values: `c` = INSERT · `u` = UPDATE · `d` = DELETE · `r` = snapshot READ

---

## Key Configuration

### PostgreSQL — logical replication (`postgresql.conf`)

```ini
wal_level = logical        # required for CDC
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 1GB        # retain WAL long enough for Debezium to catch up
```

### Debezium connector highlights (`debezium/postgres-connector.json`)

```json
"plugin.name":              "pgoutput",
"snapshot.mode":            "initial",
"replica.identity.autoset": "ecommerce.*:FULL",
"heartbeat.interval.ms":    "10000",
"provide.transaction.metadata": "true",
"decimal.handling.mode":    "double"
```

`FULL` replica identity ensures `before` values are always present on UPDATE/DELETE.

### Kafka — KRaft mode (no Zookeeper)

Two listeners are configured:
- `PLAINTEXT://kafka:9092` — for inter-container communication (Debezium, Dashboard in Docker)
- `PLAINTEXT_HOST://localhost:29092` — for connections from the host machine

---

## Reset

Full reset — wipes all data and volumes:

```bash
docker compose down
rm -rf postgres/data
docker compose up -d
# Re-register the connector (step 2 above)
```

---

# Roadmap — From CDC Demo to Streaming Lakehouse

The pipeline above is the foundation. The next direction is to turn it into an end-to-end **streaming lakehouse** that runs locally and maps cleanly onto AWS:

```text
PostgreSQL → Debezium → Kafka → Flink → Iceberg → MinIO/S3
```

A Flink job consumes the `cdc.ecommerce.*` topics, parses Debezium payloads into row-level change events, and applies them to **Iceberg** tables stored on **MinIO** (an S3-compatible object store). Once the local flow is stable, the same job maps to AWS using RDS PostgreSQL, MSK, Amazon Managed Service for Apache Flink, S3, and Glue Catalog — without rewriting the processing logic.

## Target Architecture

```
┌──────────────┐   WAL    ┌──────────────┐   JSON    ┌──────────────┐
│ PostgreSQL15 │ ───────► │   Debezium   │ ────────► │    Kafka     │
│  ecommerce   │ logical  │ Kafka Connect│  events   │   (KRaft)    │
└──────────────┘          └──────────────┘           └──────┬───────┘
                                                            │ cdc.ecommerce.*
                                                     ┌──────▼───────┐
                                                     │    Flink     │  parse Debezium,
                                                     │  stream job  │  apply by primary key
                                                     └──────┬───────┘
                                                            │ upsert / delete
                                                     ┌──────▼───────┐
                                                     │   Iceberg    │  lakehouse
                                                     │   tables     │  table format
                                                     └──────┬───────┘
                                                            │
                                                     ┌──────▼───────┐
                                                     │  MinIO / S3  │  warehouse storage
                                                     └──────────────┘
```

## Current State vs. What's Missing

**Already built:**
- `docker-compose.yml` starts PostgreSQL, Kafka, Debezium Connect, Kafka UI, Adminer, and the Streamlit dashboard.
- PostgreSQL logical replication via `postgres/postgresql.conf`.
- The `ecommerce` schema with source tables, `cdc_publication`, replication-slot config, a heartbeat table, and a Debezium signal table.
- `debezium/postgres-connector.json` — initial snapshot + CDC streaming into `cdc.ecommerce.*` topics.
- `scripts/generate_data.py` — continuous INSERT/UPDATE/DELETE generator.

**Missing for a full lakehouse:**
- A Flink job that consumes Kafka CDC events.
- A durable analytics sink (Iceberg on MinIO/S3).
- Automation scripts for registering the connector, submitting the Flink job, and verifying output.
- Checkpoint and restart behavior for the stream processor.
- Observability for Kafka lag, connector status, Flink metrics, and storage output.
- Environment-specific config for local vs. AWS deployment.

## Milestones

### M1 — Local End-to-End Lakehouse Demo

Run the full CDC flow from PostgreSQL to Iceberg on MinIO.

- Add MinIO and a Flink JobManager/TaskManager to `docker-compose.yml`, with the dependencies needed to read Kafka and write Iceberg to S3-compatible storage.
- Implement a Flink job that reads all five `cdc.ecommerce.*` topics, extracts operation type / primary key / before-after payloads / source timestamp, and writes to Iceberg tables matching the source schema.
- Add scripts to register the connector, submit the Flink job, generate test events, and verify output in MinIO/Iceberg.

**Done when:** `docker compose up -d` starts the full stack, the connector registers and topics appear, the Flink job consumes events, and inserts/updates/deletes are reflected correctly in Iceberg tables backed by MinIO objects.

### M2 — Reliability Hardening

Make the pipeline restartable and avoid event loss in local failure scenarios.

- Enable Flink checkpointing with checkpoint storage on MinIO; configure a restart strategy.
- Define primary-key and upsert/delete behavior per Iceberg table; handle Debezium tombstone/delete events.
- Validate duplicate-event handling and idempotency by primary key.
- Document the delivery semantics (at-least-once vs. exactly-once) given the final Flink/Iceberg config.

**Done when:** restarting a Flink TaskManager resumes from checkpoint, Kafka events are not lost, and updates/deletes do not create incorrect records.

### M3 — Observability and Operations

Make pipeline health, lag, and failures visible.

- Health checks and runbook notes for PostgreSQL, Kafka, Debezium, Flink, and MinIO.
- Track connector status (Kafka Connect REST API), Kafka consumer lag for the Flink group, and Flink job/checkpoint status.
- Script-based validation of Iceberg row counts and latest updates.
- Troubleshooting notes: connector failure, replication-slot lag, missing Kafka messages, Flink job failure, MinIO permission errors.

**Done when:** connector status is quick to check (RUNNING/FAILED), Flink Kafka lag is inspectable, and the latest successful Iceberg write can be verified.

### M4 — AWS-Ready Deployment Path

Structure the pipeline so it deploys to AWS without rewriting the processing flow.

- Split configuration into local and AWS profiles; make the MinIO/S3 endpoint configurable.
- Add support for Glue Catalog as the Iceberg catalog.
- Document the RDS logical-replication settings and the network/security requirements (security groups, subnets, IAM roles, S3 bucket policy, Glue permissions, MSK connectivity).
- Default AWS target is **Amazon Managed Service for Apache Flink**, with EKS/ECS as alternatives when runtime control is required.

**Done when:** the same Flink job runs against S3 + Glue Catalog, no cloud credentials are hard-coded, and a minimum AWS deployment checklist exists.

> **Principle:** endpoints, credentials, bucket names, warehouse paths, catalog type, and connector settings come from environment variables or env-specific config files. Never hard-code AWS values in application code.

## Data Design & Mapping

| Kafka topic | Iceberg table |
| --- | --- |
| `cdc.ecommerce.customers` | `ecommerce.customers` |
| `cdc.ecommerce.products` | `ecommerce.products` |
| `cdc.ecommerce.orders` | `ecommerce.orders` |
| `cdc.ecommerce.order_items` | `ecommerce.order_items` |
| `cdc.ecommerce.inventory` | `ecommerce.inventory` |

Each Iceberg table keeps the business columns from PostgreSQL and adds CDC metadata:

- `cdc_op` — Debezium operation (`c`, `u`, `d`, `r`)
- `cdc_source_ts_ms` — source event timestamp
- `cdc_processed_at` — when Flink processed the event
- `cdc_topic` / `cdc_partition` / `cdc_offset` — Kafka provenance

Updates and deletes are applied by the source table's primary key. For a current-state table, use Iceberg upsert/delete semantics rather than an append-only log; if full audit history is needed, add a separate append-only changelog layer.

## Local → AWS Migration Map

| Local component | AWS equivalent | Notes |
|----------------|----------------|-------|
| PostgreSQL 15 (Docker) | **Amazon RDS for PostgreSQL** (or Aurora) | Enable `rds.logical_replication = 1` |
| Debezium (Docker) | **Debezium on ECS Fargate** or **AWS DMS** | DMS is simpler to operate; Debezium gives more control |
| Kafka (KRaft) | **Amazon MSK** | Removes broker management overhead |
| Flink (Docker) | **Amazon Managed Service for Apache Flink** | EKS/ECS when runtime control is required |
| MinIO | **Amazon S3** | Iceberg warehouse storage |
| Local Iceberg catalog | **AWS Glue Data Catalog** | Shared metastore for Iceberg tables |
| Hard-coded local config | **AWS Secrets Manager / SSM Parameter Store** | Endpoints, credentials, bucket names |
| Local metrics & logs | **CloudWatch + MSK/Flink metrics** | Connector health, lag, checkpoints |
| `generate_data.py` (local) | **ECS Task** or **AWS Batch** | Scheduled or on-demand |

**What stays the same:** the Debezium connector config, the schema design, and the Kafka topic naming convention all carry forward. The main differences are the broker endpoint and authentication method (IAM or SASL/SCRAM on MSK), and the Iceberg catalog/storage targets.

## Proposed File Structure

When lakehouse implementation starts, add:

```text
docs/
  AWS_DEPLOYMENT.md      # added when M4 starts
  TROUBLESHOOTING.md     # added after real runtime issues are observed

flink/
  jobs/
  conf/
  Dockerfile

scripts/
  register-connector.sh
  submit-flink-job.sh
  generate-cdc-events.sh
  verify-lake-output.sh

config/
  local.env.example      # placeholders only, never real secrets
  aws.env.example
```

## Recommended Next Steps

1. Add MinIO and Flink to Docker Compose.
2. Choose the Flink job packaging approach and required Iceberg/Kafka dependencies.
3. Implement the job for one table first — start with `customers`.
4. Verify insert/update/delete behavior for `customers`.
5. Expand the job to the remaining tables.
6. Add checkpointing and restart tests.
7. Add the runbook and AWS deployment mapping.

## Out of Scope (for the first milestone)

Keep these until the local end-to-end flow is stable: real AWS deployment, full CI/CD, Terraform/IaC, complete monitoring dashboards, a dedicated data-quality framework, multi-tenant/multi-database CDC, and a Schema Registry (unless the first demo needs it).
