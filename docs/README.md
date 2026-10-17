# Real-time CDC Pipeline

A production-style **Change Data Capture (CDC)** pipeline that streams every INSERT, UPDATE, and DELETE from PostgreSQL through Kafka into a **ClickHouse data warehouse** in about a second, with a web **Control Center** to generate traffic, watch each change as it happens, and explore the warehouse. The roadmap adds a **streaming lakehouse** (Flink → Iceberg → MinIO/S3) and a migration path to AWS.

---

## Architecture

```
┌──────────────────────┐
│  CDC Control Center  │  React + FastAPI  (http://localhost:8000)
│  generator · live ·  │──────────────────────────────────────────────┐
│  warehouse analytics │  writes business events     reads gold marts │
└──────────┬───────────┘                                              │
           │ INSERT/UPDATE/DELETE                                     │
┌──────────▼──────────┐   WAL (logical replication)   ┌───────────────┴─┐
│   PostgreSQL 15     │ ─────────────────────────────► │    Debezium     │
│  schema: ecommerce  │                                │  Kafka Connect  │
└─────────────────────┘                                └────────┬────────┘
                                                                │ JSON envelopes
                                                       ┌────────▼────────┐
                                                       │      Kafka      │
                                                       │   (KRaft mode)  │
                                                       └────────┬────────┘
                         ┌──────────────────────┬───────────────┼───────────────────┐
                         │                      │               │                   │
              ┌──────────▼──────┐  ┌────────────▼───┐  ┌────────▼──────────────────┐│
              │   Kafka UI      │  │   Streamlit    │  │        ClickHouse         ││
              │  (monitoring)   │  │   dashboard    │  │ Kafka engine → bronze     ││
              └─────────────────┘  └────────────────┘  │ → silver (upserts) → gold │◄┘
                                                       └───────────────────────────┘
```

**How it works:**
1. PostgreSQL writes all changes to its **WAL (Write-Ahead Log)** in `logical` mode.
2. Debezium reads the WAL via a **replication slot** and **publication**, packaging each change as a JSON event.
3. Events land on **Kafka topics** (`cdc.ecommerce.<table>`), partitioned and retained for downstream consumers.
4. **ClickHouse** consumes the topics with its Kafka table engine and materialized views. Every event lands in `bronze`, `silver` holds the current state of each table, and `gold` exposes the business marts. See [Data Warehouse](#data-warehouse-clickhouse).
5. The **CDC Control Center** web app drives the data generator, tails Kafka for a live event feed, and charts the `gold` marts.

> Measured locally, a commit in PostgreSQL shows up in ClickHouse with a p50 of about 0.8 s and a p95 of about 1.8 s.

---

## Tech Stack

| Service | Image / Tool | Port | Role |
|---------|-------------|------|------|
| PostgreSQL 15 | `postgres:15` | `5432` | Source database — logical replication enabled |
| Kafka | `confluentinc/cp-kafka:latest` | `9092` / `29092` | Message broker — KRaft mode (no Zookeeper) |
| Debezium | `debezium/connect:2.7.3.Final` | `8083` | CDC engine — bridges Postgres WAL to Kafka |
| Kafka UI | `provectuslabs/kafka-ui:latest` | `8080` | Web UI to inspect topics and messages |
| Adminer | `adminer:latest` | `8081` | Lightweight PostgreSQL web UI |
| Dashboard | `./dashboard` (Streamlit) | `8501` | Lightweight CDC event dashboard from Kafka |
| ClickHouse | `clickhouse/clickhouse-server:25.8` | `8123` / `9000` | Data warehouse — Kafka engine ingestion, bronze/silver/gold |
| Control Center | `./webapp` (React + FastAPI) | `8000` | Simulator controls, live CDC feed, warehouse analytics |
| Source API | `./source_api` (FastAPI) | `8100` | **Only writer to PostgreSQL**: business commands, outbox events, idempotency. Docs at `/docs` |
| Simulator | `./simulator` (FastAPI + httpx) | `8200` | Simulated users; changes data only by calling the Source API |
| Validator | `./validation` (profile `tools`) | — | API tests and data-quality / reconciliation checks, run on demand |

*Coming in the roadmap:* **Apache Flink** (stream processor), **Apache Iceberg** (table format), **MinIO** (S3-compatible object store).

> ClickHouse must be 25.x or newer: `cp-kafka:latest` is now Kafka 4.x, which removed the old protocol versions that the librdkafka bundled in ClickHouse 24.x uses. The symptom is `Required feature not supported by broker`.

---

## Data Model

Schema `ecommerce` in database `enterprise_db` models a **multi-seller marketplace** in the style of Shopee or TikTok Shop. It covers many categories: phones, laptops, fashion, beauty, home, mother & baby, groceries. Money is `BIGINT` VND, and prices include VAT. No business rows are seeded by SQL: the simulator creates everything through the Source API.

```
ecommerce
├── customers, customer_addresses          buyers (segment, loyalty tier, lifetime spend) and address book
├── sellers, shops                          shop tier MALL / PREFERRED / NORMAL, ship-from province, commission, penalty points
├── categories, brands                      two-level category tree, brands
├── products, product_skus, price_history   listings per shop; price and weight per SKU; every price change
├── inventory, inventory_movements          stock per SKU (on_hand / reserved / available) + append-only stock ledger
├── carts, cart_items                       one ACTIVE cart per buyer; removing a line is a hard DELETE
├── checkouts                               one payment of a buyer, split into one order per shop
├── orders, order_items                     10-status lifecycle, money breakdown, milestone timestamps, price snapshot per line
├── order_status_history                    append-only: every move with its actor (BUYER/SELLER/PAYMENT/LOGISTICS/SYSTEM)
├── payments, refunds                       online attempts per checkout, COD collection per order; refunds
├── vouchers, voucher_usages                platform (fixed / percent / free-shipping) and shop vouchers, with limits
├── campaigns, flash_sale_items             flash sales and mega sales: sale price and quota per SKU
├── shipments, shipment_events              forward and return parcels (GHN, GHTK, SPX, J&T, VTP) and carrier tracking
├── returns, return_items                   return / refund requests, disputes, inspection
├── seller_ledger_entries, seller_payouts   seller wallet ledger (sale, commission, payment fee, payout)
├── product_reviews                         1–5 star reviews; shop rating aggregates on shops
├── outbox_events                           domain events (Outbox Event Router → events.<aggregate>)
└── idempotency_keys, sim_clock, cdc_heartbeat, debezium_signals   infrastructure, not streamed
```

**Business clock.** `ecommerce.sim_clock` and `ecommerce.business_now()` run business time at `speed` × wall-clock time.

- Every business timestamp, deadline and background rule uses business time: payment window 15 min, packing SLA 2 days, auto-complete 3 days after delivery, return answer 2 days, daily payouts.
- At ×240, one business day passes in 6 real minutes, so a demo shows full lifecycles while the rules keep their real durations.
- Debezium still records the wall-clock commit time, so pipeline lag stays in real seconds.

Change the speed from the Control Center or with `PUT /admin/clock`.

Order lifecycle: `UNPAID → TO_SHIP → READY_TO_SHIP → SHIPPED → DELIVERED → COMPLETED`, with these branches:

- `CANCELLED`: by the buyer, the seller, a payment timeout or a breach of the packing SLA.
- `DELIVERY_FAILED`: redelivery up to 3 times, then `RETURNED_TO_SELLER`.
- `RETURN_REFUND`: the buyer asked to return items after delivery.
  - The order stays here when everything is refunded.
  - It goes on to `COMPLETED` when the return is rejected or only part of it is refunded.

COD checkouts start at `TO_SHIP`.

Debezium streams every table except the infrastructure ones. Its `table.include.list` is a regex, so new tables are picked up without config changes.

---

## Project Structure

```
Real-time CDC/
├── docker-compose.yml
├── postgres/
│   ├── init.sql                  # marketplace schema, outbox, publication (no business seed data)
│   ├── postgresql.conf           # logical replication settings
│   └── pg_hba.conf
├── debezium/
│   └── postgres-connector.json   # connector config
├── source_api/                   # FastAPI: the only writer to PostgreSQL (commands + outbox + idempotency)
├── simulator/                    # traffic simulator, config.yaml holds scale presets and odds
├── validation/                   # pytest API tests + check_data.py (invariants, write path, reconciliation)
├── clickhouse/
│   ├── gen_silver.py             # generates init/02-silver.sql from a table spec
│   ├── config.d/kafka.xml        # librdkafka settings (earliest offset, fast topic discovery)
│   └── init/
│       ├── 01-bronze.sql         # Kafka engine table + raw changelog
│       ├── 02-silver.sql         # current-state tables (ReplacingMergeTree)
│       ├── 03-gold.sql           # business marts (views)
│       └── 04-domain-events.sql  # outbox events → bronze.domain_events
├── webapp/
│   ├── Dockerfile                # node build → python runtime (build context: repo root)
│   ├── backend/                  # FastAPI: generator, Kafka tail, ClickHouse queries
│   └── frontend/                 # React + Vite + Tailwind + Recharts
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

The easiest way is the **Control Center** at http://localhost:8000. Press **Start simulation** and adjust the tick interval and orders per tick with the sliders, or fire single business events (place order, advance orders, cancel, restock…) from **Manual actions**. If the Debezium connector is not registered yet, the pipeline bar at the top offers a one-click **register** link.

Or drive the simulator's API directly:

```bash
curl -X POST localhost:8200/start -H 'Content-Type: application/json' \
  -d '{"interval": 1, "orders_per_tick": 6, "new_customers_per_tick": 2}'   # orders_per_tick = buyer sessions
curl -X POST localhost:8200/stop
```

### 4. Open the UIs

| UI | URL | Credentials |
|----|-----|-------------|
| **CDC Control Center** | http://localhost:8000 | — |
| Source API docs | http://localhost:8100/docs | — |
| ClickHouse Play (SQL) | http://localhost:8123/play | User: `admin` · Password: `admin` |
| Kafka UI | http://localhost:8080 | — |
| Adminer (DB) | http://localhost:8081 | Server: `postgres` · User: `admin` · Password: `admin` · DB: `enterprise_db` |
| CDC Dashboard | http://localhost:8501 | — |
| Debezium API | http://localhost:8083 | — |

---

## Kafka Topics

| Topic | Content |
|-------|---------|
| `cdc.ecommerce.<table>` | Row changes of each business table (17 tables) |
| `events.<aggregate>` | Domain events from the outbox: `events.order`, `events.payment`, `events.checkout`, `events.cart`, `events.customer`, `events.shop`, `events.product`, `events.inventory`, `events.catalog` |
| `cdc.transaction` | Debezium transaction boundaries (one transaction = one business command) |
| `debezium-config` / `-offsets` / `-status` | Kafka Connect internals |

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

## Source API, simulator and validation

Data changes only through the **Source API** (OpenAPI docs at http://localhost:8100/docs). Each endpoint that writes is one business command:

- It runs in one transaction.
- It enforces the marketplace rules.
- It writes a domain event to the outbox.
- It accepts an `Idempotency-Key` header. Webhook senders pass their own event id, so a redelivered callback is replayed, never applied twice.

| Area | Endpoints |
|------|-----------|
| Master data | `POST /admin/categories`, `POST /admin/brands` |
| Seller center | `POST /sellers` (seller + shop), `POST /shops/{id}/products` (product + SKUs + stock), `POST /skus/{id}/restock`, `POST /orders/{id}/pack`, `POST /orders/{id}/seller-cancel` |
| Buyer | `POST /customers`, `POST /customers/{id}/addresses`, `POST/PATCH/DELETE /carts/items`, `POST /checkouts`, `POST /checkouts/{id}/payments` (retry), `POST /orders/{id}/cancel`, `POST /orders/{id}/confirm-received` |
| Promotions | `POST /admin/vouchers`, `POST /shops/{id}/vouchers`, `POST /admin/campaigns` (flash / mega sale with SKU quotas), `PATCH /skus/{id}/price` |
| Returns & reviews | `POST /orders/{id}/returns`, `POST /returns/{id}/approve\|reject\|inspect\|dispute`, `POST /admin/returns/{id}/resolve`, `POST /orders/{id}/reviews` |
| Partners | `POST /payments/webhook`, `POST /logistics/webhook` (tracking events by tracking number and carrier event id) |
| Clock | `GET /clock`, `PUT /admin/clock` |
| Queries | `GET /skus`, `/orders`, `/orders/{id}` (history, parcels, return), `/checkouts/{id}`, `/carts`, `/customers`, `/shops`, `/shipments`, `/returns`, `/vouchers`, `/campaigns`, `/stats` |

Rules enforced by the API:

- **Stock:** checkouts lock stock rows in a fixed order, so concurrent checkouts can't oversell.
- **Checkout:** a checkout splits into one order per shop. Shipping is priced from a rate card by distance (same province, same region or another region) and weight.
- **Payments:**
  - Online orders wait in `UNPAID` for the payment webhook.
  - A failed attempt can be retried.
  - A late or superseded success is refunded automatically.
  - COD is collected on delivery.
- **Stock ledger:** every stock change goes through one function and writes one `inventory_movements` row: listing, restock, reserve, release, ship, return to sender, customer return.
- **Promotions:** a flash price applies only while quota lasts. Vouchers are checked for their window, minimum spend, total limit and per-customer limit, and stacking is limited to one per type. Discounts are split down to order lines, so a partial return refunds exactly what was paid per unit. Vouchers and quota come back when an unpaid checkout ends.
- **Carriers:** callbacks are deduplicated by carrier event id. A late event older than the parcel's latest one is stored but not applied, and scan events may be skipped.
- **Returns and settlement:**
  - Returns are refunded at the net price paid, and sellable items go back into stock.
  - A completed order settles the seller's wallet with the sale minus the shop voucher, then commission and a 2 % payment fee. Wallets are paid out daily.
- **Moves and actors:** every status change goes through one state machine, which checks both the move and who makes it, for example only the carrier can mark an order delivered.
- **Cancellations:** cancelling releases the reservation and refunds a paid order. A parcel returned to the seller goes back on the shelf.
- **Background jobs** (run as `SYSTEM`):
  - unpaid checkouts expire;
  - orders past the seller's packing SLA are cancelled and the shop gets a penalty point;
  - delivered orders auto-complete;
  - idle carts are marked abandoned.

  - returns the seller doesn't answer in time are auto-approved;
  - rejected returns the buyer doesn't dispute are closed;
  - seller wallets are paid out once a business day.

  Windows are in business time and set by env, for example `PAYMENT_WINDOW_S`, `SELLER_SLA_S`, `AUTO_COMPLETE_S`.

The **simulator** has no database driver and runs on the business clock. Every behaviour is a rate per business hour. Buyer arrivals follow a Poisson process that changes with the hour of day (lunch and evening peaks), weekends and campaigns. SKU popularity is Zipf-distributed. It plays these actors, configured in `simulator/config.yaml`:

- **Buyers:** they fill carts, edit them, check out or leave the cart, retry a failed payment, cancel, and confirm receipt.
- **Mock payment gateway:** its webhooks can be delayed, fail, arrive twice, or arrive after the payment window.
- **Sellers:** they pack at a speed set by shop tier, and about 10 % of NORMAL shops are lazy and miss the SLA. They also cancel on stock-outs and restock.
- **Mock carriers:** tracking events with transit times by distance; failed attempts, redelivery and return to sender; duplicated callbacks and late, out-of-order scans.
- **Buyer segments:** new, regular, bargain hunter, loyal, business. They differ in activity, checkout rate and voucher use, and they ask for returns, dispute rejections and post reviews.
- **Platform operations:** daily platform vouchers, three flash-sale slots a day (0h, 12h, 20h), dispute resolution, and the **11.11 mega sale** (traffic ×3 and ×8 in the first hour) started from the Control Center.

On start the simulator seeds the scale preset through the API (`SIM_SCALE=small|medium|large`, default medium):

| Preset | Shops | SKUs | Customers |
|--------|-------|------|-----------|
| small | 10 | 200 | 500 |
| medium | 50 | 1,000 | 5,000 |
| large | 200 | 5,000 | 30,000 |

Presets are free to edit in the YAML.

Validation, with the simulator stopped:

```bash
docker compose --profile tools run --rm validator pytest -q          # marketplace rules, 15 scenarios
docker compose --profile tools run --rm validator python check_data.py
```

`check_data.py` reads PostgreSQL in one consistent snapshot and exits non-zero on any failure. It runs four groups of checks:

- **Money:** totals of lines, orders and checkouts; one kept payment per paid checkout; refunds; COD collections; spend and tier.
- **State:** stock balance and reservations; checkout status against order status; milestone timestamps; time rules applied on schedule.
- **Stock:** inventory equals the ledger, and each movement matches the order or return that caused it.
- **Logistics:** parcel status matches the order or return, and ignored tracking events really were stale.
- **Promo:** voucher counters, limits, windows and allocation; flash quota and prices; price at order time against `price_history`.
- **Returns and settlement:** refund amounts and the order outcome of each return; seller ledger entries per completed order, wallet balances, payouts and shop ratings.
- **Write path:** one creation event per row; status history replays through allowed moves and actors.
- **Pipeline:** PostgreSQL against `silver` FINAL for all 30 tables, and the outbox against `bronze.domain_events`.

---

## Data Warehouse (ClickHouse)

ClickHouse reads Kafka directly, so the warehouse needs no extra stream processor or sink connector. The SQL in `clickhouse/init/` runs automatically the first time the `clickhouse_data` volume is created.

```
Kafka cdc.ecommerce.*
   │  Kafka engine (bronze.kafka_cdc_events, JSONAsString)
   ▼  MV bronze.mv_cdc_events
bronze.cdc_events            append-only changelog: op, lsn, before/after JSON, Kafka offsets
   │  MV silver.mv_<table>   (one per source table)
   ▼
silver.<table>               current state, ReplacingMergeTree(_version = WAL LSN, _is_deleted)
   │  views (read with FINAL)
   ▼
gold.*                       business marts
```

| Layer | Objects | Notes |
|-------|---------|-------|
| bronze | `cdc_events` | Every Debezium event, partitioned by day. Use it for audit, replay and lag metrics. |
| bronze | `domain_events` | Outbox events from `events.*` (regex subscription), with their event ids. |
| silver | One table per source table (17). Generated by `clickhouse/gen_silver.py` from a compact spec. | Typed columns (UUID, `Int64` VND, `DateTime64`) plus `_version`, `_is_deleted`, `_synced_at`. Append-only sources such as `order_status_history` are de-duplicated by key. |
| gold | `daily_sales`, `traffic_by_hour`, `order_status_summary`, `funnel`, `product_sales`, `category_sales`, `shop_performance`, `payment_summary`, `voucher_performance`, `flash_sale_performance`, `delivery_performance`, `returns_summary`, `seller_settlement`, `platform_pnl`, `inventory_risk`, `customer_tiers`, `cdc_freshness` | Plain views, so they always reflect the latest silver state. |

**How upserts and deletes work.** Each event is inserted into silver as a new row whose `_version` is the event's PostgreSQL WAL LSN. LSNs always increase, so replays and duplicates are harmless: the highest version per primary key wins. A delete event inserts the row taken from `before` (possible because of `REPLICA IDENTITY FULL`) with `_is_deleted = 1`. ClickHouse collapses versions during background merges, so **always query silver with `FINAL`** to get exact current state.

Example queries (in http://localhost:8123/play or `docker exec -it cdc-clickhouse clickhouse-client -u admin --password admin`):

```sql
SELECT * FROM gold.daily_sales;
SELECT * FROM gold.product_sales LIMIT 10;
SELECT * FROM gold.cdc_freshness;                       -- per-table lag, last 15 min

-- Full change history of one order
SELECT op, source_ts, JSONExtractString(after, 'status') AS status
FROM bronze.cdc_events
WHERE source_table = 'orders' AND JSONExtractString(if(op = 'd', before, after), 'id') = '<order-uuid>'
ORDER BY lsn;
```

Reconciliation check (counts and revenue must match PostgreSQL exactly):

```sql
SELECT count(), sum(total), countIf(status = 'COMPLETED') FROM silver.orders FINAL;
```

---

## CDC Control Center (web app)

`webapp/` is a single container serving a React UI and a FastAPI backend on port `8000`.

| Area | What it does |
|------|--------------|
| Pipeline bar | Health of Simulator → Source API → PostgreSQL (orders, replication-slot lag) → Debezium (connector state, one-click register) → Kafka (events/s) → ClickHouse (events stored). |
| Traffic simulator | Business clock and speed (×60 to ×1440); **Flash sale now** and **Start 11.11** scenarios; tune buyer sessions and new customers per business hour; fire actions ×1/×5/×20; seeding progress and the activity log. |
| Live CDC stream | Events/s stacked by operation, busiest tables, and a live feed across all tables. Click an UPDATE for the field-level before → after diff. |
| Warehouse analytics | GMV, orders, AOV, buyers; GMV per business hour; platform take rate, promotion cost, refunds, payouts; carriers; returns by reason; vouchers; flash sales; seller wallets; order pipeline (10 statuses); purchase funnel; payments by method (success rate, refunds); revenue by category; top products; shop scorecard (cancel rate, SLA breaches, time to pack); inventory risk; customer tiers; layer row counts and end-to-end lag. |

API (used by the UI): `GET /api/health`, `GET /api/generator`, `POST /api/generator/start|stop|seed`, `PUT /api/generator/config`, `POST /api/actions/{order|cart|advance|customer|cancel|restock|flash_sale|mega_sale}`, `PUT /api/clock`, `GET /api/cdc?since=`, `GET /api/warehouse`, `POST /api/connector/register`.

Local development with hot reload (with the rest of the stack running in Docker):

```bash
# backend
cd webapp/backend
pip install -r requirements.txt
KAFKA_BROKER=localhost:29092 SIMULATOR_URL=http://localhost:8200 SOURCE_API_URL=http://localhost:8100 uvicorn main:app --reload --port 8000

# frontend (proxies /api to :8000)
cd webapp/frontend
npm install && npm run dev       # http://localhost:5173
```

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
"decimal.handling.mode":    "string"
```

`FULL` replica identity ensures `before` values are always present on UPDATE/DELETE.
`decimal.handling.mode = string` sends `DECIMAL` columns as exact strings (`"109.99"`), so money is never rounded through floating point on its way to ClickHouse's `Decimal` columns.

### Kafka — KRaft mode (no Zookeeper)

Two listeners are configured:
- `PLAINTEXT://kafka:9092` — for inter-container communication (Debezium, Dashboard in Docker)
- `PLAINTEXT_HOST://localhost:29092` — for connections from the host machine

---

## Reset

Full reset — wipes all data and volumes:

```bash
docker compose down -v           # -v also drops the kafka_data and clickhouse_data volumes
sudo rm -rf postgres/data        # owned by the container's postgres user
docker compose up -d
# Re-register the connector (step 2 above, or the "register" link in the Control Center)
```

To rebuild only the warehouse, for example after editing `clickhouse/init/*.sql`, run `docker compose rm -sf clickhouse && docker volume rm <project>_clickhouse_data && docker compose up -d clickhouse`. The new tables re-ingest everything still retained in Kafka.

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
- Source API (only writer, marketplace rules, outbox, idempotency) and an agent-based traffic simulator (buyers, sellers, mock payment gateway and carrier).
- Validation suite: API scenario tests and data-quality / reconciliation checks.
- ClickHouse warehouse with bronze/silver/gold layers fed directly from Kafka, with correct upsert/delete handling.
- CDC Control Center web app: generator control, live CDC feed, warehouse analytics.

ClickHouse already serves the real-time analytics use case. The lakehouse milestones below add an open-format, object-storage copy (Iceberg) for long-term retention and other engines.

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
| Source API + simulator (local) | **ECS Fargate** services | Simulator scaled down or off in production-like environments |

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
