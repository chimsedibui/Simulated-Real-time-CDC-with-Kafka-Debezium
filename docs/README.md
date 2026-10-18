# Real-time CDC Pipeline

A production-style **Change Data Capture (CDC)** pipeline that streams every INSERT, UPDATE, and DELETE from PostgreSQL through Kafka into a **ClickHouse data warehouse** in about a second, with a web **Control Center** to generate traffic and watch each change as it happens. Business analytics and platform health live in **Grafana**, with Prometheus alerts sent to Google Chat.

---

## Architecture

```
┌────────────────┐  commands   ┌──────────────┐  one transaction per command  ┌──────────────────┐
│   Simulator    │ ──────────► │  Source API  │ ────────────────────────────► │  PostgreSQL 15   │
│ buyers, sellers│  (HTTP)     │  rules, lock │  + outbox row                 │ schema ecommerce │
│ gateway,carrier│             │  order, idem.│                               └────────┬─────────┘
└───────▲────────┘             └──────────────┘                                        │ WAL (pgoutput)
        │ start / stop / speed                                                ┌────────▼─────────┐
┌───────┴──────────────┐                                                      │     Debezium     │
│  CDC Control Center  │                                                      │  Kafka Connect   │
│  React + FastAPI     │◄── live feed ─────────────────────────────┐          └────────┬─────────┘
│  (localhost:8000)    │                                           │                   │ cdc.ecommerce.<table>
└───────┬──────────────┘                                  ┌────────┴─────────┐         │ events.<aggregate>
        │ health                                          │  Kafka (KRaft)   │◄────────┘ cdc.transaction
        │                                                 └────────┬─────────┘
┌───────▼─────────────────────────────────────────────────────────▼──────────────────────────────┐
│ ClickHouse  bronze ─► silver ─► consistent(v) ─► gold(v) ─► mart          ops (reconciliation)   │
└───────┬──────────────────────────────────────────────────────▲──────────────────────────────────┘
        │ reads marts (grafana_reader)                         │ marts, business-day close, reconciliation
┌───────▼──────────────┐                              ┌────────┴─────────┐
│  Grafana  :3000      │◄── Prometheus :9090          │  warehouse-jobs  │◄── reads PostgreSQL (read-only)
│  business, platform  │    exporters → alerts → Chat └──────────────────┘
└──────────────────────┘
```

**How it works:**
1. The **simulator** plays buyers, sellers, the payment gateway and carriers. It changes data only through the **Source API**, which applies the marketplace rules and writes each command in one PostgreSQL transaction, with a domain event in the outbox.
2. **Debezium** reads the WAL through a replication slot and publishes every row change to `cdc.ecommerce.<table>`, domain events to `events.<aggregate>`, and one END record per transaction to `cdc.transaction`. Personal data is hashed before it leaves PostgreSQL.
3. **ClickHouse** consumes Kafka directly: `bronze` keeps every event, `silver` the current state of each table, `consistent.<table>(v)` silver as of a commit watermark, and `gold.<view>(v)` the business marts. See [Data Warehouse](#data-warehouse-clickhouse).
4. **warehouse-jobs** precomputes gold into `mart` tables at one watermark, freezes finished business days, and reconciles ClickHouse against PostgreSQL every 30 seconds.
5. The **CDC Control Center** drives the simulator and the business clock, tails Kafka for a live feed, and links to the Grafana dashboards (`cdc-business`, `cdc-platform`).

> Checkout p99 at an 11.11 peak (×1440, ~250 API requests/s): 162 ms over 5 minutes, 223 ms in the worst minute, against a 300 ms SLO.
>
> Measured on a 16-minute run at ×720 with an 11.11 sale and restarts of Debezium and ClickHouse: a commit reaches ClickHouse in 1.2 s at p50 and 2.3 s at p95; the busiest minute carried 875 events/s at a p95 of 1.8 s.

---

## Tech Stack

| Service | Image / Tool | Port | Role |
|---------|-------------|------|------|
| PostgreSQL 15 | `postgres:15` | `5432` | Source database — logical replication enabled |
| Kafka | `confluentinc/cp-kafka:8.3.2` | `9092` / `29092` | Message broker — KRaft mode (no Zookeeper) |
| Debezium | `debezium/connect:2.7.3.Final` | `8083` | CDC engine — bridges Postgres WAL to Kafka |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | `8080` | Web UI to inspect topics and messages |
| Adminer | `adminer:6.1.2` | `8081` | Lightweight PostgreSQL web UI |
| ClickHouse | `clickhouse/clickhouse-server:25.8` | `8123` / `9000` | Data warehouse — Kafka engine ingestion, bronze/silver/gold |
| SeaweedFS | `chrislusf/seaweedfs:4.47` | internal | S3-compatible bucket for bronze's cold volume |
| Control Center | `apps/control-center` (React + FastAPI) | `8000` | Simulator controls, live CDC feed, links to Grafana |
| Source API | `source/source-api` (FastAPI, 2 Uvicorn workers) | `8100` | **Only writer to PostgreSQL**: business commands, outbox events, idempotency. Docs at `/docs`, Prometheus metrics at `/metrics` |
| Simulator | `source/simulator` (FastAPI + httpx) | `8200` | Simulated users; changes data only by calling the Source API |
| Warehouse jobs | `warehouse/jobs` (Python) | `9109` (metrics, internal) | Marts, business-day close, reconciliation at a pinned watermark |
| Prometheus | `prom/prometheus:v3.2.1` | `127.0.0.1:9090` | Metrics (15 days) and 31 alert rules |
| Alertmanager | `prom/alertmanager:v0.28.1` | `127.0.0.1:9093` | Groups alerts, sends them to the chat-webhook bridge |
| Grafana | `grafana/grafana:12.3.0` | `127.0.0.1:3000` | Business Analytics (marts) and Platform Health (Prometheus) dashboards |
| Exporters | `observability/service`, `kafka-exporter`, `node-exporter` | internal | PostgreSQL / ClickHouse / Debezium metrics, Kafka offsets and lag, host CPU, memory, disk |
| chat-webhook | `observability/service` (`bridge.py`) | internal | Formats alerts for Google Chat; the webhook URL is a gitignored secret file |
| Validator | `quality` (profile `tools`) | — | API and stress tests, data checks, torn-read probe, run on demand |

> ClickHouse must be 25.x or newer: `cp-kafka:8.3.2` is Kafka 4.x, which removed the old protocol versions that the librdkafka bundled in ClickHouse 24.x uses. The symptom is `Required feature not supported by broker`.

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
├── voucher_quota_slots                     each voucher's redemption quota split into up to 16 rows (not streamed)
├── campaigns, flash_sale_items             flash sales and mega sales: sale price and quota per SKU
├── shipments, shipment_events              forward and return parcels (GHN, GHTK, SPX, J&T, VTP) and carrier tracking
├── returns, return_items                   return / refund requests, disputes, inspection
├── seller_ledger_entries, seller_payouts   seller wallet ledger (sale, commission, payment fee, payout), insert-only
├── shop_wallets, wallet_pending            wallet balance, rolled up from the ledger in batches (pending rows not streamed)
├── product_reviews                         1–5 star reviews; shop rating is computed from them
├── outbox_events                           domain events (Outbox Event Router → events.<aggregate>); always empty after commit
└── idempotency_keys, sim_clock, cdc_heartbeat, debezium_signals   infrastructure, not in the warehouse (`debezium_signals` is captured but not modelled); idempotency keys expire after 72 h
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

Folders follow responsibility groups. Each group has its own `compose.yml`; the root `docker-compose.yml` only includes them.

```
Real-time CDC/
├── docker-compose.yml            # includes the groups below (project name: realtime-cdc)
├── source/                       # the system being captured
│   ├── postgres/                 # init.sql (schema, outbox, publication, cdc_user), postgresql.conf, pg_hba.conf;
│   │                             #   also runs observability/setup-postgres.sql (monitor_reader)
│   ├── source-api/               # FastAPI: the only writer to PostgreSQL (commands + outbox + idempotency)
│   ├── simulator/                # traffic simulator; config.yaml holds scale presets and odds
│   └── compose.yml               # postgres, source-api, simulator, adminer
├── kafka/
│   └── compose.yml               # broker (KRaft), Kafka UI
├── debezium/
│   ├── postgres-connector.json   # connector config (password via ${env:…}, PII salt filled in on register)
│   ├── register.sh               # create or update the connector
│   └── compose.yml               # Kafka Connect + Debezium
├── warehouse/
│   ├── clickhouse/
│   │   ├── gen_silver.py         # silver spec → init/02-silver.sql (silver, consistent views, watermark), 07-snapshots.sql
│   │   ├── check_contract.py     # data contract: source schema vs silver spec vs connector
│   │   ├── config.d/kafka.xml    # librdkafka settings
│   │   └── init/                 # 01-bronze, 02-silver, 03-gold, 04-domain-events, 05-ops, 06-monitoring (read-only users), 07-snapshots, 08-cold-storage
│   ├── jobs/                     # warehouse-jobs: snapshots, marts, business-day close, reconciliation
│   ├── seaweedfs-start.sh        # S3 identity and the bronze-cold bucket
│   └── compose.yml               # clickhouse, warehouse-jobs
├── apps/
│   ├── control-center/           # React + FastAPI: simulator control, live CDC feed, Grafana links
│   └── compose.yml
├── quality/                      # API + stress tests, check_data.py, probe_consistency.py, gate.sh
│   └── compose.yml               # validator (profile "tools")
├── observability/                # Prometheus (alerts.yml + tests), Alertmanager, Grafana (provisioning, dashboards,
│   │                             #   generate_dashboards.py), service/ (platform-exporter, chat-webhook), start.sh
│   └── compose.yml
├── .env.example, setup-env.sh   # secrets template; setup-env.sh writes the gitignored .env with random values
├── docs/
└── .github/workflows/            # CI: data contract; observability config, alert tests, dashboards; smoke gate
```

Monitoring setup, dashboards and alert runbooks are in [observability/README.md](../observability/README.md).

---

## Quick Start

### 1. Start the stack

Every password and the PII salt live in a gitignored `.env`. Create it once; `setup-env.sh` fills each value in `.env.example` with a random secret:

```bash
./setup-env.sh
```

The chat-webhook service mounts the Google Chat webhook URL from `observability/secrets/google-chat-webhook` (gitignored). Put the URL there, or create an empty file to run without Chat notifications:

```bash
touch observability/secrets/google-chat-webhook
docker compose up -d
```

On existing volumes (init scripts only run on fresh ones), or after changing `.env`, run `./observability/start.sh`: it applies the accounts and passwords, the Grafana admin password and the connector config. Wait ~30 seconds for all services to become healthy:

```bash
docker compose ps
```

### 2. Register the Debezium connector

This step is **required** — without it Kafka receives nothing.

```bash
debezium/register.sh
```

It creates or updates the connector from `debezium/postgres-connector.json`, filling in `PII_SALT` from `.env`. The database password is not in the config at all: `${env:CDC_DB_PASSWORD}` is resolved by the Connect worker from its environment.

Verify it is running:

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

### 3. Generate data

The easiest way is the **Control Center** at http://localhost:8000. Press **Start simulation** and adjust buyer sessions and new customers per business hour and the tick interval with the sliders, or fire single business events (place order, advance orders, cancel, restock…) from **Manual actions**. If the Debezium connector is not registered yet, the pipeline bar at the top offers a one-click **register** link.

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
| ClickHouse Play (SQL) | http://localhost:8123/play | User: `admin` · Password: `CLICKHOUSE_PASSWORD` in `.env` |
| Kafka UI | http://localhost:8080 | — |
| Adminer (DB) | http://localhost:8081 | Server: `postgres` · User: `admin` · Password: `POSTGRES_PASSWORD` in `.env` · DB: `enterprise_db` |
| Debezium API | http://localhost:8083 | — |
| **Grafana** | http://localhost:3000 (`/d/cdc-business`, `/d/cdc-platform`) | User: `admin` · Password: `GRAFANA_ADMIN_PASSWORD` in `.env` |
| Prometheus | http://localhost:9090 | — |
| Alertmanager | http://localhost:9093 | — |

---

## Kafka Topics

| Topic | Content |
|-------|---------|
| `cdc.ecommerce.<table>` | Row changes of each business table (31 tables) |
| `events.<aggregate>` | Domain events from the outbox: `events.order`, `events.payment`, `events.checkout`, `events.cart`, `events.customer`, `events.shop`, `events.product`, `events.inventory`, `events.catalog`, `events.promotion`, `events.return`, `events.review`, `events.shipment` |
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
| Buyer | `POST /customers`, `POST /customers/{id}/addresses`, `POST /customers/{id}/deactivate`, `POST /carts/items`, `PATCH/DELETE /carts/items/{item_id}`, `POST /checkouts`, `POST /checkouts/{id}/payments` (retry), `POST /orders/{id}/cancel`, `POST /orders/{id}/confirm-received` |
| Promotions | `POST /admin/vouchers`, `POST /shops/{id}/vouchers`, `POST /admin/campaigns` (flash / mega sale with SKU quotas), `PATCH /skus/{id}/price` |
| Returns & reviews | `POST /orders/{id}/returns`, `POST /returns/{id}/approve\|reject\|inspect\|dispute`, `POST /admin/returns/{id}/resolve`, `POST /orders/{id}/reviews` |
| Partners | `POST /payments/webhook`, `POST /logistics/webhook` (tracking events by tracking number and carrier event id) |
| Clock | `GET /clock`, `PUT /admin/clock` |
| Queries | `GET /skus`, `/inventory/low-stock`, `/orders`, `/orders/{id}` (history, parcels, return), `/checkouts/{id}`, `/carts`, `/carts/active`, `/customers`, `/customers/{id}`, `/shops`, `/categories`, `/brands`, `/shipments`, `/shipments/{tracking_number}`, `/returns`, `/returns/{id}`, `/vouchers`, `/campaigns`, `/campaigns/{id}/items`, `/stats` |
| Ops | `GET /health`, `GET /metrics` (Prometheus) |

Rules enforced by the API:

- **Locks and hot rows:** every transaction takes row locks in one fixed order (voucher quota slots → inventory by SKU → flash-sale items), so concurrent checkouts can't oversell and never deadlock with cancellations or payment timeouts that give vouchers back. Rows that every transaction would otherwise update are split, batched or locked late:
  - a checkout only reads stock and flash quota; it takes them at its very end with conditional updates, so a hot SKU in a flash sale is locked for milliseconds. If the quota ran out meanwhile, the checkout fails with `flash_sold_out` and the retry pays the regular price;
  - a voucher's quota is spread over up to 16 slot rows; a checkout takes a random slot and skips busy ones, so 11.11 traffic on one platform voucher doesn't queue on a single row (the per-customer limit is kept with a per-customer advisory lock);
  - completing an order only inserts ledger entries; a job adds them to `shop_wallets` in batches, and payouts roll up their shop first;
  - shop ratings are computed from `product_reviews` instead of counters on `shops`.
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
- **Outbox:** each domain event row is inserted and deleted in the same transaction. Debezium reads the insert from the WAL and ignores the delete, so the table never grows.
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

The **simulator** has no database driver and runs on the business clock. Every behaviour is a rate per business hour. Buyer arrivals follow a Poisson process that changes with the hour of day (lunch and evening peaks), weekends and campaigns. SKU popularity is Zipf-distributed. It plays these actors, configured in `source/simulator/config.yaml`:

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
docker compose --profile tools run --rm validator pytest -q          # marketplace rules (25 tests) + lock-contention stress test
docker compose --profile tools run --rm validator python check_data.py
```

`check_data.py` reads PostgreSQL in one consistent snapshot and exits non-zero on any failure. It runs these groups of checks:

- **Money:** totals of lines, orders and checkouts; one kept payment per paid checkout; refunds; COD collections; spend and tier.
- **State:** stock balance and reservations; checkout status against order status; milestone timestamps; time rules applied on schedule.
- **Stock:** inventory equals the ledger, and each movement matches the order or return that caused it.
- **Logistics:** parcel status matches the order or return, and ignored tracking events really were stale.
- **Promo:** voucher counters, limits, windows and allocation; flash quota and prices; price at order time against `price_history`.
- **Returns and settlement:** refund amounts and the order outcome of each return; seller ledger entries per completed order, wallet balances, payouts and shop ratings.
- **Write path:** one creation event per row; status history replays through allowed moves and actors; no deadlocks recorded in PostgreSQL (the API retries them, so clients would never notice).
- **Pipeline:** PostgreSQL against `silver` FINAL for all 31 tables; once settled, no open transactions and `consistent.*` equal to silver; the outbox against `bronze.domain_events`.
- **Metrics:** every gold metric (daily GMV/NMV, P&L, product revenue, pack time per shop, return cohorts) recomputed in PostgreSQL with the same definition and compared; closed business days must keep their placed GMV and GMV.
- **Ops:** marts at the current watermark equal gold computed live; a scheduled reconciliation run after settling matches every table; no email, phone or address in clear anywhere in ClickHouse.

**Gates.** `quality/gate.sh quick|smoke|full` runs the whole chain; pick the smallest one that covers the change. Every gate stops at the first PostgreSQL deadlock and prints it. CI runs `smoke` on every push or pull request that touches the pipeline (`.github/workflows/gate-smoke.yml`).

| Gate | Stack | What runs | Time |
|------|-------|-----------|------|
| `quick` | running stack, data kept, code rebuilt | API tests, lock-contention stress test, 90 s of traffic with the torn-read probe, `check_data` | ~3–4 min |
| `smoke` | clean | seed, tests, 4 min at ×1440 with an 11.11 sale, probe, `check_data` | ~8–10 min |
| `full` | clean | 16 min at ×720 with an 11.11 sale and restarts of Debezium and ClickHouse, probe, lag, `check_data` | ~25 min |

`quality/test_stress.py` sends checkouts, cancellations and payments from 24 buyers at the same few SKUs, flash items and vouchers for 20 s, then requires zero deadlocks and no drift in stock, flash quota or voucher quota. Lock-order bugs that ordinary traffic shows once in 100 000 requests show up there in seconds. Put local port overrides in `docker-compose.override.yml` (gitignored).

Torn reads can only be seen while data is flowing, so they have their own probe. Run it during a simulation:

```bash
docker compose --profile tools run --rm validator python probe_consistency.py --seconds 300
```

It polls cross-table invariants that hold inside one PostgreSQL transaction (completed order has its SALE entry, order line has its order, payout has its ledger entry, inventory = Σ stock ledger, …) through `silver FINAL` and through `consistent.*(v)` with one pinned watermark per poll, and fails if `consistent.*` ever breaks one.

---

## Data Warehouse (ClickHouse)

ClickHouse reads Kafka directly, so the warehouse needs no extra stream processor or sink connector. The SQL in `warehouse/clickhouse/init/` runs automatically the first time the `clickhouse_data` volume is created.

```
Kafka cdc.ecommerce.*
   │  Kafka engine (bronze.kafka_cdc_events, JSONAsString)
   ▼  MV bronze.mv_cdc_events
bronze.cdc_events            append-only changelog: op, lsn, before/after JSON, Kafka offsets
   │  MV silver.mv_<table>   (one per source table)
   ▼
silver.<table>               current state, ReplacingMergeTree(_version = WAL LSN, _is_deleted)
   │  views: silver FINAL minus transactions that have not fully landed
   ▼
consistent.<table>(v)        silver as of commit watermark v (Kafka cdc.transaction → bronze.cdc_transactions)
   │  parameterized views, same v for the whole query
   ▼
gold.*                       business marts
```

| Layer | Objects | Notes |
|-------|---------|-------|
| bronze | `cdc_events` | Every Debezium event, partitioned by day and kept forever: parts older than 30 days move to the cold volume (S3 on SeaweedFS). Use it for audit, replay and lag metrics. |
| bronze | `domain_events` | Outbox events from `events.*` (regex subscription), with their event ids. |
| silver | One table per source table (31). Generated by `warehouse/clickhouse/gen_silver.py` from a compact spec. | Typed columns (UUID, `Int64` VND, `DateTime64`) plus `_version`, `_is_deleted`, `_synced_at`. Append-only sources such as `order_status_history` are de-duplicated by key. |
| bronze | `cdc_transactions` | One row per committed transaction (Debezium transaction metadata): how many of its events should land in `cdc_events`, in commit order. |
| consistent | `watermark`, plus one parameterized view per silver table | What gold reads. See [Consistent reads](#consistent-reads-no-torn-transactions). |
| snap | One table per silver table: `consistent.<table>` as of the last watermark warehouse-jobs reached, typed columns only | Brought forward key by key; `gold_snap.<view>` is each gold view reading these instead of `consistent.*`. See [Incremental snapshots](#incremental-snapshots-r20). |
| gold | Parameterized views `(v)`: `order_facts`, `overview_kpis`, `daily_sales`, plus the table `daily_sales_closed`, and `traffic_by_hour`, `order_status_summary`, `funnel`, `product_sales`, `category_sales`, `shop_performance`, `payment_summary`, `voucher_performance`, `flash_sale_performance`, `delivery_performance`, `return_rate_by_cohort`, `returns_summary`, `seller_settlement`, `platform_pnl`, `inventory_risk`, `customer_tiers`, `cdc_freshness` (not parameterized) | Views over `consistent.*(v)`. `daily_sales_closed` is filled by the `warehouse-jobs` service once per finished business day. |

**How upserts and deletes work.** Each event is inserted into silver as a new row whose `_version` is the event's PostgreSQL WAL LSN. LSNs always increase, so replays and duplicates are harmless: the highest version per primary key wins. A delete event inserts the row taken from `before` (possible because of `REPLICA IDENTITY FULL`) with `_is_deleted = 1`. ClickHouse collapses versions during background merges, so **always query silver with `FINAL`** to get exact current state.

### Consistent reads (no torn transactions)

Every source table has its own Kafka topic, and ClickHouse consumes the topics independently. For a few seconds, `silver.orders` can show an order as COMPLETED while `silver.seller_ledger_entries` does not have the SALE entry written in the same PostgreSQL transaction yet. A probe under load measured 20 such orders at one moment.

`consistent.*` removes that window. It works like a database snapshot that the reader pins:

1. Debezium publishes one END record per transaction on `cdc.transaction` (commit order, one partition), with the number of events per table. It lands in `bronze.cdc_transactions`; every CDC event carries its `tx_id`.
2. `SELECT v FROM consistent.watermark` returns the commit position of the first transaction whose events have not all landed. Every transaction before `v` is complete. `v` depends only on landed data, so taking it is cheap.
3. `consistent.<table>(v = …)` is silver FINAL as of `v`. Rows of transactions at or after `v` are hidden. A state row whose newest version is past `v` is shown as it was before, using the `before` image (`REPLICA IDENTITY FULL`) from bronze, or the version's own `_before` column when it reached silver after bronze was read. Old versions cannot be read from silver itself, because merges drop them at any time.
4. Gold views take the same parameter and pass it down. A reader takes `v` once and uses it for every view in the query or page:

```sql
SELECT v, held_back, oldest_held_back_commit FROM consistent.watermark;   -- e.g. v = 431977
SELECT * FROM gold.daily_sales(v = 431977);
SELECT * FROM gold.platform_pnl(v = 431977);
```

Why a parameter: ClickHouse reads each table, and each subquery, at its own moment, and `now()` differs between subqueries. Views that each picked "the latest" state on their own disagreed inside one query, which the probe caught. A pinned `v` is the only choice every view makes identically. For the same reason the business-day close runs in the small `warehouse-jobs` service rather than a refreshable materialized view: it can pin `v` and pass it on.

The result is a state that existed in PostgreSQL, usually under a second behind silver. Rows older than 30 minutes are shown without checking their transaction, so a transaction stuck for longer than that stops being held back.

### Marts, reconciliation and the data contract

The `warehouse-jobs` service runs four jobs, each at one pinned watermark:

| Job | Every | What it does |
|-----|-------|--------------|
| Snapshots | before marts and reconciliation | Brings every `snap.<table>` to the current watermark: only the keys written since the previous run, and a full rebuild every hour (`SNAP_REBUILD_S`), at startup and after any error. |
| Marts | 15 s | Computes every gold view (through `gold_snap`) at the current watermark into `mart.<view>` (tagged with `refresh_id` = that watermark) and records the refresh in `ops.mart_refreshes`. Grafana's Business Analytics dashboard reads one refresh, so a page costs milliseconds no matter how many people look at it. Mart tables have no TTL and survive restarts; a refresh that failed halfway is cleared before it is retried, and an hourly cleanup keeps the latest refresh and drops older ones after 1 h. Run one replica. |
| Business-day close | 5 s | Freezes finished business days into `gold.daily_sales_closed`. |
| Reconciliation | 30 s | For 28 tables, compares PostgreSQL and `consistent.<table>(v)`: rows created up to one business hour before the watermark, plus the sum of a column that never changes after insert. Results go to `ops.reconciliation`; `ops.reconciliation_status` flags a table that disagreed two runs in a row (one miss can be a row deleted between the two reads; `cart_items`, hard-deleted all the time, is left to `check_data` at rest), and the job logs it as an error. |

```sql
SELECT * FROM ops.reconciliation_status;                      -- latest result per table
SELECT refresh_id, finished_at, duration_ms FROM ops.mart_refreshes ORDER BY refresh_id DESC LIMIT 5;
SELECT * FROM mart.daily_sales WHERE refresh_id = (SELECT max(refresh_id) FROM ops.mart_refreshes) ORDER BY _row;
```

**Data contract.** `warehouse/clickhouse/check_contract.py` fails when a source change would reach the warehouse silently wrong: a streamed table without a silver spec, a new source column the spec does not list, a type or nullability mismatch, a hashed PII column not declared as a string, a state table without `REPLICA IDENTITY FULL`, or a stale `02-silver.sql`. The GitHub workflow `.github/workflows/data-contract.yml` runs it on every change to `source/postgres/`, `warehouse/clickhouse/` or `debezium/`, against a database built from `init.sql`.

```bash
docker run --rm --network realtime-cdc_default -v $PWD:/repo -w /repo \
  python:3.11-slim sh -c "pip install -q psycopg2-binary && python warehouse/clickhouse/check_contract.py"
```


#### Incremental snapshots (R20)

Computing gold from `consistent.*(v)` re-derives every table from silver and JSON for each mart: about 4.6 s for 18 marts at 35 k orders, growing with all history. Instead each run:

1. takes the transactions that became visible between the previous watermark and the new one, plus snapshot reads ingested since the last run (`snap.changed_keys`), and stores their keys in `snap.dirty` (the first key column for event tables, so all events of that parent are re-read);
2. for each table with dirty keys, writes a removal row for each key and then the key's row at v from `snap.<table>_delta(v)`, the same logic as `consistent.<table>` restricted to those keys. `ReplacingMergeTree(_v, _gone)` with FINAL then shows exactly `consistent.<table>(v)`.

The cost follows the keys touched, not the history: about 0.9 s per run under ~250 API requests/s, then 0.3–1 s for all marts. `check_data` requires every snapshot to equal `consistent.*` at its watermark and every mart to equal gold computed live. The snapshot tables, delta views and key view are generated by `gen_silver.py`; `gold_snap` views are derived from the gold views when warehouse-jobs starts.
### Metric definitions

They are defined once, at the top of `warehouse/clickhouse/init/03-gold.sql`, and `check_data.py` recomputes each one in PostgreSQL.

| Metric | Definition |
|--------|------------|
| Placed GMV | Order subtotal (before vouchers) of every order placed. Never changes. |
| GMV | Placed GMV of confirmed orders: paid online, or cash on delivery. Final once the 15-minute payment window closes. |
| NMV | GMV of confirmed orders that were kept (not cancelled, not returned to seller), minus the merchandise value of units refunded through returns. |
| Net revenue | `order_items.net_amount` (after vouchers) of kept orders, minus return refunds. Used to rank products and categories. |
| Pack time | From the order entering TO_SHIP (payment, or checkout for COD) to READY_TO_SHIP, from `order_status_history`. |
| Refunds | Split by cause: return refunds (a cost of sales), cancellation refunds and late-payment refunds. |
| Return rate | Returns over delivered orders of matured delivery cohorts (return window of 3 business days over). |
| Take rate | Commission and payment fees over GMV of completed orders. |

`gold.daily_sales(v)` is live and keeps moving as orders are cancelled or returned. `gold.daily_sales_closed` freezes each business day one business hour after it ends, as first reported (written by `warehouse-jobs` at a pinned watermark).

Example queries (in http://localhost:8123/play or `docker exec -it cdc-clickhouse bash -c 'clickhouse-client -u admin --password "$CLICKHOUSE_PASSWORD"'`):

```sql
SELECT v, held_back FROM consistent.watermark;           -- pin this v for the queries below
SELECT * FROM gold.daily_sales(v = <v>);
SELECT * FROM gold.product_sales(v = <v>) LIMIT 10;
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

`apps/control-center/` is a single container serving a React UI and a FastAPI backend on port `8000`.

| Area | What it does |
|------|--------------|
| Pipeline bar | Health of Simulator → Source API → PostgreSQL (orders, replication-slot lag) → Debezium (connector state, one-click register) → Kafka (events/s) → ClickHouse (events stored). |
| Traffic simulator | Business clock and speed (×60 to ×1440); **Flash sale now** and **Start 11.11** scenarios; tune buyer sessions and new customers per business hour; fire actions ×1/×5/×20; seeding progress and the activity log. |
| Live CDC stream | Events/s stacked by operation, busiest tables, and a live feed across all tables. Click an UPDATE for the field-level before → after diff. |
| Analytics links | Open the Grafana **Business Analytics** (all marts from one refresh) and **Platform Health** dashboards; see [observability/README.md](../observability/README.md). |

API (used by the UI): `GET /api/health`, `GET /api/generator`, `POST /api/generator/start|stop`, `PUT /api/generator/config`, `POST /api/actions/{order|cart|advance|customer|cancel|restock|flash_sale|mega_sale}`, `PUT /api/clock`, `GET /api/cdc?since=`, `POST /api/connector/register`.

Local development with hot reload (with the rest of the stack running in Docker):

```bash
# backend
cd apps/control-center/backend
pip install -r requirements.txt
KAFKA_BROKER=localhost:29092 SIMULATOR_URL=http://localhost:8200 SOURCE_API_URL=http://localhost:8100 uvicorn main:app --reload --port 8000

# frontend (proxies /api to :8000)
cd apps/control-center/frontend
npm install && npm run dev       # http://localhost:5173
```

---

## Monitoring (Grafana, Prometheus)

| Piece | What it does |
|-------|--------------|
| Grafana **Business Analytics** (`/d/cdc-business`) | Every mart from one completed `refresh_id`, read through the read-only `grafana_reader` ClickHouse user. Business dates follow the simulated clock, not Grafana's time picker. |
| Grafana **Platform Health** (`/d/cdc-platform`) | Host CPU, memory and disk; Source API throughput, 5xx, p95 and checkout p99; PostgreSQL sessions, locks, deadlocks, retained WAL and WAL left before slot invalidation; Kafka offsets, consumer lag and under-replicated partitions; connector and task state, Debezium behind-source time and queue; ClickHouse parts per partition and dashboard query p95; CDC lag, watermark, mart age and reconciliation. |
| Prometheus alerts | 31 rules (`observability/prometheus/alerts.yml`, tested by `alerts.test.yml`): targets and dependencies down, connector not running, Debezium behind source or queue filling, CDC lag against the freshness SLO, Kafka backlog and under-replicated partitions, watermark stalled, stale marts or reconciliation, data drift, new ingest errors, retained WAL and slot at risk, lost or missing, connections, deadlocks, checkout p99 and dashboard query p95 against their SLOs, ClickHouse parts per partition, API errors and latency, host resources, Chat delivery. |
| Alertmanager → chat-webhook | Groups alerts and posts firing and resolved messages to Google Chat; a failed delivery returns 503 so Alertmanager retries. |

Grafana, Prometheus and Alertmanager listen on localhost only. Setup, verification commands and runbooks: [observability/README.md](../observability/README.md).

---

## Key Configuration

### PostgreSQL — logical replication (`postgresql.conf`)

```ini
wal_level = logical        # required for CDC
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 1GB        # retain WAL long enough for Debezium to catch up
max_slot_wal_keep_size = 8GB  # a stalled slot is invalidated past this instead of filling the disk
```

Past `max_slot_wal_keep_size` PostgreSQL drops WAL the connector has not read yet: the disk is safe, but the connector needs a new snapshot. `PostgresSlotWALAtRisk` fires when less than 2 GiB is left, `PostgresSlotLost` once the slot has lost WAL.

### Debezium connector highlights (`debezium/postgres-connector.json`)

```json
"plugin.name":              "pgoutput",
"snapshot.mode":            "initial",
"database.user":            "cdc_user",
"publication.autocreate.mode": "disabled",
"tombstones.on.delete":     "false",
"errors.tolerance":         "none",
"heartbeat.interval.ms":    "10000",
"provide.transaction.metadata": "true",
"decimal.handling.mode":    "string",
"max.batch.size":           "4096",
"max.queue.size":           "16384",
"poll.interval.ms":         "200",
"producer.override.acks":   "all",
"producer.override.compression.type": "lz4"
```

**Throughput (R10).** Larger batches and a byte-capped queue absorb 11.11 bursts; lz4 replaced gzip. At ~990 events/s during an 11.11 sale, commit → ClickHouse went from 1.17 s / 1.86 s (p50 / p95) to 0.92 s / 1.57 s, with Debezium at 5–17 % of a core and at most 0.33 s behind PostgreSQL.

**Least privilege.** Debezium connects as `cdc_user`: the `REPLICATION` role attribute, `SELECT` on the schema (also on tables added later), and writes only to its heartbeat and signal tables. It is not a superuser and owns nothing, so the publication and replica identities are set by the table owner in `source/postgres/init.sql`.

`FULL` replica identity puts the whole old row in `before` on UPDATE and DELETE. State tables need it: silver deletes and `consistent.*` read it. Append-only tables (status history, stock ledger, tracking events, price history, seller ledger) and the outbox use `DEFAULT`, which keeps only the key and writes less WAL.
**Personal data** is hashed in the connector (`column.mask.hash.v2.SHA-256.with.salt…`): buyer names, emails and phones, recipient names, phones and streets, shipping names and addresses, seller emails. Kafka and the warehouse hold salted SHA-256 values, so buyers can still be joined and counted but never read. The salt is `PII_SALT` in `.env`; changing it changes every hash, so it is set once per environment. Domain events carry no personal data. `check_data.py` fails if an email, phone or address reaches ClickHouse in clear.

`errors.tolerance = none` stops the connector on a change it cannot convert instead of dropping it, and there are no tombstones. On the ClickHouse side, `kafka_handle_error_mode = 'stream'` sends any message it cannot read to `bronze.ingest_errors`, which `check_data.py` requires to be empty.
`decimal.handling.mode = string` sends `DECIMAL` columns as exact strings (`"109.99"`), so money is never rounded through floating point on its way to ClickHouse's `Decimal` columns.

### Kafka — KRaft mode (no Zookeeper)

Two listeners are configured:
- `PLAINTEXT://kafka:9092` — for inter-container communication (Debezium, ClickHouse, the Control Center)
- `PLAINTEXT_HOST://localhost:29092` — for connections from the host machine

---

## Reset

Full reset — wipes all data and volumes:

```bash
docker compose down -v           # -v also drops every named volume: Kafka, ClickHouse, Prometheus, Alertmanager, Grafana
sudo rm -rf source/postgres/data # owned by the container's postgres user
docker compose up -d
debezium/register.sh             # or the "register" link in the Control Center
```

To rebuild only the warehouse, for example after editing `warehouse/clickhouse/init/*.sql`, run `docker compose rm -sf clickhouse && docker volume rm realtime-cdc_clickhouse_data && docker compose up -d clickhouse`. The new tables re-ingest everything still retained in Kafka.

---

## Roadmap

The platform review (requirements R1–R27, SLOs and the phased plan) lives in the document *Đánh giá Data Platform & Yêu cầu Scale*. Still open:

- **Security:** API authentication and authorization (logins, scoped tokens, signed partner webhooks), a least-privilege database role for the Source API, file-based secrets instead of environment variables, TLS for Kafka and the databases. Plan in the artifact *Source API auth & secrets plan*. The old development passwords and PII salt remain in git history: rotate them in `.env` for any shared environment.
- **Marts at scale:** snapshots are incremental (R20), but each mart is still aggregated over the whole snapshot; at T1 volumes the heaviest marts (shop, product, category sales) would need per-day partial aggregates.
- **Cold storage:** bronze moves to S3 after 30 days (R13/R19); Kafka itself still keeps the broker default retention, and silver is not rebuilt from the lake (R21).
- **Scale (T1, 1 million orders a day):** several brokers, partitions keyed by order, Connect workers, PostgreSQL and ClickHouse replicas (R5, R7, R11–R14, R17, R18); Schema Registry (R9, needs bronze and silver rewritten from JSON to Avro); a load test at T1 peak with several simulators.

