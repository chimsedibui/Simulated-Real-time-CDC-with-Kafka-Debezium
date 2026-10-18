# Analytics and monitoring

Control Center (`http://localhost:8000`) runs the simulator and live CDC feed. Its analytics links open Grafana:

- Business Analytics: http://localhost:3000/d/cdc-business
- Platform Health: http://localhost:3000/d/cdc-platform
- Grafana local login: `admin` / `GRAFANA_ADMIN_PASSWORD` from the root `.env`. Grafana, Prometheus and Alertmanager listen on localhost only; change the port binding and the password before exposing Grafana to other devices.
- Prometheus: http://localhost:9090
- Alertmanager: http://localhost:9093

## Start

Store your Google Chat incoming webhook URL as a single line in `observability/secrets/google-chat-webhook`, then run:

```bash
chmod 600 observability/secrets/google-chat-webhook
./observability/start.sh
```

Set `GRAFANA_PUBLIC_URL` to a reachable Grafana URL when Chat notifications are opened on another device.

The secret is gitignored, mounted only into the bridge, and excluded from Docker build contexts. Never put it in dashboard JSON, alert annotations, tracked YAML, or command output. The bridge has no published host port. It returns 503 on upstream failure so Alertmanager can retry. `chat_webhook_deliveries_total` tracks delivery outcomes without logging the URL or upstream response.

`start.sh` applies the read-only database accounts and every password in the root `.env` to existing volumes; fresh volumes get the same through init scripts (`source/postgres/passwords.sh`, `warehouse/clickhouse/init/06-monitoring.sh`). `grafana_reader` can select marts and completed refresh metadata; `monitor_reader` reads monitoring data. No password is stored in tracked files.

All long-running services use `restart: unless-stopped`; the on-demand validator does not restart automatically. Docker must itself start at boot. A deliberately stopped container remains stopped.

## Data and time

Business panels pin one completed `refresh_id` using a dashboard variable. All 18 marts are covered; queries use the read-only ClickHouse datasource. Business dates follow the accelerated simulation clock, not Grafana's wall-clock time picker. Business analytics currently covers all available business history; table panels show at most 200 rows. Refresh the dashboard to select the latest completed snapshot.

Platform panels use Prometheus wall-clock time. Metrics include host CPU/memory/disk, API throughput/5xx/p95, PostgreSQL sessions/locks/deadlocks/retained WAL, Kafka offsets and consumer lag, Debezium connector/task state, ClickHouse activity, transaction watermark, mart age/duration and reconciliation. Source API metrics aggregate both Uvicorn workers through a shared per-container multiprocess directory, reset on startup.

`platform-exporter` polls SQL and Connect every 15 seconds in the background; Prometheus scrape success is separate from `cdc_dependency_up`. Kafka metrics come from kafka-exporter. Node exporter reads host `/proc`, `/sys`, and root filesystem without Docker socket access. In Docker Desktop or a VM these metrics describe the Docker host/VM, not necessarily your physical computer. Debezium streaming metrics (behind-source time, change queue, connection) come from JMX through Jolokia on port 8778, reachable only on the compose network. ClickHouse parts per partition and SELECT p95 per user come from `system.parts` and `system.query_log`. Per-container metrics and distributed request tracing are not included.

Mart snapshots survive warehouse-job restarts/reconnects. The latest snapshot is retained during idle periods; older snapshots are cleaned hourly after one hour. Gold schema changes now require an explicit mart migration rather than dropping live tables on reconnect. Run only one warehouse-jobs replica.

## Alerts and response

Prometheus evaluates `prometheus/alerts.yml`; Alertmanager groups and routes firing/recovery notifications to the internal bridge, which formats Google Chat messages. Rules have sustained windows to avoid transient spikes. Rules labelled `slo` follow the SLOs in the platform review: freshness (CDC p95 lag ≤ 5 s, ≤ 30 s at peak), write path (checkout p99 ≤ 300 ms) and dashboards (query p95 ≤ 500 ms). Other thresholds are local defaults.

- Target/dependency down: inspect service logs and exporter connectivity.
- Connector stopped / retained WAL: inspect Connect task status and PostgreSQL slot progress.
- Growing Kafka backlog / high CDC lag: compare ingress and processing rates, CPU, memory and partition count before adding consumers.
- Watermark stalled: inspect per-topic ingestion and `bronze.ingest_errors`.
- Stale marts / reconciliation: inspect warehouse-jobs; reconciliation absence is independently alerted.
- Data drift: query `ops.reconciliation_status`; identify affected tables before replay.
- API latency/errors: inspect DB locks and connection pools before adding API replicas.
- Disk low: inspect retained WAL, Kafka retention and bronze growth.
- Chat delivery failure: check Alertmanager UI and bridge metrics; notification failure cannot reliably report itself over the same unavailable channel.

These notifications advise investigation/capacity changes; they do not automatically scale services. A quiet source alone does not trigger stale-mart alerts. The warehouse consistency time-window limitations remain; monitoring does not replace CDC correctness checks.

## Verify

```bash
docker compose config --quiet
docker compose run --rm --no-deps --entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml
docker compose run --rm --no-deps --entrypoint promtool -w /etc/prometheus prometheus test rules alerts.test.yml
docker compose run --rm --no-deps --entrypoint amtool alertmanager check-config /etc/alertmanager/alertmanager.yml
docker compose exec -T chat-webhook python - < observability/service/test_bridge.py
```

Dashboard sources are provisioned from disk. Regenerate after editing the generator:

```bash
python3 observability/grafana/generate_dashboards.py
```

Datasource and dashboards reload on Grafana restart (dashboard files also get periodic reload). Reload Prometheus rule changes with `docker compose restart prometheus`. To validate notifications end to end, POST a clearly labeled synthetic alert with a short expiry to Alertmanager `/api/v2/alerts`; check `chat_webhook_deliveries_total{result="success"}` and `alertmanager_notifications_failed_total`.
