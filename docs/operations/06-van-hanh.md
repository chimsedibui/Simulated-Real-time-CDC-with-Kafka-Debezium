# 06 · Vận hành, giám sát và chất lượng

[Tài liệu dự án](../README.md) / [Vận hành](README.md)

## Đọc file này để biết gì

File này trả lời câu hỏi "chạy, theo dõi và kiểm thử hệ thống này như thế nào":

- cách dựng toàn bộ stack trên một máy, thứ tự khởi động, cổng, dữ liệu nằm ở đâu, cách reset và backup ClickHouse;
- hệ thống giám sát (Prometheus, Grafana, exporter tự viết), từng nhóm metric và vì sao có nó;
- 32 alert: ý nghĩa, ngưỡng, lý do chọn ngưỡng, nhãn SLO và việc cần làm khi nó kêu;
- các runbook viết lại thành danh sách bước;
- chiến lược kiểm thử: ba mức gate, các bộ test, probe torn-read, load test và kết quả;
- CI trên GitHub Actions và vì sao vẫn giữ CI dù mọi thứ chạy local;
- Control Center (web app điều khiển simulator và xem luồng CDC);
- các sự cố đã gặp và bài học (phần hữu ích nhất khi đi phỏng vấn);
- các lỗ hổng còn lại và lộ trình.

Tài liệu liên quan (không lặp lại ở đây):

- [01 · Nghiệp vụ và dữ liệu nguồn](../business/01-nghiep-vu-va-du-lieu-nguon.md)
- [02 · Source API và bảo mật](../business/02-source-api-va-bao-mat.md)
- [03 · CDC ingestion](../platform/03-cdc-ingestion.md)
- [04 · Warehouse](../platform/04-warehouse.md)
- [05 · Mart và metric](../platform/05-mart-va-metric.md) (chi tiết các kiểm tra dữ liệu của `check_data.py`)
- Câu hỏi ôn phỏng vấn cho phần này: [Q&A · 06-van-hanh.md](../interview/06-van-hanh.md)

## Mục lục

1. [Chạy hệ thống](#1-chạy-hệ-thống)
2. [Giám sát: Prometheus, exporter, Grafana](#2-giám-sát-prometheus-exporter-grafana)
3. [Alert](#3-alert)
4. [Runbook](#4-runbook)
5. [Chất lượng: gate và các bộ test](#5-chất-lượng-gate-và-các-bộ-test)
6. [CI](#6-ci)
7. [Control Center](#7-control-center)
8. [Sự cố và bài học](#8-sự-cố-và-bài-học)
9. [Lỗ hổng đã biết và lộ trình](#9-lỗ-hổng-đã-biết-và-lộ-trình)

---

## 1. Chạy hệ thống

### 1.1. Điều kiện cần

- Docker Engine với Docker Compose v2 (máy hiện tại: Docker Engine chạy trong Ubuntu trên WSL2, thư mục dữ liệu Docker là `/var/lib/docker`).
- `bash`, `openssl`, `python3` và `curl` trên máy host: `setup-env.sh` dùng `openssl` để tạo chứng chỉ, `debezium/register.sh` và `quality/gate.sh` dùng `python3` và `curl`.
- RAM và CPU đủ cho khoảng 20 container. Load test được đo trên laptop 16 core; stack chạy bình thường không cần nhiều như vậy.
- Tùy chọn: một Google Chat incoming webhook nếu muốn nhận thông báo alert.

### 1.2. `setup-env.sh` tạo ra gì

Chạy một lần trước khi khởi động. Script này idempotent: chạy lại chỉ thêm cái còn thiếu, không bao giờ ghi đè giá trị đã có.

| Tạo ra | Nội dung | Ai dùng |
|---|---|---|
| `secrets/` (gitignored), một file cho mỗi secret | Mật khẩu PostgreSQL admin, `cdc_user`, `source_api`, `app_reader`, `identity`, `monitor_reader`, ClickHouse admin, `grafana_reader`, Grafana admin; PII salt; access key và secret key của cold storage (S3 trên SeaweedFS) | Mỗi container chỉ mount đúng file nó cần, read-only, dưới `/run/secrets` |
| `secrets/password_pepper`, `secrets/admin_password` | Pepper băm mật khẩu và mật khẩu admin của identity service | identity |
| `secrets/clients/` | Secret của các service client: `sim-orchestrator`, `quality`, `identity` | simulator, validator, identity |
| `secrets/webhooks/` | Khóa ký webhook của từng đối tác: MOMO, VNPAY, NAPAS, GHN, GHTK, SPX, JT, VTP | Source API (kiểm chữ ký), simulator (đóng vai đối tác) |
| `secrets/tls/` | Một CA local (10 năm) và chứng chỉ server (825 ngày) cho `api.cdc.localhost`, `auth.cdc.localhost`, `app.cdc.localhost`, `localhost` | gateway (Traefik) |
| `.env` | Cấu hình không bí mật, lấy từ `.env.example` (hiện chỉ có `AUTH_MODE=enforce`) | docker compose |

Vài chi tiết đáng nhớ:

- Giá trị ngẫu nhiên là 24 byte hex. File secret không có ký tự xuống dòng cuối vì Debezium đọc file nguyên văn.
- Nếu một `.env` cũ còn chứa mật khẩu (trước khi chuyển sang "secret là file"), script chuyển giá trị đó sang file và xóa khỏi `.env`, để stack đang chạy giữ nguyên mật khẩu.
- Quyền file được nới thành 644 để user trong container đọc được; riêng `ca.key` giữ riêng tư. Lý do chấp nhận được: `secrets/` chỉ nằm trên máy này và bị gitignore.
- `./setup-env.sh --rotate` xóa và sinh lại mọi secret (giữ CA và chứng chỉ TLS). Nó chỉ chạy khi stack đã dừng, vì database chỉ đặt mật khẩu ở lần khởi động đầu trên volume mới; PII salt mới cũng làm đổi mọi giá trị băm.
- Lý do dùng file thay vì biến môi trường: mật khẩu không xuất hiện trong `docker inspect`, trong file compose hay trong cấu hình connector lưu trên Kafka.

Webhook Google Chat là secret riêng của phần giám sát, nằm ở `observability/secrets/google-chat-webhook` (cũng gitignored). `setup-env.sh` không tạo file này.

### 1.3. Bố cục compose

`docker-compose.yml` ở thư mục gốc không định nghĩa service nào. Nó đặt tên project là `realtime-cdc` và `include` tám file, mỗi file một vùng trách nhiệm:

| File | Service |
|---|---|
| `source/compose.yml` | postgres, source-api, identity, simulator, simulator-load (profile `load`), adminer |
| `kafka/compose.yml` | kafka (KRaft, một broker), schema-registry, kafka-ui |
| `debezium/compose.yml` | debezium (Kafka Connect + Debezium PostgreSQL connector) |
| `warehouse/compose.yml` | clickhouse, warehouse-jobs, seaweedfs |
| `apps/compose.yml` | control-center |
| `quality/compose.yml` | validator (profile `tools`) |
| `observability/compose.yml` | prometheus, alertmanager, chat-webhook, platform-exporter, kafka-exporter, node-exporter, grafana |
| `gateway/compose.yml` | gateway (Traefik) |

Vì sao tách: mỗi nhóm đọc và sửa độc lập, CI có thể lọc theo đường dẫn (ví dụ chỉ chạy kiểm tra observability khi `observability/**` đổi), và người đọc thấy ngay ranh giới giữa nguồn, CDC, warehouse và vận hành.

**Profiles.** Hai service không chạy mặc định:

- `tools`: `validator`, một image Python chứa toàn bộ test và script kiểm tra. Chỉ chạy theo yêu cầu (`docker compose --profile tools run --rm validator ...`), không có `restart`.
- `load`: `simulator-load`, bản sao simulator không seed dữ liệu, được `quality/load_test.sh` scale lên N bản.

Mọi service chạy lâu đều có `restart: unless-stopped`: Docker khởi động lại chúng sau khi máy bật lại (với điều kiện Docker tự khởi động), còn container bị dừng có chủ ý thì giữ nguyên trạng thái dừng.

### 1.4. Thứ tự khởi động và phụ thuộc

Compose tự sắp thứ tự nhờ `depends_on` với điều kiện `service_healthy`. Chuỗi phụ thuộc thực tế:

1. **postgres** khỏe (`pg_isready`). Lần đầu trên volume mới, PostgreSQL chạy các init script theo thứ tự `01-init.sql` (schema, outbox, publication, `cdc_user`), `02-monitoring.sql` (`monitor_reader`), `03-roles.sql` (`source_api`, `app_reader`), `04-passwords.sh` (đặt mật khẩu từ file), `05-identity.sh` (database của identity).
2. **identity** khỏe, rồi **source-api** khỏe (source-api xác minh token bằng JWKS của identity), rồi **simulator** (chỉ cần source-api đã khỏe, vì simulator chỉ ghi qua API).
3. **kafka** khỏe, rồi **schema-registry** khỏe, rồi **debezium** khỏe (Debezium cần cả Kafka, Schema Registry và PostgreSQL).
4. **seaweedfs** khỏe (healthcheck chờ file đánh dấu bucket `bronze-cold` đã tạo) và **kafka** khỏe, rồi **clickhouse** khỏe. ClickHouse chạy các init SQL trong `warehouse/clickhouse/init/` khi volume còn mới.
5. **warehouse-jobs** chờ clickhouse và postgres khỏe.
6. **control-center** chờ postgres, kafka, clickhouse khỏe và simulator đã chạy.
7. Các service giám sát và gateway không phụ thuộc gì: Prometheus chịu được việc target chưa lên, chỉ ghi `up = 0`.

Điều quan trọng: **connector Debezium không tự đăng ký**. Stack lên xong mà chưa đăng ký connector thì Kafka không nhận được thay đổi nào.

### 1.5. Các bước chạy lần đầu

1. Tạo secret và `.env`.

```bash
./setup-env.sh
```

2. Tạo file webhook Chat. Có URL thật thì ghi một dòng URL vào file; không có thì tạo file rỗng (khi đó mọi lần gửi thông báo sẽ thất bại và alert `ChatDeliveryFailed` sẽ kêu khi có alert khác).

```bash
touch observability/secrets/google-chat-webhook
```

3. Khởi động toàn bộ stack.

```bash
docker compose up -d
```

4. Đăng ký connector (bắt buộc). Script đọc `debezium/postgres-connector.json`, điền PII salt từ `secrets/pii_salt` rồi gọi `PUT /connectors/postgres-connector/config`, nên dùng được cả để tạo mới lẫn cập nhật. Mật khẩu database không nằm trong config: Connect tự đọc từ file secret qua `DirectoryConfigProvider`. Salt thì phải điền lúc đăng ký vì nó là một phần tên khóa cấu hình, thứ config provider không thay được.

```bash
debezium/register.sh
```

5. Kiểm tra connector đang RUNNING.

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status
```

6. Mở Control Center ở `http://localhost:8000`, bấm Start simulation. Simulator tự seed dữ liệu theo preset `SIM_SCALE` (mặc định medium: 50 shop, 1.000 SKU, 5.000 khách) khi khởi động.

Thay cho bước 4, Control Center có nút "register" trên thanh pipeline khi phát hiện connector chưa tồn tại.

**`observability/start.sh`** là cách khởi động "đầy đủ" cho volume đã tồn tại hoặc sau khi đổi secret. Init script chỉ chạy trên volume mới, nên script này áp lại mọi thứ một cách idempotent:

1. kiểm tra `.env`, `secrets/` và file webhook Chat có nội dung (nó từ chối chạy nếu file webhook rỗng);
2. bật postgres, kafka, clickhouse, debezium và chờ khỏe;
3. chạy lại SQL tài khoản giám sát, SQL role, script mật khẩu, script identity trên PostgreSQL;
4. chạy lại `06-monitoring.sh`, `07-snapshots.sql`, `08-cold-storage.sql` trên ClickHouse;
5. đăng ký connector;
6. build và bật các service còn lại;
7. đặt lại mật khẩu admin Grafana từ file secret.

```bash
./observability/start.sh
```

### 1.6. Cổng

Mọi cổng publish đều bind vào `127.0.0.1`, nên máy khác trong mạng LAN không truy cập được. Muốn mở ra ngoài phải đổi binding và đổi mật khẩu trước.

| Service | Cổng host | Cổng trong container | Mục đích |
|---|---|---|---|
| control-center | 8000 | 8000 | Web app điều khiển simulator, luồng CDC trực tiếp, tình trạng pipeline |
| source-api | 8100 | 8100 | API nghiệp vụ, writer duy nhất vào PostgreSQL; OpenAPI ở `/docs`; `/metrics` cho Prometheus |
| simulator | 8200 | 8200 | API điều khiển simulator (`/start`, `/stop`, `/status`, `/clock`, `/actions/...`) |
| identity | 8300 | 8300 | Đăng ký, đăng nhập, phát token, JWKS |
| gateway | 8443 | 8443 | Lối vào HTTPS duy nhất cho client bên ngoài: `api.cdc.localhost`, `auth.cdc.localhost` |
| postgres | 5432 | 5432 | PostgreSQL 15 (`enterprise_db`) |
| adminer | 8081 | 8080 | Giao diện web cho PostgreSQL |
| kafka | 29092 | 29092 | Listener cho client chạy trên host (container dùng `kafka:9092`, không publish) |
| schema-registry | 8085 | 8081 | Confluent Schema Registry |
| kafka-ui | 8080 | 8080 | Xem topic, consumer group, connector, schema |
| debezium | 8083 | 8083 | Kafka Connect REST API |
| clickhouse | 8123 | 8123 | HTTP, Play UI ở `/play` |
| clickhouse | 9000 | 9000 | Native protocol (`clickhouse-client`, Grafana) |
| grafana | 3000 | 3000 | Dashboard Business Analytics và Platform Health |
| prometheus | 9090 | 9090 | Prometheus UI và API |
| alertmanager | 9093 | 9093 | Alertmanager UI và API |

Không publish ra host (chỉ trong mạng compose): platform-exporter 9108, warehouse-jobs metrics 9109, kafka-exporter 9308, node-exporter 9100, chat-webhook 8080, Jolokia của Debezium 8778, metrics và ping của gateway 8082, SeaweedFS (S3 8333, master 9333), Kafka controller 9093 và listener nội bộ 9092.

**Trùng cổng trên máy local.** `docker-compose.override.yml` (gitignored) được compose nạp tự động. Trên máy này nó đổi postgres sang `127.0.0.1:15432` và kafka-ui sang `127.0.0.1:18082` bằng `!override` (thay hẳn danh sách ports, không cộng thêm). Vì file bị gitignore, mỗi máy tự có bản riêng và CI không bị ảnh hưởng. Lưu ý: script nào gọi thẳng cổng host (`gate.sh` gọi `localhost:8083`, `8000`, `8100`, `8200`) sẽ hỏng nếu bạn đổi các cổng đó; hai cổng đang đổi không nằm trong số này.

### 1.7. Dữ liệu nằm ở đâu

| Dữ liệu | Nơi lưu | Ghi chú |
|---|---|---|
| PostgreSQL | Bind mount `source/postgres/data` trong repo | Gitignored. Thuộc user `postgres` của container nên user thường không xóa được, cần `sudo` hoặc một container tạm |
| Kafka (topic CDC, `_schemas` của Schema Registry, offset/config/status của Kafka Connect) | Named volume `realtime-cdc_kafka_data` | Offset của Debezium nằm ở đây, không nằm ở PostgreSQL |
| ClickHouse (bronze, silver, gold, mart, ops) | `realtime-cdc_clickhouse_data` | Phần bronze cũ hơn 30 ngày chuyển sang cold disk S3 |
| SeaweedFS (cold tier S3) | `realtime-cdc_seaweedfs_data` | |
| Khóa ký Ed25519 của identity | `realtime-cdc_identity_keys` | Mất volume này thì mọi token cũ hết hiệu lực |
| Prometheus TSDB | `realtime-cdc_prometheus_data` | Giữ 15 ngày |
| Alertmanager (silence, trạng thái gửi) | `realtime-cdc_alertmanager_data` | |
| Grafana (user, preference) | `realtime-cdc_grafana_data` | Dashboard và datasource lấy từ file provisioning, không phụ thuộc volume |

| Backup ClickHouse | Bind mount `warehouse/backups/` (gitignored), mount vào ClickHouse ở `/backups` | Do `warehouse/backup.sh` tạo; vì là thư mục host nên sống sót qua `docker compose down -v` |

Trên máy này mọi named volume nằm vật lý ở `/var/lib/docker/volumes/realtime-cdc_<tên>/_data` bên trong Ubuntu WSL2. Chỉ ClickHouse có backup (mục 1.9); PostgreSQL và các volume khác thì chưa.

Hệ quả cần nhớ: PostgreSQL (có replication slot) và Kafka (có offset của connector) phải được reset cùng nhau. Xóa một bên mà giữ bên kia làm slot và offset lệch nhau.

### 1.8. Reset

**Reset toàn bộ** (mất hết dữ liệu):

1. Dừng stack và xóa mọi named volume (thêm `--profile tools` để gỡ cả validator).

```bash
docker compose --profile tools down -v
```

2. Xóa thư mục dữ liệu PostgreSQL. Cách `gate.sh` làm, không cần sudo:

```bash
docker run --rm -v "$PWD/source/postgres:/p" alpine sh -c "rm -rf /p/data"
```

3. Bật lại stack và đăng ký connector như mục 1.5.

**Chỉ dựng lại warehouse** (ví dụ sau khi sửa SQL trong `warehouse/clickhouse/init/`): xóa container clickhouse, xóa volume `realtime-cdc_clickhouse_data`, bật lại clickhouse. Bảng mới sẽ đọc lại mọi thứ Kafka còn giữ. Chi tiết và rủi ro xem [04-warehouse.md](../platform/04-warehouse.md).

```bash
docker compose rm -sf clickhouse
```

```bash
docker volume rm realtime-cdc_clickhouse_data
```

```bash
docker compose up -d clickhouse
```

### 1.9. Backup và khôi phục ClickHouse

`warehouse/backup.sh` dùng lệnh `BACKUP` / `RESTORE` có sẵn của ClickHouse, ghi vào disk `backups` (thư mục `warehouse/backups/` trên host). Chi tiết cơ chế ở [04-warehouse.md, mục 11](../platform/04-warehouse.md#11-backup-và-khôi-phục); ở đây chỉ là cách vận hành.

| Lệnh con | Làm gì |
|---|---|
| `backup` | Backup đầy đủ mọi database của warehouse (trừ các materialized view đọc Kafka, vì chúng được tạo lại từ init SQL) |
| `backup --incremental` | Chỉ các part đổi kể từ bản full mới nhất |
| `list` | Liệt kê bản backup, loại, dung lượng, bản gốc |
| `restore <tên>` | Thay warehouse bằng bản backup rồi đọc bù từ Kafka |
| `prune <n>` | Giữ n bản full mới nhất cùng các bản incremental của chúng |

Cách backup và replay phối hợp (hướng dẫn đầy đủ ở [bài thực hành](09-thuc-hanh-backup-clickhouse.md)):

1. **Khi backup:** dừng warehouse-jobs và detach các bảng Kafka engine (vài giây tới một phút), nên mọi bảng được chụp ở cùng một vị trí Kafka. Trong lúc đó Kafka vẫn giữ sự kiện mới. Mỗi bản có một file manifest ghi loại, bản gốc, thời điểm, danh sách database và **topic ID và vị trí replay của Kafka**.
2. **Khi restore:** kiểm tra topic IDs và offset replay của Kafka còn hợp lệ (không hợp lệ thì dừng trước khi xóa database; `--force` chỉ restore offline), xóa và khôi phục các database, tính từ offset lưu trong bronze offset đầu tiên **chưa** có trong backup cho từng partition, dời từng consumer group của ClickHouse tới đó, rồi tạo lại các bảng Kafka. Các sự kiện sau thời điểm backup được đọc bù; không coi đó là bảo đảm exactly-once của toàn pipeline.
3. Nếu Kafka đã xóa những sự kiện cần đọc bù (retention mặc định 7 ngày) hoặc backup "đi trước" Kafka, restore dừng và báo rõ partition nào.

```bash
warehouse/backup.sh backup
```

```bash
warehouse/backup.sh restore <tên>
```

---

## 2. Giám sát: Prometheus, exporter, Grafana

### 2.1. Kiến trúc

- **Prometheus** scrape mỗi 15 giây, đánh giá rule mỗi 15 giây, giữ dữ liệu 15 ngày.
- **Target** (job): `prometheus`, `alertmanager`, `platform` (exporter tự viết), `kafka` (kafka-exporter), `node` (node-exporter), `source-api`, `identity`, `gateway` (Traefik), `chat-webhook`, `warehouse-jobs`.
- **Alertmanager** nhận alert, gom nhóm, gửi tới **chat-webhook**, service nhỏ chuyển payload thành tin nhắn Google Chat.
- **Grafana** có hai datasource: Prometheus (mặc định) và ClickHouse.

Vì sao chọn Prometheus cộng một exporter tự viết thay vì exporter có sẵn cho từng thành phần:

- Nhiều tín hiệu quan trọng nhất là **tín hiệu nghiệp vụ của pipeline**, không có exporter nào cung cấp: lag từ commit tới lúc vào ClickHouse, tuổi giao dịch bị watermark giữ lại, tuổi mart, số bảng reconcile lệch.
- Một exporter duy nhất với tài khoản read-only (`monitor_reader`) cho cả PostgreSQL và ClickHouse thì ít quyền, ít container và kiểm soát được cardinality của nhãn.
- Exporter thu thập **chạy nền** theo vòng 15 giây; Prometheus scrape chỉ đọc giá trị trong bộ nhớ, không bao giờ phải chờ SQL. Vì thế "scrape thành công" khác "dữ liệu còn tươi", và hệ thống có metric riêng cho từng dependency (`cdc_dependency_up`, `cdc_dependency_last_success_timestamp_seconds`).
- Kafka và host thì dùng exporter có sẵn (kafka-exporter, node-exporter) vì chúng đã tốt và không có gì riêng của dự án.

### 2.2. Metric của platform-exporter (`observability/service/exporter.py`)

Mỗi vòng, exporter chạy bốn collector (clickhouse, postgres, debezium, debezium-jmx) rồi kiểm tra HTTP health của năm service. Collector nào lỗi thì `cdc_dependency_up{dependency=...}` về 0 và chỉ ghi tên loại lỗi vào log (không ghi chi tiết có thể chứa thông tin nhạy cảm).

**Sức khỏe của chính exporter**

| Metric | Ý nghĩa |
|---|---|
| `cdc_dependency_up{dependency}` | Lần thu thập gần nhất của dependency đó thành công (1) hay không (0). Dependency: `clickhouse`, `postgres`, `debezium` (REST Connect), `debezium-jmx` |
| `cdc_dependency_last_success_timestamp_seconds{dependency}` | Thời điểm thu thập thành công gần nhất. Dùng để phát hiện collector bị treo dù scrape vẫn thành công |
| `cdc_service_up{service}` | Endpoint health trả 200. Service: `source-api` (`/health`), `control-center` (`/api/health`), `simulator` (`/status`), `schema-registry` (`/config`), `identity` (`/health`) |

**ClickHouse và pipeline** (đăng nhập bằng `monitor_reader`)

| Metric | Ý nghĩa |
|---|---|
| `cdc_ingest_lag_p95_seconds` | p95 của (thời điểm ingest vào bronze trừ thời điểm commit ở nguồn) trong 5 phút gần nhất, bỏ qua sự kiện snapshot. Đây là chỉ số độ tươi chính |
| `cdc_events_total` | Tổng số dòng trong `bronze.cdc_events`, đọc từ metadata `system.parts` thay vì đếm bảng, để không quét changelog mỗi 15 giây. Có tính cả bản giao lại |
| `cdc_ingest_errors_total` | Số dòng trong `bronze.ingest_errors` (message ClickHouse không đọc được) |
| `cdc_transactions_held_back` | Số giao dịch gần đây đang bị watermark giữ lại |
| `cdc_oldest_held_transaction_age_seconds` | Tuổi của giao dịch bị giữ lâu nhất |
| `cdc_mart_last_success_timestamp_seconds` | Lúc mart refresh gần nhất hoàn tất |
| `cdc_mart_refresh_duration_seconds` | Thời lượng refresh đó |
| `cdc_reconciliation_failing_tables` | Số bảng lệch giữa nguồn và warehouse hai lần liên tiếp |
| `cdc_reconciliation_last_success_timestamp_seconds` | Lần reconcile gần nhất |
| `cdc_clickhouse_metric{metric}` | Toàn bộ `system.metrics` (ví dụ `MemoryTracking`, `Query`) |
| `cdc_clickhouse_events_total{event}` | Bốn bộ đếm của `system.events`: `Query`, `FailedQuery`, `InsertedRows`, `SelectedRows` |
| `cdc_clickhouse_max_parts_per_partition{database,table}` | Số active part nhiều nhất trong một partition của mỗi bảng. Báo trước lỗi `TOO_MANY_PARTS` |
| `cdc_clickhouse_bytes_on_disk{database,disk}` | Dung lượng active part theo database và disk (`default` là ổ nóng, `cold` là S3) |
| `cdc_clickhouse_select_p95_seconds{user}` | p95 thời gian các SELECT đã xong trong 5 phút, theo user `grafana_reader` và `admin`, từ `system.query_log`. Đo SLO dashboard |

**PostgreSQL** (đăng nhập `monitor_reader`, session read-only, `statement_timeout` 5 giây)

| Metric | Ý nghĩa |
|---|---|
| `cdc_postgres_connections`, `cdc_postgres_max_connections` | Số session hiện có và giới hạn (200) |
| `cdc_postgres_lock_waiters` | Số session đang chờ lock |
| `cdc_postgres_deadlocks_total` | Bộ đếm deadlock của database |
| `cdc_postgres_slot_retained_wal_bytes{slot}` | Lượng WAL slot logical đang giữ (từ `restart_lsn` tới LSN hiện tại) |
| `cdc_postgres_slot_safe_wal_bytes{slot}` | `safe_wal_size`: slot còn được tụt thêm bao nhiêu byte trước khi `max_slot_wal_keep_size` (8 GB) vô hiệu hóa nó. Không có giá trị khi giới hạn là vô hạn hoặc slot đã mất |
| `cdc_postgres_slot_wal_reserved{slot}` | 1 khi `wal_status` là `reserved` hoặc `extended`, 0 khi `unreserved` hoặc `lost` |

Vì sao cần cả ba metric slot: WAL giữ lại cho biết đĩa đang bị ăn; safe WAL cho biết còn bao xa tới lúc mất dữ liệu; `wal_status` cho biết đã mất thật hay chưa. Khi slot biến mất hoàn toàn thì cả ba series đều biến mất, nên có thêm alert dựa trên `absent()`.

**Kafka Connect / Debezium**

| Metric | Nguồn | Ý nghĩa |
|---|---|---|
| `cdc_connect_running{connector,component}` | REST `/connectors?expand=status` | 1 nếu connector hoặc từng task đang RUNNING. Khi chưa có connector nào, exporter tự ghi `postgres-connector` bằng 0 để alert vẫn kêu |
| `cdc_debezium_behind_source_seconds` | JMX qua Jolokia | Khoảng cách giữa commit và lúc Debezium xử lý sự kiện cuối |
| `cdc_debezium_since_last_event_seconds` | JMX | Thời gian từ sự kiện cuối |
| `cdc_debezium_queue_remaining_ratio` | JMX | Tỷ lệ còn trống của hàng đợi nội bộ |
| `cdc_debezium_connected` | JMX | Debezium đang kết nối tới PostgreSQL |
| `cdc_debezium_events_seen_total` | JMX | Số sự kiện đã thấy từ khi task khởi động |

Jolokia (JMX qua HTTP, cổng 8778) được bật bằng biến `ENABLE_JOLOKIA` trong image Debezium và chỉ truy cập được trong mạng compose. Khi task chưa streaming (chưa đăng ký, đang snapshot, đã chết), không có MBean streaming nên `debezium-jmx` báo down.

### 2.3. Metric từ các nguồn khác

| Job | Metric chính | Ghi chú |
|---|---|---|
| source-api | `http_requests_total`, `http_request_duration_seconds` (bucket chi tiết tới 5 s, dùng cho checkout p99), `http_request_duration_highr_seconds` (dùng cho p95 toàn API), `auth_decisions_total{outcome}` | Instrumentator của FastAPI; gộp mọi worker Uvicorn qua thư mục multiprocess chung, reset khi container khởi động |
| identity | `identity_logins_total{outcome}`, `identity_tokens_total{grant}` | Dùng cho alert đăng nhập thất bại hàng loạt |
| gateway | Metric Prometheus có sẵn của Traefik trên entrypoint `metrics` (8082) | Hiện chỉ được scrape, chưa có panel hay alert riêng ngoài `TargetDown` |
| warehouse-jobs | `warehouse_job_errors_total`, `warehouse_snapshot_duration_seconds{mode}` (full hoặc delta), `warehouse_snapshot_keys` | |
| chat-webhook | `chat_webhook_deliveries_total{result}` (`success` hoặc `error`) | |
| kafka | `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`, `kafka_topic_partition_under_replicated_partition`, ... | kafka-exporter |
| node | CPU, RAM, filesystem | node-exporter đọc `/proc`, `/sys`, `/` của host, không cần Docker socket. Trên WSL2 các số này là của VM WSL, không hẳn của máy vật lý |

### 2.4. Dashboard

Sáu dashboard được **sinh bằng code**: năm trang nghiệp vụ (`cdc-business`, `cdc-operations`, `cdc-growth`, `cdc-sellers`, `cdc-finance`) và `cdc-platform`. `observability/grafana/generate_dashboards.py` sinh các JSON; `business_dashboards.py` định nghĩa các trang nghiệp vụ. Grafana nạp chúng từ thư mục provisioning vào folder "Real-time CDC", không cho xóa trong UI. Vì sao sinh bằng code: JSON dashboard viết tay rất dài và khó review; một file Python ngắn liệt kê panel giúp diff dễ đọc, và CI kiểm tra JSON đã commit khớp với kết quả chạy generator.

Sau khi sửa generator, chạy lại rồi restart Grafana (file dashboard cũng được nạp lại định kỳ):

```bash
python3 observability/grafana/generate_dashboards.py
```

**Năm dashboard nghiệp vụ** bắt đầu từ `/d/cdc-business` (Điều hành sàn), rồi tới Đơn hàng & trải nghiệm, Danh mục & chiến dịch, Nhà bán & tồn kho, Thu phí & dòng tiền. Mỗi trang ghim một `refresh_id` đã hoàn tất, dùng ngày nghiệp vụ và làm mới mỗi phút. Bộ lọc có phạm vi riêng; so sánh kỳ ở trang điều hành không áp dụng ngầm cho các bảng lũy kế. Chi tiết ở [chương mart](../platform/05-mart-va-metric.md#11-dashboard-theo-vai-trò-nghiệp-vụ) và [hướng dẫn Grafana](../../observability/grafana/README.md).

**CDC · Platform Health** (`/d/cdc-platform`, 41 panel, giờ thật): target và dependency; CPU, RAM, đĩa; request/s, p95, checkout p99, 5xx của Source API; quyết định phân quyền, đăng nhập và token; ingress Kafka, consumer backlog, partition under-replicated; Debezium behind source và hàng đợi; CDC lag p95, events/s, giao dịch bị giữ; trạng thái connector; WAL giữ lại và WAL còn an toàn; kết nối, lock, deadlock PostgreSQL; bộ nhớ, query, query lỗi, dung lượng nóng/lạnh, SELECT p95, parts per partition của ClickHouse; tuổi và thời lượng mart, thời lượng snapshot, số key đọc lại; tuổi reconcile và số bảng lệch; lỗi ingest; số lần gửi Chat; alert đang kêu.

### 2.5. Datasource

- **Prometheus**: uid `prometheus`, mặc định, không sửa được trong UI.
- **ClickHouse**: plugin `grafana-clickhouse-datasource` 4.11.2, giao thức native cổng 9000, user read-only `grafana_reader`, database mặc định `mart`. Mật khẩu đọc từ file secret bằng cú pháp `$__file{...}` của Grafana. Pool nâng lên 100 kết nối mở và 100 idle vì mặc định của plugin (50/25) làm dashboard xếp hàng khi nhiều người mở cùng lúc.
- Mật khẩu admin Grafana đọc từ file qua biến `GF_SECURITY_ADMIN_PASSWORD__FILE`; tắt đăng ký user.

---

## 3. Alert

### 3.1. Cách đọc

- File: `observability/prometheus/alerts.yml`, một group `cdc-platform`, **32 rule**.
- `for` là thời gian điều kiện phải đúng liên tục trước khi alert kêu, để bỏ qua dao động ngắn.
- `severity`: `critical` (có nguy cơ mất hoặc sai dữ liệu, hoặc một thành phần chết) hoặc `warning` (cần điều tra, thường là năng lực).
- Nhãn `slo` gắn với ba SLO trong bản đánh giá nền tảng: `freshness` (CDC p95 ≤ 5 s, ≤ 30 s lúc cao điểm), `write-path` (checkout p99 ≤ 300 ms), `dashboard` (query p95 ≤ 500 ms). Rule không có nhãn `slo` dùng ngưỡng mặc định local.
- Nhiều rule có vế `and on() cdc_dependency_up{dependency="..."} == 1`: chỉ kêu khi exporter còn đọc được dependency đó. Lý do: khi ClickHouse chết, các metric của nó đứng yên ở giá trị cũ; để `DependencyDown` báo việc này thay vì một loạt alert giả.
- Alert chỉ khuyên điều tra hoặc thay đổi năng lực; không có gì tự scale.

### 3.2. Bảng alert

**Sống còn của thành phần**

| Alert | Ý nghĩa | Ngưỡng | Vì sao ngưỡng này | SLO | Làm gì |
|---|---|---|---|---|---|
| TargetDown | Prometheus không scrape được một target | `up == 0` trong 2 phút | 2 phút = 8 lần scrape lỗi liên tiếp, đủ để bỏ qua một lần restart | | Xem log service và kết nối mạng. Alertmanager ức chế các alert khác có cùng `job` và `instance` để không bị dội |
| DependencyDown | Exporter không đọc được một dependency, hoặc health của một service không trả 200 | `cdc_dependency_up == 0` hoặc `cdc_service_up == 0` trong 2 phút | Như trên | | Xem log của dependency hoặc service được nêu tên, kiểm tra kết nối database |
| CollectorStalled | Metric của một dependency không được cập nhật | Lần thành công cuối cách đây hơn 120 s, trong 2 phút | Exporter chạy mỗi 15 s; 120 s là khoảng 8 vòng bị lỡ | | Xem log platform-exporter và timeout truy vấn. Scrape thành công không có nghĩa là số liệu SQL còn tươi |

**CDC: Debezium, Kafka, độ tươi**

| Alert | Ý nghĩa | Ngưỡng | Vì sao ngưỡng này | SLO | Làm gì |
|---|---|---|---|---|---|
| ConnectorNotRunning | Connector hoặc một task không RUNNING (gồm cả chưa đăng ký) | `cdc_connect_running == 0` trong 2 phút, khi REST Connect còn trả lời | Connector chết thì không có gì vào Kafka, cần biết nhanh | | Runbook 4.1 và 4.2. Thêm consumer không chữa được connector đã dừng |
| DebeziumBehindSource | Debezium chậm hơn PostgreSQL | > 10 s trong 5 phút | Gấp đôi SLO độ tươi 5 s: nếu riêng Debezium đã trễ 10 s thì SLO chắc chắn vỡ | | Xem hàng đợi, throughput producer và Kafka. WAL giữ lại tăng trong lúc này |
| DebeziumQueueFilling | Hàng đợi nội bộ của Debezium đầy hơn 80% | Tỷ lệ còn trống < 0.2 trong 5 phút | Còn 20% là lúc cần can thiệp trước khi Debezium phải chặn việc đọc WAL | | Kafka không theo kịp connector: kiểm tra broker và cấu hình producer |
| KafkaBacklogGrowing | Consumer group (ClickHouse) tồn hơn 10k message và đang tăng | Tổng lag > 10.000 và đạo hàm 10 phút > 0, trong 5 phút | Vế "đang tăng" loại trường hợp tồn đọng cao nhưng đang giảm sau một đợt burst | | So sánh tốc độ vào và tốc độ xử lý; xem số partition trước khi thêm consumer |
| KafkaUnderReplicated | Có partition thiếu replica | > 0 trong 5 phút | Bất kỳ partition nào thiếu replica đều là rủi ro | | Với RF=1 hiện tại alert này không thể kêu; nó dành cho cụm nhiều broker |
| CDCLagHigh | CDC p95 lag vượt SLO độ tươi | > 5 s trong 10 phút | Chính là SLO 5 s; `for` dài 10 phút vì SLO cho phép tới 30 s trong cao điểm 11.11 | freshness | Xem backlog Kafka, Debezium lag, bộ nhớ và merge của ClickHouse |
| CDCLagCritical | CDC p95 lag vượt SLO cao điểm | > 30 s trong 5 phút | SLO lúc cao điểm là 30 s; vượt mức này là vỡ cả SLO nới lỏng | freshness | Runbook 4.4: tìm chặng chậm (Debezium behind source hay consumer lag) trước khi thêm năng lực |

**Warehouse**

| Alert | Ý nghĩa | Ngưỡng | Vì sao ngưỡng này | SLO | Làm gì |
|---|---|---|---|---|---|
| TransactionWatermarkStalled | Một giao dịch chưa về đủ sự kiện suốt hơn 2 phút, watermark bị kẹt | Tuổi giao dịch bị giữ lâu nhất > 120 s trong 2 phút | Bình thường giao dịch về đủ dưới 1 giây; 2 phút nghĩa là thiếu sự kiện thật chứ không phải chậm | | Runbook 4.5: xem từng topic và `bronze.ingest_errors` |
| MartRefreshStale | Mart không được làm mới trong khi CDC vẫn có dữ liệu | Refresh cuối cách đây > 120 s, có sự kiện mới trong 5 phút, trong 2 phút | Mart refresh mỗi 15 s; 120 s là 8 lần lỡ. Vế "có sự kiện mới" để nguồn im lặng không gây alert | | Xem log warehouse-jobs, thời lượng refresh và watermark. Dashboard nghiệp vụ có thể đang cũ |
| ReconciliationStale | Reconcile định kỳ không chạy | Lần chạy cuối > 180 s, trong 3 phút | Reconcile chạy mỗi 30 s; 180 s là 6 lần lỡ. Có alert riêng vì "không chạy" khác "chạy và thấy lệch" | | Xem warehouse-jobs và kết nối tới nguồn |
| WarehouseDataDrift | Nguồn và warehouse lệch nhau hai lần kiểm liên tiếp | Số bảng lệch > 0 trong 2 phút | Đã lọc nhiễu sẵn: một bảng chỉ bị tính là lệch khi lệch hai lần liền | | Runbook 4.6 |
| IngestErrors | ClickHouse cách ly message mới không đọc được | Số dòng `bronze.ingest_errors` tăng trong 10 phút (không có `for`) | Một lỗi ingest là một vi phạm data contract; không có lý do để chờ | | Xem `bronze.ingest_errors`, sửa contract trước khi replay |
| WarehouseJobsFailing | warehouse-jobs lỗi liên tục | ≥ 3 lỗi trong 5 phút, trong 1 phút | Một lỗi lẻ có thể là mất kết nối thoáng qua; ba lần là có vấn đề | | Xem log và kết nối database; mart đã publish vẫn được giữ trong lúc reconnect |
| ClickHousePartsHigh | Một partition có quá nhiều part | > 300 trong 10 phút | ClickHouse từ chối insert (`TOO_MANY_PARTS`) quanh 3.000; 300 là một phần mười, còn nhiều thời gian xử lý | | Insert quá nhỏ hoặc merge bị chậm: xem kích thước batch và tải merge |
| DashboardQuerySlow | SELECT của dashboard chậm | p95 của `grafana_reader` > 0,5 s trong 10 phút | Chính là SLO dashboard 500 ms | dashboard | Xem kích thước mart, bộ nhớ và merge ClickHouse, tải refresh đồng thời |

**PostgreSQL và replication slot**

| Alert | Ý nghĩa | Ngưỡng | Vì sao ngưỡng này | SLO | Làm gì |
|---|---|---|---|---|---|
| PostgresWALRetained | Slot giữ hơn 1 GiB WAL | > 1 GiB trong 5 phút | Bình thường connector bám sát, slot chỉ giữ vài MB; 1 GiB là dấu hiệu connector đứng | | Runbook 4.3. Không xóa slot như phản ứng đầu tiên |
| PostgresSlotWALAtRisk | Slot còn chưa tới 2 GiB trước khi bị vô hiệu hóa | `safe_wal_bytes` < 2 GiB trong 2 phút | `max_slot_wal_keep_size` là 8 GB; còn 2 GiB là 75% đã dùng, vẫn kịp khôi phục | | Runbook 4.3, khẩn cấp: khôi phục tiến độ connector ngay |
| PostgresSlotLost | Slot đã mất hoặc không còn giữ WAL | `wal_reserved == 0` (không có `for`) | Đã có thể mất thay đổi; chờ không có ích | | Runbook 4.7: re-snapshot qua signal table rồi reconcile |
| PostgresSlotMissing | Slot `debezium_slot` không tồn tại trong khi PostgreSQL vẫn sống | `absent(...)` trong 2 phút | Sau failover hoặc slot bị xóa, series biến mất chứ không về 0, nên phải dùng `absent()` | | Runbook 4.7. Debezium có thể tạo slot mới ở LSN hiện tại và bỏ qua thay đổi trong im lặng |
| PostgresConnectionsHigh | Kết nối vượt 80% `max_connections` | > 0,8 trong 5 phút | Source API dùng tối đa 6 × 12 = 72 trong 200; vượt 160 là có rò rỉ hoặc thêm client bất thường | | Xem session idle, cấu hình pool, lock wait trước khi thêm replica API |
| PostgresDeadlocks | Có deadlock | Bộ đếm tăng trong 5 phút (không có `for`) | Thiết kế lock order nhằm không có deadlock nào; mỗi cái là một bug | | Xem log PostgreSQL và thứ tự lock của lệnh nghiệp vụ |

**Source API và identity**

| Alert | Ý nghĩa | Ngưỡng | Vì sao ngưỡng này | SLO | Làm gì |
|---|---|---|---|---|---|
| APIErrorRateHigh | Tỷ lệ 5xx của Source API cao | > 5% trong 5 phút | Mặc định local | | Xem pool database, lock và request lỗi |
| APILatencyHigh | p95 toàn API chậm | > 2 s trong 5 phút | Mặc định local, rộng vì gồm cả lệnh nặng | | Xem lock wait và CPU; chỉ scale API khi tầng ứng dụng là nút thắt |
| CheckoutLatencyHigh | Checkout p99 vượt SLO | p99 POST `/checkouts` > 0,3 s và có hơn 0,1 checkout/s, trong 10 phút | Chính là SLO write path 300 ms; vế lưu lượng tối thiểu để vài request lẻ không quyết định p99 | write-path | Xem lock wait trên dòng SKU, flash sale, voucher và pool kết nối |
| LoginFailuresHigh | Phần lớn lần đăng nhập thất bại hoặc gặp tài khoản bị khóa | > 1 lần/s và > 50% tổng số lần đăng nhập, trong 10 phút | Cả hai vế để tránh kêu khi chỉ có vài lần gõ sai | | Có thể là credential stuffing: xem audit của identity theo IP trước khi chặn |

**Host và thông báo**

| Alert | Ý nghĩa | Ngưỡng | Vì sao ngưỡng này | SLO | Làm gì |
|---|---|---|---|---|---|
| HostCPUHigh | CPU host cao kéo dài | > 85% trong 10 phút | Mặc định local | | Đối chiếu với lưu lượng, backlog, latency |
| HostMemoryHigh | RAM host gần hết | > 90% trong 5 phút | Mặc định local | | Xem bộ nhớ ClickHouse và ứng dụng trước khi tăng concurrency |
| DiskSpaceLow | Đĩa còn dưới 15% | < 15% trên filesystem ghi được, trong 5 phút | Đủ thời gian xử lý trước khi PostgreSQL hoặc Kafka dừng vì đầy đĩa | | Xem WAL giữ lại, retention Kafka, dung lượng warehouse |
| ChatDeliveryFailed | Gửi thông báo Google Chat thất bại | Có lỗi trong 5 phút, trong 1 phút | Mọi lần gửi lỗi đều đáng biết | | Xem kết nối bridge và URL webhook. Alertmanager tự thử lại; nếu Chat chết thì xem UI Alertmanager, vì alert này không thể tự báo qua chính kênh đang hỏng |

### 3.3. Unit test cho alert

`observability/prometheus/alerts.test.yml` chạy bằng `promtool test rules`. Mỗi test cung cấp chuỗi giá trị giả cho series đầu vào theo từng phút, rồi khẳng định tại một thời điểm alert nào phải kêu, với nhãn và annotation chính xác.

Các trường hợp đang có:

- `TargetDown` chưa kêu ở phút 1 (chưa đủ `for`), kêu ở phút 3.
- `MartRefreshStale` **không** kêu khi số sự kiện CDC đứng yên (nguồn im lặng không phải lỗi).
- `ConnectorNotRunning` kêu khi task 0 không chạy.
- `IngestErrors` không kêu khi số lỗi đứng yên, kêu khi tăng, rồi tắt khi cửa sổ 10 phút trôi qua.
- `PostgresSlotWALAtRisk` và `PostgresSlotLost` kêu khi safe WAL tụt từ 8 GiB xuống 1 GiB và `wal_reserved` về 0.
- `DashboardQuerySlow` kêu sau 10 phút p95 ở mức 0,9 s.
- `PostgresSlotMissing` kêu khi series slot biến mất.

**Mẹo stale marker cho `absent()`.** Để test "slot biến mất", phải mô phỏng một series có giá trị rồi ngừng tồn tại. Nếu chỉ không khai báo series thì đó là "chưa từng có". Nếu ngừng ghi giá trị thì Prometheus vẫn coi giá trị cũ còn hiệu lực trong 5 phút (lookback). Giải pháp là ghi giá trị đặc biệt `stale` sau hai giá trị 1: đó là stale marker, giống thứ Prometheus tự chèn khi một target không còn trả series đó. Từ thời điểm ấy `absent()` thành đúng ngay, và alert kêu sau `for` 2 phút.

Lệnh chạy (cũng nằm trong CI):

```bash
docker compose run --rm --no-deps --entrypoint promtool -w /etc/prometheus prometheus test rules alerts.test.yml
```

### 3.4. Thông báo Google Chat

1. **Alertmanager** gom alert theo `alertname`, `job`, `service`; chờ 30 s trước khi gửi nhóm mới, 5 phút giữa hai lần cập nhật một nhóm, nhắc lại sau 4 giờ nếu vẫn kêu, và gửi cả thông báo resolved. Mỗi tin tối đa 15 alert. Rule ức chế: khi `TargetDown` kêu, mọi alert khác có cùng `job` và `instance` bị ẩn.
2. **chat-webhook** (`bridge.py`) nhận POST `/alerts` từ Alertmanager trong mạng nội bộ (không publish cổng), kiểm tra payload (giới hạn 256 KB, phải có danh sách alert), dựng tin nhắn văn bản: trạng thái, severity, tên, summary, description, service, runbook nếu có, kèm link dashboard Platform Health (`GRAFANA_PUBLIC_URL`).
3. URL webhook chỉ được chấp nhận nếu bắt đầu bằng `https://chat.googleapis.com/`. Gửi lỗi thì bridge trả **503** để Alertmanager tự thử lại, và không bao giờ ghi URL hay nội dung trả về vào log (URL chứa khóa).
4. `chat_webhook_deliveries_total{result}` đếm thành công và thất bại; `ChatDeliveryFailed` dựa vào nó. `test_bridge.py` kiểm tra định dạng tin và hợp đồng thử lại mà không gửi thật.

Kiểm thử end-to-end: POST một alert giả có nhãn rõ ràng, thời hạn ngắn vào `/api/v2/alerts` của Alertmanager, rồi xem `chat_webhook_deliveries_total{result="success"}` và `alertmanager_notifications_failed_total`.

---

## 4. Runbook

Các runbook dưới đây gom từ `docs/README.md`, `observability/README.md` và annotation của alert, viết lại thành bước.

### 4.1. Connector hoặc task không RUNNING (`ConnectorNotRunning`)

1. Xem trạng thái và lỗi của task.

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status
```

2. Đọc `trace` của task lỗi. Nếu có "Schema being registered is incompatible" thì sang runbook 4.2.
3. Nếu lỗi là kết nối PostgreSQL hoặc Kafka: kiểm tra hai service đó khỏe, rồi restart riêng task lỗi.

```bash
curl -X POST 'http://localhost:8083/connectors/postgres-connector/restart?includeTasks=true&onlyFailed=true'
```

4. Nếu connector không tồn tại (404): đăng ký lại bằng `debezium/register.sh`.
5. Trong lúc connector dừng, slot vẫn giữ WAL nên không mất dữ liệu; theo dõi `cdc_postgres_slot_safe_wal_bytes` để biết còn bao lâu (runbook 4.3).
6. Đừng thêm consumer hay scale ClickHouse: chúng không chữa được connector đã dừng.

### 4.2. Schema Registry chặn một thay đổi không tương thích

Bối cảnh: Debezium serialize bằng JSON Schema converter, mỗi topic đăng ký schema `<topic>-value` ở chế độ BACKWARD. Thay đổi phá vỡ (đổi kiểu cột, thêm cột bắt buộc) bị registry từ chối, task fail thay vì publish dữ liệu sai.

1. Xác định bảng và cột từ lỗi của task.
2. Nếu thay đổi ở nguồn là nhầm: sửa lại nguồn, restart task.
3. Nếu thay đổi là có chủ ý: chuẩn bị consumer trước (silver spec, sinh lại bằng `gen_silver.py`, xem [03-cdc-ingestion.md](../platform/03-cdc-ingestion.md) và [04-warehouse.md](../platform/04-warehouse.md)).
4. Tạm hạ compatibility **chỉ cho subject đó** về NONE.

```bash
curl -X PUT localhost:8085/config/cdc.ecommerce.<table>-value -H 'Content-Type: application/vnd.schemaregistry.v1+json' -d '{"compatibility":"NONE"}'
```

5. Restart task lỗi.

```bash
curl -X POST 'localhost:8083/connectors/postgres-connector/restart?includeTasks=true&onlyFailed=true'
```

6. Khi schema mới đã đăng ký, xóa cấu hình riêng của subject để quay về BACKWARD mặc định.

```bash
curl -X DELETE localhost:8085/config/cdc.ecommerce.<table>-value
```

7. **Không xóa dòng bronze để dọn dẹp** (kể cả của bảng thử nghiệm): watermark đếm sự kiện của từng giao dịch trong bronze, xóa đi thì giao dịch đó thiếu sự kiện và chặn mọi giao dịch sau nó (xem sự cố 8.2).

### 4.3. WAL của slot tăng (`PostgresWALRetained`, `PostgresSlotWALAtRisk`)

1. Kiểm tra connector có RUNNING không (runbook 4.1). Đây là nguyên nhân phổ biến nhất.
2. Nếu RUNNING mà vẫn tụt: xem `cdc_debezium_behind_source_seconds` và `cdc_debezium_queue_remaining_ratio`. Hàng đợi đầy nghĩa là Kafka không nhận kịp.
3. Xem dung lượng đĩa còn lại (`DiskSpaceLow`).
4. Ước lượng thời gian còn lại: `safe_wal_bytes` chia tốc độ sinh WAL. Khi `PostgresSlotWALAtRisk` kêu (còn dưới 2 GiB), ưu tiên khôi phục tiến độ connector ngay.
5. **Không xóa replication slot** như phản ứng đầu tiên: xóa slot là chấp nhận mất thay đổi và phải snapshot lại.
6. Nếu đã vượt `max_slot_wal_keep_size`: sang runbook 4.7.

### 4.4. CDC lag cao (`CDCLagHigh`, `CDCLagCritical`)

1. Xác định chặng chậm bằng Platform Health:
   - `cdc_debezium_behind_source_seconds` cao: chặng PostgreSQL tới Kafka chậm (Debezium, producer, broker).
   - Debezium bám sát nhưng `kafka_consumergroup_lag` tăng: chặng Kafka tới ClickHouse chậm.
2. Nếu ở ClickHouse: xem bộ nhớ, query đang chạy, merge, `cdc_clickhouse_max_parts_per_partition`.
3. Xem CPU và RAM host; trên một máy, mọi thành phần dùng chung tài nguyên.
4. So sánh tốc độ vào (ingress Kafka) với tốc độ xử lý trước khi thêm consumer; với một broker, số consumer hữu ích bị giới hạn bởi số partition.
5. Trong đợt 11.11, lag tới 30 s vẫn trong SLO; chỉ `CDCLagCritical` là vỡ SLO cao điểm.

### 4.5. Watermark bị kẹt (`TransactionWatermarkStalled`)

1. Xem `consistent.watermark` để biết số giao dịch bị giữ và giao dịch cũ nhất.
2. Xem từng topic có đang được ingest không (một bảng Kafka engine dừng sẽ làm giao dịch có sự kiện ở bảng đó không bao giờ đủ).
3. Xem `bronze.ingest_errors`: message bị cách ly nghĩa là sự kiện đó không vào `cdc_events`.
4. Kiểm tra không ai xóa dòng trong bronze.
5. Giao dịch kẹt quá 30 phút sẽ không còn bị giữ nữa (cơ chế trong `consistent.*`), nhưng phải tìm nguyên nhân. Chi tiết watermark ở [04-warehouse.md](../platform/04-warehouse.md).

### 4.6. Lệch dữ liệu (`WarehouseDataDrift`)

1. Truy vấn `ops.reconciliation_status` để biết bảng nào lệch.
2. Kiểm tra lỗi ingest và thay đổi schema gần đây trước.
3. Chỉ sau khi đã sửa nguyên nhân mới backfill bảng bị ảnh hưởng (re-snapshot qua signal table, xem [03-cdc-ingestion.md](../platform/03-cdc-ingestion.md)).
4. Chạy `check_data.py` khi simulator dừng để xác nhận.

### 4.7. Slot mất hoặc không tồn tại (`PostgresSlotLost`, `PostgresSlotMissing`)

1. Coi như warehouse có thể đã thiếu thay đổi.
2. Đảm bảo connector đang chạy với một slot hợp lệ.
3. Kích hoạt re-snapshot các bảng qua signal table của Debezium.
4. Chạy reconcile và `check_data.py` để xác nhận nguồn và warehouse khớp lại.

### 4.8. Các tình huống khác

- **Mart cũ / reconcile ngừng:** xem log warehouse-jobs; chỉ chạy một replica warehouse-jobs.
- **API chậm hoặc lỗi:** xem lock DB và pool kết nối trước khi thêm replica API.
- **Đĩa đầy:** xem WAL giữ lại, retention Kafka (đang là mặc định của broker), dung lượng bronze.
- **Chat không gửi được:** xem UI Alertmanager và metric của bridge.

---

## 5. Chất lượng: gate và các bộ test

### 5.1. Triết lý

Pipeline CDC hỏng theo những cách test đơn vị không bắt được: deadlock chỉ hiện dưới tải, torn read chỉ hiện khi dữ liệu đang chảy, sai số tiền chỉ hiện khi so nguồn với warehouse. Vì vậy test chủ yếu chạy **trên stack thật** và được gói thành ba mức gate có thời gian khác nhau. Chọn mức nhỏ nhất bao được thay đổi của bạn.

### 5.2. `quality/gate.sh`: ba mức

| Mức | Stack | Chạy gì | Thời gian | Khi nào dùng |
|---|---|---|---|---|
| `quick` | Stack đang chạy, giữ dữ liệu | Build lại source-api, simulator, warehouse-jobs, control-center; reset bộ đếm deadlock; API test và stress test; 90 s traffic ở ×720 kèm probe torn-read; báo lag; `check_data` | ~3–4 phút | Sau mỗi thay đổi code |
| `smoke` | Sạch (xóa volume) | Seed, test, 4 phút traffic ở ×1440 có một đợt 11.11 ở giây 120, probe, lag, `check_data` | ~8–10 phút | Khi đổi schema hoặc SQL ClickHouse (init script chỉ chạy trên volume mới); CI chạy mức này |
| `full` | Sạch | 16 phút ở ×720 (khoảng 8 ngày nghiệp vụ), 11.11 ở giây 320, **restart Debezium** ở giữa và **restart ClickHouse** ở hai phần ba thời gian, probe, lag, `check_data` | ~25 phút | Trước khi push một mốc lớn |

Cách gate chạy:

1. Nếu là `smoke` hoặc `full`: reset toàn bộ (xóa volume và thư mục dữ liệu PostgreSQL).
2. Bật dịch vụ lõi, chờ Kafka Connect, đăng ký connector nếu chưa có, in trạng thái.
3. Bật simulator và Control Center, chờ seed xong.
4. Chạy `test_source_api.py` và `test_stress.py` trong container validator; lỗi thì in 30 dòng cuối và log deadlock rồi dừng.
5. Đặt tốc độ đồng hồ, bật generator qua Control Center API (40 phiên mua mỗi tick, 6 khách mới mỗi tick), chạy probe song song.
6. Mỗi 5 giây kiểm tra bộ đếm deadlock: **có deadlock là dừng ngay** và in đoạn log PostgreSQL liên quan, không chờ hết giờ.
7. Bắn 11.11 và chaos (restart) đúng thời điểm của mức đó.
8. Dừng generator, trả đồng hồ về ×1, chờ probe; probe không PASS thì gate fail.
9. In lag commit tới ClickHouse (p50, p95) của chính lần chạy này, rồi chạy `check_data`.

`quick` tự chuyển sang dựng lõi và seed nếu Kafka Connect chưa chạy hoặc database chưa có shop nào. Kết quả trung gian ghi ở `/tmp/cdc-gate` (đổi bằng `GATE_OUT`).

```bash
quality/gate.sh quick
```

Vì sao có ba mức: nếu chỉ có một gate 25 phút, người ta sẽ không chạy nó sau mỗi thay đổi. `quick` đủ nhanh để chạy thường xuyên; `smoke` bắt lỗi init trên volume mới; `full` bắt lỗi khôi phục sau restart (offset, consumer, watermark) mà hai mức kia không thử.

### 5.3. Các bộ test

| File | Kiểm tra gì | Chạy ở đâu |
|---|---|---|
| `test_source_api.py` | 25 test chức năng cho luật marketplace: tách đơn theo shop, phí ship theo rate card, COD, chống trùng callback thanh toán, thanh toán lại, refund thanh toán trễ, không oversell khi checkout đồng thời, ai được đổi trạng thái đơn, sự kiện vận chuyển trễ / sai thứ tự / trùng, flash sale, voucher (phân bổ xuống dòng, trả lại, giới hạn dưới tải, quota chia slot), đổi giá chỉ áp cho đơn mới, trả hàng một phần / toàn phần, tranh chấp, review, đồng hồ nghiệp vụ. Mỗi test tự tạo shop, sản phẩm, người mua nên chạy song song với simulator được | Mọi gate |
| `test_stress.py` | 24 luồng bắn checkout, hủy và thanh toán vào cùng vài SKU, flash item, voucher trong 20 s; yêu cầu **không deadlock** (đọc bộ đếm của PostgreSQL, vì API tự retry deadlock nên client không thấy) và không lệch tồn kho, quota flash, quota voucher | Mọi gate |
| `test_auth.py` | Gọi ẩn danh cần token; token giả hoặc của issuer khác bị từ chối; buyer chỉ hành động cho chính mình; đơn chỉ hiện cho buyer và shop của nó; seller chỉ quản lý shop của mình; webhook phải được đúng đối tác ký; idempotency key theo từng caller; route không có `/v1` đã bỏ; xoay refresh token và phát hiện tái sử dụng; khóa tài khoản sau nhiều lần sai mật khẩu; service client không vượt quyền | Chạy tay (chưa nằm trong gate) |
| `probe_consistency.py` | Probe torn-read, xem 5.4 | Mọi gate, song song với traffic |
| `check_data.py` | Hơn chục nhóm kiểm tra tiền, trạng thái, tồn kho, vận chuyển, khuyến mãi, trả hàng, settlement, write path, pipeline (PostgreSQL so với silver cho 31 bảng), metric gold tính lại trong PostgreSQL, mart, snapshot, reconcile, PII không lọt vào ClickHouse. Chạy khi simulator đã dừng; exit khác 0 nếu có lỗi. Chi tiết ở [05-mart-va-metric.md](../platform/05-mart-va-metric.md) | Cuối mọi gate |

Chạy riêng một bộ (simulator nên dừng với `check_data`):

```bash
docker compose --profile tools run --rm validator pytest -q test_auth.py
```

```bash
docker compose --profile tools run --rm validator python check_data.py
```

### 5.4. Probe torn-read (`probe_consistency.py`)

Vấn đề: mỗi bảng nguồn có topic riêng và ClickHouse tiêu thụ các topic độc lập, nên trong vài giây silver có thể thấy đơn COMPLETED mà chưa thấy bút toán SALE được ghi cùng giao dịch. Lớp `consistent.*(v)` giải quyết việc này (xem [04-warehouse.md](../platform/04-warehouse.md)); probe chứng minh nó hoạt động.

Cách làm:

1. Định nghĩa 6 bất biến đúng bên trong một giao dịch PostgreSQL nhưng trải trên nhiều topic: đơn hoàn tất có bút toán SALE (trừ đơn đã hoàn tiền toàn bộ), dòng đơn có đơn, đơn có checkout, payout có bút toán PAYOUT, tồn kho bằng tổng sổ kho, voucher usage có checkout.
2. Mỗi 2 giây, đếm vi phạm qua hai cách đọc: `silver FINAL` và `consistent.*` với **một watermark cố định cho cả lần đọc**.
3. Bỏ qua những lần ClickHouse không trả lời (bước chaos của gate `full`).
4. In bảng: số vi phạm lớn nhất và số lần thấy vi phạm cho mỗi bất biến và mỗi cách đọc, cùng số giao dịch bị giữ lại tối đa.
5. **PASS** chỉ khi `consistent.*` không bao giờ vi phạm. Silver được phép vi phạm; con số đó chứng minh vấn đề là có thật (một lần đo dưới tải thấy 20 đơn như vậy cùng lúc).

### 5.5. Load test (`quality/load_test.sh`)

Mục tiêu: đo xem stack một máy chạm tới đâu so với mức cao điểm T1 khoảng **5.400 sự kiện CDC/s** (tương ứng 1 triệu đơn/ngày).

Cách hoạt động:

1. Bật N simulator phụ (`simulator-load`, profile `load`, không seed) bên cạnh simulator chính. Mặc định 5 bản, 600 giây, tốc độ ×1440.
2. Đặt tốc độ đồng hồ, ghi lại bộ đếm deadlock và thời điểm bắt đầu.
3. Start simulator chính qua Control Center và từng simulator phụ bằng `docker exec` vào container của nó (simulator phụ không publish cổng). Mỗi tick 50 phiên mua, 6 khách mới.
4. Ngủ hết thời gian, rồi stop từng simulator phụ (lệnh stop có timeout 120 s vì nó chờ tick đang chạy xong), stop simulator chính, trả đồng hồ về ×1, dừng các container phụ.
5. Báo cáo: sự kiện CDC mỗi phút và lag p50/p95 từ bronze; từ Prometheus: request/s, 5xx/s, checkout p99, API p95, Debezium behind tối đa, consumer lag tối đa, thời lượng snapshot delta tối đa; số deadlock.

```bash
quality/load_test.sh 7 600 1440
```

**Kết quả** (1 + 7 simulator ở ×1440, laptop 16 core):

| Chỉ số | Giá trị |
|---|---|
| Sự kiện CDC/s | khoảng 1.800 (đỉnh 1.961), so với mục tiêu T1 5.400 |
| Request API/s | khoảng 620 |
| Lag commit tới ClickHouse p95 | khoảng 1,6 s |
| Deadlock | 0 |
| Checkout p99 | tăng lên 3,4 s |

Kết luận:

- **Nút thắt là write path, không phải CDC.** PostgreSQL dùng khoảng 6 core, API khoảng 3 core, chủ yếu chờ lock trên dòng `inventory` của các SKU bán chạy (hot row). Phía CDC giữ được latency khoảng 1,6 s p95 trong suốt bài test.
- Hướng cải thiện tiếp theo: chia tồn kho của SKU bán chạy thành nhiều slot, giống cách đã làm với quota voucher.
- Các mốc trước đó: tuning Debezium (R10) đưa p95 từ 1,86 s xuống 1,57 s ở khoảng 990 sự kiện/s; claim quota voucher muộn đưa checkout p99 ở 6 simulator từ 1,96 s xuống 0,82 s.

---

## 6. CI

Ba workflow trong `.github/workflows/`, tất cả chạy trên `ubuntu-latest`:

| Workflow | Kích hoạt | Chạy gì |
|---|---|---|
| `data-contract.yml` | push hoặc PR đổi `source/postgres/**`, `warehouse/clickhouse/**`, `debezium/**` | Dựng PostgreSQL 15 làm service container (mật khẩu `admin` dùng một lần), nạp `init.sql`, chạy `check_contract.py`: bảng stream nào thiếu silver spec, cột mới chưa khai báo, lệch kiểu hoặc nullability, cột PII băm không phải string, bảng trạng thái thiếu `REPLICA IDENTITY FULL`, `02-silver.sql` lỗi thời. Chi tiết ở [04-warehouse.md](../platform/04-warehouse.md) |
| `observability.yml` | push hoặc PR đổi `observability/**`, mọi `compose.yml`, `docker-compose.yml` | Copy `.env.example`, tạo file webhook rỗng; `docker compose config --quiet`; `promtool check config`; `promtool test rules alerts.test.yml`; `amtool check-config`; unit test của bridge; sinh lại dashboard và `git diff --exit-code` để chắc JSON đã commit khớp generator |
| `gate-smoke.yml` | push lên `main` hoặc PR đổi `source/**`, `debezium/**`, `kafka/**`, `warehouse/**`, `quality/**`, `docker-compose.yml`; chạy tay được | `setup-env.sh` tạo secret ngẫu nhiên dùng một lần, file webhook rỗng (không gửi Chat), rồi `quality/gate.sh smoke`. Timeout 40 phút; lần chạy mới trên cùng ref hủy lần cũ. Fail thì gom `docker compose ps` và 300 dòng log mỗi service thành artifact |

**Secret dùng một lần.** CI không có và không cần secret thật: `setup-env.sh` sinh giá trị ngẫu nhiên mỗi lần chạy, workflow data-contract dùng mật khẩu cố định cho một container sống vài phút. Không có GitHub secret nào được cấu hình.

**Vì sao vẫn giữ CI dù mọi thứ chạy local.** Máy dev luôn có volume đã khởi tạo từ trước, nên những lỗi chỉ xuất hiện trên môi trường sạch không bao giờ lộ ra ở local:

- **Init trên volume mới bị hỏng.** Init script của PostgreSQL và ClickHouse chỉ chạy khi volume rỗng. Thứ tự init sai (sự cố 8.3) hay SeaweedFS không có volume ghi được trên đĩa nhỏ (sự cố 8.5) chỉ hiện ra trên runner sạch.
- **Alert hỏng.** Một biểu thức sai cú pháp hoặc sai logic làm alert im lặng mãi mãi; promtool test bắt được trước khi nó cần kêu.
- **Data contract trôi.** Thêm một cột ở nguồn mà quên silver spec thì warehouse sai trong im lặng; workflow chặn ngay ở PR.
- **Dashboard lệch generator** và **compose không hợp lệ** cũng bị chặn.

---

## 7. Control Center

### 7.1. Làm gì

Web app ở `http://localhost:8000`, dành cho người vận hành demo:

- **Thanh pipeline:** tình trạng từng chặng Simulator → Source API → PostgreSQL (số dòng vài bảng chính, độ trễ slot tính bằng byte từ `confirmed_flush_lsn`) → Debezium (trạng thái connector, RUNNING / DEGRADED / NOT_REGISTERED / UNREACHABLE, nút đăng ký) → Kafka (sự kiện/s) → ClickHouse (số dòng bronze). Làm mới mỗi 3 giây.
- **Simulator:** start/stop generator, chỉnh số phiên mua và khách mới mỗi giờ nghiệp vụ, tick interval; tốc độ đồng hồ ×60, ×240, ×720, ×1440 (×1440 nghĩa là một ngày nghiệp vụ trôi trong 1 phút thật); nút **Flash sale now** và **Start 11.11** (mega sale); bắn từng hành động ×1/×5/×20; tiến độ seed và activity log.
- **Luồng CDC trực tiếp:** sự kiện/s theo loại thao tác (create, update, delete, read), bảng bận nhất, feed sự kiện mới nhất; bấm vào một UPDATE để xem diff từng trường trước và sau (bỏ `updated_at` vì luôn đổi).
- **Phân tích:** link sang hai dashboard Grafana. Các màn hình phân tích warehouse trước đây đã chuyển hẳn sang Grafana; Control Center chỉ còn kiểm tra ClickHouse sống và đếm bronze.

Control Center **không bao giờ ghi vào PostgreSQL**: nó đọc bằng `app_reader` (read-only) và chuyển tiếp lệnh điều khiển cho simulator, còn simulator chỉ ghi qua Source API.

### 7.2. Cấu trúc

- **Một container**, build hai bước: bước 1 dùng Node 20 build React UI (Vite, Tailwind, TypeScript); bước 2 là Python 3.11 chạy FastAPI phục vụ cả API lẫn UI đã build. Build context là gốc repo vì image cần copy `debezium/postgres-connector.json` để nút đăng ký dùng.
- **Backend** (`apps/control-center/backend/`):
  - `main.py`: `/api/health` (gom tình trạng PostgreSQL, Debezium, Kafka, ClickHouse, simulator, Source API), `/api/connector/register` (điền PII salt rồi POST tạo connector; 409 "đã tồn tại" được coi là thành công), các route `/api/generator/*`, `/api/actions/{name}`, `/api/clock` chuyển tiếp sang simulator, `/api/cdc?since=` cho feed.
  - `cdc_stream.py`: một luồng nền tiêu thụ mọi topic `cdc.ecommerce.*` bằng regex, group id ngẫu nhiên mỗi lần khởi động, đọc từ `latest`, không commit offset. Giữ 400 sự kiện gần nhất và bộ đếm theo giây trong cửa sổ 120 s; tự kết nối lại sau 5 s khi lỗi.
  - `warehouse.py`: client ClickHouse riêng cho mỗi thread (thư viện không thread-safe), chỉ dùng cho ping và đếm bronze.
- **Frontend** (`apps/control-center/frontend/src/`): `App.tsx` (bố cục, link Grafana), `components/PipelineBar.tsx`, `GeneratorPanel.tsx` (poll mỗi 1,5 s), `LivePanel.tsx` (poll feed theo `since`, tự reset khi backend khởi động lại), `hooks.ts` (hook poll giữ giá trị tốt cuối cùng khi lỗi thoáng qua).
- Dev có hot reload: backend chạy uvicorn trên host trỏ vào `localhost:29092`, frontend chạy Vite ở 5173 và proxy `/api` sang 8000.

### 7.3. Bỏ header Schema Registry

Từ khi có Schema Registry, mỗi message Kafka bắt đầu bằng 5 byte header của Confluent: một magic byte `0x00` và 4 byte schema id, sau đó mới là JSON như cũ. Feed của Control Center kiểm tra byte đầu tiên: nếu là `0x00` thì bỏ 5 byte đầu rồi parse JSON. Message không parse được thì bị bỏ qua. Cách làm này giống phía ClickHouse (đọc `RawBLOB` rồi cắt header), nên không cần client Schema Registry: schema chỉ dùng để chặn thay đổi phá vỡ, còn payload vẫn là JSON tự mô tả.

---

## 8. Sự cố và bài học

Mỗi sự cố: chuyện gì xảy ra, vì sao, xử lý thế nào, bài học.

### 8.1. Healthcheck Schema Registry dùng `curl` mà image không có

- **Chuyện gì:** sau khi thêm Schema Registry, healthcheck viết bằng `curl` luôn fail vì image `cp-schema-registry` không có `curl`. Registry thực ra vẫn chạy, nhưng compose coi nó là unhealthy, và Debezium (phụ thuộc `service_healthy`) không bao giờ khởi động.
- **Ảnh hưởng:** không có sự kiện CDC nào trong thời gian đó, nhưng **không mất dữ liệu**: replication slot giữ toàn bộ WAL chưa đọc, khi Debezium lên nó đọc tiếp từ offset cũ.
- **Xử lý:** viết lại healthcheck bằng HTTP thô qua `/dev/tcp` của bash, gửi `GET /config` và chờ dòng trạng thái 200.
- **Bài học:** healthcheck phải được kiểm chứng trên chính image đó; một healthcheck sai nguy hiểm hơn không có healthcheck vì nó chặn cả chuỗi `depends_on`. Và đây là ví dụ thực tế về giá trị của slot: dừng consumer không làm mất thay đổi, miễn là slot còn trong giới hạn WAL.

### 8.2. Xóa dòng bronze của bảng thử làm kẹt watermark

- **Chuyện gì:** khi kiểm chứng Schema Registry bằng một bảng thử (đổi cột `INT` sang `TEXT`), các dòng bronze của bảng thử bị xóa để dọn dẹp. Watermark ngừng tiến.
- **Vì sao:** `consistent.watermark` so số sự kiện mỗi giao dịch khai báo (từ `cdc.transaction`) với số sự kiện thực sự có trong bronze. Xóa sự kiện thì giao dịch đó vĩnh viễn "chưa về đủ" và giữ lại mọi giao dịch sau nó.
- **Xử lý:** xóa 2 dòng giao dịch mồ côi tương ứng trong bảng giao dịch để watermark chạy tiếp.
- **Bài học:** quy tắc **không bao giờ xóa bronze**; bronze là nguồn sự thật cho audit, replay và watermark. Quy tắc này đã được ghi vào runbook.

### 8.3. Init script PostgreSQL chạy sai thứ tự trên volume mới

- **Chuyện gì:** file tên `02a-roles` chạy **trước** `02-monitoring` trên volume mới, và init thất bại. Trên máy dev không thấy vì volume đã khởi tạo từ trước.
- **Vì sao:** entrypoint của image postgres sắp xếp file theo glob, và cách so sánh theo locale bỏ qua dấu câu, nên thứ tự không như mắt người đọc.
- **Phát hiện:** khi khởi tạo PostgreSQL và ClickHouse + SeaweedFS trên container tạm với secret dạng file.
- **Xử lý:** đánh số lại thành `01` tới `05` thuần chữ số, ghi chú ngay trong compose vì sao phải đánh số như vậy.
- **Bài học:** đừng dựa vào thứ tự sắp xếp có dấu câu; luôn thử khởi tạo trên volume sạch (đó là việc của gate `smoke` và CI).

### 8.4. Image MinIO không còn được publish

- **Chuyện gì:** khi làm cold tier S3 cho bronze, image MinIO không còn tải được.
- **Xử lý:** chuyển sang SeaweedFS 4.47 (có API S3), chạy một process gồm master, volume, filer và S3, với một identity lấy từ file secret và bucket `bronze-cold` tạo sẵn.
- **Bài học:** phụ thuộc vào image bên thứ ba là một rủi ro vận hành; ghim phiên bản cụ thể và giữ phần tích hợp ở mức API chuẩn (S3) để thay được.

### 8.5. SeaweedFS không có volume ghi được trên đĩa nhỏ của CI

- **Chuyện gì:** cấu hình mặc định của SeaweedFS dùng volume 30 GB mỗi cái và tính số volume theo dung lượng đĩa trống. Trên runner CI đĩa nhỏ, kết quả là không có volume nào ghi được, ClickHouse không ghi được sang cold disk.
- **Xử lý:** cố định kích thước volume 1 GB và số volume tối đa 50.
- **Bài học:** giá trị mặc định "tự thích nghi" theo tài nguyên máy làm môi trường hành xử khác nhau; với hạ tầng, cấu hình tường minh an toàn hơn.

### 8.6. API bị chặn ở khoảng 380 request/s

- **Chuyện gì:** load test cho thấy Source API không vượt quá khoảng 380 request/s.
- **Vì sao:** Python chạy một core mỗi process; với 2 worker Uvicorn, API chỉ dùng được 2 core.
- **Xử lý:** tăng lên 6 worker (`API_WORKERS`), pool 12 kết nối mỗi worker (`DB_POOL_MAX`), tổng 72 trên 200 kết nối của PostgreSQL (nâng `max_connections` lên 200).
- **Bài học:** đo trước khi tối ưu; khi tăng worker phải tính lại tổng kết nối tới database.

### 8.7. Timeout trong load script để simulator chạy mãi

- **Chuyện gì:** lệnh stop gửi tới simulator phụ có timeout ngắn, mà stop phải chờ tick đang chạy xong; dưới tải, tick lâu hơn timeout, lệnh stop "lỗi" và simulator tiếp tục sinh tải sau khi bài test đã kết thúc, làm bẩn số liệu lần sau.
- **Xử lý:** lệnh stop giờ có timeout 120 s; script hiện in cảnh báo cho từng simulator dừng lỗi và cuối cùng dừng hẳn các container `simulator-load`, nên tải không thể kéo dài sau bài test.
- **Bài học:** script test phải dọn dẹp chắc chắn kể cả khi một bước lỗi.

### 8.8. Đóng sổ sai 15 ngày nghiệp vụ

- **Chuyện gì:** warehouse-jobs đóng băng ngày nghiệp vụ đã qua vào `gold.daily_sales_closed`. Ở tốc độ đồng hồ cao, một giao dịch thanh toán chậm có thể commit **sau** khi ngày của đơn đã đóng, nên con số "đã báo cáo" sai; 15 ngày bị đóng sai.
- **Xử lý:** chỉ đóng một ngày khi không còn đơn nào của ngày đó đang chờ thanh toán (`UNPAID`); đóng lại các ngày bị sai. `check_data` kiểm tra ngày đã đóng phải giữ nguyên GMV.
- **Bài học:** "đã qua theo đồng hồ" không có nghĩa là "đã xong theo dữ liệu"; điều kiện đóng sổ phải dựa trên trạng thái nghiệp vụ.

### 8.9. Hot row quota voucher

- **Chuyện gì:** trong 11.11, mọi checkout dùng cùng một voucher nền tảng đều cập nhật một dòng quota, xếp hàng chờ nhau.
- **Xử lý (hai bước):** chia quota thành tối đa 16 slot, checkout chọn ngẫu nhiên và bỏ qua slot bận; sau đó chỉ đọc quota lúc đầu checkout và **claim ở cuối**, nên slot chỉ bị khóa vài mili giây. Checkout p99 ở 6 simulator giảm từ 1,96 s xuống 0,82 s.
- **Bài học:** với hot row, giảm thời gian giữ lock hiệu quả ngang với chia nhỏ dòng. Tồn kho của SKU bán chạy là hot row tiếp theo cần làm (mục 5.5).

### 8.10. Race giá khi checkout

- **Chuyện gì:** ở ×1440 với 8 simulator, một lần đổi giá commit trong lúc checkout đang chạy làm đơn mang giá cũ sau khi giá mới đã có hiệu lực. `check_data` (so giá lúc đặt với `price_history`) phát hiện.
- **Xử lý:** checkout đọc giá SKU với `FOR SHARE`, nên đổi giá phải chờ checkout xong (hoặc ngược lại).
- **Bài học:** đọc rồi dùng giá trị trong cùng giao dịch vẫn có race nếu không khóa; kiểm tra dữ liệu dưới tải cao bắt được lỗi mà test chức năng không thấy.

---

## 9. Lỗ hổng đã biết và lộ trình

- **Backup mới có cho ClickHouse.** `warehouse/backup.sh` (mục 1.9, chi tiết ở [04-warehouse.md](../platform/04-warehouse.md#11-backup-và-khôi-phục)) backup full và incremental, restore rồi đọc bù từ Kafka. Còn thiếu: lịch chạy tự động, bản sao ra ngoài máy (backup nằm cùng đĩa với dữ liệu), alert khi backup cũ, test khôi phục định kỳ trong gate; PostgreSQL và các volume khác chưa có backup. Dựng silver từ bronze trên lake (R21) cũng chưa làm.
- **Sự kiện trùng chưa được test có chủ đích.** Thiết kế chịu được trùng (silver lấy phiên bản LSN cao nhất, `cdc_events_total` đã tính cả bản giao lại), gate `full` có restart Debezium và ClickHouse, nhưng chưa có test bơm sự kiện trùng rồi kiểm tra kết quả.
- **Một node.** Một broker Kafka (RF=1), một PostgreSQL, một ClickHouse, một Connect worker; không có failover. Nghiên cứu HA: [tài liệu HA](https://claude.ai/code/artifact/61133ce8-61eb-4076-ab92-31738254ec53).
- **Thông lượng T1.** Đo được khoảng 1.800 sự kiện/s so với 5.400 cần cho cao điểm T1, bị chặn ở write path (tồn kho hot row). Ở quy mô T1 còn cần nhiều broker, partition theo order, nhiều Connect worker, replica cho PostgreSQL và ClickHouse, và aggregate từng ngày cho các mart nặng.
- **Kafka và database chưa có TLS/SASL.** Chỉ gateway có HTTPS; trong mạng compose mọi thứ là plaintext.
- **Control Center không có xác thực.** Nó chỉ bind localhost; dự định đặt sau oauth2-proxy. Nó cũng kết nối ClickHouse bằng user `admin` dù chỉ cần đọc.
- **Secret cũ trong lịch sử git.** Mật khẩu dev và PII salt cũ từng nằm trong repo trước khi chuyển sang `secrets/`; với mọi môi trường dùng chung phải đổi. Có hai cách: sửa từng file rồi chạy `./observability/start.sh` để áp lên volume đang có, hoặc `./setup-env.sh --rotate` để sinh lại toàn bộ secret (giữ CA và chứng chỉ TLS). Cách thứ hai từ chối chạy khi stack còn container, vì database chỉ đặt mật khẩu ở lần khởi động đầu, nên nó đi kèm một lần reset dữ liệu. Đổi PII salt làm thay đổi mọi giá trị băm, nên chỉ đặt một lần cho mỗi môi trường.
- **Chưa chia sẻ secret an toàn cho nhóm.** Dự định SOPS + age.
- **Retention Kafka** vẫn là mặc định của broker.
- **Thiếu giám sát gateway và per-container.** Metric Traefik được scrape nhưng chưa có panel hay alert; không có metric theo container và không có distributed tracing.
