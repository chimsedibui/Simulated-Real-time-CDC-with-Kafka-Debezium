# Q&A 06 · Vận hành, giám sát và chất lượng

[Tài liệu dự án](../README.md) / [Phỏng vấn](README.md)

## Đọc file này để biết gì

Bộ câu hỏi ôn phỏng vấn cho phần vận hành của dự án: chạy stack, giám sát, alert, runbook, kiểm thử, CI, Control Center và các sự cố đã xử lý. Mỗi câu có mức độ **[Cơ bản]**, **[Trung bình]** hoặc **[Nâng cao]**. Câu trả lời bám vào code hiện tại; giải thích đầy đủ nằm ở [06-van-hanh.md](../operations/06-van-hanh.md).

## Mục lục

- [A. Chạy hệ thống](#a-chạy-hệ-thống) (Q1–Q13)
- [B. Giám sát và metric](#b-giám-sát-và-metric) (Q14–Q30)
- [C. Alert và thông báo](#c-alert-và-thông-báo) (Q31–Q47)
- [D. Tình huống xử lý sự cố](#d-tình-huống-xử-lý-sự-cố) (Q48–Q64)
- [E. Chiến lược kiểm thử](#e-chiến-lược-kiểm-thử) (Q65–Q79)
- [F. Load test và hiệu năng](#f-load-test-và-hiệu-năng) (Q80–Q86)
- [G. CI](#g-ci) (Q87–Q92)
- [H. Control Center](#h-control-center) (Q93–Q97)
- [I. Sự cố đã gặp và câu hỏi mở](#i-sự-cố-đã-gặp-và-câu-hỏi-mở) (Q98–Q106)
- [J. Lỗ hổng và lộ trình](#j-lỗ-hổng-và-lộ-trình) (Q107–Q114)

---

## A. Chạy hệ thống

### Q1. Để chạy dự án lần đầu cần những bước nào? [Cơ bản]

Chạy `./setup-env.sh` để tạo `secrets/` và `.env`, tạo file `observability/secrets/google-chat-webhook` (có URL thật hoặc để rỗng), rồi `docker compose up -d`. Sau đó bắt buộc đăng ký connector bằng `debezium/register.sh` (hoặc nút register trong Control Center), vì connector không tự đăng ký. Cuối cùng mở Control Center ở cổng 8000 và bấm Start simulation; simulator tự seed theo preset `SIM_SCALE`.

### Q2. `setup-env.sh` tạo ra những gì? [Cơ bản]

Nó tạo thư mục `secrets/` (gitignored) với một file cho mỗi secret: mật khẩu các tài khoản PostgreSQL, ClickHouse, Grafana, PII salt, khóa S3 cho cold storage; thêm pepper và mật khẩu admin của identity, secret của các service client, khóa ký webhook cho 8 đối tác thanh toán và vận chuyển, và một CA local cùng chứng chỉ server cho gateway. Nó cũng tạo `.env` từ `.env.example` cho cấu hình không bí mật như `AUTH_MODE`. Script idempotent: chạy lại chỉ thêm cái thiếu, và chuyển mật khẩu từ `.env` cũ sang file.

### Q3. Vì sao secret là file mount dưới `/run/secrets` thay vì biến môi trường? [Trung bình]

Biến môi trường hiện ra trong `docker inspect`, trong file compose và dễ lọt vào log hay cấu hình connector lưu trên Kafka. File mount read-only chỉ tồn tại trong container cần nó, và mỗi container chỉ mount đúng file của mình. PostgreSQL, ClickHouse, Grafana đọc qua cơ chế `*_FILE` có sẵn; Debezium đọc qua `DirectoryConfigProvider`, nên config connector chỉ chứa tham chiếu tới file chứ không chứa mật khẩu.

### Q4. Bố cục compose được tổ chức thế nào và vì sao? [Cơ bản]

`docker-compose.yml` gốc chỉ đặt tên project `realtime-cdc` và include 8 file `compose.yml`, mỗi file một vùng: source, kafka, debezium, warehouse, apps, quality, observability, gateway. Cách này giữ ranh giới trách nhiệm rõ, mỗi phần đọc và sửa độc lập, và CI lọc được theo đường dẫn (ví dụ chỉ kiểm tra observability khi thư mục đó đổi). Compose vẫn coi tất cả là một project, nên `depends_on` xuyên file vẫn hoạt động.

### Q5. Profile `tools` và `load` dùng để làm gì? [Cơ bản]

Profile giữ những service không nên chạy mặc định. `tools` chứa `validator`, image chứa toàn bộ test và script kiểm tra, chạy theo yêu cầu với `docker compose --profile tools run --rm validator ...`. `load` chứa `simulator-load`, bản sao simulator không seed, được `load_test.sh` scale lên N bản để tạo tải.

### Q6. Thứ tự khởi động được đảm bảo như thế nào? [Trung bình]

Bằng `depends_on` với điều kiện `service_healthy`, mỗi service có healthcheck thật. Chuỗi chính: postgres → identity → source-api → simulator; kafka → schema-registry → debezium (cũng cần postgres); seaweedfs (chờ file đánh dấu bucket đã tạo) và kafka → clickhouse → warehouse-jobs; control-center chờ postgres, kafka, clickhouse và simulator. Service giám sát không phụ thuộc gì vì Prometheus chịu được target chưa lên.

### Q7. `observability/start.sh` khác `docker compose up -d` ở điểm nào? [Trung bình]

Init script của PostgreSQL và ClickHouse chỉ chạy trên volume mới, nên khi volume đã tồn tại hoặc sau khi đổi secret thì `up -d` không áp lại tài khoản và mật khẩu. `start.sh` bật lõi, chạy lại SQL role, script mật khẩu, script identity, các SQL monitoring/snapshot/cold storage của ClickHouse, đăng ký connector, bật phần còn lại và đặt lại mật khẩu admin Grafana. Mọi bước idempotent. Nó yêu cầu file webhook Chat có nội dung.

### Q8. Các cổng được publish thế nào và vì sao bind 127.0.0.1? [Cơ bản]

Mọi cổng publish đều bind `127.0.0.1`, ví dụ Control Center 8000, Source API 8100, simulator 8200, identity 8300, gateway 8443, PostgreSQL 5432, Kafka 29092, Schema Registry 8085, Debezium 8083, ClickHouse 8123/9000, Grafana 3000, Prometheus 9090, Alertmanager 9093. Lý do là các service nội bộ không có TLS và nhiều cái không có xác thực (Kafka UI, Connect REST, Control Center), nên không được lộ ra mạng LAN. Muốn mở ra ngoài phải đổi binding và mật khẩu trước. Các exporter, Jolokia, SeaweedFS và listener Kafka nội bộ hoàn toàn không publish.

### Q9. Máy local bị trùng cổng 5432 thì làm sao? [Cơ bản]

Tạo `docker-compose.override.yml` (gitignored); compose nạp file này tự động. Trên máy hiện tại nó đổi PostgreSQL sang `127.0.0.1:15432` và Kafka UI sang `18082`, dùng `!override` để thay hẳn danh sách cổng. Vì gitignored nên mỗi máy có bản riêng và CI không bị ảnh hưởng. Cần tránh đổi các cổng mà `gate.sh` gọi thẳng (8000, 8083, 8100, 8200).

### Q10. Dữ liệu nằm vật lý ở đâu? [Trung bình]

PostgreSQL nằm ở bind mount `source/postgres/data` trong repo, thuộc user postgres của container. Còn lại là named volume: Kafka (gồm cả offset của Connect và topic `_schemas`), ClickHouse, SeaweedFS, khóa ký của identity, Prometheus, Alertmanager, Grafana; trên máy này chúng nằm ở `/var/lib/docker/volumes/realtime-cdc_<tên>/_data` trong Ubuntu WSL2. Dashboard và datasource Grafana không nằm trong volume mà provisioning từ file trong repo.

### Q11. Reset toàn bộ hệ thống thế nào? [Cơ bản]

Chạy `docker compose --profile tools down -v` để xóa container và mọi named volume, rồi xóa `source/postgres/data`. Vì thư mục đó thuộc user của container, `gate.sh` xóa bằng một container alpine tạm thay vì `sudo`. Sau đó bật lại stack và đăng ký connector.

### Q12. Vì sao phải reset PostgreSQL và Kafka cùng nhau? [Nâng cao]

Replication slot nằm trong PostgreSQL, còn offset đã đọc của Debezium nằm trong topic `debezium-offsets` của Kafka. Xóa Kafka mà giữ PostgreSQL thì connector mất offset và slot cũ có trạng thái không khớp; xóa PostgreSQL mà giữ Kafka thì offset trỏ vào LSN của một database không còn tồn tại. Cả hai trường hợp đều làm connector hành xử khó đoán (snapshot lại hoặc lỗi), nên reset luôn đi theo cặp.

### Q13. Muốn dựng lại riêng warehouse thì sao? [Trung bình]

Xóa container clickhouse, xóa volume `realtime-cdc_clickhouse_data` rồi bật lại clickhouse; init SQL chạy lại và các bảng Kafka engine đọc lại mọi thứ Kafka còn giữ. Cách này dùng khi sửa SQL trong `warehouse/clickhouse/init/`. Hạn chế là chỉ khôi phục được những gì còn trong retention của Kafka; phần bronze cũ hơn không tự quay lại.

---

## B. Giám sát và metric

### Q14. Làm sao biết pipeline đang chậm? [Cơ bản]

Nhìn `cdc_ingest_lag_p95_seconds` trên dashboard Platform Health: p95 của thời gian từ commit ở PostgreSQL tới lúc dòng vào bronze trong 5 phút gần nhất. SLO là 5 s (30 s lúc cao điểm), có alert `CDCLagHigh` và `CDCLagCritical`. Để biết chậm ở đâu, xem thêm `cdc_debezium_behind_source_seconds` (chặng Debezium) và `kafka_consumergroup_lag` (chặng ClickHouse tiêu thụ).

### Q15. Vì sao chọn Prometheus cộng một exporter tự viết? [Trung bình]

Những tín hiệu quan trọng nhất là tín hiệu riêng của pipeline: lag commit tới ingest, tuổi giao dịch bị watermark giữ, tuổi mart, số bảng reconcile lệch, safe WAL của slot. Không có exporter có sẵn nào cung cấp chúng. Một exporter duy nhất với tài khoản read-only `monitor_reader` cho cả PostgreSQL và ClickHouse thì ít quyền, ít container và kiểm soát được cardinality. Kafka và host thì dùng exporter có sẵn vì không có gì đặc thù.

### Q16. Vì sao exporter thu thập chạy nền thay vì truy vấn khi bị scrape? [Nâng cao]

Nếu chạy SQL trong lúc scrape, một ClickHouse chậm sẽ làm scrape timeout, Prometheus ghi `up = 0` và ta mất cả những metric khác lẫn khả năng phân biệt lỗi. Chạy nền mỗi 15 giây thì scrape luôn nhanh, còn sức khỏe từng dependency được báo riêng bằng `cdc_dependency_up` và `cdc_dependency_last_success_timestamp_seconds`. Nhờ đó có alert `CollectorStalled` cho trường hợp scrape vẫn thành công nhưng số liệu đã cũ.

### Q17. `cdc_dependency_up` và `cdc_service_up` khác nhau thế nào? [Trung bình]

`cdc_dependency_up` cho biết collector của exporter có đọc được dependency không: clickhouse, postgres, debezium (REST Connect) và debezium-jmx. `cdc_service_up` là kiểm tra HTTP health đơn giản cho năm service: source-api, control-center, simulator, schema-registry, identity. Cả hai dùng chung alert `DependencyDown`.

### Q18. Exporter theo dõi replication slot bằng những metric nào và vì sao cần cả ba? [Nâng cao]

`cdc_postgres_slot_retained_wal_bytes` là lượng WAL slot đang giữ, cho biết đĩa đang bị ăn. `cdc_postgres_slot_safe_wal_bytes` là `safe_wal_size`, còn bao nhiêu byte trước khi `max_slot_wal_keep_size` (8 GB) vô hiệu hóa slot, tức khoảng cách tới mất dữ liệu. `cdc_postgres_slot_wal_reserved` phản ánh `wal_status`, 0 khi đã `unreserved` hoặc `lost`. Ba metric trả lời ba câu khác nhau: có đang tụt không, còn bao lâu, đã mất chưa.

### Q19. `cdc_ingest_lag_p95_seconds` được tính thế nào? [Trung bình]

Exporter truy vấn `bronze.cdc_events` trong 5 phút gần nhất, lấy p95 của hiệu `ingested_at` trừ `source_ts` (thời điểm commit ở nguồn), bỏ các sự kiện snapshot (`op = 'r'`) vì chúng không có ý nghĩa độ trễ. Giá trị âm do lệch đồng hồ bị kẹp về 0. Không có sự kiện nào thì trả 0, nên nguồn im lặng không bị coi là chậm.

### Q20. Vì sao `cdc_events_total` đọc từ `system.parts` thay vì `count()`? [Trung bình]

`count()` trên changelog lớn mỗi 15 giây là quét không cần thiết; tổng `rows` của các active part trong metadata cho cùng con số gần như miễn phí. Nhược điểm là con số gồm cả bản giao lại (redelivery) và có thể dao động nhẹ khi merge, nên dashboard ghi là "approximate". Nó dùng làm tín hiệu "CDC có hoạt động" cho `MartRefreshStale`, không phải số liệu kế toán.

### Q21. Metric Debezium lấy từ đâu? [Trung bình]

Trạng thái connector và task lấy từ REST API của Kafka Connect (`/connectors?expand=status`). Metric streaming (behind source, thời gian từ sự kiện cuối, hàng đợi còn trống, đang kết nối, số sự kiện đã thấy) lấy từ JMX qua Jolokia, bật bằng `ENABLE_JOLOKIA` trong image, cổng 8778 chỉ trong mạng compose. Khi task chưa streaming thì không có MBean, nên `debezium-jmx` báo down.

### Q22. Exporter theo dõi gì ở ClickHouse? [Trung bình]

Toàn bộ `system.metrics`, vài bộ đếm `system.events` (Query, FailedQuery, InsertedRows, SelectedRows), số part tối đa mỗi partition theo bảng, dung lượng theo database và disk (nóng `default` hay lạnh `cold` trên S3), và p95 SELECT theo user từ `system.query_log`. Thêm vào đó là các tín hiệu pipeline đọc từ bảng của dự án: lỗi ingest, watermark, refresh mart, reconcile.

### Q23. Vì sao đo SELECT p95 theo user? [Trung bình]

SLO dashboard (500 ms) chỉ áp cho truy vấn của người xem dashboard, tức user `grafana_reader`. Truy vấn của `admin` (warehouse-jobs refresh mart, kiểm tra dữ liệu) nặng hơn nhiều và không nên làm nhiễu SLO. Tách theo user cho phép alert chỉ nhìn đúng nhóm truy vấn cần thiết.

### Q24. Metric của identity và gateway lấy thế nào? [Cơ bản]

Prometheus scrape trực tiếp: identity tự expose `identity_logins_total{outcome}` và `identity_tokens_total{grant}`, gateway Traefik expose metric có sẵn trên entrypoint `metrics` cổng 8082. Source API expose request, latency và `auth_decisions_total`. Metric login dùng cho alert `LoginFailuresHigh`; metric gateway hiện chỉ được scrape, chưa có panel hay alert riêng.

### Q25. Source API chạy 6 worker, metric có bị tách theo worker không? [Nâng cao]

Không, vì Source API dùng chế độ multiprocess của thư viện Prometheus với một thư mục chung trong container (`PROMETHEUS_MULTIPROC_DIR`). Mọi worker ghi vào đó và endpoint `/metrics` gộp lại. Thư mục được reset khi container khởi động để không cộng dồn dữ liệu cũ.

### Q26. Có những dashboard nào? [Cơ bản]

Có năm trang nghiệp vụ theo vai trò, bắt đầu từ **Điều hành sàn** (`/d/cdc-business`), đọc mart từ ClickHouse qua user `grafana_reader`; các trang còn lại phục vụ vận hành, danh mục/chiến dịch, seller/tồn kho và tài chính. Ngoài ra có **Platform Health** (`/d/cdc-platform`) đọc Prometheus với 41 panel về host, API, Kafka, Debezium, PostgreSQL, ClickHouse, mart, reconcile và thông báo. Business dùng ngày nghiệp vụ của đồng hồ mô phỏng; Platform dùng giờ thật.

### Q27. Vì sao dashboard được sinh bằng script Python? [Trung bình]

JSON dashboard viết tay dài hàng nghìn dòng và gần như không review được. `generate_dashboards.py` liệt kê panel bằng vài dòng mỗi cái, nên diff dễ đọc và layout đồng nhất. CI chạy lại generator rồi `git diff --exit-code` để đảm bảo JSON đã commit khớp với nguồn.

### Q28. Vì sao mọi panel Business đọc cùng một `refresh_id`? [Trung bình]

Mart được refresh mỗi 15 giây, mỗi lần gắn `refresh_id` là watermark của lần đó. Nếu mỗi panel tự lấy "mới nhất" thì có thể đọc hai lần refresh khác nhau và các con số trên một trang mâu thuẫn. Một biến dashboard chọn refresh đã hoàn tất mới nhất, và mọi panel lọc theo nó.

### Q29. Datasource ClickHouse của Grafana được cấu hình thế nào? [Trung bình]

Plugin ClickHouse chính thức, giao thức native cổng 9000, user read-only `grafana_reader`, database mặc định `mart`, mật khẩu đọc từ file secret qua `$__file{...}`. Pool được nâng lên 100 kết nối mở và 100 idle vì mặc định 50/25 làm dashboard xếp hàng khi nhiều người mở cùng lúc. Datasource không sửa được trong UI.

### Q30. Node-exporter trên WSL2 đo cái gì? [Cơ bản]

Nó đọc `/proc`, `/sys` và `/` của host được mount read-only vào container, không cần Docker socket. Trên WSL2 hay Docker Desktop, "host" là VM chạy Docker, không hẳn là máy vật lý. Vì vậy số CPU, RAM, đĩa phản ánh giới hạn của VM WSL.

---

## C. Alert và thông báo

### Q31. Có bao nhiêu alert và chia thành nhóm nào? [Cơ bản]

32 rule trong `observability/prometheus/alerts.yml`. Nhóm: sống còn thành phần (TargetDown, DependencyDown, CollectorStalled), CDC (connector, Debezium, Kafka, lag), warehouse (watermark, mart, reconcile, drift, lỗi ingest, job lỗi, parts, query dashboard), PostgreSQL (WAL, slot, kết nối, deadlock), API và identity, host và thông báo Chat.

### Q32. Những alert nào gắn SLO và SLO là gì? [Cơ bản]

Bốn alert có nhãn `slo`: `CDCLagHigh` và `CDCLagCritical` (freshness: p95 ≤ 5 s, ≤ 30 s lúc cao điểm), `CheckoutLatencyHigh` (write-path: checkout p99 ≤ 300 ms) và `DashboardQuerySlow` (dashboard: query p95 ≤ 500 ms). Các alert khác dùng ngưỡng mặc định local. Nhãn này giúp phân biệt "vỡ cam kết" với "dấu hiệu cần điều tra".

### Q33. Vì sao `CDCLagHigh` có `for` 10 phút còn `CDCLagCritical` chỉ 5 phút? [Trung bình]

SLO cho phép lag lên tới 30 s trong cao điểm 11.11, nên lag vượt 5 s trong vài phút lúc burst là chấp nhận được; `for` 10 phút lọc các đợt tăng ngắn. Vượt 30 s là đã vỡ cả SLO nới lỏng, cần phản ứng sớm hơn. Ngưỡng cao hơn đi với thời gian chờ ngắn hơn là cách phân cấp thông thường.

### Q34. Vì sao `PostgresSlotWALAtRisk` dùng ngưỡng 2 GiB? [Trung bình]

`max_slot_wal_keep_size` là 8 GB: vượt mức này PostgreSQL bỏ WAL connector cần và Debezium phải snapshot lại. Còn 2 GiB nghĩa là đã dùng khoảng 75% ngân sách, vẫn đủ thời gian khôi phục connector. Alert đặt `critical` vì bước tiếp theo là mất dữ liệu.

### Q35. Vì sao `ClickHousePartsHigh` dùng ngưỡng 300? [Trung bình]

ClickHouse từ chối insert với lỗi `TOO_MANY_PARTS` khi một partition có khoảng 3.000 part. 300 là một phần mười, báo sớm khi insert quá nhỏ hoặc merge bị chậm, trước khi ingest bị chặn. `for` 10 phút bỏ qua lúc part tăng tạm thời trước khi merge.

### Q36. Vì sao nhiều rule có vế `and on() cdc_dependency_up{...} == 1`? [Nâng cao]

Khi exporter không đọc được ClickHouse, các gauge của nó đứng yên ở giá trị cuối và có thể vượt ngưỡng hoặc trông "bình thường" một cách giả. Vế này chỉ cho alert kêu khi dependency đang đọc được, còn tình trạng không đọc được thì giao cho `DependencyDown`. Kết quả là một sự cố tạo một alert đúng nghĩa thay vì một loạt alert giả. `on()` cần thiết vì hai vế có tập nhãn khác nhau.

### Q37. Vì sao cần cả `PostgresSlotLost` lẫn `PostgresSlotMissing`? [Nâng cao]

`PostgresSlotLost` bắt slot vẫn tồn tại nhưng `wal_status` đã `lost` hoặc `unreserved`. Khi slot bị xóa hoặc sau failover sang một máy không có slot, series biến mất chứ không về 0, nên rule so sánh `== 0` không bao giờ đúng. `PostgresSlotMissing` dùng `absent()` cho đúng trường hợp đó, kèm điều kiện PostgreSQL vẫn được đọc để không kêu khi chính database chết. Nguy hiểm của trường hợp này là Debezium có thể tạo slot mới ở LSN hiện tại và bỏ qua thay đổi trong im lặng.

### Q38. `MartRefreshStale` tránh báo động giả khi không có traffic thế nào? [Trung bình]

Rule chỉ kêu khi mart cũ hơn 120 s **và** `cdc_events_total` tăng trong 5 phút gần nhất. Khi nguồn im lặng thì mart không đổi là đúng, không phải lỗi. Có hẳn một unit test khẳng định alert không kêu trong trường hợp này.

### Q39. Vì sao `IngestErrors`, `PostgresDeadlocks`, `PostgresSlotLost` không có `for`? [Trung bình]

Mỗi cái là một sự kiện rời rạc đã là lỗi thật: một message không đọc được là vi phạm data contract, một deadlock là bug lock order, slot mất là có thể đã mất thay đổi. Chờ thêm không làm tín hiệu đáng tin hơn. Hai cái đầu đã dùng `increase(...[10m])` hoặc `[5m]`, nên tự tắt khi cửa sổ trôi qua.

### Q40. `KafkaBacklogGrowing` vì sao có hai điều kiện? [Trung bình]

Lag trên 10.000 message đơn thuần có thể chỉ là một đợt burst đang được tiêu hóa. Vế thứ hai yêu cầu đạo hàm 10 phút của lag dương, tức backlog đang tăng. Chỉ khi cả hai đúng trong 5 phút mới là consumer không theo kịp.

### Q41. `KafkaUnderReplicated` có ý nghĩa gì khi chỉ có một broker? [Cơ bản]

Với replication factor 1 thì không thể có partition thiếu replica, nên alert này hiện không bao giờ kêu. Nó được giữ để sẵn sàng cho cụm nhiều broker (RF=3), nơi một broker chết làm partition under-replicated và mất thêm một broker nữa là mất dữ liệu. Annotation của alert ghi rõ điều này.

### Q42. Unit test alert hoạt động thế nào? [Trung bình]

`alerts.test.yml` chạy bằng `promtool test rules`: khai báo chuỗi giá trị giả cho từng series theo phút, rồi khẳng định tại thời điểm nào alert nào phải kêu, với đúng nhãn và annotation. Các test bao TargetDown, MartRefreshStale (không kêu), ConnectorNotRunning, IngestErrors (kêu rồi tắt), SlotWALAtRisk và SlotLost, DashboardQuerySlow, PostgresSlotMissing. CI chạy chúng mỗi khi observability đổi.

### Q43. "Stale marker trick" là gì? [Nâng cao]

Để test `absent()`, cần một series có giá trị rồi biến mất. Nếu chỉ ngừng cung cấp giá trị, Prometheus vẫn dùng giá trị cũ trong cửa sổ lookback 5 phút nên `absent()` không đúng ngay. Ghi giá trị `stale` sau hai giá trị 1 chèn stale marker, giống thứ Prometheus tự chèn khi target không còn trả series, nên series lập tức coi như không tồn tại và alert kêu sau `for` 2 phút.

### Q44. Alert đi tới Google Chat qua đường nào? [Cơ bản]

Prometheus gửi alert cho Alertmanager; Alertmanager gom nhóm theo `alertname`, `job`, `service` rồi gửi webhook tới service nội bộ `chat-webhook`; bridge này định dạng tin nhắn và POST sang URL incoming webhook của Google Chat. Tin gồm trạng thái, severity, summary, description, service, runbook nếu có và link dashboard Platform Health. Thông báo resolved cũng được gửi.

### Q45. Vì sao bridge trả 503 khi gửi lỗi? [Trung bình]

Alertmanager chỉ thử lại khi receiver trả mã không phải 2xx. Nếu bridge nuốt lỗi và trả 200, thông báo mất hẳn. Trả 503 để Alertmanager retry, đồng thời tăng `chat_webhook_deliveries_total{result="error"}` cho alert `ChatDeliveryFailed`.

### Q46. Bridge bảo vệ URL webhook thế nào? [Trung bình]

URL chứa khóa nên được coi là secret: nằm trong file gitignored, chỉ mount vào bridge, không publish cổng. Bridge không bao giờ log URL, nội dung phản hồi hay exception có thể chứa URL. Nó chỉ chấp nhận URL bắt đầu bằng `https://chat.googleapis.com/` và giới hạn payload 256 KB.

### Q47. Cấu hình gom nhóm và ức chế của Alertmanager là gì? [Trung bình]

Chờ 30 s trước khi gửi nhóm mới để gom alert cùng lúc, 5 phút giữa các lần cập nhật nhóm, nhắc lại sau 4 giờ nếu vẫn kêu, tối đa 15 alert mỗi tin. Rule ức chế: khi `TargetDown` kêu cho một job và instance, mọi alert khác cùng job và instance bị ẩn. Mục tiêu là một sự cố ra một thông báo, không phải một cơn mưa.

---

## D. Tình huống xử lý sự cố

### Q48. Lag tăng lên 30 s, bạn làm gì? [Trung bình]

Đầu tiên xác định chặng chậm: nếu `cdc_debezium_behind_source_seconds` cao thì vấn đề ở PostgreSQL tới Kafka (Debezium, producer, broker); nếu Debezium bám sát mà `kafka_consumergroup_lag` tăng thì ở ClickHouse. Với ClickHouse, xem bộ nhớ, query đang chạy, merge và parts per partition. Xem CPU và RAM host vì mọi thứ chạy chung một máy. Nếu đang trong 11.11 thì 30 s vẫn là ngưỡng SLO cao điểm; vượt nữa mới là vỡ.

### Q49. `ConnectorNotRunning` kêu, bạn làm gì? [Cơ bản]

Xem `/connectors/postgres-connector/status` và đọc `trace` của task lỗi. Nếu lỗi kết nối thì kiểm tra PostgreSQL và Kafka khỏe rồi restart riêng task lỗi; nếu 404 thì đăng ký lại bằng `register.sh`; nếu là "Schema being registered is incompatible" thì theo runbook Schema Registry. Trong lúc đó theo dõi safe WAL của slot để biết còn bao nhiêu thời gian.

### Q50. Debezium báo "Schema being registered is incompatible". Xử lý thế nào? [Nâng cao]

Đó là Schema Registry chặn một thay đổi phá vỡ BACKWARD ở nguồn, ví dụ đổi kiểu cột. Nếu thay đổi là nhầm thì sửa nguồn rồi restart task. Nếu có chủ ý, chuẩn bị consumer trước (silver spec, sinh lại SQL), hạ compatibility về NONE **chỉ cho subject đó**, restart task, rồi xóa cấu hình riêng của subject để quay về BACKWARD. Không bao giờ xóa dòng bronze để dọn dẹp.

### Q51. Slot WAL tăng liên tục, bạn làm gì? [Trung bình]

Kiểm tra connector còn RUNNING không, vì đó là nguyên nhân phổ biến nhất. Nếu vẫn chạy mà tụt, xem behind source và hàng đợi Debezium; hàng đợi đầy nghĩa là Kafka không nhận kịp. Ước lượng thời gian còn lại bằng safe WAL chia tốc độ sinh WAL, và kiểm tra đĩa. Không xóa slot như phản ứng đầu tiên, vì đó là chấp nhận mất thay đổi.

### Q52. Slot đã bị vô hiệu hóa (`PostgresSlotLost`). Giờ sao? [Nâng cao]

Coi như warehouse có thể đã thiếu thay đổi trong khoảng WAL bị bỏ. Đưa connector chạy lại với slot hợp lệ, kích hoạt re-snapshot các bảng qua signal table của Debezium, rồi chạy reconcile và `check_data.py` để xác nhận nguồn và warehouse khớp. Về lâu dài phải tìm vì sao connector đứng lâu tới mức vượt 8 GB.

### Q53. Watermark bị kẹt (`TransactionWatermarkStalled`). Bạn kiểm tra gì? [Nâng cao]

Xem `consistent.watermark` để biết giao dịch cũ nhất bị giữ. Kiểm tra từng topic có đang được ingest không, vì một bảng Kafka engine dừng làm giao dịch có sự kiện ở bảng đó không bao giờ đủ. Xem `bronze.ingest_errors`, vì message bị cách ly không vào `cdc_events`. Và kiểm tra không ai xóa dòng bronze, nguyên nhân của một sự cố thật trong dự án.

### Q54. `WarehouseDataDrift` kêu. Bạn có backfill ngay không? [Trung bình]

Không. Trước hết truy vấn `ops.reconciliation_status` để biết bảng nào lệch, rồi tìm nguyên nhân: lỗi ingest, thay đổi schema, connector từng dừng. Backfill (re-snapshot) khi nguyên nhân chưa được sửa chỉ làm lệch lại. Sau khi sửa và backfill, chạy `check_data.py` để xác nhận.

### Q55. `MartRefreshStale` kêu nhưng CDC lag bình thường. Nguyên nhân có thể là gì? [Trung bình]

Dữ liệu vẫn vào bronze nhưng warehouse-jobs không publish mart: job lỗi (xem `WarehouseJobsFailing` và log), refresh quá lâu, hoặc watermark không tiến nên job không có gì mới để tính. Xem log warehouse-jobs, `cdc_mart_refresh_duration_seconds` và watermark. Trong lúc đó dashboard nghiệp vụ hiển thị refresh cũ nhưng vẫn nhất quán.

### Q56. Checkout p99 vượt 300 ms. Bạn nghi gì trước? [Trung bình]

Nghi lock wait trên hot row: dòng tồn kho SKU bán chạy, flash sale item, quota voucher. Xem `cdc_postgres_lock_waiters`, deadlock và pool kết nối. Load test cho thấy nút thắt của dự án nằm đúng ở dòng `inventory` của SKU bán chạy. Chỉ scale API khi đã chắc tầng ứng dụng là nút thắt.

### Q57. `PostgresDeadlocks` kêu trong production giả lập. Làm gì? [Trung bình]

Đọc log PostgreSQL để thấy hai giao dịch và các lock chúng giữ. Dự án thiết kế để không có deadlock: mọi giao dịch khóa theo một thứ tự cố định (slot quota voucher → tồn kho theo SKU → flash item), nên mỗi deadlock là một đường code vi phạm thứ tự. API tự retry nên client không thấy, nhưng vẫn phải sửa. `test_stress.py` là chỗ tái hiện nhanh nhất.

### Q58. Đăng nhập thất bại tăng vọt. Alert nào kêu và làm gì? [Cơ bản]

`LoginFailuresHigh` kêu khi có hơn 1 lần thất bại/s và hơn 50% lần đăng nhập thất bại hoặc gặp tài khoản bị khóa, kéo dài 10 phút. Đó có thể là credential stuffing. Xem audit của identity theo IP client trước khi chặn, để không chặn nhầm người dùng thật.

### Q59. Prometheus vẫn scrape được exporter nhưng số liệu ClickHouse không đổi. Vì sao và alert nào bắt? [Nâng cao]

Exporter thu thập nền, nên scrape thành công không có nghĩa collector còn chạy; có thể truy vấn đang treo hoặc lỗi liên tục. `cdc_dependency_up` về 0 khi lỗi, còn `CollectorStalled` kêu khi lần thành công cuối cách đây hơn 120 s. Xem log platform-exporter, nó ghi tên loại lỗi cho từng collector.

### Q60. ClickHouse báo `TOO_MANY_PARTS`. Phòng ngừa thế nào? [Trung bình]

Alert `ClickHousePartsHigh` báo từ 300 part mỗi partition, rất xa ngưỡng 3.000. Nguyên nhân thường là insert quá nhỏ và quá dày, hoặc merge bị chậm vì thiếu tài nguyên. Hướng xử lý là tăng kích thước batch ở phía tiêu thụ Kafka, kiểm tra tải merge và bộ nhớ. Chi tiết cấu hình ingest ở tài liệu warehouse.

### Q61. Đĩa sắp đầy, bạn kiểm tra theo thứ tự nào? [Trung bình]

Trước hết WAL slot giữ lại (connector đứng là nguyên nhân hay gặp nhất và đáng sợ nhất), rồi retention Kafka (đang là mặc định của broker), rồi dung lượng bronze trên ổ nóng (`cdc_clickhouse_bytes_on_disk` theo disk) và việc chuyển part sang cold tier. Prometheus cũng giữ 15 ngày dữ liệu. `DiskSpaceLow` kêu ở 15% để còn thời gian.

### Q62. Không nhận được thông báo Chat nào trong một sự cố. Vì sao có thể như vậy? [Trung bình]

Bản thân kênh Chat có thể hỏng: URL sai, file webhook rỗng, mạng ra ngoài lỗi. Alertmanager sẽ retry và `ChatDeliveryFailed` kêu, nhưng nó cũng đi qua cùng kênh hỏng. Lúc đó phải xem UI Alertmanager (9093) và metric `chat_webhook_deliveries_total`. Đây là giới hạn đã biết: kênh thông báo không thể tự báo cáo sự cố của chính nó.

### Q63. Sau khi restart máy, stack có tự lên không? [Cơ bản]

Có, nếu Docker tự khởi động: mọi service chạy lâu có `restart: unless-stopped`. Container bị dừng có chủ ý trước đó thì vẫn dừng. Connector đã đăng ký được lưu trong topic config của Kafka nên tự chạy lại, và slot giữ WAL trong thời gian máy tắt.

### Q64. Debezium restart giữa chừng thì có mất hay trùng dữ liệu không? [Nâng cao]

Không mất: slot giữ WAL và Debezium đọc tiếp từ offset đã commit. Có thể trùng một ít sự kiện đã gửi mà chưa kịp commit offset, nhưng silver dùng LSN làm version nên bản trùng không đổi kết quả. Chỉ gate `full` restart Debezium (và ClickHouse) giữa lúc có tải rồi vẫn yêu cầu probe và `check_data` PASS; `quick` và `smoke` không restart gì nên không chứng minh được việc đọc lại sau restart là an toàn. Ngay cả `full` cũng chỉ là một lần restart trong một lần chạy, và chưa có test bơm sự kiện trùng có chủ đích.

---

## E. Chiến lược kiểm thử

### Q65. Ba mức gate là gì? [Cơ bản]

`quick` chạy trên stack đang có, giữ dữ liệu, build lại code, chạy API và stress test, 90 s traffic có probe rồi `check_data`, khoảng 3–4 phút. `smoke` reset sạch, seed, test, 4 phút ở ×1440 có 11.11, khoảng 8–10 phút. `full` reset sạch, 16 phút ở ×720 có 11.11 và restart Debezium, ClickHouse, khoảng 25 phút.

### Q66. Vì sao chia ba mức gate thay vì một? [Trung bình]

Một gate 25 phút sẽ không được chạy sau mỗi thay đổi nhỏ, nên lỗi tích lũy. `quick` đủ nhanh cho mọi thay đổi code; `smoke` cần khi đổi schema hay SQL ClickHouse vì init chỉ chạy trên volume mới; `full` thử khả năng phục hồi sau restart, thứ hai mức kia không làm. Quy tắc là chọn mức nhỏ nhất bao được thay đổi.

### Q67. Vì sao gate dừng ngay ở deadlock đầu tiên? [Trung bình]

Deadlock là bug lock order, và khi có một thì dữ liệu sau đó đã không còn đáng tin để đánh giá. Chờ hết 16 phút chỉ lãng phí thời gian. Gate đọc bộ đếm deadlock của PostgreSQL mỗi 5 giây, có tăng thì dừng generator, in đoạn log deadlock và fail.

### Q68. Vì sao test chủ yếu chạy trên stack thật thay vì mock? [Trung bình]

Những lỗi đáng sợ của pipeline CDC không nằm trong một hàm: deadlock chỉ hiện dưới tải đồng thời, torn read chỉ hiện khi dữ liệu đang chảy qua nhiều topic, lệch số tiền chỉ hiện khi so nguồn với warehouse. Mock PostgreSQL hay Kafka sẽ bỏ qua đúng những thứ đó. Đổi lại, test chậm hơn và cần Docker, nên mới có gate nhiều mức.

### Q69. `test_stress.py` kiểm tra gì và vì sao nó hữu ích? [Trung bình]

24 luồng bắn checkout, hủy và thanh toán vào cùng vài SKU, flash item và voucher trong 20 s. Nó yêu cầu không deadlock và không lệch tồn kho, quota flash, quota voucher so với đơn. Lỗi lock order mà traffic bình thường chỉ lộ một lần trong 100.000 request sẽ lộ trong vài giây.

### Q70. Vì sao `test_stress.py` đọc bộ đếm deadlock của PostgreSQL thay vì đếm request lỗi? [Nâng cao]

Source API tự retry khi gặp deadlock và chỉ trả 503 khi hết số lần retry, nên phần lớn deadlock không bao giờ thành request lỗi. Nếu chỉ nhìn phía client, test sẽ báo xanh dù hệ thống đang deadlock liên tục. Bộ đếm `deadlocks` trong `pg_stat_database` là nguồn sự thật.

### Q71. `test_source_api.py` bao những gì? [Cơ bản]

25 test chức năng cho luật marketplace: tách đơn theo shop và phí ship, COD, chống trùng callback thanh toán, thanh toán lại, refund thanh toán trễ, không oversell khi checkout đồng thời, ai được đổi trạng thái đơn, sự kiện vận chuyển trễ, sai thứ tự, trùng, flash sale, voucher, đổi giá, trả hàng, tranh chấp, review và đồng hồ nghiệp vụ. Mỗi test tự tạo dữ liệu của mình nên chạy được song song với simulator.

### Q72. `test_auth.py` kiểm tra gì? Nó có trong gate không? [Trung bình]

Nó kiểm tra xác thực và phân quyền: gọi ẩn danh cần token, token giả bị từ chối, buyer chỉ hành động cho mình, seller chỉ quản lý shop mình, webhook phải được đúng đối tác ký, idempotency key theo từng caller, xoay refresh token và phát hiện tái sử dụng, khóa tài khoản sau nhiều lần sai, service client không vượt quyền. Hiện gate (mọi mức) chạy `test_source_api.py`, `test_stress.py` và `test_validation_wait.py`; CI chỉ chạy gate `smoke` nên cũng không gọi `test_auth.py`. Vì vậy `test_auth.py` phải chạy tay.

### Q73. Torn read là gì và probe phát hiện nó thế nào? [Nâng cao]

Torn read là khi người đọc thấy một phần của giao dịch: ví dụ đơn đã COMPLETED nhưng chưa có bút toán SALE ghi cùng giao dịch, vì hai bảng nằm ở hai topic được tiêu thụ độc lập. Probe định nghĩa 6 bất biến đúng trong một giao dịch, mỗi 2 giây đếm vi phạm qua `silver FINAL` và qua `consistent.*` với một watermark cố định. Silver được phép vi phạm (chứng minh vấn đề có thật); `consistent.*` phải luôn bằng 0 thì mới PASS.

### Q74. Vì sao probe phải chạy trong lúc có traffic? [Trung bình]

Torn read chỉ tồn tại trong khoảng vài giây khi một giao dịch mới về một phần. Khi simulator dừng và pipeline đã lắng, silver và consistent giống nhau, không còn gì để bắt. Vì vậy gate chạy probe song song với giai đoạn traffic, còn `check_data` chạy sau khi đã dừng.

### Q75. Vì sao `check_data.py` chạy khi simulator đã dừng? [Cơ bản]

Nó so PostgreSQL với ClickHouse và yêu cầu khớp chính xác, kể cả không còn giao dịch nào bị giữ lại. Khi dữ liệu còn chảy, warehouse luôn trễ vài giây nên so sánh sẽ fail giả. Nó đọc PostgreSQL trong một snapshot nhất quán và exit khác 0 khi có bất kỳ lỗi nào.

### Q76. `check_data.py` kiểm tra những nhóm nào? [Cơ bản]

Tiền, trạng thái, tồn kho, vận chuyển, khuyến mãi, trả hàng, settlement, write path, pipeline (PostgreSQL so với silver cho 31 bảng), metric (tính lại gold trong PostgreSQL), và ops (mart, snapshot, reconcile, PII không lọt vào ClickHouse). Chi tiết từng kiểm tra nằm ở tài liệu 05.

### Q77. Gate `full` thêm chaos gì và để chứng minh điều gì? [Trung bình]

Ở giữa thời gian traffic, gate restart connector Debezium kèm task; ở hai phần ba, gate restart container ClickHouse. Mục tiêu là chứng minh pipeline phục hồi đúng sau restart: offset, consumer, watermark tiếp tục, không mất và không sai dữ liệu. Probe bỏ qua những lần ClickHouse không trả lời, và `check_data` cuối cùng phải PASS.

### Q78. Có kiểm thử nào cho cấu hình giám sát không? [Cơ bản]

Có: `promtool check config` cho Prometheus, `promtool test rules` cho alert, `amtool check-config` cho Alertmanager, unit test cho bridge Chat, và kiểm tra dashboard đã commit khớp generator. Tất cả chạy trong workflow `observability.yml`.

### Q79. Có gì chưa được test? [Trung bình]

Sự kiện trùng chưa được bơm có chủ đích để kiểm tra; `test_auth.py` chưa nằm trong gate; backup và khôi phục ClickHouse có script nhưng chưa được gate chạy định kỳ; chưa có test failover vì chỉ có một node. Thông lượng T1 cũng chưa đạt nên chưa có test ở quy mô đó.

---

## F. Load test và hiệu năng

### Q80. `load_test.sh` hoạt động thế nào? [Trung bình]

Nó bật N bản `simulator-load` (profile `load`, không seed) cạnh simulator chính, đặt tốc độ đồng hồ, ghi lại bộ đếm deadlock và thời điểm bắt đầu, rồi start tất cả, chờ đủ thời gian và stop. Simulator phụ được điều khiển bằng `docker exec` vào từng container vì chúng không publish cổng. Cuối cùng nó báo sự kiện CDC mỗi phút và lag từ bronze, cùng request/s, 5xx, checkout p99, API p95, Debezium behind, consumer lag, thời lượng snapshot từ Prometheus, và số deadlock.

### Q81. Kết quả load test là gì? [Cơ bản]

Với 1 + 7 simulator ở ×1440 trên laptop 16 core: khoảng 1.800 sự kiện CDC/s (đỉnh 1.961), khoảng 620 request API/s, lag commit tới ClickHouse p95 khoảng 1,6 s, 0 deadlock, nhưng checkout p99 tăng lên 3,4 s. Mục tiêu cao điểm T1 là 5.400 sự kiện/s, nên mới đạt khoảng một phần ba.

### Q82. Nút thắt nằm ở đâu và làm sao bạn biết? [Trung bình]

Ở write path, không phải CDC. PostgreSQL dùng khoảng 6 core, API khoảng 3 core, chủ yếu chờ lock trên dòng `inventory` của SKU bán chạy, trong khi lag CDC vẫn giữ khoảng 1,6 s p95. Nếu CDC là nút thắt thì lag và consumer backlog phải tăng theo tải, nhưng chúng không tăng.

### Q83. Bạn sẽ làm gì để tiến tới 5.400 sự kiện/s? [Nâng cao]

Bước tiếp theo rõ nhất là chia tồn kho của SKU bán chạy thành nhiều slot như đã làm với quota voucher, để checkout không xếp hàng trên một dòng. Ở quy mô T1 thật còn cần nhiều broker, partition theo order, nhiều Connect worker, replica PostgreSQL và ClickHouse, và aggregate theo ngày cho các mart nặng. Một máy laptop chạy tất cả cũng là giới hạn tự nhiên.

### Q84. Vì sao API từng bị chặn ở 380 request/s? [Trung bình]

Python chạy một core mỗi process, và với 2 worker Uvicorn thì API chỉ dùng được 2 core. Tăng lên 6 worker giải quyết; mỗi worker có pool 12 kết nối, tổng 72 trên `max_connections` 200 của PostgreSQL. Khi tăng worker luôn phải tính lại tổng kết nối tới database.

### Q85. Tuning Debezium (R10) cải thiện được gì? [Trung bình]

Batch lớn hơn (4096), hàng đợi lớn hơn có giới hạn theo byte, poll 200 ms, producer `acks=all` với idempotence, nén lz4 thay gzip, batch producer 128 KB. Ở khoảng 990 sự kiện/s trong đợt 11.11, lag p95 từ commit tới ClickHouse giảm từ 1,86 s xuống 1,57 s. Chi tiết ở tài liệu 03.

### Q86. Claim quota voucher muộn giúp gì? [Trung bình]

Trước đó checkout khóa slot quota voucher từ đầu tới cuối giao dịch. Sau khi đổi, checkout chỉ đọc quota lúc đầu và claim ở cuối, ngay trước tồn kho và quota flash, nên slot chỉ bị khóa vài mili giây. Checkout p99 ở 6 simulator giảm từ 1,96 s xuống 0,82 s.

---

## G. CI

### Q87. Có những workflow CI nào? [Cơ bản]

`data-contract.yml` kiểm tra schema nguồn khớp silver spec và connector; `observability.yml` kiểm tra compose, cấu hình Prometheus, unit test alert, Alertmanager, bridge và dashboard; `gate-smoke.yml` chạy `quality/gate.sh smoke` trên stack sạch. Mỗi workflow được kích hoạt theo đường dẫn file bị đổi.

### Q88. Mọi thứ chạy local, vậy vì sao vẫn cần CI? [Trung bình]

Máy dev luôn có volume đã khởi tạo, nên lỗi chỉ xuất hiện trên môi trường sạch không bao giờ lộ ở local. CI đã bắt hoặc được thiết kế để bắt: init script sai thứ tự trên volume mới, SeaweedFS không có volume ghi được trên đĩa nhỏ, alert sai logic sẽ im lặng mãi, data contract trôi khi thêm cột mà quên silver spec. CI cũng là một môi trường trung lập, không phụ thuộc trạng thái máy cá nhân.

### Q89. CI lấy secret ở đâu? [Cơ bản]

Không có secret thật nào. `gate-smoke` chạy `setup-env.sh` để sinh secret ngẫu nhiên dùng một lần và tạo file webhook rỗng (không gửi Chat); `data-contract` dùng mật khẩu cố định cho một PostgreSQL container sống vài phút. Không cần cấu hình GitHub secret.

### Q90. Gate smoke trong CI xử lý khi fail thế nào? [Cơ bản]

Nó gom `docker compose ps` và 300 dòng log cuối của mỗi service, cùng thư mục output của gate, rồi upload thành artifact để xem lại. Timeout của job là 40 phút. Lần chạy mới trên cùng nhánh hủy lần cũ để tiết kiệm runner.

### Q91. Workflow data-contract kiểm tra gì? [Trung bình]

Nó dựng PostgreSQL 15, nạp `init.sql`, rồi chạy `check_contract.py`: bảng stream thiếu silver spec, cột nguồn mới chưa khai báo, lệch kiểu hoặc nullability, cột PII băm không phải string, bảng trạng thái thiếu `REPLICA IDENTITY FULL`, `02-silver.sql` đã lỗi thời. Mục tiêu là chặn những thay đổi làm warehouse sai trong im lặng ngay ở PR.

### Q92. Vì sao kiểm tra dashboard bằng `git diff --exit-code`? [Trung bình]

Dashboard JSON là output của generator. Nếu ai đó sửa JSON tay mà không sửa generator, lần sinh tiếp theo sẽ xóa thay đổi đó; nếu sửa generator mà quên sinh lại, Grafana chạy JSON cũ. CI chạy generator và fail nếu kết quả khác file đã commit, nên hai thứ luôn đồng bộ.

---

## H. Control Center

### Q93. Control Center làm được gì? [Cơ bản]

Nó là web app ở cổng 8000: thanh pipeline hiển thị tình trạng từng chặng từ simulator tới ClickHouse, điều khiển simulator (start/stop, tốc độ đồng hồ ×60 tới ×1440, flash sale, 11.11, hành động lẻ), luồng CDC trực tiếp với diff trước/sau của từng UPDATE, nút đăng ký connector và link sang hai dashboard Grafana. Phần phân tích warehouse đã chuyển hẳn sang Grafana.

### Q94. Control Center có ghi vào database không? [Cơ bản]

Không. Nó đọc PostgreSQL bằng `app_reader` read-only để đếm dòng và đo độ trễ slot, và chuyển tiếp mọi lệnh điều khiển sang simulator. Simulator chỉ ghi qua Source API, nên Source API vẫn là writer duy nhất của PostgreSQL.

### Q95. Backend và frontend được tổ chức thế nào? [Trung bình]

Một container build hai bước: Node 20 build React (Vite, Tailwind, TypeScript), rồi Python 3.11 chạy FastAPI phục vụ cả API lẫn UI tĩnh. Backend có `main.py` (health, đăng ký connector, proxy sang simulator, feed CDC), `cdc_stream.py` (consumer Kafka chạy nền) và `warehouse.py` (client ClickHouse theo thread). Frontend gồm PipelineBar, GeneratorPanel và LivePanel, poll backend định kỳ.

### Q96. Luồng CDC trực tiếp đọc Kafka thế nào? [Trung bình]

Một thread nền subscribe mọi topic `cdc.ecommerce.*` bằng regex, với group id ngẫu nhiên mỗi lần khởi động, đọc từ `latest` và không commit offset, nên nó không ảnh hưởng consumer của ClickHouse và không đọc lại lịch sử. Nó giữ 400 sự kiện gần nhất và bộ đếm theo giây trong cửa sổ 120 s, tự kết nối lại sau 5 s nếu lỗi. Frontend poll `/api/cdc?since=` để chỉ lấy sự kiện mới.

### Q97. Control Center bỏ header Schema Registry thế nào và vì sao không dùng client registry? [Nâng cao]

Message từ JSON Schema converter bắt đầu bằng 5 byte: magic byte `0x00` và 4 byte schema id, sau đó là JSON. Control Center thấy byte đầu là `0x00` thì cắt 5 byte rồi parse JSON; message không parse được bị bỏ qua. Không cần client registry vì payload vẫn là JSON tự mô tả, còn schema chỉ dùng để chặn thay đổi phá vỡ phía producer. ClickHouse làm đúng cách tương tự với `RawBLOB`.

---

## I. Sự cố đã gặp và câu hỏi mở

### Q98. Kể một sự cố bạn đã xử lý. [Trung bình]

Khi thêm Schema Registry, healthcheck của nó dùng `curl` nhưng image không có `curl`, nên registry luôn bị coi là unhealthy và Debezium (phụ thuộc `service_healthy`) không khởi động. Không có sự kiện CDC nào trong thời gian đó, nhưng không mất dữ liệu vì replication slot giữ toàn bộ WAL chưa đọc; khi Debezium lên nó đọc tiếp từ offset. Tôi viết lại healthcheck bằng HTTP thô qua `/dev/tcp` của bash. Bài học: healthcheck phải được thử trên chính image đó, vì healthcheck sai chặn cả chuỗi phụ thuộc.

### Q99. Kể một sự cố do chính bạn gây ra. [Trung bình]

Khi kiểm chứng Schema Registry bằng một bảng thử, tôi xóa các dòng bronze của bảng đó để dọn dẹp, và watermark ngừng tiến. Watermark so số sự kiện mỗi giao dịch khai báo với số thực có trong bronze; thiếu sự kiện thì giao dịch đó mãi "chưa đủ" và giữ mọi giao dịch sau. Tôi xử lý bằng cách xóa 2 dòng giao dịch mồ côi tương ứng. Từ đó có quy tắc ghi trong runbook: không bao giờ xóa bronze.

### Q100. Kể một lỗi chỉ xuất hiện trên môi trường sạch. [Trung bình]

Init script `02a-roles` của PostgreSQL chạy trước `02-monitoring` trên volume mới vì entrypoint sắp xếp bằng glob theo locale, bỏ qua dấu câu, và init thất bại. Máy dev không thấy vì volume đã khởi tạo từ trước; lỗi lộ ra khi khởi tạo trên container tạm. Tôi đánh số lại `01` tới `05` và ghi lý do ngay trong compose. Đây cũng là lý do gate `smoke` và CI luôn dựng từ volume sạch.

### Q101. Kể một lỗi do giá trị mặc định. [Trung bình]

SeaweedFS mặc định dùng volume 30 GB và tính số volume theo dung lượng đĩa trống. Trên runner CI đĩa nhỏ, kết quả là không có volume nào ghi được, nên ClickHouse không ghi được cold tier. Tôi cố định volume 1 GB và tối đa 50 volume. Bài học: với hạ tầng, cấu hình tường minh an toàn hơn giá trị tự thích nghi.

### Q102. Kể một lỗi dữ liệu nghiệp vụ mà kiểm tra tự động bắt được. [Nâng cao]

Warehouse-jobs đóng sổ ngày nghiệp vụ đã qua vào `gold.daily_sales_closed`. Ở tốc độ đồng hồ cao, một thanh toán chậm có thể commit sau khi ngày của đơn đã đóng, nên con số "đã báo cáo" sai; 15 ngày bị đóng sai. Sửa bằng cách chỉ đóng ngày khi không còn đơn nào của ngày đó ở trạng thái UNPAID, và đóng lại các ngày sai. Bài học: "đã qua theo đồng hồ" không có nghĩa là "đã xong theo dữ liệu".

### Q103. Kể một race condition bạn đã sửa. [Nâng cao]

Ở ×1440 với 8 simulator, một lần đổi giá commit trong lúc checkout đang chạy làm đơn mang giá cũ sau khi giá mới đã có hiệu lực. `check_data` phát hiện khi so giá lúc đặt với `price_history`. Sửa bằng cách đọc giá SKU với `FOR SHARE`, nên đổi giá và checkout không chen nhau. Đọc rồi dùng giá trị trong cùng giao dịch vẫn có race nếu không khóa.

### Q104. Kể một lần bạn xử lý hot row. [Nâng cao]

Trong 11.11 mọi checkout dùng cùng một voucher nền tảng đều cập nhật một dòng quota. Tôi chia quota thành tối đa 16 slot, checkout chọn slot ngẫu nhiên và bỏ qua slot bận; sau đó chuyển việc claim quota xuống cuối checkout, nên slot chỉ bị khóa vài mili giây. Checkout p99 ở 6 simulator từ 1,96 s xuống 0,82 s. Hot row tiếp theo là tồn kho SKU bán chạy.

### Q105. Kể một lỗi trong chính công cụ kiểm thử. [Cơ bản]

Lệnh stop gửi cho simulator phụ trong `load_test.sh` phải chờ tick đang chạy xong; với timeout ngắn, dưới tải lệnh stop "lỗi" và simulator tiếp tục sinh tải sau bài test, làm bẩn số liệu lần sau. Giờ lệnh stop có timeout 120 s, script in cảnh báo cho simulator dừng lỗi và cuối cùng dừng hẳn container `simulator-load`. Bài học: script test phải dọn dẹp chắc chắn.

### Q106. Nếu được thiết kế lại phần vận hành, bạn đổi gì? [Nâng cao]

Đưa backup vào từ đầu và kiểm thử khôi phục trong gate, vì backup ClickHouse chỉ mới được thêm sau cùng và chưa có lịch chạy hay bản sao ngoài máy. Đưa `test_auth.py` vào gate và thêm test sự kiện trùng. Gắn `runbook_url` vào từng alert (bridge đã hỗ trợ) thay vì runbook nằm rải trong README. Thêm panel và alert cho gateway, và metric theo container để thấy service nào ăn tài nguyên.

---

## J. Lỗ hổng và lộ trình

### Q107. Dự án đã có backup chưa? [Cơ bản]

Có cho ClickHouse: `warehouse/backup.sh` dùng `BACKUP`/`RESTORE` của ClickHouse, full hoặc incremental, ghi vào `warehouse/backups/` trên host nên sống sót qua `docker compose down -v`. PostgreSQL và các volume khác chưa có backup. Backup ClickHouse cũng chưa có lịch chạy tự động, chưa được sao ra ngoài máy và chưa có alert khi bản backup đã cũ.

### Q108. Hệ thống có HA không? [Cơ bản]

Không: một broker Kafka với RF=1, một PostgreSQL, một ClickHouse, một Connect worker. Đã có tài liệu nghiên cứu HA riêng. Một số alert như `KafkaUnderReplicated` và `PostgresSlotMissing` (trường hợp failover) đã được chuẩn bị sẵn cho lúc có nhiều node.

### Q109. Những gì còn thiếu về bảo mật vận hành? [Trung bình]

Kafka và database chưa có TLS/SASL, chỉ gateway có HTTPS. Control Center chưa có xác thực (dự định đặt sau oauth2-proxy) và kết nối ClickHouse bằng user `admin`. Mật khẩu dev và PII salt cũ còn trong lịch sử git nên phải đổi cho mọi môi trường dùng chung. Chưa có cách chia sẻ secret an toàn cho nhóm (dự định SOPS + age).

### Q110. Đổi PII salt có vấn đề gì? [Trung bình]

Salt dùng để băm dữ liệu cá nhân trong connector; đổi salt làm thay đổi mọi giá trị băm, nên các bản ghi cũ và mới của cùng một người không còn join được. Vì vậy salt chỉ đặt một lần cho mỗi môi trường. Đổi mật khẩu thì dễ hơn: sửa file trong `secrets/` rồi chạy `observability/start.sh`. `setup-env.sh --rotate` sinh lại toàn bộ secret nhưng chỉ chạy khi stack đã dừng, mọi volume của project đã bị xóa và thư mục `source/postgres/data` không còn, tức đi kèm reset dữ liệu, vì database chỉ đặt mật khẩu lần đầu.

### Q111. Phần giám sát còn thiếu gì? [Trung bình]

Metric Traefik được scrape nhưng chưa có panel hay alert. Không có metric theo container và không có distributed tracing, nên khó biết service nào ăn CPU hay một request chậm ở chặng nào. Alert chưa có `runbook_url`. Kênh thông báo duy nhất là Google Chat, không có kênh dự phòng.

### Q112. Bạn ưu tiên làm gì tiếp theo và vì sao? [Nâng cao]

Thứ nhất là hoàn thiện backup: lên lịch cho `backup.sh`, sao bản backup ra ngoài máy, thêm backup PostgreSQL và thử khôi phục định kỳ, vì mất dữ liệu là rủi ro không đảo ngược được. Thứ hai là đổi secret cũ trong lịch sử git và thêm TLS cho Kafka, database, vì đó là điều kiện tối thiểu để chia sẻ môi trường. Thứ ba là chia tồn kho SKU bán chạy thành slot để tiến tới thông lượng T1, vì đo đạc đã chỉ rõ đó là nút thắt. HA để sau, vì nó tốn nhiều nhất và chỉ có giá trị khi ba việc trên đã xong.

### Q113. Backup ClickHouse đảm bảo nhất quán thế nào? [Nâng cao]

Trong lúc backup, script dừng warehouse-jobs và detach mọi bảng Kafka engine, nên không bảng nào nhận dữ liệu mới và mọi bảng được chụp ở cùng một vị trí Kafka. Việc này chỉ mất vài giây tới một phút; Kafka vẫn giữ sự kiện mới trong lúc đó. Các materialized view đọc Kafka không được backup mà tạo lại từ init SQL khi restore. Mỗi bản có manifest, `.topics` lưu topic IDs và `.offsets` lưu vị trí replay lúc backup.

### Q114. Restore xong thì những sự kiện sau thời điểm backup đi đâu? [Nâng cao]

Script dùng vị trí replay đã lưu khi backup, kiểm tra topic IDs và retention trước khi xóa database, rồi dời consumer group ClickHouse và bật lại ingestion. Nếu không đủ lịch sử, restore thông thường từ chối; `--force` chỉ restore offline và không bật consumers/jobs. Cơ chế này đọc bù từ Kafka nhưng không chứng minh exactly-once của toàn pipeline: Connect và Kafka engine vẫn có thể replay. Xem [bài thực hành](../operations/09-thuc-hanh-backup-clickhouse.md) để biết cách kiểm chứng sau phục hồi.
