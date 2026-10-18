# Thực hành đưa pipeline mô phỏng lên server

[Tài liệu dự án](../README.md) / [Triển khai](README.md)

Đây là lộ trình triển khai lần đầu cho repo này, đọc cùng [chương 07](07-trien-khai-va-devops.md). **Thiết kế dưới đây chưa được triển khai lên một server thật.** Domain `example.com`, IP, đường dẫn và tên image là mẫu. Repo chưa có `compose.server.yml`, `deploy.sh`, Loki/Tempo hay workflow CD hoàn chỉnh; không chạy các tên đó như thể đã được cung cấp.

Mục tiêu đầu tiên: một **staging chạy simulation**, có domain và TLS hợp lệ, dữ liệu bền trên server, quản trị riêng, release có thể truy nguyên, quan sát được độ trễ và khôi phục được ClickHouse. Môi trường này đủ để học vận hành; chưa cần đặt tên production hoặc làm HA nhiều node.

## 1. Đổi cách đặt câu hỏi

Local thường trả lời “code chạy được chưa?”. Với server, phải trả lời thêm: ai được truy cập, trạng thái nằm ở đâu, cái gì thay đổi khi release, phát hiện lỗi bằng tín hiệu nào, và khôi phục bằng cách nào.

| Câu hỏi | Quyết định cho staging đầu tiên | Bằng chứng cần lưu |
|---|---|---|
| Ai dùng? | Bạn và người xem demo được cấp quyền | API/Control Center có lớp xác thực; thử truy cập không có quyền |
| Chạy ở đâu? | Một VM x86_64, Docker Compose | Region, cấu hình VM, dung lượng đĩa, phiên bản Docker |
| Mất gì nếu VM mất? | Dữ liệu simulation có thể seed lại; lịch sử CH cần backup | Bản off-site, tên backup, kết quả restore drill |
| Ai deploy? | User triển khai riêng; bước đầu deploy thủ công có ghi chép | Release SHA/digest, thời gian, người chạy, kết quả verify |
| Khi nào khỏe? | Không chỉ HTTP 200: connector chạy, lag thấp, mart mới, dữ liệu khớp | Dashboard, probe, reconciliation và số đo theo workload |

Đừng thay toàn bộ `localhost` bằng domain. Có ba không gian địa chỉ: browser dùng public domain, container dùng tên service, operator dùng VPN/SSH. `kafka:9092` là địa chỉ nội bộ đúng; `localhost` trong container lại chỉ chính container đó.

## 2. Tách môi trường theo trạng thái và quyền

| Thành phần | Local | CI | Staging | Production nếu có sau này |
|---|---|---|---|---|
| Host và dữ liệu | Máy dev | Máy dùng một lần | VM riêng | VM/tài khoản riêng nếu có thể |
| Secret | Dev secret | Random dùng một lần | Secret staging | Secret production độc lập |
| PG/Kafka/ClickHouse | Bộ local | Bộ tạm | Bộ staging | Bộ production |
| TLS/domain | CA local | Theo bài kiểm tra | Domain staging | Domain chính |
| Simulator | Tùy thao tác | Tải hữu hạn | Bật theo lịch/demo | Cần quyết định rõ đây vẫn là simulation |
| Reset | Được khi chủ dữ liệu cho phép | Mỗi gate sạch | Chỉ qua thao tác vận hành có ghi log | Không dùng reset để deploy |
| Chat/bucket | Kênh và đường dẫn dev | Placeholder | Riêng staging | Riêng production |

Tách cả replication slot, Connect internal topics, consumer group, Schema Registry subjects khi dùng chung broker. Cách đơn giản cho lần đầu là không chia sẻ broker giữa các môi trường. Không nối PG staging vào Kafka production để tiết kiệm VM.

`docker compose -p` chỉ đổi namespace một phần. Repo còn container_name cố định, host ports, bind mount `source/postgres/data` và script lọc project `realtime-cdc`; phải sửa/tham số hóa các điểm này trước khi chạy nhiều bộ trên cùng host. Đừng coi đổi `-p` là đủ cách ly.

## 3. Tạo bản hợp đồng cấu hình

Tạo bảng cấu hình trước khi tạo file env. Với mỗi giá trị, ghi: ai đọc, đọc lúc build hay runtime, secret hay không, và đổi nó cần restart/migration gì.

| Nhóm | Ví dụ | Nơi cấp |
|---|---|---|
| Artifact | Image digest, release SHA | CI/release manifest |
| Định danh môi trường | Tên môi trường, domain, project, đường dữ liệu | Config không bí mật |
| Địa chỉ nội bộ | postgres, kafka:9092, clickhouse:8123 | Compose/network |
| Địa chỉ public | API/auth/app/Grafana URL | Config của browser và gateway |
| Năng lực | API workers, JVM heap, memory limit, retention | Config có số đo làm căn cứ |
| Credential | DB password, client secret, pepper, signing key, webhook | Secret manager hoặc file giải mã trên host |

**Mẫu dự kiến**, chưa phải các biến đều đã được repo hỗ trợ:

```dotenv
DEPLOY_ENV=staging
COMPOSE_PROJECT_NAME=cdc-staging
BASE_DOMAIN=staging.example.com
PUBLIC_API_URL=https://api.staging.example.com
PUBLIC_AUTH_URL=https://auth.staging.example.com
DATA_ROOT=/srv/cdc/staging/data
AUTH_MODE=enforce
```

Chỉ thêm biến sau khi xác định code/config thực sự đọc nó. Env của Compose không tự nội suy file YAML được mount vào Traefik. Muốn domain trong dynamic.yml thay đổi, cần bước render template có kiểm tra hoặc file cấu hình riêng cho mỗi môi trường.

Frontend hiện tạo Grafana URL bằng hostname của browser cộng `:3000`; lên server phải đổi sang URL HTTPS được cấu hình. Biến build-time của frontend đóng vào bundle: đổi `.env` sau khi build không tự thay nội dung JS. URL public, CORS origins, JWT issuer/audience và JWKS phải được kiểm tra cùng nhau, không sửa từng nơi độc lập.

## 4. Chọn host và dự trù tiền

Với toàn stack stateful, bắt đầu bằng VM chạy Compose dễ học hơn nền tảng chỉ chạy web stateless. Cấu hình **ứng viên để benchmark**, không phải mức tối thiểu đã được chứng minh trên cloud: 8 vCPU, 16–24 GiB RAM, khoảng 160 GiB SSD trở lên. Dữ liệu nghiệp vụ dưới 10 GB vẫn cần chỗ cho WAL, Kafka, merge tạm, Docker images, log và backup.

| Lựa chọn tham khảo | Vì sao đưa vào danh sách khảo sát | Cần kiểm tra khi mua |
|---|---|---|
| [DigitalOcean Droplets](https://www.digitalocean.com/pricing/droplets) | Có bảng cấu hình/giá công khai để dự toán | Shared/dedicated CPU, region, disk và transfer |
| [Hetzner Cloud](https://www.hetzner.com/cloud/) | Có VM và [location Singapore](https://docs.hetzner.com/cloud/general/locations/) để cân nhắc gần khu vực | Giá theo location, loại CPU, IPv4, traffic, gói còn đặt được |
| [AWS Lightsail](https://aws.amazon.com/lightsail/pricing/) | Một phương án VM theo bundle để so với cloud theo từng thành phần | Bundle/region, transfer, snapshot, dịch vụ phát sinh |

Đây là shortlist để so sánh, không phải khẳng định một nhà cung cấp luôn nhanh/rẻ nhất. Chọn region bằng độ trễ từ người dùng và vị trí object storage, đo IO thực, đọc điều khoản billing và đặt cảnh báo chi phí. Xác nhận kiến trúc x86_64/arm64 khớp toàn bộ image trước khi chọn ARM chỉ vì giá.

Ví dụ tại lần đọc bảng giá chính thức 18/10/2026: DigitalOcean Basic Regular 8 vCPU / 16 GiB / 320 GiB niêm yết $96/tháng; General Purpose 4 vCPU / 16 GiB / 50 GiB niêm yết $126/tháng. Hai gói không tương đương về CPU/disk; không so bằng mỗi RAM. Đây chỉ là ví dụ tính VM, phải kiểm tra lại region, thuế và extras trước khi đặt. [Nguồn DigitalOcean](https://www.digitalocean.com/pricing/droplets).

```text
Tổng = VM + disk/IPv4 tính riêng + snapshot + backup object storage
       + băng thông + domain gia hạn + registry/CI/observability vượt hạn mức + thuế
```

Tắt VM không mặc nhiên dừng billing cho disk/IP/snapshot. Không mặc định mọi free tier hoặc dịch vụ quan sát đều miễn phí cho workload của bạn.

## 5. Chuẩn bị server trước khi đưa application lên

1. Bật MFA cho tài khoản cloud và registrar. Tạo VM, cloud firewall và quyền khôi phục qua console trước khi siết SSH.
2. Tạo operator user dùng SSH key; thử phiên đăng nhập thứ hai trước khi tắt login/password cũ. Tách deploy key khỏi key cá nhân.
3. Cài Docker từ [hướng dẫn chính thức cho Ubuntu](https://docs.docker.com/engine/install/ubuntu/), bật khởi động cùng máy và cấu hình log rotation. Thành viên nhóm docker có quyền rất mạnh với host, không xem đó là tài khoản hạn chế.
4. Chốt đường dữ liệu `/srv/cdc/staging/data`, secret và release. Không đặt dữ liệu bên trong thư mục release sẽ bị xóa khi rollback.
5. Cấu hình đồng bộ thời gian, cập nhật bảo mật và reboot có lịch; chạy lại sau reboot để kiểm tra volume và restart policy.
6. Xác nhận các dịch vụ dữ liệu không publish Internet; chỉ mở entrypoint đã chủ động chọn và đường quản trị hạn chế theo IP/VPN.

Docker published ports có thể đi vòng qua rule ufw. Dùng cloud firewall và kiểm tra rule Docker phù hợp backend mạng; **không chỉ bật ufw rồi kết luận an toàn**. Kiểm tra TCP và IPv6 từ một máy bên ngoài, sau mỗi lần thay compose. [Docker: packet filtering và firewall](https://docs.docker.com/engine/network/packet-filtering-firewalls/).

## 6. Domain, DNS, TLS và đường quản trị

| Hostname mẫu | Đích nội bộ | Chính sách |
|---|---|---|
| api.staging.example.com | Source API | HTTPS, JWT/ownership, rate limit |
| auth.staging.example.com | Identity | HTTPS, issuer nhất quán, chống thử password |
| app.staging.example.com | Control Center | VPN hoặc lớp đăng nhập trước proxy |
| grafana.staging.example.com | Grafana | VPN/SSO, quyền đọc phù hợp |

Tạo A record tới IPv4 VM; chỉ tạo AAAA nếu đã cấu hình đường IPv6 và firewall tương ứng. DNS đúng không làm TLS tự hoạt động: proxy còn cần router đúng Host, certificate resolver, persistent certificate storage và điều kiện ACME challenge được đáp ứng. Thử ACME staging trước để kiểm cấu hình, rồi dùng issuer thật; chứng chỉ ACME staging không được browser tin cậy.

Gateway hiện bind localhost:8443, dùng CA local và chỉ route API/auth. Cần thêm entrypoint 80/443, cấu hình TLS public và route app/Grafana có xác thực. Nếu dùng HTTP-01 phải cho challenge tới được cổng 80; DNS-01 cần credential DNS có quyền tối thiểu và quản lý riêng. Không mount Docker socket nếu không cần cơ chế discovery đó.

Giữ Kafka, Connect REST, Schema Registry, PG, ClickHouse, Prometheus và exporter trong mạng nội bộ. Dùng service DNS để giao tiếp container; dùng VPN/SSH tunnel để quản trị. Với lần đầu, ưu tiên app/Grafana chỉ xem qua VPN thay vì mở chúng rồi tự viết thêm một hệ đăng nhập.

Kiểm thử ngoài mạng: tên miền resolve đúng, TLS hợp lệ, `/v1/categories` đọc được theo chính sách, request cần auth bị từ chối khi thiếu token, app không cho người lạ bấm Start/Stop. Gateway hiện không expose `/health`; không dùng URL đó làm kết luận gateway hỏng. Tách liveness nội bộ khỏi synthetic check public.

## 7. Secret: từ tạo mới tới rotate

Luồng đề xuất: sinh secret độc lập cho staging → mã hóa bằng SOPS/age hoặc lưu secret manager → cấp cho đúng host → giải mã vào file runtime → mount read-only vào đúng service. File env không bí mật chỉ trỏ tới vị trí/config; không chứa password để Compose in ra hoặc lọt vào image.

[SOPS](https://getsops.io/docs/) mã hóa nội dung, không bảo vệ được khi private key age và ciphertext cùng bị lấy. Khóa giải mã phải nằm ngoài git, có người chịu trách nhiệm khôi phục và thu hồi. Server bị chiếm vẫn có thể đọc secret mà workload cần dùng; mã hóa file trong git không thay thế bảo vệ host.

Cụ thể cho repo: giữ các giao diện `/run/secrets/...` đang có, thay nguồn cấp file. Chọn quyền và UID/GID sao cho tiến trình container đọc được nhưng user host khác không tùy tiện đọc được; không đặt 0600 cho root rồi để container non-root lỗi permission. Không log Authorization, token, webhook URL hoặc nguyên nội dung env.

Rotation trên staging có dữ liệu phải làm hai phía: đổi credential trong database/service, cập nhật secret của client, reconnect và kiểm tra. Pepper/JWT signing key/PII salt có hậu quả khác password DB: có thể ảnh hưởng đăng nhập, token đang sống hoặc nối dữ liệu lịch sử. Viết kế hoạch riêng theo từng loại.

`setup-env.sh --rotate` hiện dành cho **reset simulation** với volumes/data đã bỏ. Nó không phải quy trình rotate không mất dữ liệu trên server sống lâu. Nếu secret lộ, revoke credential cũ trước; xóa lịch sử git không làm credential đó tự hết hiệu lực.

## 8. Release đầu tiên: làm tay để hiểu đường đi

Thiết kế cây thư mục, chưa có sẵn trong repo:

```text
/srv/cdc/staging/
  releases/<release-id>/      compose và config của một release
  current                    con trỏ release đang dùng
  runtime/secrets/            file cấp cho container, ngoài git
  data/                      dữ liệu bền, độc lập release
  backup/                    bản tạm trước khi đẩy ra ngoài máy
```

Chuẩn bị image trên CI theo commit, ghi digest từng service và phiên bản schema vào release manifest. Server kéo đúng image đó, không build lại từ source để tránh khác artifact đã kiểm thử. Các service bên thứ ba cũng cần phiên bản/digest xác định và có lịch cập nhật bản vá.

Các bước release thủ công cần được tự động hóa sau này:

1. Kiểm tra disk/RAM, release hiện tại, backup cần thiết và migration tương thích. Bước deploy không được gọi `gate.sh smoke/full` vì gate xóa dữ liệu.
2. Cấp config/secret; render cấu hình gateway theo domain. Validate Compose mà không in nội dung nhạy cảm vào log công khai.
3. Pull image theo manifest. Chạy migration có thứ tự và lock; không dựa init SQL trên volume cũ. Với schema CDC, phải tính tương thích Source API, connector/registry và silver cùng lúc.
4. Khởi động dependencies rồi application; chờ health trong timeout hữu hạn. Đăng ký/cập nhật connector bằng đúng config và lịch sử offset hiện có.
5. Verify từ ngoài và trong: TLS/auth, connector/task RUNNING, không lỗi ingest, watermark đi tiếp, mart được công bố, reconciliation khớp. Tạo một kịch bản simulation có định danh để theo dõi tới báo cáo.
6. Ghi release thành công và cách quay lại bản trước. Giữ log và manifest cả khi thất bại; không ghi password hoặc token vào artifact.

Rollback image chỉ an toàn khi schema và protocol còn tương thích. Migration phá dữ liệu cần kế hoạch phục hồi; không hứa chạy lại image cũ là đủ. Một VM không có rolling update tự động cho mọi stateful component: chấp nhận maintenance window rõ ràng cho demo.

## 9. Thiết kế CD sau khi deploy thủ công thành công

| Giai đoạn | Việc làm | Quyền cần |
|---|---|---|
| PR CI | Unit/contract/observability/smoke trên môi trường tạm | Không có quyền server hoặc secret staging |
| Build release | Build image, scan, push GHCR, ghi digest/manifest | Quyền push package, giới hạn ở repo cần thiết |
| Deploy staging | Nhận release đã pass, cấp config/secret, chạy deploy có lock | Credential chỉ của staging |
| Verify | Synthetic business flow, health/lag/mart, lưu bằng chứng | Test identity riêng, không dùng admin nếu không cần |
| Promote | Chọn đúng artifact đã thử sang môi trường tiếp theo | Quyền environment tương ứng |

Dùng GitHub Environment cho phạm vi secret/protection; tính khả dụng của required reviewers phụ thuộc plan và loại repo, cần kiểm tra tài khoản thực. Dùng concurrency group theo môi trường và **không cancel ngang deployment đang migrate**; lock trên server vẫn cần vì workflow và operator có thể chạy cùng lúc. [GitHub deployment controls](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/control-deployments).

Khi cloud/secret service hỗ trợ, dùng OIDC đổi credential ngắn hạn với điều kiện trust giới hạn repo/ref/environment. OIDC không tự tạo SSH session vào một VPS bất kỳ. Nếu dùng SSH deploy, pin host key đã xác minh; không tắt host verification. Deploy key/registry token phải có phạm vi hạn chế. [GitHub OIDC](https://docs.github.com/en/actions/concepts/security/openid-connect).

Thêm job timeout, health timeout, log lỗi, release status và khả năng deploy lại một release cũ. Không cho pull request không tin cậy chạy self-hosted runner có quyền Docker trên server. Deploy script cũng phải kiểm tham số release để input không trở thành lệnh shell tùy ý.

## 10. Đo hiệu năng và trace luồng realtime

Ba câu hỏi khác nhau: request API mất bao lâu; sự kiện đi tới warehouse mất bao lâu; dashboard dùng dữ liệu mới tới đâu. Không dùng một biểu đồ CPU để trả lời cả ba.

| Tín hiệu | Đã có trong repo | Phần cần thêm trên server |
|---|---|---|
| Metrics | Prometheus, exporter, Grafana, alerts | Label môi trường, budget tài nguyên, uptime ngoài máy |
| Logs | stdout service, access log gateway | JSON có request_id/env/service/release, thu gom và retention |
| Traces | Chưa có phân tán end-to-end | OpenTelemetry + Collector + backend như Tempo, instrumentation |
| CDC correlation | LSN, tx_id, Kafka topic/partition/offset | Liên kết command/request với domain event và refresh |

Log JSON mẫu thiết kế, không chứa payload/credential:

```json
{"env":"staging","service":"source-api","release":"<sha>","request_id":"<uuid>","trace_id":"<id>","command":"checkout","status":201,"duration_ms":42}
```

Đường điều tra: request_id → command/order ID → transaction/domain event → Kafka position → bronze → watermark → mart refresh. Giữ ID chi tiết ở log/trace; không dùng order_id hoặc request_id làm label Prometheus vì cardinality tăng theo lượng giao dịch.

HTTP trace không tự kéo dài qua WAL. Debezium không tự biết request_id của request đã tạo row nếu không có cơ chế mang correlation đó. Muốn nối phải thiết kế metadata trong outbox/header, capture vào nơi phù hợp, kiểm schema compatibility và chính sách PII; có thể dùng span links cho xử lý bất đồng bộ. Tránh giữ một HTTP span mở nhiều phút chờ mart. [OpenTelemetry traces và span links](https://opentelemetry.io/docs/concepts/signals/traces/).

Loki/Alloy cho thu log và Tempo cho trace là các lựa chọn kiến trúc, chưa được cài. Chọn retention/sampling theo disk; thử event lỗi trước khi dựa vào error-only sampling, vì giữ toàn bộ trace có thể rất tốn. Hạn chế quyền đọc log và lọc PII trước khi gửi ra dịch vụ ngoài.

### Đo một lần staging có thể đem trình bày

1. Ghi release, region, cấu hình máy, giới hạn container, Kafka retention và kích thước dữ liệu ban đầu.
2. Ghi workload: preset, seed size, số simulator, business speed, thời lượng warm-up và thời lượng đo. Business speed không bằng số request/s.
3. Đo API throughput/p95/p99, business rejections và technical failures riêng; CDC throughput/p95, Kafka backlog, watermark held-back, tuổi mart và reconcile.
4. Đo RAM working set, OOM, CPU throttling, disk IO/latency và tốc độ tăng dung lượng. CPU thấp nhưng IO bão hòa vẫn có thể làm CDC chậm.
5. Sau tải, dừng generator và đợi job/CDC ổn định; chạy bộ kiểm tra dữ liệu phù hợp mà không reset staging. Lưu kết quả, lỗi và baseline để lần release sau so sánh.

Đặt mục tiêu từ số đo và nhu cầu demo, không công bố năng lực cloud bằng số đo laptop. Nếu Kafka/ClickHouse tranh IO, giảm tải hoặc tách workload trước khi thêm broker chỉ để trông “production”. Đặt uptime check ngoài VM, vì Prometheus cùng VM không thể báo khi cả máy mất điện/mạng.

## 11. Các bài diễn tập phải làm trước khi tin môi trường

| Bài diễn tập | Điều cần chứng minh |
|---|---|
| Reboot VM | Dữ liệu, mounts và service lên lại đúng; không seed/reset ngoài ý muốn |
| Deploy image mới rồi quay lại | Artifact/schema tương thích; biết release nào đang chạy |
| Revoke một test credential | Credential cũ không còn quyền; client dùng bản mới thành công |
| Dừng Connect ngắn hạn | Có cảnh báo, WAL được theo dõi, lên lại bắt kịp trong giới hạn |
| Restore CH từ off-site | Có đủ full/base/metadata, đọc bù đúng nếu lịch sử Kafka còn phù hợp |
| Giả lập collector ngừng | Phân biệt scrape thành công với dependency data cũ |
| Kiểm tra từ mạng ngoài | Chỉ entrypoint và đường quản trị được chọn mới tới được |

Không backup PG trong môi trường simulation theo lựa chọn hiện tại; chấp nhận seed lại khi mất nguồn. Điều này khác với cho phép trộn backup ClickHouse cũ vào Kafka mới: restore offline nếu khác lịch sử. Nếu sau này nguồn thành dữ liệu có giá trị, phải quyết định lại RPO/backup của PG, không mang nguyên giả định demo sang production.

## 12. Thứ tự triển khai thực tế cho bạn

**Mốc A — thiết kế xong:** có bảng cấu hình, domain dự kiến, budget, host/region, đường dữ liệu và phương án quản trị. Chưa cần mua dịch vụ observability hoặc Kubernetes.

**Mốc B — staging thủ công chạy được:** cấu hình server của repo đã được viết và kiểm thử riêng; deploy một release, TLS/auth đúng, chạy một phiên simulation, mart khớp, backup ngoài máy restore được. Ghi lại toàn bộ bước thủ công.

**Mốc C — CD tái hiện được:** CI build đúng artifact, workflow deploy vào staging với lock/verify; đổi image không mất dữ liệu; failure có log và rollback đã thử.

**Mốc D — có bằng chứng vận hành:** số đo server theo workload xác định, logs truy theo request, tracing được instrument thật, alert và uptime ngoài máy hoạt động. Lúc này có thể giải thích một sự cố bằng timeline thay vì chỉ nói “container vẫn xanh”.

Các thông tin cần chốt khi chuyển từ viết docs sang triển khai: ngân sách tháng, region mong muốn, domain sở hữu, nhà cung cấp/tài khoản, ai được xem demo, server mới hay đã có dữ liệu. Không tự bịa domain thật hoặc coi quyền viết tài liệu là quyền mua VM và public service.
