# Q&A 03 · Q&A · Luồng CDC: PostgreSQL → Debezium → Kafka

[Tài liệu dự án](../README.md) / [Phỏng vấn](README.md)

## Đọc file này để biết gì

Bộ câu hỏi phỏng vấn cho phần lõi của dự án: CDC trên PostgreSQL, Kafka Connect/Debezium, offset và at-least-once, cấu hình connector, outbox, Kafka, Schema Registry, kịch bản lỗi, tinh chỉnh và so sánh công nghệ. Mỗi câu có mức độ [Cơ bản], [Trung bình] hoặc [Nâng cao], và câu trả lời bám theo đúng những gì dự án làm. Tài liệu giải thích đầy đủ: [../03-cdc-ingestion.md](../platform/03-cdc-ingestion.md). Warehouse, watermark: [../04-warehouse.md](../platform/04-warehouse.md). Vận hành: [../06-van-hanh.md](../operations/06-van-hanh.md).

## Mục lục

- [A. Khái niệm CDC](#a-khái-niệm-cdc) (Q1–Q16)
- [B. PostgreSQL cho CDC](#b-postgresql-cho-cdc) (Q17–Q32)
- [C. Kafka Connect và Debezium](#c-kafka-connect-và-debezium) (Q33–Q46)
- [D. Offset và đảm bảo giao nhận](#d-offset-và-đảm-bảo-giao-nhận) (Q47–Q60)
- [E. Cấu hình connector](#e-cấu-hình-connector) (Q61–Q80)
- [F. Outbox pattern](#f-outbox-pattern) (Q81–Q89)
- [G. Kafka](#g-kafka) (Q90–Q103)
- [H. Schema Registry](#h-schema-registry) (Q104–Q113)
- [I. Kịch bản lỗi](#i-kịch-bản-lỗi) (Q114–Q127)
- [J. Hiệu năng và tinh chỉnh](#j-hiệu-năng-và-tinh-chỉnh) (Q128–Q135)
- [K. So sánh và đánh đổi](#k-so-sánh-và-đánh-đổi) (Q136–Q143)
- [L. Câu hỏi mở](#l-câu-hỏi-mở) (Q144–Q150)

---

## A. Khái niệm CDC

### Q1. CDC là gì? [Cơ bản]
Change Data Capture là kỹ thuật bắt mọi thay đổi INSERT, UPDATE, DELETE của database và phát chúng ra thành dòng sự kiện theo thứ tự commit, gần thời gian thực. Consumer (warehouse, cache, search, service khác) cập nhật theo dòng sự kiện đó thay vì đọc lại toàn bộ bảng. Dự án dùng CDC log-based: Debezium đọc WAL của PostgreSQL và đẩy sang Kafka, rồi ClickHouse đọc từ Kafka.

### Q2. Có những kiểu CDC nào? [Cơ bản]
Ba kiểu chính: query-based (polling theo cột updated_at hoặc id tăng dần), trigger-based (trigger ghi vào bảng log), và log-based (đọc log giao dịch như WAL của PostgreSQL, binlog của MySQL). Log-based là kiểu tốt nhất về độ đầy đủ và tải lên nguồn: thấy cả DELETE, đúng thứ tự, chỉ transaction đã commit, không phải sửa ứng dụng. Dự án dùng log-based.

### Q3. WAL là gì và vì sao nó là nguồn tốt cho CDC? [Cơ bản]
Write-Ahead Log là log ghi nối tiếp mà PostgreSQL ghi trước khi sửa trang dữ liệu, để phục hồi sau sự cố. Mọi thay đổi đã commit đều có trong WAL, theo thứ tự, với vị trí LSN tăng dần. Vì vậy đọc WAL cho một lịch sử đầy đủ và có thứ tự mà không phải query bảng hay thêm trigger.

### Q4. LSN là gì? Dự án dùng LSN để làm gì? [Cơ bản]
LSN (Log Sequence Number) là vị trí byte trong WAL, luôn tăng. Debezium đưa LSN của mỗi thay đổi vào `source.lsn`. Dự án dùng LSN làm cột version ở silver (ReplacingMergeTree): bản có LSN lớn nhất của mỗi khóa thắng, nên gửi lặp hay phát lại không làm sai trạng thái. LSN cũng được dùng trong offset của Debezium và trong slot (confirmed_flush_lsn).

### Q5. Logical decoding là gì? [Trung bình]
WAL ở dạng vật lý (block, trang), không phải dòng dữ liệu. Logical decoding là cơ chế của PostgreSQL dịch WAL thành thay đổi logic theo bảng và dòng, qua một output plugin. Nó chỉ phát các transaction đã commit, theo thứ tự commit. Yêu cầu `wal_level = logical`.

### Q6. pgoutput là gì? Vì sao chọn nó thay vì decoderbufs hay wal2json? [Trung bình]
pgoutput là output plugin có sẵn trong PostgreSQL từ bản 10, dùng cho logical replication gốc. Chọn nó vì không phải cài extension lên server (quan trọng với dịch vụ managed như RDS, Cloud SQL), được bảo trì cùng PostgreSQL, và làm việc với publication để lọc bảng. decoderbufs cần biên dịch và cài thêm; wal2json là extension ngoài và có giới hạn với transaction lớn.

### Q7. Publication là gì? [Cơ bản]
Publication là tập bảng mà pgoutput được phép phát thay đổi. Dự án tạo `cdc_publication` cho mọi bảng trong schema ecommerce (cả bảng tạo sau). Việc chọn bảng nào thực sự stream do `table.include.list` của Debezium quyết định. Publication được tạo bởi chủ bảng trong init.sql vì cdc_user không có quyền tạo.

### Q8. Replication slot là gì và nó bảo vệ dữ liệu thế nào? [Cơ bản]
Slot là một "dấu trang" bền vững trên server, ghi nhớ consumer đã xác nhận đến LSN nào và giữ lại WAL từ đó trở đi. Khi Debezium dừng, WAL chưa xác nhận không bị xóa, nên khi chạy lại nó đọc tiếp đúng chỗ. Slot sống qua restart PostgreSQL. Mặt trái là WAL có thể phình nếu consumer dừng lâu, nên cần `max_slot_wal_keep_size` và alert.

### Q9. Phân biệt restart_lsn và confirmed_flush_lsn. [Trung bình]
`confirmed_flush_lsn` là vị trí consumer đã xác nhận xử lý xong; khi nối lại, server gửi tiếp từ đó. `restart_lsn` là LSN cũ nhất server phải giữ để decode lại được, có thể cũ hơn vì cần đọc từ đầu các transaction còn dở tại thời điểm đó. WAL trước restart_lsn mới được giải phóng. Exporter của dự án tính WAL bị giữ là khoảng từ LSN hiện tại đến restart_lsn.

### Q10. REPLICA IDENTITY là gì? DEFAULT và FULL khác gì? [Trung bình]
REPLICA IDENTITY quyết định WAL ghi gì về dòng cũ khi UPDATE/DELETE. DEFAULT chỉ ghi khóa chính, FULL ghi toàn bộ dòng cũ, tức `before` của Debezium sẽ đầy đủ. Dự án đặt FULL cho bảng trạng thái vì silver cần dòng cũ khi xóa và `consistent.*` cần ảnh before; DEFAULT cho bảng append-only và outbox vì chúng không bị sửa, FULL chỉ tốn WAL.

### Q11. Cái giá của REPLICA IDENTITY FULL là gì? [Nâng cao]
WAL to hơn vì mỗi UPDATE/DELETE ghi cả dòng cũ, nên tốn I/O, tốn băng thông replication và message Kafka to hơn. Với bảng rộng, update thường xuyên, chi phí đáng kể. Ngoài ra nếu bảng không có khóa chính thì FULL còn làm việc áp dụng thay đổi phía subscriber chậm (không ảnh hưởng Debezium). Dự án chấp nhận vì warehouse cần before, và giới hạn FULL ở bảng trạng thái.

### Q12. Snapshot và streaming khác nhau thế nào? [Cơ bản]
Slot chỉ chứa thay đổi từ lúc tạo nó, nên dữ liệu có sẵn phải đọc bằng snapshot: Debezium đọc toàn bộ bảng, phát `op = r`. Sau đó streaming đọc slot và phát `c`, `u`, `d`. Dự án dùng `snapshot.mode = initial`: snapshot một lần khi chưa có offset, sau đó chỉ stream.

### Q13. Làm sao snapshot và streaming không bị hở hoặc chồng lên nhau? [Nâng cao]
Khi tạo slot, PostgreSQL trả về một điểm nhất quán và cho phép xuất snapshot của đúng thời điểm đó. Debezium đọc bảng trong một transaction REPEATABLE READ gắn với snapshot này, rồi bắt đầu stream từ đúng LSN của slot. Mọi thay đổi commit trước điểm đó nằm trong snapshot, mọi thay đổi sau nằm trong stream. Vì thế không mất và không lặp tại ranh giới.

### Q14. Incremental snapshot là gì, dự án dùng khi nào? [Nâng cao]
Incremental snapshot đọc lại bảng theo từng khúc (chunk) trong khi vẫn stream, được kích hoạt bằng cách chèn một dòng signal vào `ecommerce.debezium_signals`. Debezium đánh dấu cửa sổ bằng cách ghi vào chính bảng signal (vì vậy cdc_user có quyền INSERT ở đó) và loại bỏ dòng snapshot bị thay đổi bởi stream trong cùng cửa sổ. Dự án dùng nó làm đường phục hồi khi slot bị mất hoặc bị tạo lại, theo sau là reconciliation.

### Q15. Một sự kiện Debezium trông như thế nào? [Cơ bản]
Đó là envelope gồm `before`, `after`, `op` (c/u/d/r), `ts_ms` (lúc Debezium xử lý), `source` (bảng, LSN, txId, `ts_ms` lúc commit) và khối `transaction` (id transaction và thứ tự). Insert có before null, delete có after null. Dự án giữ nguyên envelope này, không unwrap, và lưu before/after thô trong bronze.

### Q16. Vì sao dự án không dùng SMT ExtractNewRecordState (unwrap)? [Trung bình]
Unwrap làm phẳng envelope chỉ còn trạng thái mới, mất `before`. Warehouse cần before: silver ghi bản "đã xóa" từ before khi có DELETE, và `consistent.*` dùng before để hiển thị dòng như trước một transaction chưa đến đủ. Cấu hình unwrap vẫn có trong file nhưng bị tắt (khóa bắt đầu bằng dấu gạch dưới).

---

## B. PostgreSQL cho CDC

### Q17. Những setting PostgreSQL nào bắt buộc cho CDC? [Cơ bản]
`wal_level = logical` là bắt buộc. Cần đủ `max_replication_slots` và `max_wal_senders` (dự án đặt 10 và 10, dùng 1). Cần pg_hba cho phép kết nối replication, và một user có thuộc tính REPLICATION. Dự án thêm `max_slot_wal_keep_size = 8GB` để giới hạn rủi ro.

### Q18. max_slot_wal_keep_size = 8GB có ý nghĩa gì? Vì sao chọn vậy? [Trung bình]
Đó là lượng WAL tối đa một slot được giữ; vượt quá, PostgreSQL invalidate slot và xóa WAL cũ. Mặc định là không giới hạn, tức slot treo có thể làm đầy đĩa và làm sập database nghiệp vụ. Dự án chọn ưu tiên DB nguồn: thà phải snapshot lại còn hơn sập DB. 8 GB đủ để Debezium dừng nhiều giờ ở tải thường mà vẫn đuổi kịp, và alert bắn từ 1 GiB WAL giữ lại và khi còn dưới 2 GiB trước ngưỡng.

### Q19. Điều gì xảy ra khi slot vượt max_slot_wal_keep_size? [Trung bình]
`wal_status` của slot chuyển sang `unreserved` rồi `lost`, WAL mà slot cần bị xóa. Debezium không thể stream tiếp từ vị trí cũ vì dữ liệu không còn. Thay đổi trong khoảng đó đã mất khỏi WAL; phải tạo lại slot, snapshot lại và reconcile. Alert `PostgresSlotLost` bắn ngay khi exporter thấy slot không còn reserved.

### Q20. wal_keep_size khác max_slot_wal_keep_size thế nào? [Trung bình]
`wal_keep_size` (1GB) là lượng WAL tối thiểu luôn giữ cho standby vật lý không dùng slot. `max_slot_wal_keep_size` là trần cho WAL bị slot giữ. Cái đầu là "giữ ít nhất", cái sau là "giữ nhiều nhất do slot". Với Debezium, setting quan trọng là cái sau.

### Q21. cdc_user có những quyền gì và vì sao không dùng superuser? [Cơ bản]
cdc_user có thuộc tính REPLICATION để dùng slot, SELECT trên mọi bảng schema ecommerce (kể cả bảng tạo sau, qua default privileges) để snapshot, INSERT/UPDATE trên cdc_heartbeat cho heartbeat, INSERT trên debezium_signals cho incremental snapshot, và CONNECT vào database. Nó không sở hữu bảng, không ALTER, không tạo publication. Least privilege: nếu credential của Debezium lộ, kẻ tấn công chỉ đọc được dữ liệu chứ không sửa được nghiệp vụ hay schema.

### Q22. Vì sao publication và REPLICA IDENTITY phải đặt trong init.sql? [Trung bình]
Tạo publication cho bảng và ALTER TABLE ... REPLICA IDENTITY đều cần quyền chủ sở hữu bảng. cdc_user không có, và Debezium được cấu hình không tự tạo publication. Nên chủ bảng làm sẵn khi khởi tạo DB. Điều này cũng giúp những thay đổi đó được review như schema thay vì xảy ra ngầm khi connector chạy.

### Q23. Vì sao publication là "FOR TABLES IN SCHEMA" thay vì liệt kê bảng? [Trung bình]
Để bảng nghiệp vụ mới tự động có trong publication, không cần sửa publication mỗi lần. Việc lọc thực sự làm ở `table.include.list` bằng regex loại trừ. Cái giá là pgoutput vẫn gửi thay đổi của bảng không cần (ví dụ idempotency_keys) cho Debezium, rồi Debezium lọc bỏ; tốn một ít băng thông replication.

### Q24. Heartbeat table dùng để làm gì? [Trung bình]
Slot chỉ tiến khi Debezium xác nhận một LSN. Nếu một lúc chỉ có ghi vào bảng không được stream, WAL tăng mà Debezium không có sự kiện nào để xác nhận, nên slot đứng yên và WAL bị giữ vô ích. Mỗi 10 giây Debezium chạy câu upsert vào cdc_heartbeat (tạo thay đổi thật đi qua publication) và phát message heartbeat, nhờ đó offset và slot tiếp tục tiến.

### Q25. View cdc_replication_status cho biết gì? [Cơ bản]
View đọc pg_replication_slots cho các slot debezium: tên slot, có active (đang có consumer kết nối) không, restart_lsn, confirmed_flush_lsn và độ trễ dạng byte dễ đọc từ LSN hiện tại đến confirmed_flush_lsn. Đây là chỗ đầu tiên người vận hành nhìn khi nghi CDC bị kẹt. Control Center cũng hiển thị lag theo byte của slot.

### Q26. Những bảng nào không được stream, vì sao? [Trung bình]
cdc_heartbeat, cdc_metrics, audit_logs, idempotency_keys, sim_clock, wallet_pending và voucher_quota_slots bị loại khỏi include list. Chúng là bảng hạ tầng hoặc nội bộ của Source API (khóa idempotency, đồng hồ mô phỏng, slot chia quota voucher), không có ý nghĩa phân tích. outbox_events thì có stream nhưng bị router chuyển sang events.*; debezium_signals được bắt nhưng không được mô hình hóa ở warehouse.

### Q27. Vì sao bảng outbox dùng REPLICA IDENTITY DEFAULT? [Trung bình]
Mỗi dòng outbox được insert rồi xóa ngay trong cùng transaction. Insert luôn có đủ dữ liệu trong WAL bất kể replica identity; chỉ DELETE mới cần before. Router bỏ qua DELETE, nên ghi cả dòng cũ cho DELETE là lãng phí WAL. DEFAULT chỉ ghi khóa.

### Q28. Có những rủi ro gì nếu DDL chạy trên bảng đang CDC? [Nâng cao]
Với pgoutput, Debezium nhận cấu trúc mới qua message relation và cập nhật schema trong bộ nhớ. Thêm cột nullable thường an toàn. Đổi kiểu cột có thể làm schema không tương thích, Schema Registry chặn và task dừng. Ngoài ra DDL cần lock và có thể xung đột với snapshot (snapshot.locking.mode shared giữ lock nhẹ chặn DDL lúc đọc cấu trúc). Dự án log mọi DDL và có check_contract.py chạy trong CI để phát hiện thay đổi nguồn chưa có ở silver.

### Q29. max_connections = 200 liên quan gì đến CDC? [Cơ bản]
Debezium chỉ dùng vài kết nối: một kết nối replication và một kết nối JDBC thường (đọc metadata, heartbeat). Phần lớn kết nối là pool của Source API và các reader. Setting quan trọng cho CDC là max_wal_senders; max_connections chủ yếu để không bị cạn kết nối khi tải cao. Alert PostgresConnectionsHigh bắn khi vượt 80%.

### Q30. log_replication_commands và log_statement = ddl giúp gì? [Cơ bản]
Ghi log lệnh replication (tạo slot, START_REPLICATION) giúp điều tra khi slot bị tạo lại hoặc consumer kết nối bất thường. Ghi log DDL giúp truy ngược thay đổi schema nào đã làm connector dừng. Cả hai rẻ và có giá trị khi có sự cố.

### Q31. Vì sao chỉ thay đổi đã commit mới ra Kafka? Transaction rollback thì sao? [Trung bình]
Logical decoding gom các thay đổi của một transaction và chỉ phát khi gặp bản ghi COMMIT trong WAL; transaction rollback bị bỏ. Vì vậy consumer không bao giờ thấy dữ liệu "bẩn". Transaction rất lớn có thể làm PostgreSQL phải spill ra đĩa khi decode, nhưng consumer vẫn chỉ thấy khi đã commit.

### Q32. Một transaction dài đang mở ảnh hưởng gì đến slot? [Nâng cao]
restart_lsn không thể vượt qua điểm bắt đầu của transaction cũ nhất còn đang chạy mà slot phải decode, nên WAL từ đó bị giữ lại dù Debezium đã xác nhận các transaction khác. Transaction mở hàng giờ có thể làm WAL giữ lại tăng và kích hoạt PostgresWALRetained. Source API của dự án dùng transaction ngắn mỗi lệnh, nên ít gặp.

---

## C. Kafka Connect và Debezium

### Q33. Kafka Connect và Debezium khác nhau thế nào? [Cơ bản]
Kafka Connect là runtime: quản lý worker, connector, task, REST API, producer, offset, converter và SMT. Debezium là plugin source connector chạy bên trong Connect, biết cách nói chuyện với PostgreSQL (slot, snapshot, decode). Container `cdc-debezium` chính là một worker Connect dùng image của Debezium có sẵn plugin.

### Q34. Worker, connector và task là gì? [Cơ bản]
Worker là tiến trình JVM chạy Connect; nhiều worker cùng group.id tạo thành cluster. Connector là cấu hình logic (postgres-connector). Task là đơn vị thực thi thật, được phân cho worker. Debezium PostgreSQL luôn có một task vì một slot chỉ có một người đọc.

### Q35. Ba internal topic của Connect chứa gì? [Cơ bản]
debezium-config chứa cấu hình connector (1 partition), debezium-offsets chứa offset nguồn (25 partition), debezium-status chứa trạng thái connector và task (5 partition). Cả ba là topic compact, RF = 1 ở môi trường local. Nhờ vậy worker không có trạng thái cục bộ: tạo lại container, connector chạy tiếp từ đúng offset.

### Q36. Chế độ distributed và standalone khác gì? Dự án dùng gì? [Trung bình]
Standalone lưu offset trong file cục bộ và cấu hình qua file, không có HA. Distributed lưu mọi thứ trong Kafka, quản lý qua REST API và có thể thêm worker. Dự án dùng distributed với một worker (group debezium-cluster), nên muốn HA chỉ cần thêm worker cùng group.

### Q37. Converter làm gì? Dự án dùng converter nào? [Trung bình]
Converter chuyển record nội bộ của Connect (có schema) thành byte gửi Kafka và ngược lại. Mặc định worker là JsonConverter không schema. Connector ghi đè: key dùng JsonConverter không schema (key gọn là khóa chính), value dùng JsonSchemaConverter của Confluent để đăng ký schema vào Schema Registry.

### Q38. SMT là gì? Khi nào không nên dùng SMT? [Trung bình]
Single Message Transform là biến đổi nhẹ từng record trong Connect trước converter: đổi topic, thêm/bỏ field, lọc. Dự án dùng EventRouter cho outbox, áp có điều kiện bằng predicate. Không nên dùng SMT cho logic nặng, join, aggregate hay gọi dịch vụ ngoài; những việc đó thuộc stream processor hoặc warehouse.

### Q39. Predicate trong Connect dùng để làm gì? [Trung bình]
Predicate cho phép áp SMT chỉ cho một số record. Dự án dùng TopicNameMatches với mẫu `cdc.ecommerce.outbox_events`, nên chỉ record của bảng outbox đi qua EventRouter; record của 31 bảng khác đi nguyên vẹn. Thiếu predicate, EventRouter sẽ cố xử lý mọi record và lỗi vì thiếu cột outbox.

### Q40. Bên trong task Debezium có những luồng nào? [Nâng cao]
Một luồng streaming đọc slot qua kết nối replication, dựng sự kiện và đẩy vào hàng đợi nội bộ có giới hạn. Luồng poll của Connect lấy batch từ hàng đợi (tối đa max.batch.size, chờ tối đa poll.interval.ms khi rỗng), chạy SMT và converter rồi giao cho producer. Khi hàng đợi đầy, luồng streaming bị chặn, tạo backpressure tự nhiên về phía PostgreSQL (WAL được slot giữ).

### Q41. REST API của Connect dùng để làm gì trong dự án? [Cơ bản]
register.sh dùng PUT config để tạo hoặc cập nhật connector. Control Center đọc trạng thái và có nút register. Platform exporter đọc trạng thái mọi connector/task để ra metric cdc_connect_running. Runbook dùng endpoint restart với includeTasks và onlyFailed để chạy lại task bị FAILED.

### Q42. Vì sao dùng PUT /connectors/<name>/config thay vì POST /connectors? [Trung bình]
PUT config là idempotent: tạo nếu chưa có, cập nhật nếu đã có, chạy lại bao nhiêu lần cũng được. POST chỉ tạo và lỗi nếu connector đã tồn tại. Script khởi động và setup có thể gọi lại register.sh an toàn. Đổi config qua PUT làm Connect khởi động lại task, task đọc tiếp từ offset.

### Q43. Jolokia dùng để làm gì? [Trung bình]
Jolokia là agent cho phép đọc JMX qua HTTP. Image Debezium bật nó ở cổng 8778 (chỉ trong mạng compose). Exporter đọc MBean streaming: MilliSecondsBehindSource, dung lượng còn lại của queue, Connected, thời gian từ sự kiện cuối, tổng số sự kiện. Từ đó có alert DebeziumBehindSource và DebeziumQueueFilling.

### Q44. Vì sao phải build image Debezium riêng? [Cơ bản]
Image debezium/connect 2.7.3.Final không kèm JsonSchemaConverter của Confluent (Confluent phân phối riêng). Dockerfile tải plugin bản 7.9.0 và giải nén vào thư mục plugin. Không có nó, value không đăng ký được vào Schema Registry.

### Q45. Vì sao pin converter bằng sha256? [Trung bình]
Để build tái lập được và chống supply chain: nếu file trên server bị thay đổi hoặc tải lỗi, kiểm tra hash thất bại và build dừng. Pin phiên bản (7.9.0) và hash nghĩa là image hôm nay và sáu tháng sau giống nhau. Đây là thực hành chuẩn khi tải binary từ internet trong Dockerfile.

### Q46. DirectoryConfigProvider hoạt động thế nào? [Trung bình]
Worker được bật config provider tên dir. Trong config connector, giá trị mật khẩu là một tham chiếu tới thư mục `/run/secrets` và tên file. Worker thay tham chiếu bằng nội dung file khi khởi động task, còn config lưu trong debezium-config và trả về qua REST vẫn chỉ là tham chiếu. File được mount chỉ đọc, nên mật khẩu không nằm trong compose, biến môi trường hay docker inspect.

---

## D. Offset và đảm bảo giao nhận

### Q47. Offset của Debezium PostgreSQL chứa gì? [Trung bình]
Không phải offset Kafka mà là vị trí ở nguồn: LSN đã xử lý, LSN commit của transaction gần nhất, txId, timestamp và cờ đang snapshot hay không. Nó nằm trong topic debezium-offsets, key theo tên connector và server (topic.prefix). Khi task khởi động, Debezium đọc offset này và xin PostgreSQL stream từ vị trí tương ứng.

### Q48. Mô tả chuỗi xác nhận từ PostgreSQL đến Kafka. [Nâng cao]
Debezium đọc sự kiện từ slot, Connect gửi vào Kafka và producer nhận ack với acks=all. Mỗi 60 giây (offset.flush.interval.ms), Connect chờ mọi record đã gửi được ack rồi ghi offset mới vào debezium-offsets. Sau đó Connect gọi commit của task, Debezium gửi flush LSN cho PostgreSQL và confirmed_flush_lsn tiến lên. Thứ tự này đảm bảo PostgreSQL không bao giờ xóa WAL của thay đổi mà Kafka chưa có.

### Q49. Delivery guarantee của pipeline là gì? [Cơ bản]
At-least-once: mọi thay đổi đã commit đều đến Kafka ít nhất một lần, có thể nhiều hơn. Không mất nhờ slot và thứ tự xác nhận; có thể trùng sau crash hay restart. Downstream thiết kế idempotent nên kết quả cuối cùng như exactly-once.

### Q50. Khi nào xuất hiện bản ghi trùng? [Trung bình]
Khi task dừng không sạch giữa hai lần flush offset: worker crash, container bị kill, task FAILED (ví dụ Schema Registry chặn), rebalance. Lúc khởi động lại, task đọc offset cũ và gửi lại mọi sự kiện sau đó, tối đa cỡ 60 giây sự kiện. Ngoài ra ClickHouse có thể đọc lại block cuối nếu chết sau khi ghi nhưng trước khi commit offset Kafka.

### Q51. Ước lượng số bản trùng tối đa sau một lần crash. [Trung bình]
Bằng lượng sự kiện sinh ra giữa lần flush offset cuối và lúc crash, tối đa khoảng offset.flush.interval.ms. Với 60 giây và khoảng 1.000 sự kiện/giây, có thể lên đến vài chục nghìn message. Giảm interval làm giảm trùng nhưng tăng số lần ghi offset và báo flush cho PostgreSQL; dự án giữ mặc định vì sink đã idempotent.

### Q52. Producer idempotence có chống được trùng sau restart không? [Trung bình]
Không. Idempotence gán producer id và số thứ tự cho mỗi batch, giúp broker loại bản trùng khi chính producer đó retry trong một phiên (ví dụ mất ack do mạng). Sau restart là producer mới, và các sự kiện đọc lại từ slot là message mới với offset mới. Loại trùng này phải xử lý ở consumer.

### Q53. Vì sao slot chỉ tiến sau khi offset được commit? [Trung bình]
Nếu slot tiến trước khi Kafka xác nhận, một crash ở giữa sẽ làm PostgreSQL bỏ WAL của các sự kiện chưa ở Kafka, tức mất dữ liệu vĩnh viễn. Bằng cách chỉ báo flush LSN sau khi offset (vốn chỉ ghi sau khi Kafka ack) đã được lưu, trường hợp xấu nhất là gửi lại chứ không phải mất. Đây là lựa chọn "trùng thay vì mất".

### Q54. Downstream xử lý trùng như thế nào trong dự án? [Trung bình]
Silver dùng ReplacingMergeTree với khóa chính và version = LSN: bản trùng cùng LSN, cùng nội dung, được gộp, đọc bằng FINAL. Bảng append-only ở silver gộp theo khóa. Watermark đếm sự kiện theo cặp (bảng, LSN) khác nhau và gộp bản END theo tx_id, nên trùng không làm sai. Bronze giữ cả bản trùng (sổ cái), domain_events phải khử trùng theo event_id.

### Q55. Exactly-once có làm được không? Vì sao dự án không làm? [Nâng cao]
Kafka Connect từ 3.3 hỗ trợ exactly-once cho source connector bằng transaction Kafka (offset và record ghi cùng transaction), và Debezium đã hỗ trợ ở một số connector. Nhưng đầu đọc ClickHouse Kafka engine vẫn là at-least-once, nên đầu cuối vẫn cần idempotent. Thêm vào đó là độ phức tạp, consumer phải đọc read_committed, và độ trễ tăng. Với LSN làm version, idempotent ở sink rẻ và chắc chắn hơn.

### Q56. "Effectively-once" nghĩa là gì trong dự án này? [Nâng cao]
Message có thể đến nhiều lần, nhưng trạng thái nhìn thấy ở silver, consistent và mart giống như mỗi thay đổi được áp đúng một lần. Điều này có được vì phép áp là idempotent và có thứ tự theo LSN: áp lại một version cũ không đè lên version mới hơn. Đó là mục tiêu về hiệu ứng idempotent, không phải bằng chứng rằng toàn pipeline này đã được kiểm chứng exactly-once. Cần test replay của dữ liệu hàng và transaction metadata, đặc biệt phần watermark; hiện chưa có bài bơm trùng có chủ đích trong gate.

### Q57. Thứ tự có bị đảo khi gửi lại không? [Nâng cao]
Trong Kafka, bản gửi lại nằm ở offset mới, sau các message mới hơn đã gửi. Nếu consumer áp theo thứ tự offset kiểu "cái sau ghi đè", nó sẽ quay về trạng thái cũ. Dự án tránh được vì silver chọn theo LSN lớn nhất chứ không theo thứ tự đến. Đây là lý do không dùng offset Kafka làm version.

### Q58. Dừng connector bình thường có tạo trùng không? [Trung bình]
Thường không hoặc rất ít: khi dừng có kiểm soát, Connect cố flush offset lần cuối trước khi tắt task. Trùng nhiều chủ yếu xuất hiện sau crash, kill, hoặc task FAILED. Dù vậy pipeline không dựa vào việc dừng sạch.

### Q59. Debezium có tự bỏ các sự kiện đã xử lý khi khởi động lại không? [Nâng cao]
Có ở mức nhất định: Debezium lưu cả LSN commit và LSN của sự kiện cuối trong offset, và khi stream lại có thể bỏ các sự kiện của transaction đã được ghi nhận. Nhưng những sự kiện sau lần flush offset cuối thì nó không biết đã gửi hay chưa, nên vẫn gửi lại. Thiết kế đúng là coi trùng là bình thường.

### Q60. Làm sao kiểm chứng rằng có trùng và nó không gây hại? [Trung bình]
check_data.py đếm số dòng trùng trong bronze (cdc_events theo topic/partition/offset, domain_events theo event_id) ở mức cảnh báo. Đồng thời nó so sánh PostgreSQL với silver cho 31 bảng và consistent.* với silver khi hệ thống lắng: nếu trùng gây hại, các phép so này sẽ lệch. Reconciliation mỗi 30 giây cũng so nguồn với warehouse.

---

## E. Cấu hình connector

### Q61. topic.prefix = cdc ảnh hưởng gì? [Cơ bản]
Mọi topic dữ liệu có dạng `<prefix>.<schema>.<table>`, ra cdc.ecommerce.orders. Topic transaction metadata là cdc.transaction, heartbeat là __debezium-heartbeat.cdc. Prefix cũng là tên logic của server trong offset, nên đổi prefix giống như tạo một connector mới (mất offset).

### Q62. table.include.list được viết thế nào và vì sao? [Trung bình]
Đó là regex khớp mọi bảng trong schema ecommerce trừ một danh sách bảng hạ tầng (dùng negative lookahead). Viết dạng loại trừ để bảng nghiệp vụ mới tự được stream mà không phải sửa connector. Đổi lại, một bảng mới phải có spec silver, nếu không check_contract.py trong CI sẽ báo lỗi.

### Q63. snapshot.mode = initial nghĩa là gì? Có những mode nào khác? [Trung bình]
initial: snapshot khi chưa có offset, sau đó stream. Các mode khác có thể là always (luôn snapshot khi khởi động), never/no_data (chỉ stream, không đọc dữ liệu có sẵn), initial_only (chỉ snapshot rồi dừng), when_needed (snapshot khi offset không còn dùng được). Dự án chọn initial vì an toàn và đơn giản; phục hồi sau mất slot làm bằng incremental snapshot có kiểm soát.

### Q64. snapshot.locking.mode = shared có chặn ghi không? [Trung bình]
Không. Nó chỉ giữ lock nhẹ trong pha đọc cấu trúc bảng, đủ để chặn DDL làm đổi bảng giữa chừng, nhưng không chặn INSERT/UPDATE/DELETE. Dữ liệu được đọc nhất quán nhờ snapshot REPEATABLE READ gắn với slot, không nhờ lock. Source API vẫn chạy bình thường khi snapshot.

### Q65. heartbeat.interval.ms và heartbeat.action.query khác nhau thế nào? [Trung bình]
heartbeat.interval.ms (10 s) làm Debezium phát message heartbeat vào topic heartbeat, cho Connect một offset mới để commit. heartbeat.action.query là câu SQL Debezium chạy trên nguồn mỗi lần heartbeat, ở đây upsert cdc_heartbeat, tạo thay đổi thật trong WAL đi qua publication. Câu query cần thiết khi trong database chỉ có ghi vào bảng không được stream, để luồng replication có sự kiện và slot tiến.

### Q66. Vì sao tombstones.on.delete = false? [Trung bình]
Tombstone là message value null sau DELETE, dùng cho log compaction để xóa key. Dự án không compact topic dữ liệu (retention theo thời gian), nên tombstone vô ích. Tệ hơn, message null sẽ làm hỏng ingest của ClickHouse. Sự kiện DELETE vẫn có, với before đầy đủ.

### Q67. Vì sao decimal.handling.mode = string? [Trung bình]
Mặc định precise mã hóa DECIMAL thành byte kèm scale, khó đọc từ JSON; double làm tròn qua số thực, không chấp nhận được với tiền. string giữ giá trị chính xác dạng chuỗi như "109.99", ClickHouse parse vào Decimal. Tiền của dự án là BIGINT VND, nên đây là lưới an toàn cho cột DECIMAL.

### Q68. provide.transaction.metadata làm gì? [Cơ bản]
Debezium phát bản ghi BEGIN và END cho mỗi transaction vào cdc.transaction, và gắn khối transaction (id, thứ tự) vào mỗi sự kiện dữ liệu. Bản END có tổng số sự kiện và số sự kiện theo từng bảng. ClickHouse dùng nó để biết khi nào mọi thay đổi của một transaction đã đến.

### Q69. Watermark được tính từ cdc.transaction như thế nào? [Nâng cao]
ClickHouse lưu bản END vào bronze.cdc_transactions kèm offset Kafka (thứ tự commit vì topic có một partition) và số sự kiện cần đến bronze (trừ phần outbox). Watermark v là offset của transaction đầu tiên mà số sự kiện đã đến (đếm theo bảng và LSN khác nhau) nhỏ hơn số cần; nếu không có thì là offset lớn nhất cộng một. Mọi transaction trước v đều đã đủ, nên consistent.*(v) không bao giờ thấy nửa transaction. Chi tiết ở 04-warehouse.md.

### Q70. Vì sao cần watermark khi Kafka đã có thứ tự? [Trung bình]
Kafka chỉ có thứ tự trong một partition, mà mỗi bảng là một topic riêng và ClickHouse đọc các topic độc lập. Một transaction cập nhật orders và seller_ledger_entries có thể đến ClickHouse ở hai thời điểm cách nhau vài trăm ms. Một lần đo dưới tải thấy 20 đơn ở trạng thái đó cùng lúc. Watermark ẩn các transaction chưa đủ để báo cáo không bao giờ thấy trạng thái chưa từng tồn tại.

### Q71. Băm PII được làm thế nào? [Cơ bản]
Setting column.mask.hash.v2 với SHA-256 và salt liệt kê các cột cá nhân (tên, email, điện thoại khách; tên người nhận, điện thoại, đường; tên và địa chỉ giao hàng; email người bán). Debezium thay giá trị bằng hash trước khi gửi, nên Kafka và warehouse không có dữ liệu gốc. Cùng giá trị cho cùng hash, nên vẫn join và đếm được.

### Q72. Vì sao cần salt khi băm PII? [Trung bình]
Email và số điện thoại có không gian giá trị nhỏ và dễ đoán; không có salt, kẻ tấn công băm sẵn mọi số điện thoại Việt Nam và tra ngược. Salt bí mật làm bảng tra sẵn vô dụng. Salt đặt một lần mỗi môi trường, vì đổi salt đổi mọi hash và phá join với dữ liệu cũ.

### Q73. Vì sao register.sh phải tự thay salt thay vì dùng config provider? [Nâng cao]
Salt là một phần của **tên** setting (column.mask.hash.v2.SHA-256.with.salt.<salt>), không phải giá trị. Config provider của Connect chỉ thay giá trị, không thay tên khóa. Vì vậy register.sh đọc secrets/pii_salt và thay chuỗi giữ chỗ trong file trước khi PUT. Hệ quả là salt nằm trong config lưu ở debezium-config và đọc được qua REST, nên cổng 8083 chỉ mở trên localhost.

### Q74. Băm PII ở Debezium có ưu nhược điểm gì so với băm ở warehouse? [Nâng cao]
Ưu: dữ liệu gốc không bao giờ vào Kafka, ClickHouse, backup hay S3, giảm phạm vi tuân thủ và rủi ro lộ. Nhược: warehouse không bao giờ khôi phục được giá trị gốc (muốn gửi email thì phải hỏi hệ nguồn), đổi danh sách cột chỉ áp cho sự kiện mới, và hash vẫn là dữ liệu bí danh hóa chứ không phải ẩn danh tuyệt đối. Với mục đích phân tích, đánh đổi này hợp lý.

### Q75. errors.tolerance = none có nghĩa gì? Vì sao chọn vậy? [Trung bình]
Một record không chuyển đổi được (lỗi converter, lỗi SMT, schema bị từ chối) làm task FAILED thay vì bị bỏ qua. Dự án chọn fail fast: bỏ một sự kiện sẽ làm warehouse sai âm thầm và làm watermark kẹt (transaction thiếu sự kiện). Khi task dừng, slot giữ WAL nên không mất gì; alert ConnectorNotRunning báo cho người vận hành.

### Q76. Vì sao không dùng dead letter queue? [Nâng cao]
DLQ của Kafka Connect (errors.deadletterqueue.*) chỉ dành cho sink connector, không áp dụng cho source. Ngay cả nếu có, đưa sự kiện CDC ra chỗ khác làm phá thứ tự và tính đầy đủ của transaction. Phía ClickHouse thì có cơ chế tương đương: message không đọc được vào bronze.ingest_errors để phát lại.

### Q77. Vì sao bảng signal được bật qua kênh source? [Nâng cao]
Kênh source nghĩa là Debezium đọc tín hiệu từ một bảng trong chính database nguồn, và bảng đó đi qua WAL như mọi bảng khác. Nhờ vậy tín hiệu nằm đúng vị trí trong dòng thay đổi, và Debezium cũng ghi các mốc mở/đóng cửa sổ incremental snapshot vào đó để khử trùng chính xác. Có kênh khác như Kafka hay JMX, nhưng incremental snapshot trên PostgreSQL cần bảng signal ở nguồn.

### Q78. time.precision.mode = connect ảnh hưởng gì? [Nâng cao]
Các kiểu date/time/timestamp không múi giờ dùng kiểu logic chuẩn của Kafka Connect, độ chính xác mili giây. Cột TIMESTAMPTZ thì Debezium luôn gửi dạng chuỗi ISO có múi giờ, giữ micro giây. Silver parse theo spec từng cột. Nên kiểm tra mode này khi cần độ chính xác micro giây cho cột không múi giờ.

### Q79. Vì sao các khóa _comment và _transforms.unwrap vẫn có trong file? [Cơ bản]
Connect lưu mọi khóa nhưng Debezium bỏ qua khóa nó không biết, nên dùng chúng làm ghi chú ngay trong JSON (JSON không có comment). Cấu hình unwrap được giữ ở dạng tắt để ghi lại một quyết định: đã cân nhắc và loại bỏ vì mất before. Đây là cách tài liệu hóa quyết định nằm ngay cạnh cấu hình.

### Q80. Nếu thêm một cột PII mới vào bảng thì phải làm gì? [Trung bình]
Thêm cột vào danh sách băm trong postgres-connector.json, chạy lại register.sh, cập nhật spec silver (cột hash khai báo là chuỗi) và chạy check_contract.py. Phải làm **trước** khi cột có dữ liệu thật, vì sự kiện đã gửi ở dạng rõ sẽ nằm vĩnh viễn trong Kafka (7 ngày) và bronze. check_data.py sẽ báo lỗi nếu email, điện thoại hay địa chỉ rõ xuất hiện trong ClickHouse.

---

## F. Outbox pattern

### Q81. Outbox pattern giải quyết vấn đề gì? [Cơ bản]
Vấn đề dual write: ứng dụng ghi DB và gửi message là hai hệ thống không có transaction chung, một bên lỗi là lệch vĩnh viễn. Outbox ghi sự kiện vào một bảng trong cùng transaction với thay đổi nghiệp vụ, nên sự kiện tồn tại khi và chỉ khi thay đổi commit. CDC đọc bảng outbox từ WAL và gửi sự kiện ra Kafka.

### Q82. Dự án ghi outbox như thế nào? [Trung bình]
Hàm emit của Source API chèn một dòng với aggregatetype, aggregateid, type, payload JSON và created_at theo đồng hồ nghiệp vụ, rồi xóa chính dòng đó ngay, trong cùng transaction của lệnh. WAL vẫn giữ INSERT nên Debezium thấy sự kiện; bảng thì luôn rỗng. created_at lấy giờ tại lúc gọi chứ không phải lúc bắt đầu transaction, nên sự kiện của cùng aggregate có thứ tự theo thời điểm xảy ra.

### Q83. Vì sao insert rồi delete ngay dòng outbox? [Trung bình]
Để bảng outbox không bao giờ phình và không cần job dọn dẹp (job dọn có thể xóa nhầm dòng chưa được đọc). Logical decoding đọc từ WAL, không đọc bảng, nên dòng đã xóa vẫn được phát. Router bỏ qua DELETE. Mẫu này được chính tài liệu Debezium gợi ý.

### Q84. Outbox Event Router làm gì? [Trung bình]
Đó là SMT của Debezium: lấy sự kiện insert của bảng outbox, đổi topic thành events.<aggregatetype>, đặt key là aggregateid, value là payload, và đặt thêm eventId, eventType, aggregateType, aggregateId, occurredAt theo cấu hình. Sự kiện DELETE của bảng outbox bị bỏ. Router chỉ áp cho topic outbox nhờ predicate.

### Q85. Vì sao payload outbox giữ dạng chuỗi JSON? [Nâng cao]
Một topic events.order chứa nhiều loại sự kiện với hình dạng khác nhau (OrderPlaced, OrderCancelled...). Nếu bung payload thành object, schema của value thay đổi theo từng loại và Schema Registry với BACKWARD sẽ chặn. Giữ là chuỗi, schema của topic ổn định; consumer parse payload theo eventType. bronze.domain_events đọc được cả hai dạng (chuỗi và object cũ).

### Q86. So sánh outbox và table CDC. Khi nào dùng cái nào? [Trung bình]
Table CDC phản ánh đúng trạng thái DB, không cần ứng dụng làm gì, thấy cả DELETE và before, nhưng gắn với cấu trúc bảng và thiếu ý nghĩa nghiệp vụ. Outbox là hợp đồng sự kiện do ứng dụng định nghĩa, ổn định khi refactor bảng, mang ý nghĩa nghiệp vụ, nhưng ứng dụng phải ghi đúng và chỉ có những gì ứng dụng ghi. Dự án dùng table CDC cho warehouse (cần trạng thái đầy đủ) và outbox cho sự kiện domain dành cho consumer nghiệp vụ.

### Q87. Key của sự kiện domain là gì, và vì sao? [Trung bình]
Key là aggregateid, ví dụ id đơn hàng. Kafka đưa cùng key vào cùng partition, nên mọi sự kiện của một đơn có thứ tự, kể cả khi topic có nhiều partition. Consumer xử lý theo aggregate (state machine đơn hàng) cần đúng điều này.

### Q88. Consumer của events.* phải khử trùng thế nào? [Trung bình]
Mỗi sự kiện có eventId là UUID của dòng outbox, không đổi khi gửi lại. Consumer lưu eventId đã xử lý (hoặc dùng làm khóa upsert) và bỏ bản trùng. bronze.domain_events là MergeTree thường nên có thể chứa bản trùng; truy vấn đếm phải dùng số eventId khác nhau, như check_data.py làm.

### Q89. Sự kiện outbox ảnh hưởng gì đến watermark? [Nâng cao]
Bản END của cdc.transaction đếm cả insert và delete của bảng outbox (mỗi sự kiện domain là hai thay đổi), nhưng những thay đổi đó không đến bronze.cdc_events mà đến events.*. ClickHouse trừ số thay đổi của outbox_events ra khỏi tổng để có số sự kiện cần đến bronze. Nếu không trừ, mọi transaction có outbox sẽ trông như chưa đủ và watermark kẹt.

---

## G. Kafka

### Q90. KRaft là gì? Vì sao không dùng ZooKeeper? [Cơ bản]
KRaft là chế độ Kafka tự quản lý metadata bằng giao thức Raft nội bộ, controller nằm trong Kafka. Không cần cụm ZooKeeper riêng, ít thành phần hơn, khởi động và failover controller nhanh hơn, và Kafka 4.x đã bỏ hẳn ZooKeeper. Dự án chạy một node vừa là broker vừa là controller.

### Q91. Hai listener của Kafka để làm gì? [Cơ bản]
kafka:9092 dành cho các container trong mạng compose (Debezium, ClickHouse, Control Center). localhost:29092 dành cho công cụ chạy trên máy host. Advertised listener phải khớp với địa chỉ client dùng được, nếu không client nhận metadata với địa chỉ không truy cập được.

### Q92. Các topic được đặt tên và tổ chức thế nào? [Cơ bản]
cdc.ecommerce.<table> cho 31 bảng, events.<aggregate> cho 13 loại aggregate, cdc.transaction cho metadata transaction, __debezium-heartbeat.cdc cho heartbeat, debezium-config/offsets/status cho Connect, _schemas cho Schema Registry. Tên có cấu trúc giúp consumer đăng ký bằng regex: ClickHouse đọc ^cdc\.ecommerce\..* và ^events\..*, nên bảng và aggregate mới tự được đọc.

### Q93. Mỗi topic có bao nhiêu partition và key là gì? [Trung bình]
Topic được tự tạo với mặc định broker: 1 partition, RF 1. Key của table topic là khóa chính dạng JSON, của events.* là aggregateId. Một partition nghĩa là thứ tự toàn phần trong mỗi bảng, đủ cho tải hiện tại. Khi tăng partition, key theo khóa chính giữ thứ tự theo từng dòng.

### Q94. Kafka đảm bảo thứ tự ở mức nào? Pipeline có thứ tự gì? [Trung bình]
Kafka chỉ đảm bảo thứ tự trong một partition. Pipeline có thứ tự theo commit trong từng table topic, theo aggregate trong events.*, và thứ tự commit toàn cục trên cdc.transaction. Không có thứ tự giữa các topic; watermark bù cho điều đó. Sau gửi lặp, thứ tự offset không còn trùng LSN, nên downstream so bằng LSN.

### Q95. Vì sao cdc.transaction phải có đúng một partition? [Nâng cao]
Watermark dùng offset của bản END làm thứ tự commit toàn cục: "mọi transaction có offset nhỏ hơn v đều đủ". Nếu topic có nhiều partition, offset ở các partition không so sánh được với nhau và không còn thứ tự toàn cục. Nếu cần scale, phải giữ topic này một partition (tải của nó nhỏ, một bản ghi cho mỗi transaction).

### Q96. Retention của topic là bao lâu, và vì sao quan trọng? [Trung bình]
Mặc định broker, 168 giờ (7 ngày), cleanup policy delete. Đây là khoảng thời gian ClickHouse có thể dừng mà không mất gì, và cũng là khoảng có thể dựng lại warehouse từ Kafka. Lưu trữ lâu dài là việc của bronze (giữ vĩnh viễn, phần cũ chuyển xuống S3). Disk của Kafka phải đủ cho 7 ngày ở tải cao.

### Q97. Có những consumer group nào? [Cơ bản]
clickhouse-cdc đọc mọi table topic, clickhouse-cdc-transactions đọc cdc.transaction, clickhouse-events đọc events.*. Control Center tạo group cdc-webapp-<ngẫu nhiên> mỗi lần chạy, đọc từ latest và không commit, chỉ để hiển thị live feed. Các group độc lập, nên một consumer chậm không ảnh hưởng consumer khác.

### Q98. Vì sao Control Center dùng group id ngẫu nhiên và không commit offset? [Trung bình]
Live feed chỉ cần sự kiện từ bây giờ, không cần lịch sử hay tiếp tục sau restart. Group mới với auto.offset.reset latest bắt đầu ở cuối topic; không commit để không để lại offset vô ích. Group riêng cũng đảm bảo nó không chia partition với ClickHouse.

### Q99. ClickHouse Kafka engine đọc message như thế nào? [Trung bình]
Bảng Kafka engine đọc mỗi message như một chuỗi thô (RawBLOB), một consumer, flush mỗi 1 giây. Materialized view bỏ 5 byte header của Schema Registry nếu byte đầu là 0, rồi trích op, LSN, txId, thời điểm, before, after vào bronze.cdc_events cùng topic/partition/offset. Message không phải JSON hoặc lỗi đi vào bronze.ingest_errors (handle_error_mode stream) thay vì bị bỏ.

### Q100. Vì sao ClickHouse đọc thẳng Kafka thay vì dùng sink connector hay Flink? [Trung bình]
Ít thành phần hơn: không cần thêm Connect sink hay cụm Flink. ClickHouse Kafka engine kết hợp materialized view là đủ để ghi bronze và dựng silver; logic upsert dựa trên ReplacingMergeTree. Đổi lại, xử lý lỗi và quan sát kém linh hoạt hơn sink chuyên dụng. Với một warehouse một node, đây là lựa chọn gọn.

### Q101. acks=all có ý nghĩa gì với một broker RF=1? [Nâng cao]
acks=all chờ mọi replica in-sync xác nhận; với RF=1 chỉ có leader, nên tương đương acks=1. Nó không tăng độ bền ở local nhưng là cấu hình đúng cho production với RF=3 và min.insync.replicas=2, nơi nó đảm bảo message sống sót khi mất một broker. Đặt sẵn để chuyển môi trường không phải sửa connector.

### Q102. Khi nào nên tăng partition cho table topic? [Nâng cao]
Khi một consumer không theo kịp một topic (ví dụ orders ở quy mô T1 hàng triệu đơn/ngày) và cần nhiều consumer song song. Key theo khóa chính giữ thứ tự theo dòng, đủ cho silver vì silver chọn theo LSN. Lưu ý tăng partition đổi ánh xạ key sang partition cho message mới, và cdc.transaction vẫn phải một partition. Hiện nút thắt là đường ghi nguồn, nên chưa cần.

### Q103. Kafka UI được dùng để làm gì? [Cơ bản]
Kafka UI (cổng 8080 trên localhost) xem topic, message, consumer group và lag, kết nối với Connect và Schema Registry. Nó tiện để xem một message cụ thể, kiểm tra schema đã đăng ký, hoặc xem connector. Đây là công cụ điều tra, không phải giám sát; giám sát dùng Prometheus.

---

## H. Schema Registry

### Q104. Schema Registry là gì và vì sao dự án cần nó? [Cơ bản]
Đó là dịch vụ lưu schema của message theo subject và kiểm tra tương thích khi schema đổi. Mỗi topic có subject <topic>-value. Không có registry, một thay đổi kiểu cột ở PostgreSQL sẽ trôi thẳng xuống ClickHouse và làm sai âm thầm; có registry, thay đổi phá vỡ bị chặn ngay ở Debezium và có alert.

### Q105. Message có Schema Registry trông khác gì? [Trung bình]
Value vẫn là JSON như trước, nhưng có 5 byte đầu: một byte magic bằng 0 và 4 byte id schema. Consumer biết schema nào đã dùng để ghi. ClickHouse và Control Center kiểm tra byte đầu và bỏ 5 byte trước khi parse. Key không qua registry, vẫn là JSON thường.

### Q106. BACKWARD compatibility nghĩa là gì? [Cơ bản]
Schema mới phải đọc được dữ liệu ghi bằng schema cũ: consumer nâng cấp lên schema mới vẫn đọc được message cũ trong topic. Thêm trường optional là tương thích; đổi kiểu trường là không. BACKWARD là mặc định phổ biến vì cho phép nâng cấp consumer trước rồi mới đổi producer, và consumer chính (ClickHouse) đọc lại cả dữ liệu cũ.

### Q107. BACKWARD, FORWARD và FULL khác nhau thế nào? [Trung bình]
BACKWARD: schema mới đọc dữ liệu cũ (nâng consumer trước). FORWARD: schema cũ đọc được dữ liệu mới (nâng producer trước). FULL: cả hai chiều. Các biến thể TRANSITIVE kiểm tra với mọi phiên bản trước chứ không chỉ phiên bản ngay trước. Dự án dùng BACKWARD (không transitive).

### Q108. Điều gì xảy ra khi có thay đổi schema không tương thích? [Trung bình]
Debezium thấy cấu trúc mới, converter đăng ký schema mới, registry trả lỗi "Schema being registered is incompatible". Với errors.tolerance none, task FAILED, offset không tiến, slot giữ WAL nên không mất dữ liệu. Sau 2 phút alert ConnectorNotRunning bắn; nếu kéo dài, PostgresWALRetained bắn. Đã kiểm chứng bằng bảng probe đổi cột INT thành TEXT.

### Q109. Runbook khi registry chặn một thay đổi? [Trung bình]
Xem lỗi task qua REST status để xác nhận nguyên nhân. Nếu thay đổi là nhầm, sửa lại nguồn rồi restart task. Nếu có chủ đích, chuẩn bị consumer trước (spec silver, gen_silver.py, check_contract.py), đặt compatibility của đúng subject đó thành NONE, restart task FAILED, rồi xóa cấu hình riêng của subject để quay về BACKWARD. Cuối cùng kiểm tra slot tiến lại, alert hết và ingest_errors rỗng.

### Q110. Vì sao chỉ nới compatibility cho một subject và trả lại ngay? [Trung bình]
Nới toàn cục sẽ cho mọi thay đổi phá vỡ khác lọt qua trong lúc đó. Chỉ nới cho subject có thay đổi đã chuẩn bị và trả về BACKWARD ngay sau khi schema mới được đăng ký, các topic khác vẫn được bảo vệ, và từ phiên bản mới đó subject lại được kiểm tra như thường.

### Q111. Vì sao chọn JSON Schema thay vì Avro hay Protobuf? [Nâng cao]
Message vẫn là JSON, nên ClickHouse chỉ cần bỏ 5 byte header và mọi lớp silver trở lên không phải đổi; đó là chuyển đổi rẻ nhất cho hệ thống đã có. JSON cũng dễ đọc khi điều tra. Avro nhỏ gọn hơn nhiều (không lặp tên trường), nhanh hơn và hệ sinh thái tốt hơn, nhưng ClickHouse phải đọc bằng định dạng AvroConfluent và toàn bộ ingest phải viết lại. Ở quy mô lớn hơn, Avro là lựa chọn hợp lý để giảm băng thông và dung lượng Kafka.

### Q112. Schema được lưu ở đâu? Registry chết thì sao? [Nâng cao]
Schema được lưu trong topic _schemas của Kafka, registry đọc lại khi khởi động. Nếu registry chết, converter không đăng ký hoặc tra id cho schema mới được (schema đã cache vẫn dùng được trong một thời gian), và task có thể fail; slot giữ WAL nên không mất. Consumer của dự án không gọi registry (chỉ bỏ header), nên ClickHouse vẫn đọc bình thường.

### Q113. Kể lại sự cố healthcheck của Schema Registry. [Trung bình]
Healthcheck đầu tiên gọi curl, nhưng image cp-schema-registry không có curl, nên container không bao giờ healthy. Debezium phụ thuộc vào registry healthy, nên không khởi động trong vài phút. Không mất dữ liệu vì slot giữ WAL, Debezium đuổi kịp khi chạy. Sửa bằng healthcheck HTTP thuần qua /dev/tcp của bash. Bài học: healthcheck phải dùng công cụ có trong image, và một healthcheck sai chặn cả chuỗi phụ thuộc.

---

## I. Kịch bản lỗi

### Q114. Debezium dừng 2 giờ. Điều gì xảy ra và bạn thấy gì? [Trung bình]
Source API vẫn chạy, PostgreSQL vẫn ghi, slot giữ WAL từ confirmed_flush_lsn. Không có sự kiện mới vào Kafka, nên trễ CDC tăng và mart cũ dần. Alert: TargetDown hoặc DependencyDown, ConnectorNotRunning, sau đó PostgresWALRetained khi WAL giữ lại vượt 1 GiB, và CDCLagHigh. Khi chạy lại, task đọc từ offset, gửi lại phần chưa commit (có trùng) và đuổi kịp; silver idempotent.

### Q115. Kafka dừng. Điều gì xảy ra? [Trung bình]
Producer không gửi được và retry; queue nội bộ của Debezium đầy, luồng đọc slot bị chặn, nên Debezium ngừng tiêu thụ WAL. Offset không tiến, nên slot không tiến và giữ WAL. ClickHouse không có gì để đọc. Alert DebeziumQueueFilling, DebeziumBehindSource, TargetDown, rồi WAL retained. Kafka lên lại, producer gửi tiếp; nếu task đã FAILED thì restart task.

### Q116. ClickHouse dừng. Điều gì xảy ra? [Cơ bản]
Debezium và Kafka chạy bình thường, message tích lại trong Kafka (retention 7 ngày). Offset của group ClickHouse không tiến, nên khi lên lại nó đọc tiếp từ offset đã commit, có thể đọc lại block cuối. Alert KafkaBacklogGrowing, TargetDown, mart cũ dần. Không mất dữ liệu trừ khi dừng lâu hơn retention.

### Q117. PostgreSQL restart. Slot có mất không? [Trung bình]
Không. Logical slot được lưu bền vững trên đĩa và sống qua restart, kể cả crash (vị trí slot được ghi khi checkpoint). Kết nối replication đứt, Debezium thử kết nối lại hoặc task fail và cần restart. Sau đó stream tiếp từ confirmed_flush_lsn; có thể gửi lại một ít sự kiện.

### Q118. Slot bị invalidate vì WAL vượt 8 GB. Phục hồi thế nào? [Nâng cao]
Thay đổi trong khoảng mất đã không còn trong WAL, Debezium không stream tiếp được. Trước đó PostgresSlotWALAtRisk đã cảnh báo (còn dưới 2 GiB); khi mất, PostgresSlotLost bắn. Phục hồi: xóa slot hỏng để Debezium tạo slot mới, chạy incremental snapshot qua bảng signal để lấy lại trạng thái hiện tại của các bảng, rồi chạy reconciliation. Incremental snapshot không tái tạo được các DELETE trong khoảng mất, nên reconciliation và kiểm tra là bắt buộc.

### Q119. Vì sao "đừng xóa slot" là phản xạ đúng khi WAL phình? [Trung bình]
Xóa slot giải phóng đĩa ngay nhưng làm mất vị trí đọc, Debezium sẽ tạo slot mới ở vị trí hiện tại và bỏ qua mọi thay đổi ở giữa. Việc đúng là tìm vì sao connector không tiến (task FAILED, Kafka chết, schema bị chặn) và khôi phục nó; khi chạy lại, WAL được giải phóng tự nhiên. Chỉ khi sắp đầy đĩa mới cân nhắc hy sinh slot, và chấp nhận snapshot lại.

### Q120. Slot biến mất sau failover. Rủi ro là gì? [Nâng cao]
Trên PostgreSQL 15, slot logical không được sao chép sang standby. Primary mới không có debezium_slot; Debezium có thể tạo slot mới ở LSN hiện tại và tiếp tục như không có gì, bỏ qua thay đổi trong khoảng failover một cách âm thầm. Alert PostgresSlotMissing tồn tại vì lý do này. Phục hồi giống mất slot: incremental snapshot và reconciliation.

### Q121. Connector được restart. Có trùng không, có sao không? [Cơ bản]
Có thể có trùng: task đọc từ offset commit cuối và gửi lại sự kiện sau đó. Không sao: silver chọn theo LSN, watermark đếm theo LSN khác nhau. Bronze sẽ có bản trùng và check_data cảnh báo ở mức warn. Metric trễ có thể tăng tạm vì bản gửi lại có thời điểm commit cũ.

### Q122. Một sự kiện không đọc được ở ClickHouse. Điều gì xảy ra? [Trung bình]
Kafka engine với handle_error_mode stream không dừng và không bỏ message: materialized view lỗi đưa nó vào bronze.ingest_errors cùng topic, partition, offset và nội dung thô. Alert IngestErrors bắn ngay. Nếu sự kiện thuộc một transaction, transaction đó thiếu sự kiện, watermark bị giữ và có thể TransactionWatermarkStalled. Sửa hợp đồng dữ liệu rồi phát lại.

### Q123. Kể lại bài học "không xóa dòng khỏi bronze". [Trung bình]
Sau bài thử bảng probe cho Schema Registry, các dòng của nó bị xóa khỏi bronze để dọn. Nhưng bản END của transaction đó vẫn nói có N sự kiện cần đến bronze; giờ đếm được ít hơn, nên transaction bị coi là chưa đủ và giữ lại mọi transaction sau nó (đến khi ra khỏi cửa sổ 60 phút). TransactionWatermarkStalled bắn và mart đứng. Bronze là sổ cái mà watermark dựa vào; không bao giờ xóa dòng trong đó.

### Q124. Debezium chậm hơn PostgreSQL (behind source cao). Điều tra thế nào? [Nâng cao]
So sánh DebeziumBehindSource với DebeziumQueueFilling: queue đầy nghĩa là phía Kafka/producer chậm (broker, mạng, nén); queue không đầy mà vẫn trễ nghĩa là đọc slot chậm (CPU Debezium, transaction quá lớn, decode). Xem CPU container, metric Jolokia, lag theo byte của slot. Sau đó so với consumer lag của ClickHouse để biết tầng nào chậm, trước khi thêm tài nguyên.

### Q125. CDC lag cao nhưng Debezium không trễ. Lỗi ở đâu? [Trung bình]
Khi Debezium không trễ so với nguồn, phần trễ nằm sau Kafka: ClickHouse đọc chậm (merge, bộ nhớ, quá nhiều part) hoặc consumer bị dừng. Xem KafkaBacklogGrowing theo consumer group, ClickHouse memory và ClickHousePartsHigh. Thêm consumer chỉ giúp khi topic có nhiều partition.

### Q126. Task Debezium FAILED. Có tự khởi động lại không? [Trung bình]
Kafka Connect không tự restart task đã FAILED; cần gọi REST restart (hoặc qua công cụ). Debezium có cơ chế thử lại cho lỗi tạm thời như mất kết nối DB, nhưng lỗi chuyển đổi hay schema bị từ chối làm task FAILED ngay. Alert ConnectorNotRunning bắn sau 2 phút khi Connect vẫn sống mà connector/task không RUNNING. Thêm consumer không sửa được connector dừng.

### Q127. Làm sao biết pipeline không mất dữ liệu sau một sự cố? [Nâng cao]
Reconciliation chạy mỗi 30 giây so PostgreSQL với consistent.*(v) cho 28 bảng (số dòng theo khoảng thời gian và tổng một cột không đổi), bảng nào lệch hai lần liên tiếp thì WarehouseDataDrift bắn. check_data.py so toàn bộ 31 bảng giữa nguồn và silver, outbox với domain_events, và yêu cầu ingest_errors rỗng. Trạng thái slot (wal_status reserved hoặc extended) cho biết WAL cần thiết chưa bị xóa.

---

## J. Hiệu năng và tinh chỉnh

### Q128. Những setting nào đã đổi ở R10 và vì sao? [Trung bình]
max.batch.size 4096 và max.queue.size 16384 (gấp đôi mặc định) để hấp thụ đột biến 11.11, queue giới hạn 64 MB để không ăn hết heap. poll.interval.ms 200 (từ 500) để giảm trễ. Producer: acks all, idempotence, nén lz4 thay gzip, linger 10 ms, batch.size 128 KB để batch to và tốn ít CPU. Kết quả p95 commit→ClickHouse từ 1,86 s xuống 1,57 s ở khoảng 990 sự kiện/giây.

### Q129. Vì sao giới hạn queue theo byte? [Trung bình]
Giới hạn theo số sự kiện không kiểm soát được bộ nhớ: với REPLICA IDENTITY FULL và các dòng chứa JSON lớn, 16384 sự kiện có thể chiếm nhiều trăm MB. Giới hạn 64 MB đảm bảo queue không làm JVM hết heap dù sự kiện to; queue đầy theo điều kiện nào đến trước.

### Q130. lz4 so với gzip, snappy, zstd? [Trung bình]
gzip nén tốt nhưng tốn CPU nhiều. lz4 và snappy rất nhanh, tỷ lệ nén vừa phải; zstd nén tốt gần gzip với CPU thấp hơn. Dự án chuyển gzip sang lz4 vì CPU là chi phí trên đường nóng và ClickHouse đọc được lz4. Debezium sau R10 chỉ dùng 5–17% một nhân.

### Q131. linger.ms và batch.size ảnh hưởng thế nào đến trễ và thông lượng? [Trung bình]
linger.ms là thời gian producer chờ để gom thêm record vào batch; batch.size là kích thước tối đa một batch mỗi partition. Batch to giảm số request, nén tốt hơn, tăng thông lượng, đổi lại tối đa linger.ms trễ thêm. 10 ms là không đáng kể so với SLO 5 giây, trong khi lợi ích lúc burst rõ rệt.

### Q132. poll.interval.ms ảnh hưởng gì? [Trung bình]
Khi queue rỗng, poll của Connect chờ tối đa khoảng này trước khi trả batch. Mặc định 500 ms nghĩa là lúc tải thấp, một sự kiện có thể chờ đến nửa giây trong Debezium. Giảm xuống 200 ms giảm trễ với chi phí CPU nhỏ. Khi tải cao, queue luôn có sự kiện nên setting này ít tác dụng.

### Q133. Các con số hiệu năng đã đo là gì? [Cơ bản]
Ở khoảng 990 sự kiện/giây trong đợt 11.11: p50/p95 commit→ClickHouse từ 1,17/1,86 s xuống 0,92/1,57 s sau R10; Debezium tối đa 0,33 s sau PostgreSQL. Dưới tải nặng, p95 khoảng 1,6 s. Thông lượng tối đa toàn hệ khoảng 1.800 sự kiện/giây, bị giới hạn bởi đường ghi nguồn (khóa dòng SKU bán chạy), không phải CDC. SLO freshness là p95 ≤ 5 s (≤ 30 s cao điểm).

### Q134. Trễ được đo như thế nào? [Trung bình]
Exporter tính p95 của hiệu giữa lúc dòng được ghi vào bronze và source.ts_ms (lúc commit ở PostgreSQL) trong 5 phút gần nhất, bỏ dòng snapshot. Đây là trễ đầu cuối thật từ commit đến khi có trong warehouse. Debezium chỉ chiếm tối đa khoảng 0,33 s; flush 1 giây của Kafka engine là một phần đáng kể còn lại. Lưu ý source.ts_ms dùng giờ thật, không phải đồng hồ nghiệp vụ tăng tốc.

### Q135. Nếu phải đạt 5.400 sự kiện/giây (T1), bạn sẽ làm gì ở tầng CDC? [Nâng cao]
Trước hết giải nút thắt thật ở đường ghi nguồn (chia stock của SKU bán chạy thành slot như quota voucher). Ở CDC: một task Debezium vẫn có dư địa, nhưng sẽ tăng partition cho topic lớn (key theo khóa chính), cân nhắc Avro để giảm kích thước message, nhiều consumer ClickHouse, Kafka 3 broker RF 3. Giữ một connector và cdc.transaction một partition để watermark còn đúng. Đo lại trước khi thêm tài nguyên.

---

## K. So sánh và đánh đổi

### Q136. Debezium so với Maxwell và Fivetran? [Trung bình]
Maxwell chỉ hỗ trợ MySQL, đọc binlog và ghi JSON ra Kafka, đơn giản nhưng hạn chế. Fivetran là dịch vụ SaaS quản lý trọn gói, nạp vào warehouse theo lô vài phút, trả tiền theo dòng, không có sự kiện thời gian thực trên Kafka và ít kiểm soát. Debezium mã nguồn mở, nhiều database, chạy trên Kafka Connect, có snapshot, outbox, transaction metadata, sub-second; đổi lại phải tự vận hành. Dự án cần thời gian thực, transaction metadata và outbox, nên chọn Debezium.

### Q137. Outbox so với table CDC: chọn gì cho tích hợp giữa microservice? [Trung bình]
Cho tích hợp giữa service, outbox tốt hơn: hợp đồng sự kiện ổn định, mang ý nghĩa nghiệp vụ, không lộ cấu trúc bảng nội bộ, service tự do refactor DB. Table CDC hợp với warehouse, cache, search index, nơi cần bản sao trạng thái. Dự án làm cả hai vì có cả hai nhu cầu.

### Q138. JSON Schema so với Avro? [Trung bình]
JSON Schema giữ message dạng JSON dễ đọc, chuyển từ JSON thường rất rẻ (chỉ thêm header), nhưng message to vì lặp tên trường và parse chậm hơn. Avro nhị phân gọn, nhanh, quy tắc tiến hóa schema chặt chẽ và được hỗ trợ rộng, nhưng không đọc trực tiếp được và cần consumer hỗ trợ registry. Dự án chọn JSON Schema để không phải viết lại ingest; Avro là bước hợp lý khi băng thông/dung lượng trở thành vấn đề.

### Q139. At-least-once so với exactly-once? [Trung bình]
At-least-once đơn giản, nhanh, không mất dữ liệu nhưng có trùng, đòi hỏi consumer idempotent. Exactly-once trong Kafka dùng transaction, tốn thêm độ trễ, phức tạp, và chỉ đúng trong phạm vi Kafka; khi ra hệ thống ngoài vẫn cần sink idempotent hoặc transaction với sink. Dự án chọn at-least-once kèm sink idempotent theo LSN, đạt effectively-once ở lớp đọc.

### Q140. CDC log-based so với polling theo updated_at? [Cơ bản]
Polling không thấy DELETE, bỏ sót các update trung gian giữa hai lần poll, tốn tải query, trễ bằng chu kỳ poll và có thể bỏ sót transaction dài commit muộn với timestamp cũ. Log-based thấy mọi thay đổi, đúng thứ tự commit, trễ dưới giây, tải thấp. Đổi lại phải quản lý slot và WAL. Polling chỉ hợp lý khi không có quyền đọc log.

### Q141. Một connector so với nhiều connector? [Nâng cao]
Nhiều connector cho song song hơn và cô lập lỗi (bảng này lỗi không dừng bảng kia). Nhưng mỗi connector có slot riêng ở vị trí riêng và topic transaction riêng: bản END chỉ đếm bảng của connector đó, transaction trải hai nhóm không còn một thứ tự commit chung, watermark mất ý nghĩa. Mỗi slot còn decode lại toàn bộ WAL và giữ WAL riêng. Dự án giữ một connector vì đọc nhất quán quan trọng hơn và một task đã dư sức.

### Q142. Kafka Connect + Debezium so với Debezium Server hay Debezium Engine nhúng? [Nâng cao]
Debezium Server chạy độc lập và gửi đến nhiều đích (Kinesis, Pub/Sub, Redis) không cần Kafka; Debezium Engine nhúng vào ứng dụng Java, tự lo offset. Kafka Connect có REST API, internal topics, cluster worker, hệ sinh thái converter/SMT và Schema Registry. Vì dự án đã dùng Kafka làm xương sống và cần Schema Registry, Connect là lựa chọn tự nhiên.

### Q143. pgoutput so với logical replication gốc của PostgreSQL sang ClickHouse (MaterializedPostgreSQL)? [Nâng cao]
ClickHouse có engine đọc trực tiếp logical replication của PostgreSQL, bỏ qua Kafka. Gọn hơn, nhưng mất Kafka làm bộ đệm (retention, nhiều consumer, replay), mất outbox và transaction metadata, không có Schema Registry, và engine này còn được đánh dấu thử nghiệm. Dự án cần nhiều consumer (ClickHouse, live feed, consumer domain sau này), nên dùng Kafka ở giữa.

---

## L. Câu hỏi mở

### Q144. Hãy vẽ và giải thích kiến trúc CDC của bạn. [Nâng cao]
Simulator gọi Source API; mỗi lệnh là một transaction PostgreSQL kèm một dòng outbox. PostgreSQL ghi WAL; Debezium chạy trong Kafka Connect đọc WAL qua slot debezium_slot với pgoutput và publication cdc_publication, băm PII, chuyển outbox thành events.<aggregate>, phát transaction metadata vào cdc.transaction, đăng ký schema ở Schema Registry, gửi vào Kafka KRaft một broker. ClickHouse đọc Kafka bằng Kafka engine vào bronze (sổ cái), dựng silver idempotent theo LSN, consistent.*(v) theo watermark từ cdc.transaction, rồi gold và mart cho Grafana. Prometheus giám sát slot, connector, Debezium JMX, consumer lag, watermark và gửi alert sang Google Chat. Nên vẽ kèm đường xác nhận ngược: Kafka ack → offset → flush LSN → slot.

### Q145. Làm sao bạn đảm bảo không mất dữ liệu? [Nâng cao]
Mỗi tầng chỉ xác nhận cho tầng trước khi đã ghi bền ở tầng sau. PostgreSQL giữ WAL qua slot đến khi Debezium báo flush; Debezium chỉ báo flush sau khi offset được commit, và offset chỉ commit sau khi Kafka ack với acks=all. Kafka giữ 7 ngày; ClickHouse commit offset sau khi ghi bronze; bronze không bao giờ xóa. Lỗi chuyển đổi làm dừng chứ không bỏ (errors.tolerance none, ingest_errors). Rủi ro còn lại (slot vượt 8 GB, mất slot khi failover) đều có alert sớm và runbook snapshot lại + reconciliation, và reconciliation liên tục kiểm chứng.

### Q146. Làm sao bạn xử lý bản ghi trùng? [Trung bình]
Chấp nhận trùng là bình thường với at-least-once và làm downstream idempotent. Danh tính của một thay đổi bảng là (khóa chính, LSN): silver dùng ReplacingMergeTree với version = LSN và FINAL, nên trùng gộp và gửi lại bản cũ không đè bản mới. Watermark đếm LSN khác nhau. Sự kiện domain dùng eventId. Bronze cố ý giữ trùng để audit, và check_data đếm chúng.

### Q147. Một thay đổi schema ở nguồn đi qua pipeline như thế nào? [Nâng cao]
Thêm cột nullable: Debezium nhận cấu trúc mới, schema mới tương thích BACKWARD được đăng ký, message có cột mới; silver chỉ có cột đó khi spec được cập nhật, và check_contract.py trong CI báo nếu spec thiếu. Đổi kiểu cột: registry từ chối, task dừng, ConnectorNotRunning bắn, slot giữ WAL; theo runbook chuẩn bị consumer, nới compatibility cho subject đó, restart task, trả lại BACKWARD. Bảng mới: tự được stream nhờ include list dạng loại trừ và regex của ClickHouse, nhưng cần spec silver.

### Q148. Nếu triển khai lên production, bạn đổi gì? [Nâng cao]
PostgreSQL HA với Patroni permanent slots hoặc nâng lên PG17 để có failover slots; Kafka 3 broker, RF 3, min.insync.replicas 2; Connect nhiều worker; Schema Registry nhiều instance; TLS/SASL cho Kafka và DB; secrets qua vault hoặc SOPS. Tạo topic chủ động với partition và retention rõ ràng thay vì tự tạo. Cân nhắc Avro. Giữ một connector, giữ cdc.transaction một partition, giữ alert về slot.

### Q149. Điểm yếu lớn nhất của thiết kế hiện tại là gì? [Nâng cao]
Single-node: một broker RF 1, một PostgreSQL, một worker, nên mất đĩa Kafka là mất bộ đệm, và PG15 không có failover slots. Topic tự tạo với mặc định broker (1 partition, 7 ngày). Salt PII nằm trong config của Connect và đọc được qua REST. Phục hồi khi mất slot không tái tạo được các DELETE trong khoảng mất. Tất cả đều có lý do (chạy trên một máy) và đã được ghi lại, nhưng phải xử lý trước khi lên production.

### Q150. Bạn học được gì quan trọng nhất từ phần CDC? [Trung bình]
CDC dễ bật nhưng khó vận hành đúng: phải hiểu chuỗi xác nhận để biết khi nào mất và khi nào trùng, rồi thiết kế downstream idempotent. Slot vừa bảo vệ dữ liệu vừa đe dọa đĩa, cần giới hạn và alert. Đọc nhất quán theo transaction không tự có khi mỗi bảng một topic, cần transaction metadata và watermark. Và các sự cố nhỏ (healthcheck dùng curl, xóa dòng probe) dạy rằng mọi tầng phải được giám sát và sổ cái không được đụng vào.
