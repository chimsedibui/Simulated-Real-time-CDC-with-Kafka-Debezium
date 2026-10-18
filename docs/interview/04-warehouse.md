# Q&A 04 · Data warehouse ClickHouse

[Tài liệu dự án](../README.md) / [Phỏng vấn](README.md)

## Đọc file này để biết gì

Bộ câu hỏi ôn phỏng vấn cho phần warehouse: MergeTree và Kafka engine, các lớp bronze → silver → consistent → gold → snap → mart, watermark, incremental snapshot, storage, vận hành, edge case và các câu hỏi mở. Mỗi câu trả lời bám theo đúng cách dự án đang làm. Tài liệu gốc: [../04-warehouse.md](../platform/04-warehouse.md).

Mức độ: **[Cơ bản]** khái niệm, **[Trung bình]** cơ chế trong dự án, **[Nâng cao]** trade-off, edge case, thiết kế.

## Mục lục

1. [Tổng quan và lựa chọn công nghệ](#1-tổng-quan-và-lựa-chọn-công-nghệ) (Q1–Q12)
2. [MergeTree, ReplacingMergeTree, FINAL](#2-mergetree-replacingmergetree-final) (Q13–Q27)
3. [Kafka engine và bronze](#3-kafka-engine-và-bronze) (Q28–Q43)
4. [Silver](#4-silver) (Q44–Q56)
5. [Watermark và consistent view](#5-watermark-và-consistent-view) (Q57–Q78)
6. [Gold, mart, ops](#6-gold-mart-ops) (Q79–Q86)
7. [Incremental snapshot (R20)](#7-incremental-snapshot-r20) (Q87–Q101)
8. [warehouse-jobs](#8-warehouse-jobs) (Q102–Q108)
9. [Storage, bảo mật, quyền](#9-storage-bảo-mật-quyền) (Q109–Q118)
10. [Sự cố và edge case](#10-sự-cố-và-edge-case) (Q119–Q132)
11. [Hiệu năng](#11-hiệu-năng) (Q133–Q138)
12. [Câu hỏi mở](#12-câu-hỏi-mở) (Q139–Q146)

---

## 1. Tổng quan và lựa chọn công nghệ

### Q1. Mô tả ngắn luồng dữ liệu trong warehouse. [Cơ bản]
Kafka engine của ClickHouse đọc ba nhóm topic: `cdc.ecommerce.*` (mỗi bảng một topic), `cdc.transaction` (siêu dữ liệu giao dịch) và `events.*` (outbox). Materialized view parse message vào bronze (`cdc_events`, `cdc_transactions`, `domain_events`). Từ `cdc_events`, 31 MV đổ vào `silver.<table>` (trạng thái hiện tại). `consistent.<table>(v)` cho silver tại watermark v, gold định nghĩa metric trên consistent, `warehouse-jobs` giữ snapshot `snap.*` và tính mart `mart.*` cho Grafana.

### Q2. Tại sao chọn ClickHouse làm warehouse? [Trung bình]
ClickHouse là columnar OLAP, aggregate rất nhanh trên một node, và có sẵn Kafka engine nên tự làm consumer, không cần Spark/Flink hay Kafka Connect sink. Họ engine ReplacingMergeTree hợp với CDC: chèn mỗi thay đổi như một phiên bản mới, engine giữ phiên bản lớn nhất. Parameterized view cho phép truyền watermark vào view, là nền của đọc nhất quán. Toàn bộ logic là SQL trong một hệ thống, dễ chạy local bằng docker compose.

### Q3. Tại sao không dùng Spark Structured Streaming hoặc Flink? [Nâng cao]
Với một node và vài nghìn event/s, thêm một stream processor là thêm một cluster, checkpoint, state backend và một chỗ hỏng nữa. Việc cần làm (parse JSON, upsert theo version, ghép giao dịch) ClickHouse làm được bằng MV và engine Replacing. Roadmap cũ có Flink/Iceberg nhưng đã bỏ để giữ hệ thống nhỏ. Flink sẽ đáng cân nhắc khi cần join stream có state lớn, exactly-once end-to-end vào nhiều sink, hoặc tách compute khỏi storage ở quy mô lớn.

### Q4. Tại sao không dùng dbt? [Nâng cao]
SQL ở đây phần lớn là sinh tự động từ một spec (`gen_silver.py`), và các đối tượng chính là MV, Kafka engine và parameterized view nhận watermark — không khớp tự nhiên với mô hình materialization (table/view/incremental) của dbt. Việc chạy lại định kỳ cần giữ một v cho cả lần chạy, nên một service Python nhỏ đơn giản hơn. dbt vẫn hữu ích cho lineage, test và tài liệu; có thể dùng nó cho lớp gold nếu số mart tăng nhiều, nhưng phải giải quyết việc truyền v.

### Q5. Medallion architecture là gì và dự án dùng nó thế nào? [Cơ bản]
Medallion chia dữ liệu theo độ tinh chế: bronze (thô), silver (đã làm sạch, có kiểu), gold (sẵn cho nghiệp vụ). Dự án thêm hai lớp đặc thù: consistent (silver tại một watermark giao dịch) và snap/mart (lớp tốc độ). Mỗi lớp sửa được độc lập: logic silver sai thì dựng lại từ bronze; metric sai thì chỉ sửa gold.

### Q6. Tại sao cần lớp consistent chứ không cho gold đọc thẳng silver? [Trung bình]
Mỗi bảng đến từ một topic riêng và được đọc độc lập, nên trong vài giây silver có thể thấy nửa giao dịch, ví dụ order COMPLETED nhưng chưa có bút toán SALE cùng giao dịch. Probe dưới tải đếm được 20 order như vậy tại một thời điểm. Consistent chỉ hiện các giao dịch đã về đầy đủ, nên gold luôn nhìn một trạng thái đã thật sự tồn tại ở PostgreSQL.

### Q7. ClickHouse có transaction xuyên bảng không? Ảnh hưởng gì? [Nâng cao]
Không có snapshot isolation xuyên bảng trong một truy vấn: mỗi bảng, mỗi subquery được đọc ở thời điểm riêng, `now()` cũng có thể khác giữa các subquery. Vì vậy dự án không thể để mỗi view tự chọn "trạng thái mới nhất"; phải có một con số v cố định truyền vào mọi view. Đó là lý do v là tham số.

### Q8. Có bao nhiêu bảng nguồn và chúng chia thế nào? [Cơ bản]
31 bảng, chia hai loại trong spec: bảng trạng thái (state, ví dụ orders, customers, inventory) có update/delete, và bảng sự kiện (event, append-only: order_status_history, inventory_movements, shipment_events, price_history, seller_ledger_entries). Hai loại dùng engine và logic consistent khác nhau.

### Q9. Tại sao sinh SQL bằng script thay vì viết tay? [Trung bình]
31 bảng × (bảng silver + MV + consistent view + bảng snap + delta view) là hàng nghìn dòng gần giống nhau; viết tay chắc chắn lệch. Một spec duy nhất (cột, kiểu, khoá, loại bảng) sinh ra cả `02-silver.sql` và `07-snapshots.sql`. `check_contract.py` dùng cùng spec để so với PostgreSQL và báo lỗi nếu file sinh ra đã cũ.

### Q10. Data contract trong dự án kiểm gì? [Trung bình]
`check_contract.py` fail khi: bảng được stream mà không có spec silver, cột nguồn mới không có trong spec (và không nằm trong danh sách loại trừ), kiểu hoặc nullability không khớp, cột PII bị băm mà không khai là string, bảng trạng thái thiếu `REPLICA IDENTITY FULL`, hoặc `02-silver.sql` chưa sinh lại. CI chạy nó khi thay đổi source, warehouse hoặc debezium.

### Q11. Warehouse lưu tiền và thời gian thế nào? [Cơ bản]
Tiền là VND dạng Int64 (không số thực, không làm tròn). Thời gian là DateTime64(6, UTC); ngày kinh doanh tính theo giờ Việt Nam trên đồng hồ nghiệp vụ (`created_at`), không theo thời điểm nạp.

### Q12. PII có vào warehouse không? [Cơ bản]
Không ở dạng gốc. Debezium băm SHA-256 có salt các cột tên, email, SĐT, địa chỉ trước khi dữ liệu rời PostgreSQL. Warehouse vẫn join và đếm người mua được (băm ổn định) nhưng không thấy giá trị thật. `check_data` có kiểm PII không lọt vào warehouse.

---

## 2. MergeTree, ReplacingMergeTree, FINAL

### Q13. Part trong MergeTree là gì? [Cơ bản]
Mỗi INSERT tạo một part bất biến trên đĩa, cột lưu riêng, đã sắp xếp và nén. Không update tại chỗ; mọi thay đổi là part mới. Tiến trình nền merge các part nhỏ thành part lớn. Quá nhiều part (insert quá nhỏ, quá dày) làm chậm đọc và có thể bị ClickHouse từ chối insert.

### Q14. Kafka engine flush mỗi 1 s có tạo quá nhiều part không? [Trung bình]
Mỗi giây có một part mới cho bronze và cho mỗi bảng silver có dữ liệu trong block đó, tức tối đa vài chục part mỗi giây. Merge nền theo kịp ở tải hiện tại; dự án giám sát số part mỗi partition qua `system.parts` và có alert. Nếu tải tăng nhiều, tăng thời gian flush hoặc kích thước block là cách giảm số part, đổi lại tăng độ trễ.

### Q15. ORDER BY trong MergeTree có vai trò gì? [Cơ bản]
Nó quyết định thứ tự dữ liệu trong part và tạo sparse primary index (mỗi granule một mốc) để bỏ qua block khi lọc theo tiền tố của khoá. Với ReplacingMergeTree, ORDER BY còn là khoá khử trùng. Ví dụ bronze xếp theo (bảng nguồn, event_ts, …) nên truy vấn "bảng orders trong 35 phút qua" chỉ đọc một vùng nhỏ.

### Q16. Partition khác ORDER BY thế nào? [Cơ bản]
Partition chia part thành nhóm độc lập (thường theo thời gian); merge không bao giờ gộp part của hai partition, và TTL/move/drop làm theo partition. ORDER BY là thứ tự trong part. Partition nên thô (ngày, tháng), không dùng để lọc chi tiết.

### Q17. Tại sao bronze partition theo ngày còn silver không partition? [Trung bình]
Bronze cần di chuyển dữ liệu cũ sang cold storage theo thời gian, partition theo ngày giúp cả ngày cũ đi cùng nhau. Silver dùng ReplacingMergeTree: khử trùng chỉ xảy ra giữa các part trong cùng partition, nên nếu partition theo thời gian thì phiên bản cũ và mới của một key nằm ở hai partition và không bao giờ gộp được. Không partition đảm bảo mọi phiên bản của một key có thể merge.

### Q18. ReplacingMergeTree hoạt động thế nào? [Cơ bản]
Khi merge, với các dòng cùng khoá ORDER BY, engine chỉ giữ một dòng: dòng có version lớn nhất (nếu có cột version), hoặc dòng chèn sau cùng (nếu không). Với cột is_deleted, nếu dòng thắng có cờ xoá thì key coi như đã xoá. Vì merge không đoán trước được, trước khi merge có thể còn nhiều dòng cho một key.

### Q19. FINAL làm gì và tốn kém ra sao? [Cơ bản]
FINAL bắt ClickHouse khử trùng ngay lúc đọc như thể mọi merge đã xong, và loại dòng có is_deleted. Nó tốn CPU và bộ nhớ hơn đọc thường vì phải gộp theo khoá qua các part. Trong dự án mọi chỗ đọc silver/snap đều dùng FINAL; snap tồn tại một phần để FINAL chạy trên cột có kiểu thay vì phải tính lại từ JSON.

### Q20. Có cách nào thay FINAL không? [Trung bình]
Có thể dùng argMax theo version với GROUP BY khoá, hoặc OPTIMIZE FINAL định kỳ (đắt, không nên chạy liên tục). Các bản ClickHouse mới đã tối ưu FINAL khá nhiều (xử lý song song, bỏ qua part không giao nhau). Dự án chọn FINAL vì đơn giản và đúng; chi phí được giảm bằng snapshot và mart.

### Q21. Tại sao không dùng CollapsingMergeTree hoặc VersionedCollapsingMergeTree? [Nâng cao]
Collapsing cần chèn dòng "hủy" (sign = −1) chứa đúng giá trị cũ, tức người ghi phải biết trạng thái trước; với CDC có `before` thì làm được nhưng rắc rối và nhạy với thứ tự. ReplacingMergeTree với version = LSN chỉ cần chèn trạng thái mới, chịu được trùng và lệch thứ tự một cách tự nhiên. Collapsing hợp hơn khi muốn aggregate tăng dần (sum theo sign) mà không cần FINAL.

### Q22. Version của silver là gì, tại sao không dùng updated_at? [Trung bình]
Version là LSN của WAL PostgreSQL. LSN tăng đơn điệu và được PostgreSQL gán, còn `updated_at` do ứng dụng ghi, có thể trùng (hai update cùng micro giây), lùi (đồng hồ) hoặc quên cập nhật. Thay đổi trên cùng một dòng được tuần tự hoá bằng row lock nên LSN phản ánh đúng thứ tự.

### Q23. Snap dùng version gì? [Trung bình]
`_v` = 2v cho dòng gỡ bỏ và 2v + 1 cho dòng thật tại watermark v, cùng cờ `_gone`. Cách mã hoá này bảo đảm trong cùng một lần chạy, dòng thật thắng dòng gỡ bỏ, và mọi dòng của lần chạy sau thắng mọi dòng của lần chạy trước.

### Q24. TTL trong MergeTree dùng được cho việc gì? [Cơ bản]
TTL có thể xoá dòng/part hết hạn hoặc di chuyển part sang disk/volume khác. Dự án dùng cả hai: bronze chuyển sang volume cold sau 30 ngày; `ops.reconciliation` xoá sau 7 ngày, `ops.mart_refreshes`, `snap.runs` sau 1 ngày, `snap.dirty` sau 1 giờ.

### Q25. Materialized view của ClickHouse khác MV của PostgreSQL thế nào? [Cơ bản]
MV của ClickHouse là trigger trên INSERT: mỗi block chèn vào bảng nguồn đi qua câu SELECT và chèn kết quả vào bảng đích; nó không đọc lại dữ liệu cũ và không "refresh". MV của PostgreSQL là kết quả truy vấn lưu lại, phải REFRESH. Vì vậy MV ClickHouse tạo sau khi đã có dữ liệu sẽ không backfill dữ liệu cũ.

### Q26. Chuỗi MV bronze → silver chạy đồng bộ hay bất đồng bộ? [Trung bình]
Đồng bộ trong cùng lần insert: block từ Kafka engine được ghi vào `bronze.cdc_events`, và chính insert đó kích hoạt các MV silver ghi tiếp. Nếu một MV ném lỗi, insert của block thất bại và Kafka engine thử lại block. Điều này giữ silver gần như cùng nhịp với bronze trên một node.

### Q27. Parameterized view là gì? [Cơ bản]
Là view có tham số kiểu `{v:UInt64}` trong định nghĩa, gọi như hàm bảng với giá trị cụ thể. Nó không lưu dữ liệu; mỗi lần gọi chạy lại truy vấn với tham số đó. Dự án dùng để truyền watermark xuống mọi view consistent và gold.

---

## 3. Kafka engine và bronze

### Q28. Kafka engine table là gì? [Cơ bản]
Là bảng đại diện cho một Kafka consumer; SELECT trực tiếp sẽ tiêu thụ message (một lần), nên thực tế luôn gắn MV để kéo liên tục vào bảng MergeTree. Nó có cột ảo như `_topic`, `_partition`, `_offset`, `_error`. Offset được commit vào Kafka theo consumer group.

### Q29. Dự án có những consumer group nào? [Cơ bản]
`clickhouse-cdc` cho mọi topic `cdc.ecommerce.*`, `clickhouse-cdc-transactions` cho `cdc.transaction`, và `clickhouse-events` cho `events.*`. Mỗi bảng Kafka engine một consumer.

### Q30. Tại sao subscribe bằng regex? [Cơ bản]
Regex `^cdc\.ecommerce\..*` và `^events\..*` bắt mọi topic hiện tại và tương lai, nên bảng mới ở nguồn được nạp vào bronze mà không phải sửa ClickHouse. Metadata topic làm mới mỗi 5 giây nên topic mới được nhận nhanh. Silver cho bảng mới vẫn cần thêm spec.

### Q31. Tại sao đọc RawBLOB thay vì format JSON? [Trung bình]
Từ R9 mỗi message có header Schema Registry 5 byte (0x00 + 4 byte schema id) trước JSON; format JSON của ClickHouse không đọc được. RawBLOB lấy cả message thành một chuỗi, MV tự bỏ 5 byte đầu khi chuỗi bắt đầu bằng byte 0. Cùng MV đọc được cả message cũ (JSON trần) và mới, nên silver trở lên không đổi.

### Q32. Tại sao không dùng format JSON Schema/Avro có sẵn của ClickHouse? [Nâng cao]
ClickHouse hỗ trợ AvroConfluent đọc header, nhưng dự án dùng JSON Schema converter (giữ JSON dễ đọc, schema tương thích ngược). Cách RawBLOB + bóc header đơn giản, không phụ thuộc registry lúc đọc, và giữ nguyên bản message để lưu lỗi. Đổi lại, ClickHouse không tận dụng schema để ép kiểu; ép kiểu nằm ở silver.

### Q33. `rawblob-migration.sh` dùng khi nào? [Trung bình]
Cho warehouse tạo trước R9, khi bảng Kafka còn đọc JSONAsString. Script xoá các bảng Kafka và MV liên quan (chỉ khi còn dạng cũ) rồi chạy lại init SQL (mọi lệnh đều IF NOT EXISTS). Offset nằm ở Kafka nên nạp tiếp đúng chỗ. Phải chạy trước khi Debezium dùng converter mới, vì JSONAsString không đọc được header.

### Q34. bronze.cdc_events lưu những gì? [Cơ bản]
Mỗi message một dòng: bảng nguồn, op (c/u/d/r), LSN, tx_id (0 cho snapshot), thời điểm commit nguồn, thời điểm Debezium xử lý, before/after dạng JSON thô, topic/partition/offset Kafka và thời điểm nạp. MergeTree append-only, partition theo ngày, giữ mãi mãi.

### Q35. Tại sao bronze giữ before/after dạng JSON thô thay vì có kiểu? [Trung bình]
Bronze là bản ghi gốc, không phụ thuộc schema từng bảng, nên thêm bảng hay đổi cột không phải sửa bronze. JSON thô cho phép dựng lại silver nếu logic ép kiểu sai, và cho phép consistent lấy ảnh `before` khi cần khôi phục. Đổi lại, đọc bronze tốn parse JSON; đó là lý do các truy vấn trên bronze luôn giới hạn theo bảng và thời gian.

### Q36. Message lỗi được xử lý thế nào? [Cơ bản]
Bảng Kafka dùng chế độ lỗi `stream`, nên message không đọc được không dừng consumer mà có `_error`. Một MV thứ hai bắt message có `_error` hoặc không phải JSON hợp lệ và lưu nguyên văn kèm topic/partition/offset vào `bronze.ingest_errors`. `check_data` đòi bảng này rỗng, exporter đếm và có alert.

### Q37. Có message nào bị bỏ qua mà không vào bảng lỗi không? [Nâng cao]
Có: JSON hợp lệ nhưng không có trường `op` (với cdc_events) hoặc không có `eventId` (với domain_events) không vào bảng dữ liệu, cũng không vào bảng lỗi, vì MV lỗi chỉ bắt JSON không hợp lệ. Với `cdc.transaction`, bản ghi BEGIN cố ý bị bỏ, chỉ giữ END. Thực tế Debezium luôn gửi `op`, và tombstone (message null) đã bị tắt ở connector.

### Q38. Tại sao tắt tombstone ở Debezium? [Trung bình]
Tombstone là message value null sau mỗi delete, phục vụ log compaction. Ở đây không topic nào compacted, và message null sẽ làm ClickHouse dừng nạp. Delete đã có đủ thông tin trong event `d` với ảnh `before`.

### Q39. bronze.cdc_transactions chứa gì và tính "expected" ra sao? [Trung bình]
Mỗi giao dịch commit một dòng từ bản ghi END: tx_id, event_count, `expected` (số sự kiện sẽ vào `cdc_events` = event_count trừ sự kiện của bảng outbox), `outbox_changes`, commit_ts và offset trên topic. Outbox bị trừ vì Outbox Event Router chuyển nó sang `events.*`, không vào `cdc_events`. Ví dụ giao dịch 1 order + 3 item + 1 inventory + 1 domain event: event_count 7, expected 5, outbox_changes 2.

### Q40. Tại sao topic cdc.transaction chỉ có 1 partition là quan trọng? [Trung bình]
Một partition giữ thứ tự tuyệt đối, nên offset của các bản ghi END chính là thứ tự commit. Watermark dựa vào đó: "mọi giao dịch có offset nhỏ hơn v đã đủ" chỉ có nghĩa khi offset là thứ tự toàn cục. Nhiều partition sẽ chỉ có thứ tự trong từng partition.

### Q41. bronze.domain_events dùng để làm gì? [Cơ bản]
Lưu domain event từ outbox (`events.<aggregate>`) với event_id, loại event, aggregate, payload, thời điểm xảy ra. ORDER BY theo aggregate để tra lịch sử một aggregate nhanh. `check_data` kiểm số event đã commit (từ `outbox_changes / 2`) bằng số event_id duy nhất đã về.

### Q42. Tại sao không bao giờ được xoá dòng bronze? [Trung bình]
Watermark đếm sự kiện của từng giao dịch trong bronze. Xoá dòng làm giao dịch đó vĩnh viễn thiếu sự kiện, nên nó và mọi giao dịch sau bị giữ lại cho tới khi END của nó ra khỏi cửa sổ 60 phút. Dự án gặp thật khi xoá dòng bronze của bảng probe dùng để thử Schema Registry. Ngoài ra bronze là nguồn audit/replay, mất là mất vĩnh viễn.

### Q43. `auto_offset_reset = earliest` có ý nghĩa gì ở đây? [Cơ bản]
Consumer group mới (warehouse mới dựng) bắt đầu từ đầu topic, nên nạp được cả snapshot ban đầu của Debezium và mọi thứ Kafka còn giữ. Group đã có offset thì tiếp tục từ offset đã commit.

---

## 4. Silver

### Q44. Silver bảng trạng thái được thiết kế thế nào? [Cơ bản]
ReplacingMergeTree với version `_version` = LSN và cờ `_is_deleted`, ORDER BY primary key của bảng nguồn. Cột nghiệp vụ có kiểu, cộng `_synced_at`, `_tx_id`, `_before` (ảnh trước, nén ZSTD). MV lấy ảnh `before` cho delete và `after` cho các op khác.

### Q45. Delete được xử lý ở silver như thế nào? [Cơ bản]
Event delete tạo một phiên bản mới với `_is_deleted = 1`, dựng từ ảnh `before` (nhờ REPLICA IDENTITY FULL nên mọi cột có giá trị). Vì LSN của delete lớn nhất, nó thắng; FINAL không trả key đó. Merge sau đó có thể dọn hẳn.

### Q46. Tại sao silver giữ cột `_before`? [Trung bình]
Consistent view cần trạng thái trước của một key khi phiên bản mới nhất thuộc giao dịch chưa visible. Thường lấy từ bronze, nhưng có lúc silver đã có phiên bản mới mà lần đọc bronze (trước đó vài mili giây) chưa thấy; khi đó `_before` của chính phiên bản là nguồn dự phòng. Nén ZSTD vì là JSON dài và ít đọc.

### Q47. `_tx_id` và `_synced_at` dùng vào đâu? [Trung bình]
`_tx_id` để consistent quyết định phiên bản có thuộc giao dịch đã visible không. `_synced_at` (thời điểm nạp) dùng cho quy tắc tuổi: dòng nạp hơn 30 phút coi như visible mà không cần kiểm, để tập giao dịch phải kiểm luôn nhỏ.

### Q48. Bảng sự kiện ở silver khác bảng trạng thái thế nào? [Trung bình]
Chỉ nhận op c/r, ReplacingMergeTree không version, ORDER BY (khoá cha, id), ví dụ (order_id, id). Một sự kiện không bao giờ đổi nên không cần version; Replacing chỉ để gộp bản giao lại. Khoá cha đứng đầu giúp đọc mọi sự kiện của một cha nhanh.

### Q49. Silver chịu được event đến lệch thứ tự không? [Trung bình]
Trong cùng một dòng thì có: version là LSN, nên update cũ đến sau vẫn thua update mới khi merge/FINAL. Giữa các bảng thì silver không xử lý; đó là việc của watermark.

### Q50. REPLICA IDENTITY FULL là gì và tại sao cần? [Trung bình]
Mặc định PostgreSQL chỉ ghi khoá chính vào WAL cho ảnh cũ của update/delete. FULL ghi cả dòng cũ, nên Debezium gửi `before` đầy đủ. Dự án cần nó cho bảng trạng thái: delete ở silver cần đủ cột, và consistent cần ảnh trước để khôi phục. Bảng append-only dùng DEFAULT vì không bị sửa. Contract check bắt lỗi nếu bảng trạng thái thiếu FULL.

### Q51. Có bao nhiêu MV trên `bronze.cdc_events`? Có vấn đề gì không? [Trung bình]
31 MV, mỗi bảng một, lọc theo `source_table`. Mỗi block chèn vào bronze phải qua cả 31 câu lọc; với tải hiện tại chi phí nhỏ. Ở quy mô lớn hơn có thể tách topic theo nhóm hoặc dùng một Kafka engine cho mỗi nhóm bảng để giảm công việc lặp.

### Q52. Kiểu dữ liệu ở silver được chọn thế nào? [Cơ bản]
Theo mã trong spec: UUID, String, LowCardinality(String) cho cột ít giá trị khác nhau (status, province…), Int64 cho tiền, Int32 cho số nhỏ, Bool, DateTime64(6, UTC); thêm `?` thành Nullable. LowCardinality dùng dictionary encoding nên nén tốt và group by nhanh.

### Q53. Silver có khớp với PostgreSQL không, kiểm thế nào? [Trung bình]
`check_data` (khi simulator dừng) so silver FINAL với PostgreSQL cho 31 bảng bằng số dòng và tổng/đếm một số cột, chờ tối đa vài phút cho pipeline lắng. Trong lúc chạy, warehouse-jobs đối soát 28 bảng mỗi 30 s trên snapshot với cutoff một giờ kinh doanh.

### Q54. Thêm một cột mới ở nguồn thì silver thế nào? [Nâng cao]
MV silver chỉ trích các cột trong spec, nên cột mới bị bỏ qua (vẫn nằm trong JSON ở bronze). Contract check fail cho tới khi spec cập nhật và SQL sinh lại. Vì bảng tạo bằng IF NOT EXISTS, volume cũ không tự có cột mới: cần ALTER bảng và tạo lại MV, hoặc rebuild warehouse. Dữ liệu cũ của cột có thể backfill từ bronze.

### Q55. Đổi kiểu một cột ở nguồn thì sao? [Nâng cao]
Schema Registry với BACKWARD compatibility từ chối thay đổi không tương thích; Debezium task fail và alert `ConnectorNotRunning` bắn, thay vì đẩy dữ liệu sai. Nếu thay đổi là chủ ý, cập nhật spec silver trước, rồi nới compatibility riêng cho subject đó và restart task. Chi tiết ở [03](../platform/03-cdc-ingestion.md).

### Q56. Op `r` là gì và silver xử lý thế nào? [Cơ bản]
`r` là dòng đọc từ snapshot ban đầu của Debezium. Silver coi nó như insert; `tx_id` được đặt 0 ở bronze vì không thuộc giao dịch streaming nào. Consistent luôn coi dòng `tx_id = 0` là visible.

---

## 5. Watermark và consistent view

### Q57. Watermark v là gì? [Cơ bản]
Là vị trí (offset) trên topic `cdc.transaction` của giao dịch đầu tiên còn thiếu sự kiện trong bronze. Mọi giao dịch có vị trí nhỏ hơn v đã về đầy đủ. Lấy bằng một SELECT rẻ trên `consistent.watermark`.

### Q58. "Complete up to v" nghĩa chính xác là gì? [Trung bình]
Tiền tố commit [.., v) đã đủ: với mọi giao dịch có END ở offset nhỏ hơn v, số sự kiện khác nhau trong bronze bằng `expected`. Không có nghĩa là không có gì sau v đã về; sự kiện của giao dịch sau v có thể đã nằm trong silver nhưng bị ẩn.

### Q59. Tại sao v là offset chứ không phải tx_id? [Trung bình]
tx_id của PostgreSQL được cấp khi giao dịch bắt đầu, nên giao dịch có id nhỏ có thể commit sau giao dịch id lớn. Thứ tự hiển thị phải là thứ tự commit. Offset END trên topic một partition đúng là thứ tự commit.

### Q60. Watermark được tính thế nào? [Trung bình]
Lấy các END nạp trong 60 phút qua (gom theo tx_id), đếm số cặp (bảng, LSN) khác nhau của mỗi giao dịch trong bronze (3 giờ qua), so với `expected`. Nếu có giao dịch thiếu, v = offset nhỏ nhất trong các giao dịch thiếu; nếu không, v = offset lớn nhất + 1. Kèm `held_back` và `oldest_held_back_commit` để chẩn đoán.

### Q61. Tại sao đếm (bảng, LSN) duy nhất thay vì count()? [Trung bình]
Kafka giao at-least-once nên bronze có thể có bản trùng; kết quả chỉ effectively-once khi lớp đọc khử trùng. count() sẽ đếm dư và có thể cho rằng giao dịch đủ khi thực ra đang thiếu một sự kiện khác. Đếm cặp duy nhất khiến bản trùng không ảnh hưởng.

### Q62. Kể ví dụ về giao dịch rách (torn transaction). [Cơ bản]
Một giao dịch tạo order và 3 order_items. Event order ở topic orders, item ở topic order_items, đến ClickHouse ở thời điểm khác nhau. Trong khoảng đó silver thấy order mà chưa đủ item; một dashboard tính "giá trị trung bình mỗi order" từ item sẽ sai. Với consistent, cả order và 3 item chỉ hiện ra cùng lúc khi đủ 4/4 sự kiện.

### Q63. Nếu END về trước sự kiện dữ liệu thì sao? Ngược lại thì sao? [Trung bình]
END về trước: giao dịch có trong danh sách ended nhưng landed < expected, nên v dừng ở nó và mọi dòng của nó bị ẩn. Sự kiện về trước END: giao dịch chưa có END nên không thuộc tập visible, dòng của nó cũng bị ẩn. Hai thứ tự đều an toàn.

### Q64. Điều gì làm watermark đứng? [Trung bình]
Một giao dịch có END mà thiếu sự kiện: do một topic trễ, message rơi vào `ingest_errors`, hoặc dòng bronze bị xoá. Mọi giao dịch sau bị giữ lại theo, kể cả trên bảng khác, vì chỉ hiện tiền tố đầy đủ. `held_back` tăng và có alert watermark stalled.

### Q65. Watermark có bị kẹt vĩnh viễn không? [Nâng cao]
Không. END chỉ được xét trong 60 phút, nên một giao dịch kẹt lâu hơn rơi khỏi danh sách và không chặn nữa; dòng silver nạp hơn 30 phút cũng được hiện không cần kiểm. Đây là trade-off có chủ đích: sau 30–60 phút chấp nhận khả năng đọc không nhất quán thay vì dừng dashboard mãi mãi.

### Q66. Tại sao không tính v bên trong mỗi view? [Nâng cao]
ClickHouse đọc mỗi bảng/subquery ở thời điểm riêng, nên hai view cùng tính "v mới nhất" trong một truy vấn có thể ra hai giá trị khác nhau và bất đồng. Phiên bản đầu làm vậy và probe bắt được lệch. Một số v cố định truyền từ ngoài là điều duy nhất mọi view chọn giống hệt.

### Q67. Người đọc dùng watermark thế nào? [Cơ bản]
Lấy v một lần, rồi truyền cùng v cho mọi view trong truy vấn hoặc trong cả một trang. warehouse-jobs lấy một v cho mỗi vòng; probe lấy một v cho mỗi lần poll; Grafana không cần v vì đọc mart đã gắn `refresh_id = v`.

### Q68. Consistent view của bảng sự kiện làm gì? [Cơ bản]
Silver FINAL, bỏ các dòng không visible (giao dịch chưa có END trước v, không phải snapshot, nạp chưa quá 30 phút). Sự kiện chỉ thêm, nên "tại v" đơn giản là "những sự kiện đã xảy ra trước v".

### Q69. Consistent view của bảng trạng thái khó hơn ở chỗ nào? [Nâng cao]
Silver chỉ giữ phiên bản mới nhất, phiên bản cũ có thể đã bị merge vứt. Nếu phiên bản mới nhất thuộc giao dịch sau v, phải dựng lại trạng thái trước nó. View lấy từ bronze ảnh `before` của thay đổi sớm nhất chưa visible của key đó; ảnh này chính là trạng thái tại v.

### Q70. Tại sao ảnh before của thay đổi sớm nhất chưa visible là trạng thái tại v? [Nâng cao]
Visible là một tiền tố theo thứ tự commit. Trên cùng một dòng, các thay đổi được tuần tự hoá bằng row lock, nên giao dịch commit sau có LSN lớn hơn trên dòng đó. Vậy mọi thay đổi trước thay đổi chưa visible sớm nhất đều visible, và ảnh before của nó là kết quả của tất cả chúng.

### Q71. Nếu ảnh before là null thì nghĩa là gì? [Trung bình]
Thay đổi chưa visible sớm nhất là INSERT: tại v key chưa tồn tại, nên view ẩn key. Ví dụ order mới tạo trong giao dịch chưa đủ sự kiện.

### Q72. Delete chưa visible thì consistent hiện gì? [Trung bình]
Silver FINAL đã không trả key (phiên bản mới nhất là xoá). Nhánh restore từ bronze tìm thấy delete chưa visible và trả ảnh before, nên key vẫn hiện như trước khi xoá. Khi giao dịch xoá visible, key biến mất.

### Q73. Tại sao nhánh silver và nhánh restore được FULL OUTER JOIN chứ không dùng CTE hai lần? [Nâng cao]
ClickHouse tính lại CTE mỗi lần được tham chiếu, ở thời điểm khác nhau, nên dữ liệu có thể khác giữa hai lần và một key xuất hiện ở cả hai phía hoặc không phía nào. Đọc mỗi nhánh đúng một lần rồi join theo key loại được lỗi này.

### Q74. Tại sao nhánh restore chỉ nhìn 35 phút bronze? [Trung bình]
Để truy vấn rẻ: bronze ORDER BY (bảng, event_ts) nên lọc theo thời gian cắt được phần lớn dữ liệu. 35 phút đủ phủ quy tắc tuổi 30 phút (dòng cũ hơn coi như visible) cộng độ lệch. Tập giao dịch visible lấy END trong 90 phút vì cùng lý do.

### Q75. Nếu silver có phiên bản mới mà bronze (đọc trước vài ms) chưa thấy thì sao? [Nâng cao]
Phiên bản đó không visible và nhánh restore không có key. View khi đó dùng `_before` của chính phiên bản silver: nếu là dòng thì hiện dòng cũ, nếu null (insert) thì ẩn. Đây là lý do silver giữ `_before`.

### Q76. Consistent chậm hơn silver bao nhiêu? [Cơ bản]
Thường dưới một giây ở tải bình thường: chỉ là thời gian để mọi topic của một giao dịch về đủ. Khi một topic trễ, khoảng này tăng theo topic chậm nhất.

### Q77. Probe tính nhất quán kiểm những gì? [Trung bình]
Chạy trong lúc simulator ghi, mỗi poll lấy một v rồi kiểm 6 bất biến qua silver FINAL và consistent: order hoàn tất có bút toán SALE, item có order, order có checkout, payout có bút toán PAYOUT, tồn kho bằng tổng sổ kho, voucher usage có checkout. Silver được phép vi phạm tạm thời; consistent vi phạm một lần là FAIL. Gate chạy probe ở mọi chế độ.

### Q78. Consistent có phải là snapshot isolation thật không? [Nâng cao]
Gần như: kết quả là một trạng thái đã tồn tại ở PostgreSQL tại một điểm commit. Nhưng nó dựa trên các giả định có cửa sổ thời gian (30/35/60/90 phút) và trên việc silver ghi cùng chuỗi insert với bronze. Ngoài các cửa sổ đó (giao dịch kẹt rất lâu) thì đảm bảo bị nới.

---

## 6. Gold, mart, ops

### Q79. Gold view được tổ chức thế nào? [Cơ bản]
Parameterized view nhận v và đọc `consistent.*(v)`, đôi khi đọc gold khác (ví dụ `daily_sales` đọc `order_facts`). Định nghĩa metric đặt ở một chỗ đầu file gold. Logic từng metric ở [05](../platform/05-mart-va-metric.md).

### Q80. View gold nào không có tham số v, tại sao? [Trung bình]
`gold.cdc_freshness` đọc bronze trực tiếp để đo lag commit → ClickHouse theo bảng trong 15 phút qua. Đây là metric hạ tầng về tốc độ nạp, không cần nhất quán giao dịch.

### Q81. Mart là gì và tại sao cần? [Cơ bản]
`mart.<view>` là kết quả gold tính sẵn tại một watermark, có `refresh_id = v`. Dashboard đọc bảng nhỏ này thay vì tính gold cho mỗi người xem, nên chi phí không tăng theo số viewer.

### Q82. Làm sao Grafana không bao giờ đọc phải refresh viết dở? [Trung bình]
Mỗi refresh ghi đủ 18 mart rồi mới ghi một dòng vào `ops.mart_refreshes` (bước cuối). Grafana lấy `max(refresh_id)` từ bảng này và đọc mọi mart tại refresh đó. Refresh hỏng giữa chừng chưa có dòng đó nên vô hình, và bị xoá trước khi ghi lại.

### Q83. `ops.reconciliation_status` đánh dấu failing khi nào? [Trung bình]
Khi hai lần đối soát gần nhất của một bảng đều lệch. Một lần lệch có thể do dòng bị xoá ở PostgreSQL giữa hai lần đọc; hai lần liên tiếp là trôi dữ liệu thật và job ghi log lỗi.

### Q84. Đối soát so sánh cái gì? [Trung bình]
Với 28 bảng, số dòng và tổng một cột không đổi sau insert (ví dụ `total` của orders) cho các dòng tạo trước cutoff = created_at mới nhất trừ một giờ kinh doanh. Cutoff lùi để cả hai phía chắc chắn đã có các dòng đó. `cart_items` bị bỏ vì bị xoá liên tục, `check_data` so nó lúc nghỉ.

### Q85. `gold.daily_sales_closed` khác `gold.daily_sales` thế nào? [Cơ bản]
`daily_sales(v)` sống, thay đổi theo huỷ và trả hàng. `daily_sales_closed` là bảng chốt mỗi ngày đã kết thúc một lần (sau một giờ kinh doanh, khi không còn order chờ thanh toán của ngày đó), giữ "số đã báo cáo". Do warehouse-jobs ghi tại một v.

### Q86. Tại sao chốt ngày không dùng refreshable materialized view của ClickHouse? [Nâng cao]
Refreshable MV chạy truy vấn riêng, không giữ được một watermark chung với các phần khác. warehouse-jobs lấy một v và dùng nó cho snapshot, mart và chốt ngày trong cùng vòng, nên các con số khớp nhau.

---

## 7. Incremental snapshot (R20)

### Q87. Vấn đề nào khiến cần R20? [Trung bình]
Tính 18 mart trực tiếp từ consistent nghĩa là mỗi mart đọc silver FINAL, quét bronze, parse JSON, join — khoảng 4.6 s ở 35k order và tăng theo lịch sử. Giữa hai lần refresh chỉ vài trăm key đổi, nên phần lớn công việc lặp lại.

### Q88. Ý tưởng chính của incremental snapshot? [Cơ bản]
Giữ `snap.<table>` = `consistent.<table>(v)` dưới dạng bảng vật lý có kiểu, và mỗi lần chỉ cập nhật các key bị ghi bởi giao dịch vừa visible. Gold đọc snap qua `gold_snap.*` nên chỉ quét cột có kiểu.

### Q89. snap.changed_keys làm gì? [Trung bình]
Với (prev, v, since), trả các cặp (bảng, key) được ghi bởi giao dịch có vị trí trong [prev, v), cộng dòng snapshot (tx 0) nạp từ `since`. Bảng sự kiện trả khoá cha (cột đầu), ví dụ `order_id`. `since` chỉ để giới hạn vùng quét bronze theo thời gian.

### Q90. `since` được chọn thế nào và tại sao trừ 2 phút? [Nâng cao]
Là giá trị nhỏ hơn giữa thời điểm commit sớm nhất của các giao dịch mới và thời điểm chạy trước, trừ 2 phút. Bronze lọc theo `event_ts` (thời điểm Debezium xử lý), vốn không trùng khít với commit_ts hay giờ ClickHouse; 2 phút là đệm cho độ lệch đó để không bỏ sót sự kiện.

### Q91. Giải thích _v = 2v và 2v + 1. [Trung bình]
Mỗi lần chạy delta tại v, với mỗi key bẩn của bảng trạng thái, chèn dòng gỡ bỏ (_v = 2v, _gone = 1) rồi chèn dòng thật tại v (_v = 2v + 1) nếu key còn tồn tại. FINAL giữ _v lớn nhất: key còn tồn tại thì dòng thật thắng; key đã mất thì dòng gỡ bỏ thắng và bị ẩn. Mọi dòng từ lần trước có _v ≤ 2v' + 1 < 2v nên thua.

### Q92. Tại sao cần dòng gỡ bỏ? [Trung bình]
Delta view chỉ trả key tồn tại tại v; nó không thể nói "key này đã biến mất". Gỡ trước mọi key bẩn rồi chèn lại những key còn tồn tại xử lý đồng thời cả xoá, sửa, và insert hoá ra chưa visible.

### Q93. Bảng sự kiện trong snap xử lý thế nào? [Trung bình]
Không có dòng gỡ bỏ, vì sự kiện chỉ thêm. Key bẩn là khoá cha; delta đọc lại mọi sự kiện visible của cha đó và chèn với _v = 2v + 1, thay bản cũ cùng (cha, id).

### Q94. Đi qua ví dụ một lần delta. [Nâng cao]
prev = 5000, v = 5003, ba giao dịch: tạo order A + 3 item; đổi trạng thái order B (thêm một dòng lịch sử); xoá cart_item C. Key bẩn: orders {A, B}, order_items {3 item}, order_status_history {A, B}, cart_items {C}. Ghi dòng gỡ bỏ _v = 10006 cho A, B, 3 item, C; ghi dòng thật _v = 10007 cho A, B, 3 item và mọi lịch sử của A, B; C không có dòng thật. FINAL cho kết quả bằng `consistent.*(5003)`, với C bị ẩn.

### Q95. Khi nào full rebuild? [Cơ bản]
Lúc khởi động, sau bất kỳ lỗi nào trong vòng lặp, mỗi giờ (`SNAP_REBUILD_S`), và khi v mới nhỏ hơn v của snapshot. Full = TRUNCATE rồi chép toàn bộ `consistent.<table>(v)` với _v = 2v + 1.

### Q96. Tại sao vẫn cần full rebuild hằng giờ khi đã có delta? [Nâng cao]
Là lưới an toàn cho những thay đổi delta không thấy. Ví dụ một giao dịch kẹt quá 30 phút trở nên visible nhờ quy tắc tuổi chứ không nhờ watermark đi qua nó, nên changed_keys (lọc theo vị trí giao dịch) không bắt được key của nó. Full rebuild cũng sửa mọi sai lệch tích luỹ chưa biết.

### Q97. Delta view khác consistent view ở đâu? [Trung bình]
Cùng logic nhưng chỉ cho các key trong `snap.dirty` tại v, và chỉ trả cột nghiệp vụ (không cột meta). Cả hai được sinh từ cùng một hàm trong `gen_silver.py`, nên không lệch.

### Q98. Lỗi CANNOT_PARSE_UUID trong delta view là gì? [Nâng cao]
Nhánh restore đọc bronze của một bảng và lọc theo key bẩn. Optimizer đẩy điều kiện "key thuộc tập bẩn" xuống trước điều kiện "đúng bảng", nên phép ép chuỗi → UUID chạy trên dòng của bảng khác (ví dụ inventory không có trường `id`) và ném lỗi. Sửa bằng cách lọc key ở HAVING sau GROUP BY (không bị đẩy xuống dưới bộ lọc bảng) và dùng phép ép kiểu trả 0 khi lỗi trong delta view.

### Q99. gold_snap được tạo thế nào? [Trung bình]
Lúc khởi động, warehouse-jobs đọc DDL các gold view, viết lại mỗi lần đọc `consistent.X(v)` thành `snap.X FINAL` (giữ alias, đặt trước FINAL) và `gold.Y(v)` thành `gold_snap.Y`, rồi tạo theo thứ tự phụ thuộc, có kiểm tra vòng. View vẫn còn tham số v sau khi viết lại thì bỏ qua. Logic metric chỉ viết một lần.

### Q100. Lỗi "alias trước FINAL" là gì? [Trung bình]
Cú pháp ClickHouse là "bảng AS alias FINAL"; viết "bảng FINAL AS alias" bị từ chối. Phép viết lại gold → gold_snap ban đầu đặt FINAL sai chỗ, nên phải chuyển alias lên trước.

### Q101. Làm sao biết snapshot đúng? [Trung bình]
`check_data` lấy v của lần chạy snapshot gần nhất, so mỗi `snap.<table> FINAL` với `consistent.<table>(v)` bằng số dòng và tổng hash của mọi cột; và so mỗi mart với gold tính trực tiếp. `snap.runs` ghi mode, số key, thời lượng; Grafana có panel thời lượng và số key mỗi lần delta.

---

## 8. warehouse-jobs

### Q102. warehouse-jobs làm những việc gì? [Cơ bản]
Bốn việc, mỗi vòng ở một watermark: đưa snapshot tới v, refresh mart, chốt ngày kinh doanh, đối soát PostgreSQL ↔ ClickHouse; thêm dọn refresh mart cũ mỗi giờ. Viết bằng Python, dùng clickhouse-connect và psycopg2, xuất metric Prometheus ở cổng 9109.

### Q103. Các chu kỳ là bao nhiêu? [Cơ bản]
Vòng lặp thức dậy mỗi 5 s (`JOB_INTERVAL_S`), mart mỗi 15 s (`MART_REFRESH_S`), đối soát mỗi 30 s (`RECONCILE_S` trong compose; mặc định code là 60), full rebuild snapshot mỗi 3600 s. Chốt ngày chạy cùng nhịp mart.

### Q104. Thứ tự trong một vòng và tại sao? [Trung bình]
Lấy v, rồi snapshot tới v, rồi mart và chốt ngày (nếu tới hạn), rồi đối soát (nếu tới hạn). Snapshot phải đi đầu vì cả ba việc sau đều đọc snapshot. Nếu không việc nào tới hạn, vòng chỉ ngủ.

### Q105. Khi job gặp lỗi thì sao? [Trung bình]
Mọi exception được bắt: tăng counter lỗi, ghi log, bỏ kết nối và xoá trạng thái snapshot. Vòng sau kết nối lại và full rebuild, nên một lần chạy dở luôn được theo sau bằng bản đầy đủ. Đây là cách xử lý "ClickHouse/PostgreSQL chưa lên hoặc đang restart".

### Q106. Refresh mart có idempotent không? [Trung bình]
Có. Nếu refresh mới nhất đã ở v và mart đã có dữ liệu, bỏ qua. Nếu mart đã có dòng refresh_id = v từ lần hỏng trước, xoá chúng (mutation đồng bộ) rồi ghi lại. Việc công bố chỉ xảy ra khi ghi `ops.mart_refreshes`.

### Q107. Có chạy nhiều replica warehouse-jobs được không? [Nâng cao]
Không nên: hai bản sẽ cùng truncate/ghi snapshot và mart, gây trùng hoặc xoá lẫn nhau. Muốn HA cho job cần leader election hoặc lock (ví dụ một bảng lease), hoặc để orchestrator đảm bảo đúng một instance.

### Q108. Tại sao dùng một service Python thay vì Airflow? [Nâng cao]
Chu kỳ 5–15 giây quá dày cho Airflow (scheduler và overhead mỗi task tính bằng giây). Việc cần giữ một v cho cả vòng và trạng thái snapshot trong bộ nhớ hợp với một vòng lặp đơn giản. Airflow hợp hơn cho batch hằng giờ/ngày có nhiều phụ thuộc và backfill.

---

## 9. Storage, bảo mật, quyền

### Q109. Tiered storage hoạt động thế nào ở đây? [Trung bình]
Policy `tiered` có volume `default` (đĩa local) và `cold` (disk S3 trên SeaweedFS, bucket `bronze-cold`). Ba bảng bronze dùng policy này với TTL chuyển part sang cold khi mọi dòng trong part đủ 30 ngày. Truy vấn đọc cả hai volume trong suốt.

### Q110. Tại sao volume nóng phải tên `default`? [Nâng cao]
Đổi storage policy của bảng đã có dữ liệu, ClickHouse yêu cầu policy mới chứa mọi volume/disk của policy cũ với cùng tên. Policy mặc định có volume `default` trên disk `default`, nên volume nóng của `tiered` phải giữ tên đó; đặt tên khác thì lệnh bị từ chối.

### Q111. Tại sao chọn SeaweedFS thay vì MinIO? [Cơ bản]
Cần object store tương thích S3 chạy local thay cho S3/GCS. Image MinIO không còn được phát hành công khai, nên dự án chọn SeaweedFS 4.47: một process gồm master, volume, filer và S3 gateway, chỉ trong mạng nội bộ.

### Q112. Khoá S3 được truyền vào ClickHouse thế nào? [Trung bình]
Khoá là file trong `secrets/`, mount read-only. Entrypoint container đọc file, export thành biến môi trường chỉ trong process, rồi chạy entrypoint gốc; cấu hình disk đọc khoá từ biến môi trường. Khoá không nằm trong `.env`, compose hay `docker inspect`. SeaweedFS đọc cùng file để tạo identity.

### Q113. Tại sao chỉ bronze ra cold mà không phải silver? [Trung bình]
Bronze tăng mãi và phần cũ ít khi đọc (audit, replay). Silver là trạng thái hiện tại, kích thước theo số dòng nguồn chứ không theo lịch sử, và được đọc liên tục bởi consistent/snapshot; đặt nó trên S3 sẽ chậm.

### Q114. Truy vấn chạm dữ liệu cold có vấn đề gì? [Trung bình]
Chậm hơn vì đọc qua mạng từ object store, và tốn request S3. Các view nóng (watermark, consistent, changed_keys) chỉ đọc bronze vài phút đến vài giờ gần nhất nên không chạm cold. Truy vấn lịch sử dài (audit một order cũ) mới chạm.

### Q115. Có những user ClickHouse nào? [Cơ bản]
`admin` (toàn quyền, dùng bởi init, warehouse-jobs, Control Center, validator); `grafana_reader` (chỉ SELECT `mart.*` và `ops.mart_refreshes`, readonly); `monitor_reader` (SELECT `system.*`, bronze, silver, consistent, ops, readonly, dùng bởi platform-exporter). Mật khẩu từ file secret.

### Q116. Tại sao Grafana chỉ được đọc mart? [Trung bình]
Nguyên tắc least privilege và bảo vệ hiệu năng: một panel viết sai không thể chạy truy vấn nặng trên silver/consistent hay đọc dữ liệu thô. Dashboard chỉ cần kết quả đã tính sẵn.

### Q117. `readonly = 1` với `max_execution_time CHANGEABLE_IN_READONLY` để làm gì? [Nâng cao]
readonly = 1 cấm ghi và cấm đổi setting. Plugin Grafana gửi setting `max_execution_time` theo truy vấn; nếu không cho phép đổi riêng setting này, truy vấn của Grafana sẽ bị từ chối.

### Q118. Cổng ClickHouse có mở ra ngoài không? [Cơ bản]
8123 (HTTP) và 9000 (native) chỉ bind 127.0.0.1 trên host; trong mạng compose các service gọi bằng hostname `clickhouse`. SeaweedFS không publish cổng.

---

## 10. Sự cố và edge case

### Q119. Cùng một sự kiện CDC đến hai lần thì sao? [Trung bình]
Đây là hệ quả của giao at-least-once; hệ thống đạt effectively-once nhờ mọi lớp đọc khử trùng. Bronze lưu hai dòng (không khử trùng, `check_data` chỉ cảnh báo). Silver: cùng LSN, cùng nội dung nên FINAL giữ một. Watermark đếm cặp (bảng, LSN) duy nhất nên không đếm dư. Snapshot: changed_keys DISTINCT, FINAL khử bản trùng. Số đếm đọc thẳng bronze như `cdc_freshness` bị thổi phồng.

### Q120. ClickHouse restart giữa lúc đang nạp một batch? [Trung bình]
Kafka engine commit offset sau khi ghi; nếu chết trước khi commit, block được giao lại khi khởi động lại → bản trùng ở bronze, xử lý như Q119. warehouse-jobs mất kết nối → lần sau full rebuild snapshot; refresh mart dở bị xoá và ghi lại. Gate full restart ClickHouse giữa tải và probe vẫn phải PASS.

### Q121. Sự kiện đến muộn vài phút thì sao? [Trung bình]
Nếu là sự kiện của một giao dịch đã có END, watermark đứng ở giao dịch đó; consistent vẫn đúng nhưng cũ hơn, `held_back` tăng. Khi sự kiện về, v nhảy qua cả loạt giao dịch bị giữ. Silver không bị ảnh hưởng vì version là LSN.

### Q122. Event của cùng một dòng đến lệch thứ tự? [Trung bình]
Mỗi bảng một topic một partition nên thứ tự trong một bảng được giữ; nhưng kể cả nếu lệch (ví dụ redelivery), version = LSN bảo đảm phiên bản mới thắng. Consistent cũng dùng LSN nhỏ nhất để tìm thay đổi chưa visible sớm nhất, không phụ thuộc thứ tự đến.

### Q123. Một message bị lỗi parse thì ảnh hưởng gì? [Nâng cao]
Message vào `ingest_errors`, consumer chạy tiếp. Nhưng giao dịch chứa nó thiếu một sự kiện, nên watermark đứng ở giao dịch đó cho tới khi END ra khỏi cửa sổ 60 phút. Đây là lý do có alert ingest error mới: cần xử lý (replay) nhanh.

### Q124. Một giá trị trong JSON không ép kiểu được ở silver? [Nâng cao]
Các cột bắt buộc dùng hàm ép kiểu ném lỗi (ví dụ UUID). Lỗi trong MV làm cả insert của block thất bại, Kafka engine thử lại, nên nạp dừng và lag/alert tăng — "fail loud" thay vì ghi sai. Contract check và Schema Registry giảm khả năng xảy ra. (Suy ra từ cách viết MV.)

### Q125. Debezium snapshot lại (op r) giữa chừng thì sao? [Nâng cao]
Dòng `r` có tx_id 0 nên luôn visible, và silver coi nó như insert với version là LSN đi kèm event. Vì dòng snapshot chứa trạng thái hiện tại ở PostgreSQL lúc đọc, ghi đè phiên bản cũ là đúng; nếu LSN của nó nhỏ hơn phiên bản streaming đã có thì phiên bản streaming (mới hơn) thắng. Snapshot delta bắt riêng các dòng tx 0 nạp từ `since`. Dự án chưa có kiểm thử riêng cho snapshot lại giữa chừng, nên đây là suy luận từ thiết kế.

### Q126. Xoá cứng hàng loạt ở nguồn (ví dụ cart_items)? [Trung bình]
Mỗi delete thành phiên bản `_is_deleted = 1`; FINAL ẩn, snapshot có dòng gỡ bỏ. Vì cart_items bị xoá liên tục, PostgreSQL hiện tại và ClickHouse chậm vài giây không bao giờ khớp khi có tải, nên đối soát định kỳ bỏ bảng này; `check_data` so lúc nghỉ.

### Q127. Watermark lùi (v nhỏ hơn lần trước) khi nào và xử lý ra sao? [Nâng cao]
Có thể xảy ra khi tập END trong cửa sổ thay đổi, ví dụ hệ thống nghỉ lâu hơn 60 phút (danh sách ended rỗng) hoặc END cũ được nạp lại. Snapshot phát hiện v < v cũ và full rebuild thay vì delta. Mart gắn refresh_id theo v nên refresh mới có id nhỏ hơn; người đọc lấy `max(refresh_id)` có thể vẫn chọn refresh cũ cho tới khi v vượt lại mốc cũ — một điểm cần lưu ý khi vận hành (suy ra từ code).

### Q128. Dựng lại warehouse khi Kafka còn giữ dữ liệu cũ nhiều giờ? [Nâng cao]
Suy ra từ code (chưa kiểm thử): END cũ được nạp lại với thời điểm nạp mới nên vào cửa sổ 60 phút, nhưng sự kiện có event_ts cũ hơn 3 giờ không được đếm là landed, nên watermark có thể bị giữ ở giao dịch cũ cho tới khi các END đó hết cửa sổ. Trong lúc đó consistent chỉ hiện dòng snapshot và dòng nạp hơn 30 phút. Silver vẫn đúng. Sau tối đa khoảng một giờ hệ thống tự ổn định.

### Q129. Bảng probe bị xoá dòng bronze: chuyện gì đã xảy ra? [Trung bình]
Khi thử Schema Registry, dự án tạo một bảng probe ở PostgreSQL. Xoá các dòng bronze của bảng đó để dọn dẹp làm các giao dịch chứa chúng thiếu sự kiện vĩnh viễn, watermark đứng và mọi giao dịch sau bị giữ. Bài học: không bao giờ xoá bronze; README ghi rõ.

### Q130. Nếu SeaweedFS chết thì sao? [Trung bình]
Đọc dữ liệu bronze cũ hơn 30 ngày và việc move part sang cold sẽ lỗi; dữ liệu nóng vẫn đọc/ghi bình thường. Lúc khởi động, ClickHouse phụ thuộc SeaweedFS healthy nên không lên nếu SeaweedFS không lên. Mất volume SeaweedFS có thể làm mất bronze ở cold tier; cần backup và diễn tập khôi phục cả part trên S3, không chỉ dữ liệu nóng.

### Q131. Kafka xoá dữ liệu theo retention trước khi ClickHouse đọc kịp? [Nâng cao]
Sự kiện mất vĩnh viễn với warehouse; giao dịch có END mà thiếu sự kiện sẽ chặn watermark tới khi ra khỏi cửa sổ, và silver lệch PostgreSQL (đối soát báo failing). Phải giám sát consumer lag so với retention; khi đó cần snapshot lại từ Debezium (signal) để dựng lại trạng thái.

### Q132. Hai giao dịch sửa cùng một dòng, giao dịch sau đã đủ mà giao dịch trước chưa đủ? [Nâng cao]
Watermark dừng ở giao dịch trước (giao dịch đầu tiên thiếu), nên cả hai đều chưa visible. Consistent lấy before của thay đổi sớm nhất chưa visible (của giao dịch trước) → trạng thái trước cả hai. Khi giao dịch trước đủ, v vượt qua cả hai và trạng thái mới nhất hiện ra.

---

## 11. Hiệu năng

### Q133. Lag từ PostgreSQL tới ClickHouse là bao nhiêu? [Cơ bản]
Commit → ClickHouse p95 khoảng 1.57 s ở ~990 event/s sau tuning Debezium (R10). Consistent thường chậm thêm dưới một giây. SLO freshness là p95 ≤ 5 s (≤ 30 s lúc cao điểm).

### Q134. R20 cải thiện bao nhiêu? [Cơ bản]
Refresh 18 mart từ ~4.6 s (35k order, tính từ consistent) xuống ~0.9 s cho snapshot delta dưới tải ~250 request/s cộng 0.3–1 s cho mart. Chi phí tỉ lệ số key bị đụng, không tỉ lệ lịch sử.

### Q135. Dashboard nhanh tới đâu và vì sao? [Trung bình]
p95 truy vấn dashboard khoảng 35 ms với 50–100 người xem, đo khi dashboard còn tự làm mới 30 s; hiện các dashboard nghiệp vụ làm mới 1 phút và con số này chưa được đo lại. SLO là 500 ms. Mỗi panel chỉ đọc một bảng mart nhỏ tại một refresh_id; việc tính toán làm một lần mỗi 15 s ở job, không nhân theo số viewer.

### Q136. Tại sao Grafana dùng native protocol với maxOpenConns 100? [Trung bình]
Native protocol (cổng 9000) gọn và nhanh hơn HTTP cho nhiều truy vấn nhỏ. Mặc định plugin là 50 connection mở / 25 idle, khiến dashboard bị xếp hàng khi nhiều người mở cùng lúc; tăng lên 100 bỏ được nút thắt đó.

### Q137. Phần nào sẽ chậm đầu tiên khi dữ liệu tăng? [Nâng cao]
Mart vẫn aggregate toàn bộ snapshot mỗi 15 s, nên các mart nặng (shop/product/category sales) tăng theo tổng số order. Hướng xử lý: aggregate từng phần theo ngày (chỉ tính lại ngày bị đụng) hoặc AggregatingMergeTree. Tiếp theo là FINAL trên silver lớn và số part khi tải ghi tăng.

### Q138. Tối ưu nào giúp các view quét bronze rẻ? [Trung bình]
Bronze ORDER BY (bảng nguồn, event_ts, …) nên lọc theo bảng + cửa sổ thời gian dùng được primary index; mọi truy vấn hot trên bronze đều có cả hai điều kiện (35 phút, 2–3 giờ, `since`). Partition theo ngày cũng cắt bỏ các ngày cũ.

---

## 12. Câu hỏi mở

### Q139. Nếu thiết kế lại, bạn có giữ watermark kiểu này không? [Nâng cao]
Có giữ ý tưởng (hiện tiền tố commit đầy đủ dựa trên metadata giao dịch của Debezium) vì nó cho đọc nhất quán xuyên bảng mà ClickHouse không có sẵn. Có thể cải thiện: thay cửa sổ thời gian bằng trạng thái lưu bền (bảng "đã hoàn tất tới v") để không phụ thuộc giờ nạp, và xử lý tốt trường hợp rebuild từ Kafka cũ. Một lựa chọn khác là gộp giao dịch ở stream processor (Flink) rồi ghi atomically, đổi lại thêm một hệ thống.

### Q140. Làm sao đưa hệ thống lên HA? [Nâng cao]
ClickHouse: ReplicatedMergeTree + Keeper. Khó khăn: replica sao chép part theo từng bảng độc lập, không theo thứ tự insert, nên một replica có thể có END và bronze của giao dịch mà chưa có part silver tương ứng; watermark khi đó không còn đảm bảo. Cách giải: tính "landed" trên chính silver thay vì bronze, hoặc đọc qua một replica và chờ đồng bộ, hoặc dùng insert quorum. Kafka cần nhiều broker với replication factor 3, Debezium/Connect nhiều worker. Dự án có tài liệu nghiên cứu HA riêng.

### Q141. So sánh cách làm này với lakehouse (Iceberg/Delta + Spark). [Nâng cao]
Lakehouse cho snapshot isolation thật theo bảng, time travel, tách compute/storage và mở cho nhiều engine; đổi lại cần Spark/Flink, catalog, compaction, và độ trễ thường tính bằng phút. Cách hiện tại cho độ trễ giây và vận hành nhỏ, nhưng nhất quán xuyên bảng phải tự xây và gắn với một node ClickHouse. Ở quy mô lớn có thể kết hợp: lake là bản lưu lâu dài (R21), ClickHouse là lớp phục vụ.

### Q142. Bạn sẽ thêm backfill/replay từ bronze thế nào? [Nâng cao]
Vì MV không backfill, cách đơn giản là tạo bảng silver mới, chạy INSERT … SELECT từ bronze với cùng biểu thức parse (đã có trong generator), rồi đổi tên bảng. Với dữ liệu trên cold, truy vấn vẫn đọc được trong suốt nhưng chậm. Cần làm trong lúc chặn hoặc ghi song song MV để không mất sự kiện giữa chừng.

### Q143. Kiểm thử một pipeline như thế này thế nào? [Trung bình]
Nhiều tầng: contract check trong CI (schema), probe tính nhất quán dưới tải, `check_data` so từng lớp với PostgreSQL lúc nghỉ (silver, consistent, snapshot, mart, metric), đối soát định kỳ trong production, và gate có chaos (restart Debezium, ClickHouse). Mỗi tầng bắt một loại lỗi khác nhau: sai schema, giao dịch rách, sai logic, trôi dữ liệu.

### Q144. Exactly-once trong pipeline này đạt được ở đâu? [Nâng cao]
Không ở đâu cả: không có exactly-once end-to-end, Debezium và Kafka engine giao at-least-once. Bảo đảm của dự án là **effectively-once**: giao at-least-once cộng lớp đọc khử trùng/idempotent (version = LSN ở silver, đếm duy nhất ở watermark, ReplacingMergeTree ở snap, event_id duy nhất ở domain event). Bronze cố ý giữ bản trùng.

### Q145. Nếu business muốn báo cáo "tại thời điểm 9:00 sáng qua" thì sao? [Nâng cao]
consistent chỉ nhìn được các trạng thái gần (dựa trên silver mới nhất và bronze 35 phút). Muốn time travel tuỳ ý cần dựng từ bronze: lấy phiên bản có LSN lớn nhất trước mốc đó cho mỗi key, tốn kém nhưng làm được vì bronze giữ mọi thay đổi. Với số đã chốt theo ngày thì dùng `daily_sales_closed`. Nếu nhu cầu thường xuyên, nên có bảng lịch sử (SCD type 2) hoặc lakehouse.

### Q146. Điều bạn tự hào nhất và điều còn nợ ở warehouse này? [Trung bình]
Tự hào: đọc nhất quán xuyên bảng trên ClickHouse mà không cần stream processor, được kiểm bằng probe dưới tải và chaos; incremental snapshot giảm chi phí refresh nhiều lần mà vẫn được chứng minh bằng `check_data`. Còn nợ: một node không HA, backup/restore đã có script nhưng chưa có lịch, off-site hay diễn tập catch-up tự động trong gate, silver chưa dựng lại được từ lake, các cửa sổ thời gian cố định, mart chưa incremental theo ngày.
