# 09 · Thực hành backup và restore ClickHouse

[Tài liệu dự án](../README.md) / [Vận hành](README.md)

## Backup bảo vệ điều gì?

ClickHouse có bronze lưu lịch sử, silver lưu phiên bản và các bảng phục vụ watermark, snapshot, mart, đối soát. Kafka chỉ giữ sự kiện trong retention; nếu mất ClickHouse sau khi Kafka đã xóa dữ liệu cũ thì không thể dựng lại toàn bộ lịch sử từ Kafka. Bản backup chụp dữ liệu và định nghĩa database để khôi phục warehouse.

Ở dự án này không backup PostgreSQL theo lựa chọn của người dùng: dữ liệu nguồn là mô phỏng và có thể tạo lại. Điều đó không làm backup ClickHouse tự động tương thích với một PostgreSQL/Kafka đã reset; mỗi lần reset toàn bộ tạo một lịch sử mới.

Backup nằm ở `warehouse/backups/` trên host, mount thành `/backups` trong ClickHouse. `docker compose down -v` không xóa thư mục này. Mất ổ đĩa vẫn mất cả dữ liệu và backup, vì vậy nếu muốn bảo vệ sự cố máy thật phải sao chép cả bộ backup ra một thiết bị hoặc máy khác.

## 1. Tạo full backup

Từ thư mục gốc, với stack đang chạy:

```bash
warehouse/backup.sh backup
warehouse/backup.sh list
```

Script dừng warehouse-jobs, detach bảng Kafka engine, gọi BACKUP gốc của ClickHouse, ghi vị trí replay và danh tính topic, rồi nối lại ingestion và job. API và Debezium vẫn có thể tạo sự kiện trong thời gian này; chúng chờ ở Kafka. Lag và độ mới mart tăng tạm thời.

Một bản hoàn chỉnh có bốn phần cùng tên: thư mục `ch-<UTC timestamp>/`, manifest `.json`, vị trí `.offsets`, danh tính `.topics`. Khi sao chép phải giữ đủ cả bốn; `.offsets` không phải offset của Debezium mà là điểm ClickHouse cần đọc lại ở từng topic/partition. Không sửa những file này bằng tay.

Script dùng `flock`, nên cần tiện ích này trên host Linux. Chỉ một thao tác backup/restore/prune của project được chạy cùng lúc. Nó không khóa những thao tác Docker hoặc reset từ một terminal khác: phải phối hợp người vận hành.

## 2. Tạo incremental backup

```bash
warehouse/backup.sh backup --incremental
warehouse/backup.sh list
```

Bản incremental tham chiếu full gần nhất và lưu các part cần bổ sung. Incremental không phải bản độc lập: khi sao chép hay phục hồi phải giữ cả full mà manifest ghi ở trường `base`. ClickHouse merge part có thể làm bản incremental lớn dù số dòng mới ít.

## 3. Restore và đọc bù từ cùng Kafka

**Lệnh này thay thế các database warehouse hiện tại.** Chọn đúng tên từ `list`; phối hợp với mọi người hoặc agent đang dùng stack. Nếu cần giữ trạng thái hiện tại để quay lại, tạo một full backup mới trước khi restore.

```bash
warehouse/backup.sh restore ch-YYYYMMDD-HHMMSS
```

Trước khi xóa database, script kiểm tra topic IDs còn khớp và từng offset cần replay còn nằm trong khoảng Kafka giữ. Kafka có cluster ID cấu hình cố định, nên chỉ so cluster ID không phát hiện được việc Kafka đã reset; topic ID mới cho biết topic đã bị tạo lại. Nếu retention đã xóa sự kiện cần thiết thì script từ chối trước bước xóa.

Sau khi kiểm tra, script dừng job và ingestion, thay database từ backup, đợi consumer group không còn thành viên, dời offset ClickHouse tới điểm replay đã lưu, rồi tạo lại bảng Kafka/MV và bật job. Nó không sửa source offset của Connect.

Điểm replay tính từ những offset đã lưu trong bronze. Partition chưa có dòng được ghi lại bằng offset đầu tiên còn trong Kafka lúc backup, tránh âm thầm bỏ qua sự kiện nếu retention trôi đi sau đó. Topic/partition sinh sau backup phải còn dữ liệu từ đầu; script không tự chữa được trường hợp đã mất lịch sử.

Đây không phải cam kết exactly-once cho toàn pipeline. Kafka engine có thể đọc lại block sau lỗi, và Connect có thể phát lại cùng thay đổi ở Kafka offset mới. Bản thân bronze append-only có thể có bản trùng; silver, watermark và mart cần được kiểm chứng với replay. Sau restore phải xác nhận offset lag, watermark, reconcile và số liệu nghiệp vụ, không chỉ nhìn thấy thông báo RESTORE thành công.

## 4. Backup thuộc Kafka cũ hoặc chỉ cần xem dữ liệu cũ

```bash
warehouse/backup.sh restore ch-YYYYMMDD-HHMMSS --force
```

`--force` luôn là **restore offline**: không tạo lại Kafka consumers và không bật warehouse-jobs, kể cả Kafka vẫn cùng lịch sử. Nó cho phép xem bản backup mà không trộn lịch sử cũ với dữ liệu nguồn mới. Không dùng như cách bỏ qua retention rồi tiếp tục pipeline bình thường. Các service khác dùng ClickHouse có thể báo thiếu dữ liệu hoặc lag; đây là trạng thái đọc dữ liệu khôi phục, không phải stack CDC đang khỏe.

Backup cũ được tạo trước khi có `.offsets`/`.topics` chỉ được restore offline. Đừng tự suy ra điểm replay hiện tại từ dữ liệu Kafka mới.

## 5. Diễn tập có giá trị

1. Tạo full và incremental, ghi lại tên và thời gian.
2. Tạo thêm một đợt giao dịch qua simulator, dừng simulator, hạ clock và đợi scheduler/CDC ổn định.
3. Ghi fingerprint nghiệp vụ: số dòng và tổng tiền ở các bảng quan trọng, số khóa duy nhất, watermark và Kafka lag. Số dòng bronze đơn thuần không đủ vì heartbeat và job vẫn có thể ghi.
4. Restore bản trước đợt giao dịch, chờ đọc bù.
5. So trạng thái silver với PostgreSQL, mart với gold tại cùng watermark, kiểm tra ingest errors và running connector.
6. Ghi thời gian backup, thời gian restore và thời gian đến khi pipeline bắt kịp. Tổng thời gian đó mới có ý nghĩa cho RTO.

Có thể chạy `docker compose --profile tools run --rm validator python check_data.py` sau khi simulator dừng. Bộ kiểm tra có chờ scheduler và CDC trong giới hạn; lỗi kéo dài vẫn phải fail. Một lần kiểm tra dữ liệu cũ lỗi không chứng minh restore sai: đối chiếu lỗi đã tồn tại trước restore, không reset chỉ để che lỗi.

## 6. Giữ dung lượng có kiểm soát

```bash
warehouse/backup.sh prune 2
```

Lệnh giữ hai full mới nhất và incremental phụ thuộc chúng, xóa các bộ cũ cùng metadata. Không xóa full bằng `rm` nếu còn incremental cần nó. Chạy `list` trước và sau prune. Mốc 10 GB là mục tiêu sử dụng của bạn, chưa có quota tự động: bronze giữ lịch sử, named volume, image Docker và backup đều tiêu tốn dung lượng; dừng simulator không làm các file tự nhỏ lại.

Chưa cài lịch backup tự động hoặc gửi off-site trong lượt này. Với local, tạo bản theo mốc trước những lần sửa schema/reset là điểm bắt đầu; nếu chạy lâu dài cần lịch, retention dung lượng, theo dõi lần backup thành công gần nhất và diễn tập restore định kỳ.
