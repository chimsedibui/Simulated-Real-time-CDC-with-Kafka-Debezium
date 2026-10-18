# Nội dung thông báo cảnh báo

[Tài liệu dự án](../README.md) / [Vận hành](README.md)

Bridge chuyển payload Alertmanager sang tin nhắn Google Chat có ngữ cảnh và thời gian. Formatter không truy vấn database hoặc gọi lại target để suy đoán root cause.

## Một tin nhắn có những gì?

- Trạng thái nhóm và số alert FIRING/RESOLVED; trạng thái riêng của từng alert được giữ nếu nhóm trộn cả hai.
- Môi trường, project, service, instance và các nhãn ngữ cảnh như dependency, connector, consumer group hoặc SLO khi có.
- Giờ tạo thông báo tại bridge, giờ bắt đầu alert, giờ kết thúc khi resolved và thời lượng.
- Summary/description của rule, điều kiện và giá trị đánh giá nếu annotations cung cấp. TargetDown có hướng kiểm tra theo job; mất Kafka exporter không bị mô tả thành broker chắc chắn đã chết.
- Link Grafana Platform Health với khoảng thời gian sự cố cộng biên 5 phút; runbook nếu annotation có URL.

## Quy ước thời gian

Các mốc hiển thị theo **giờ Việt Nam, UTC+7**, không phụ thuộc timezone của container. Đây là wall clock giám sát, không phải ngày nghiệp vụ được tăng tốc bởi simulator.

Bắt đầu/kết thúc lấy từ `startsAt`/`endsAt` của từng alert. Không lấy giờ nhóm gửi tin làm giờ bắt đầu sự cố. Alert còn firing không được dùng endsAt làm giờ phục hồi: trường này có thể là timeout tương lai. Mốc thiếu, không parse được, không có timezone hoặc bằng zero time của Go hiển thị là không có dữ liệu; không tự bịa thời gian. Duration là thời lượng alert theo hai mốc đó, không phải cam kết đo chính xác downtime hoặc RTO của dịch vụ.

Giờ tạo thông báo là lúc bridge dựng nội dung, không phải thời điểm Google Chat xác nhận người dùng đã đọc. Group wait/repeat/retry có thể làm giờ thông báo muộn hơn giờ kết thúc. [Định nghĩa các trường Alertmanager](https://prometheus.io/docs/alerting/latest/notifications/).

## Đọc đúng RESOLVED

RESOLVED là trạng thái Alertmanager gửi, không đủ để tự chứng minh toàn pipeline đã phục hồi. Với TargetDown, cần xác nhận scrape/health thực tế; với warehouse-jobs còn phải xem tuổi mart và reconciliation. Bridge giữ summary/giá trị rule với nhãn “khi cảnh báo” để tránh coi giá trị lỗi cũ là số đo hiện tại.

Ví dụ dưới đây là **payload giả định để xem định dạng**, không phải lịch sử sự cố thật và không được gửi vào Chat:

```text
[CDC] RESOLVED
Môi trường: local | Dự án: realtime-cdc
Thời điểm tạo thông báo: 19/10/2026 00:07:00 (UTC+7)
Nhóm nhận được: 0 FIRING | 1 RESOLVED

[RESOLVED] critical — TargetDown
Dịch vụ: warehouse-jobs
Target: warehouse-jobs:9109
Bắt đầu cảnh báo: 19/10/2026 00:00:00 (UTC+7)
Kết thúc cảnh báo: 19/10/2026 00:06:30 (UTC+7)
Thời lượng cảnh báo: 00:06:30
Tóm tắt khi cảnh báo: Prometheus cannot scrape warehouse-jobs (warehouse-jobs:9109)
Diễn giải: Alertmanager đã đánh dấu RESOLVED. Cần xác nhận target scrape lại thành công và đối chiếu job, dữ liệu nghiệp vụ trước khi kết luận phục hồi toàn bộ.
Ảnh hưởng khi cảnh báo: Mart, snapshot và đối soát có thể chậm cập nhật; cần kiểm tra job thực tế.
Kiểm tra: docker compose logs --tail=100 warehouse-jobs; kiểm tra endpoint warehouse-jobs:9109/metrics, tuổi mart và reconciliation.
Điều kiện: up == 0 for 2m
Giá trị rule khi cảnh báo (payload): 0
Grafana (khoảng thời gian cảnh báo): http://localhost:3000/d/cdc-platform?from=1792342500000&to=1792343490000
```

## Cấu hình và kiểm tra

`ALERT_ENVIRONMENT` trong environment của bridge mặc định `local`; đặt `staging`/`production` khi triển khai. Nhãn environment/env từ payload được ưu tiên nếu có. `GRAFANA_PUBLIC_URL` phải là URL người nhận truy cập được khi xem từ xa; localhost chỉ hữu ích trên máy chạy stack.

```bash
docker compose run --build --rm --no-deps chat-webhook python -m unittest -v test_bridge.py
docker compose run --rm --no-deps --entrypoint promtool -w /etc/prometheus prometheus test rules alerts.test.yml
```

Bridge giới hạn 15 alert mỗi tin và khoảng 25 KB nội dung UTF-8, giữ link dashboard khi rút gọn. Số alert bị cắt từ Alertmanager hoặc tại bridge được ghi rõ. Upstream lỗi vẫn trả 503 để Alertmanager retry; URL webhook và nội dung lỗi upstream không được in vào log.

Đừng POST alert giả vào Alertmanager chỉ để xem format nếu nó đang nối với Chat thật. Unit tests mock delivery; bản xem trước gọi trực tiếp formatter không gửi tin. Khi cần diễn tập gửi thật, chốt rõ kênh nhận và nội dung thử trước.
