# Cải thiện nội dung cảnh báo Google Chat

Người dùng phản ánh tin TargetDown RESOLVED thiếu ngày giờ và mô tả cụ thể. Đã áp dụng vào stack local.

## Thay đổi

- Bridge thêm giờ tạo thông báo, startsAt/endsAt, thời lượng, UTC+7 độc lập timezone host, environment/project, service/instance và các labels ngữ cảnh.
- Firing không dùng endsAt làm giờ phục hồi. Timestamp thiếu/invalid/zero/không có timezone và duration âm không bị suy diễn. Mixed groups giữ trạng thái của từng alert và đếm firing/resolved.
- TargetDown có impact và hướng kiểm tra theo job (warehouse-jobs/source-api/kafka exporter/platform exporter). RESOLVED chỉ báo trạng thái từ Alertmanager, yêu cầu kiểm tra scrape và dữ liệu trước khi kết luận phục hồi toàn bộ.
- Rule TargetDown có summary chứa instance, description đúng phạm vi scrape, condition và observed_value. Giá trị trong tin resolved được gắn nhãn “khi cảnh báo”, không phải metric hiện tại được truy vấn lại.
- Link Grafana ghim khoảng cảnh báo với biên 5 phút. Cảnh báo bị cắt và nội dung UTF-8 quá dài được báo rõ; giữ link khi rút gọn. Retry 503 và không log credential vẫn được giữ.
- Compose và env.example thêm ALERT_ENVIRONMENT mặc định local. Tài liệu vận hành có mẫu và giải thích thời gian, không dùng business clock tăng tốc.

## Kiểm thử và triển khai

- Host Python thiếu prometheus_client nên chạy bộ test trong image service đúng dependencies; 16 tests pass.
- Promtool alert rule tests pass, gồm annotation mới và ngưỡng for của TargetDown. Compose validation và diff whitespace pass.
- Rebuild/recreate riêng chat-webhook bằng --no-deps; gửi HUP cho Prometheus để nạp rule. Bridge health 200, API rules xác nhận condition mới đã được nạp. Không reset dữ liệu hoặc restart warehouse.
- Không gửi synthetic alert tới Alertmanager/Chat thật. Preview được tạo từ formatter trong container; delivery trong test được mock.
- User đã yêu cầu commit/push ngày 19/10/2026. Chạy lại 16 test webhook, Promtool rule tests và Compose validation trước khi commit: đều pass; log bên dưới đã cập nhật. Thay đổi webhook được commit riêng với cleanup dead code.

## Bằng chứng

- [16 tests webhook](logs/2026-10-19-alert-webhook-tests.txt)
- [Rule tests](logs/2026-10-19-alert-rule-tests.txt)
- [Tin mẫu từ formatter, không gửi thật](logs/2026-10-19-alert-message-preview.txt)
- [Hướng dẫn đọc tin](../operations/alert-notifications.md)
