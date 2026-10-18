# Nhật ký bàn giao và bằng chứng

[Tài liệu dự án](../README.md) / Bàn giao

| Nhật ký | Nội dung |
|---|---|
| [Triển khai và kiểm thử CDC](2026-10-18-codex.md) | Backup/restore, reset/rotation, smoke/auth, Grafana và cập nhật docs |
| [Rà soát và dọn dead code](2026-10-19-dead-code-audit.md) | Helper/export thừa, cấu hình cũ, metric và bằng chứng kiểm tra |
| [Cải thiện cảnh báo Chat](2026-10-19-alert-webhook.md) | Timestamp UTC+7, thời lượng, target, ngữ cảnh và hướng kiểm tra |
| [Sắp xếp lại tài liệu](2026-10-19-docs-reorganization.md) | Cấu trúc thư mục mới, đường dẫn đã chuyển và kiểm tra liên kết |

Thư mục [logs/](logs/) giữ output kiểm thử và checksum để đối chiếu. Log của lần thử lỗi cũng được giữ để giải thích nguyên nhân; kết luận cuối nằm trong nhật ký tương ứng.

Các đường dẫn dạng chữ trong nhật ký cũ mô tả trạng thái tại thời điểm ghi. Dùng mục lục hiện tại và bảng di chuyển để tìm file mới; các liên kết Markdown đã được cập nhật.
