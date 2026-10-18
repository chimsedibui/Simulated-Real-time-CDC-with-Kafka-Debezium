# Vận hành và phục hồi

[Tài liệu dự án](../README.md) / Vận hành

Hướng dẫn dùng stack hiện tại, kiểm tra dữ liệu và phục hồi warehouse.

| Tài liệu | Nội dung |
|---|---|
| [06 · Vận hành và chất lượng](06-van-hanh.md) | Khởi động/reset, volume, metrics, alert, runbook và ba mức gate |
| [09 · Thực hành backup ClickHouse](09-thuc-hanh-backup-clickhouse.md) | Full/incremental, retained offsets, restore/catch-up và giới hạn offline |
| [Q&A vận hành](../interview/06-van-hanh.md) | Các tình huống kiểm thử và xử lý sự cố |

`quality/gate.sh smoke` và `full` xóa dữ liệu. Đọc phạm vi từng gate trước khi chạy; không dùng chúng như deploy script trên server có dữ liệu cần giữ.

Muốn dựng môi trường mới trên server, chuyển sang [triển khai và DevOps](../deployment/README.md). Kết quả từng lần chạy được lưu riêng trong [bàn giao](../handoff/README.md).
