# Triển khai và DevOps

[Tài liệu dự án](../README.md) / Triển khai

Phần này hướng dẫn chuyển từ local sang môi trường server có domain, quyền truy cập và quy trình vận hành rõ ràng. **Server/CD/tracing trong các tài liệu này hiện là thiết kế đề xuất, chưa phải hạ tầng đã triển khai thật.**

| Đọc theo thứ tự | Mục đích |
|---|---|
| [1. Thực hành local → staging](first-deployment.md) | Lộ trình cho lần đầu: chọn host, tách môi trường, config/secret, DNS/TLS, release, CD và quan sát |
| [2. Chương 07 · Triển khai và DevOps](07-trien-khai-va-devops.md) | Giải thích sâu nguyên tắc, đánh đổi, mạng, migration, rollback và tài nguyên |
| [3. Q&A DevOps](../interview/07-trien-khai-va-devops.md) | 66 câu hỏi kiểm tra hiểu biết và xử lý tình huống |

## Các mốc thực hành

1. Chốt budget, region, domain, bảng cấu hình và ranh giới quyền.
2. Dựng staging thủ công, xác nhận TLS/auth, dữ liệu bền và restore.
3. Tự động hóa release bằng CD có lock, verify và rollback đã thử.
4. Thu metrics/logs/traces trên server, ghi năng lực theo workload cụ thể.

Trước khi triển khai, đọc [vận hành stack hiện tại](../operations/README.md). Không dùng reset simulation như quy trình cập nhật server sống lâu.
