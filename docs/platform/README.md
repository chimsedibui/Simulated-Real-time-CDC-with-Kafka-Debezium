# Data platform

[Tài liệu dự án](../README.md) / Data platform

Theo dõi một thay đổi đã commit từ PostgreSQL tới số liệu trên dashboard.

| Chương | Câu hỏi chính | Ôn tập |
|---|---|---|
| [03 · CDC ingestion](03-cdc-ingestion.md) | WAL, slot, Connect/Debezium, Kafka, outbox, schema và replay hoạt động thế nào? | [Q&A 03](../interview/03-cdc-ingestion.md) |
| [04 · Warehouse](04-warehouse.md) | Bronze/silver, before-image, watermark và snapshot xử lý tính nhất quán ra sao? | [Q&A 04](../interview/04-warehouse.md) |
| [05 · Mart và metric](05-mart-va-metric.md) | Grain, định nghĩa tiền/trạng thái và cách tính báo cáo là gì? | [Q&A 05](../interview/05-mart-va-metric.md) |

Nên đọc theo thứ tự 03 → 04 → 05. Tra quan hệ nguồn ở [ERD theo nhóm](../architecture/entity_relationships.md); xem cách dùng báo cáo ở [hướng dẫn Grafana](../../observability/grafana/README.md).
