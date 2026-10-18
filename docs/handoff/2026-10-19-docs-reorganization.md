# Sắp xếp lại thư mục tài liệu

Ngày thực hiện: 19/10/2026. Người dùng yêu cầu làm gọn toàn bộ folder docs.

## Cấu trúc mới

Root docs chỉ giữ README làm mục lục. Nội dung được chia thành business, platform, architecture, operations, deployment, interview, maintenance và handoff. Mỗi nhóm có README; từng chương có đường dẫn quay lại nhóm và mục lục chung.

Số chương 01–09 và số câu hỏi được giữ để đối chiếu. Tài liệu triển khai thực hành chuyển sang deployment/first-deployment.md; deployment/README.md trở thành trang dẫn đường. Log kiểm thử và checksum lịch sử được giữ nguyên ở handoff/logs.

## Bảng di chuyển

| Đường dẫn cũ (tính từ repo) | Đường dẫn hiện tại |
|---|---|
| `docs/01-nghiep-vu-va-du-lieu-nguon.md` | [`docs/business/01-nghiep-vu-va-du-lieu-nguon.md`](../../docs/business/01-nghiep-vu-va-du-lieu-nguon.md) |
| `docs/02-source-api-va-bao-mat.md` | [`docs/business/02-source-api-va-bao-mat.md`](../../docs/business/02-source-api-va-bao-mat.md) |
| `docs/03-cdc-ingestion.md` | [`docs/platform/03-cdc-ingestion.md`](../../docs/platform/03-cdc-ingestion.md) |
| `docs/04-warehouse.md` | [`docs/platform/04-warehouse.md`](../../docs/platform/04-warehouse.md) |
| `docs/05-mart-va-metric.md` | [`docs/platform/05-mart-va-metric.md`](../../docs/platform/05-mart-va-metric.md) |
| `docs/06-van-hanh.md` | [`docs/operations/06-van-hanh.md`](../../docs/operations/06-van-hanh.md) |
| `docs/07-trien-khai-va-devops.md` | [`docs/deployment/07-trien-khai-va-devops.md`](../../docs/deployment/07-trien-khai-va-devops.md) |
| `docs/08-ke-hoach-don-code.md` | [`docs/maintenance/08-ke-hoach-don-code.md`](../../docs/maintenance/08-ke-hoach-don-code.md) |
| `docs/09-thuc-hanh-backup-clickhouse.md` | [`docs/operations/09-thuc-hanh-backup-clickhouse.md`](../../docs/operations/09-thuc-hanh-backup-clickhouse.md) |
| `docs/data-arch/ecommerce_system_flow.md` | [`docs/architecture/ecommerce_system_flow.md`](../../docs/architecture/ecommerce_system_flow.md) |
| `docs/data-arch/erDiagram.txt` | [`docs/architecture/erDiagram.txt`](../../docs/architecture/erDiagram.txt) |
| `docs/data-arch/README.md` | [`docs/architecture/README.md`](../../docs/architecture/README.md) |
| `docs/data-arch/entity_relationships.md` | [`docs/architecture/entity_relationships.md`](../../docs/architecture/entity_relationships.md) |
| `docs/qa/01-nghiep-vu-va-du-lieu-nguon.md` | [`docs/interview/01-nghiep-vu-va-du-lieu-nguon.md`](../../docs/interview/01-nghiep-vu-va-du-lieu-nguon.md) |
| `docs/qa/02-source-api-va-bao-mat.md` | [`docs/interview/02-source-api-va-bao-mat.md`](../../docs/interview/02-source-api-va-bao-mat.md) |
| `docs/qa/07-trien-khai-va-devops.md` | [`docs/interview/07-trien-khai-va-devops.md`](../../docs/interview/07-trien-khai-va-devops.md) |
| `docs/qa/03-cdc-ingestion.md` | [`docs/interview/03-cdc-ingestion.md`](../../docs/interview/03-cdc-ingestion.md) |
| `docs/qa/06-van-hanh.md` | [`docs/interview/06-van-hanh.md`](../../docs/interview/06-van-hanh.md) |
| `docs/qa/05-mart-va-metric.md` | [`docs/interview/05-mart-va-metric.md`](../../docs/interview/05-mart-va-metric.md) |
| `docs/qa/04-warehouse.md` | [`docs/interview/04-warehouse.md`](../../docs/interview/04-warehouse.md) |
| `docs/deployment/README.md` | [`docs/deployment/first-deployment.md`](../../docs/deployment/first-deployment.md) |

## Kiểm tra

- Đã cập nhật liên kết Markdown theo vị trí file trước và sau khi di chuyển, gồm cả tài liệu ngoài docs trong observability.
- Đã thống nhất tiêu đề Q&A và thêm breadcrumb cho các chương.
- Các đường dẫn dạng chữ trong log cũ là lịch sử, không được sửa như thể các lệnh đã chạy với tên mới.
- Việc này chỉ tổ chức tài liệu; chưa commit/push. Kết quả kiểm tra link được bổ sung bên dưới.

### Kết quả cuối

- Đã di chuyển 21 tài liệu; root docs chỉ còn một file README và tám thư mục nhóm.
- Kiểm tra 259 liên kết file nội bộ trong docs và observability: 0 link hỏng.
- Kiểm tra các anchor trỏ tới heading trong docs: 0 anchor không khớp.
- Bộ phỏng vấn vẫn đủ 850 câu; không xóa log kiểm thử hoặc checksum lịch sử.
- `git diff --check`: pass.
