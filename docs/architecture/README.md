# Bản đồ kiến trúc dữ liệu

[Tài liệu dự án](../README.md) / Kiến trúc

Thư mục này mô tả **hệ thống đang được triển khai**: marketplace nhiều shop, PostgreSQL → Debezium/Kafka Connect → Kafka → ClickHouse, với Grafana cho phân tích và giám sát. Đối chiếu schema và code ngày 18/10/2026.

| Muốn tìm hiểu | Đọc |
|---|---|
| Một giao dịch đi từ người dùng tới báo cáo như thế nào | [Luồng nghiệp vụ và dữ liệu](ecommerce_system_flow.md) |
| Các bảng nối với nhau ra sao, theo từng nhóm dễ đọc | [ERD theo nhóm](entity_relationships.md) |
| ERD vật lý đầy đủ để mở trong công cụ Mermaid | [erDiagram.txt](erDiagram.txt) |
| Toàn bộ tài liệu nghiệp vụ, API, CDC, warehouse và Q&A | [Mục lục dự án](../README.md) |

## Phạm vi mô hình

`enterprise_db.ecommerce` hiện có **38 bảng, 319 cột và 57 khóa ngoại vật lý**. ERD đầy đủ dùng đúng tên bảng và cột của schema hiện tại. Nó không gộp bảng tài khoản của `identity_db` hoặc bảng phân tích ClickHouse vào cùng một sơ đồ quan hệ PostgreSQL.

| Phạm vi | Số bảng | Đi qua CDC thế nào? |
|---|---:|---|
| Bảng nghiệp vụ được mô hình hóa trong silver | 31 | Envelope thay đổi hàng → `cdc.ecommerce.<table>` → bronze → silver |
| `outbox_events` | 1 | Event Router đổi thành domain events trên `events.<aggregate>`; không có silver.outbox_events |
| `debezium_signals` | 1 | Được connector capture, dùng cho tín hiệu snapshot; không có mô hình silver nghiệp vụ |
| `sim_clock`, `idempotency_keys`, `cdc_heartbeat`, `wallet_pending`, `voucher_quota_slots` | 5 | Bị loại bởi cấu hình capture; vẫn hiện trong ERD vì thật sự tồn tại ở nguồn |

Publication bao phủ schema, nhưng **publication, bộ lọc connector và mô hình silver là ba phạm vi khác nhau**. Có bảng trong PostgreSQL không có nghĩa nó được publish vào Kafka; được capture cũng không có nghĩa có bảng silver tương ứng.

Identity có database riêng với `users`, `memberships`, `refresh_tokens`, `audit`. `memberships.resource_id` tham chiếu customer/shop ở mức ứng dụng, không phải FK xuyên database. Password hash, refresh token và audit đăng nhập không được đưa vào publication của `enterprise_db`.

## Cách đọc ERD

- `PK`: thành phần khóa chính; `FK`: cột có ràng buộc khóa ngoại thật; `UK`: unique vô điều kiện trên riêng cột đó. Một cột có thể vừa `PK` vừa `FK`.
- `"nullable"`: cột cho phép NULL. Một FK nullable nghĩa bản ghi con có thể không gắn với cha ở vai trò đó.
- `A ||--o{ B`: mỗi B phải có một A, còn một A có thể chưa có B hoặc có nhiều B.
- `A |o--o{ B`: mỗi B có thể không có A hoặc gắn với một A; một A có thể được nhiều B tham chiếu.
- `A ||--o| B`: mỗi B có một A, mỗi A có tối đa một B. Ví dụ mỗi order có tối đa một return request.

Kiểu dùng tên kiểu nội bộ PostgreSQL: `int8` là BIGINT, `int4` là INTEGER, `int2` là SMALLINT, `bool` là BOOLEAN, `timestamptz` là timestamp có múi giờ. Tiền lưu bằng số nguyên VND; không suy ra cột tiền là số thực từ cách hiển thị trên dashboard.

ERD biểu diễn FK và tính duy nhất của FK, không biểu diễn đầy đủ CHECK, partial unique index, quy tắc state machine hay điều kiện phân quyền. Ví dụ:

| Quy tắc | Cách được bảo đảm |
|---|---|
| Tối đa một giỏ ACTIVE cho mỗi khách | Partial unique index theo customer |
| Tối đa một địa chỉ mặc định của khách | Partial unique index theo customer khi `is_default` |
| Một SKU chỉ xuất hiện một lần trong giỏ | Unique ghép `(cart_id, sku_id)` |
| Một SKU trong mỗi campaign | Unique ghép `(campaign_id, sku_id)` |
| Tối đa một kiện mỗi chiều của order | Unique ghép `(order_id, direction)` |
| Một checkout gắn với một cart; một order tối đa một return | Unique trên FK `checkouts.cart_id`, `returns.order_id` |
| Một order item tối đa một dòng return và một review | Unique trên `return_items.order_item_id`, `product_reviews.order_item_id` |
| Quota voucher chia nhiều slot | PK ghép `(voucher_id, slot)`; không phải voucher_id duy nhất |
| `available = on_hand - reserved` | CHECK và đường cập nhật tồn kho của Source API |
| Chuyển trạng thái, refund và quyền sở hữu | Logic command trong API, transaction và row lock |

`inventory_movements.ref_type/ref_id` và `outbox_events.aggregateid` là tham chiếu đa hình, không phải FK tới mọi bảng có thể liên quan. `wallet_pending.shop_id` cũng không có FK vật lý riêng. Không vẽ các đường này như quan hệ được PostgreSQL cưỡng chế.

## Những quyết định quan trọng

Mô hình bán theo **product → SKU → tồn kho của SKU tại shop**, không có supplier, purchase order hoặc bảng warehouse theo địa điểm. Một product thuộc một category; cây category có parent. Một checkout gom một lần thanh toán của người mua nhưng tách thành nhiều order theo shop. Địa chỉ giao trên order là snapshot, không phải FK đến địa chỉ đang sống trong sổ địa chỉ.

Payment attempt và refund là hai bảng khác nhau. Thanh toán online có grain checkout; thu COD có thêm order_id. Dòng seller ledger là biến động ví khi quyết toán, không phải bản sao payment. Vì vậy không cộng các bảng này vào cùng một chỉ tiêu “doanh thu” khi chưa chọn định nghĩa nghiệp vụ.

ClickHouse không kế thừa FK của PostgreSQL. Tính nhất quán xuyên bảng được xử lý bằng transaction metadata và watermark; số liệu dashboard đọc mart ở một refresh đã hoàn tất. Xem [luồng dữ liệu](ecommerce_system_flow.md#8-từ-kafka-đến-warehouse-và-dashboard).

## Cập nhật tài liệu về sau

Nguồn đối chiếu là schema trong `source/postgres/init.sql`, cấu hình connector, hợp đồng silver, state machine của Source API và các job warehouse. Database đang chạy phải đã áp dụng đúng phiên bản schema trước khi dùng catalog PostgreSQL để cập nhật ERD.

Khi schema đổi, kiểm tra cả cột, PK/UK/FK, nullable và cardinality. Khi nghiệp vụ đổi, kiểm tra các luồng checkout, thanh toán, tồn kho, returns và settlement. Khi pipeline đổi, kiểm tra lại phạm vi capture và đường tạo mart. Tên entity trong sơ đồ không phải tên hiển thị tùy ý: cần giữ đúng tên thật để người đọc tra cứu được.
