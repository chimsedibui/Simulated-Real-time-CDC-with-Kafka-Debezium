# Rà soát dead code · 19/10/2026

[Tài liệu dự án](../README.md) / [Bảo trì](README.md)

## Kết luận và phạm vi

Đã dọn các phần xác nhận không còn người dùng ở Control Center frontend, helper auth của Source API và cấu hình Debezium vô hiệu hóa. Chưa có bằng chứng để xóa thêm dependency frontend. Có ba metric Debezium chưa tìm thấy consumer trong repo; vẫn giữ vì chúng được thu thập và công bố qua exporter.

Mốc đối chiếu: commit `7c9d9ffbeb54fca55bfd9a801c762af8d8238446`. Working tree đã chứa thay đổi cải thiện Alert Webhook trước khi rà soát; lượt cleanup này giữ nguyên chúng. Xem [bàn giao cảnh báo](../handoff/2026-10-19-alert-webhook.md) để phân biệt phạm vi.

Phạm vi Python gồm backend Control Center, Source API, identity, simulator, warehouse jobs, observability service/generator, quality và generator/contract checker ClickHouse. Frontend được phân tích từ entrypoint và dependency bằng Knip, rồi build bằng TypeScript/Vite. Các tham chiếu metric được tìm trong file runtime, cấu hình và test được Git quản lý, loại docs khỏi kết quả. Script vận hành, router và artifact provisioning được xem thêm theo cách đăng ký/gọi thực tế.

## Những phần đã dọn

| Vị trí | Thay đổi | Bằng chứng và giới hạn |
|---|---|---|
| `apps/control-center/frontend/src/ui.tsx` | Xóa `fmt.money`, `fmt.moneyCents`, `fmt.compactMoney`, `compactVnd` và formatter `vnd` | Không có caller trong frontend; caller hiện dùng `fmt.num`, `fmt.ago`, `fmt.bytes`. Không có truy cập động tới các thuộc tính đã xóa. |
| Cùng file | Bỏ `export` của `CHART_PALETTE` và `tableColor` | Knip báo không có import từ ngoài module. Giữ implementation vì `TableChip` vẫn dùng màu bảng. |
| `apps/control-center/frontend/src/api.ts` | Bỏ `export` của interface `Seeding` | Chỉ dùng trong các interface cùng module; không xóa type hay thay đổi API response. |
| `source/source-api/app/auth.py` | Xóa helper `read_shop` | Chỉ có định nghĩa, không có import/caller; Vulture báo ở mức 60%, sau đó xác minh bằng tìm kiếm. Giữ `shop`, `read_customer` và các guard còn được gọi. |
| `debezium/postgres-connector.json` | Xóa `_comment_unwrap` và bốn khóa `_transforms.unwrap.*` | Đây là khối cũ đã vô hiệu hóa. Toàn bộ khóa active không bắt đầu bằng `_` giữ nguyên; transform đang dùng vẫn là Outbox Event Router. Không đăng ký lại connector. |

Các formatter tiền là phần còn sót sau khi WarehousePanel được bỏ và phần analytics chuyển sang Grafana. Nếu cần hiển thị tiền ở một màn hình mới, định nghĩa lại theo đơn vị dữ liệu của màn đó; không khôi phục formatter cũ theo tên gọi.

## Những phần nhìn giống code thừa nhưng cần giữ

| Phần | Lý do giữ |
|---|---|
| `recharts` | `LivePanel.tsx` vẫn import AreaChart và các thành phần biểu đồ realtime. Knip không báo dependency thừa. |
| `apps/control-center/backend/warehouse.py` | Endpoint health vẫn gọi `warehouse.rows` để lấy số dòng bronze; không phải toàn bộ API warehouse đã hết người dùng. |
| Handler FastAPI và startup | Decorator/router/lifecycle đăng ký hàm; không cần lời gọi trực tiếp trong Python. |
| `do_GET`, `do_POST`, `log_message`, `auth_flow`, context manager và fixture test | Framework, giao thức Python hoặc pytest gọi gián tiếp. |
| Pydantic `initial_stock`, `price_not_above_list` | Trường request và validator được Pydantic sử dụng; kiểm tra caller dạng hàm thông thường không đủ. |
| Router Source API không có `/v1`; auth `off/permissive/enforce` | Là các chế độ tương thích có điều kiện đang được hỗ trợ, chưa có quyết định bỏ. |
| `warehouse/clickhouse/rawblob-migration.sh` | Entrypoint migration thủ công dành cho volume cũ. Không có caller tự động chưa chứng minh hết nhu cầu vận hành. Không chạy migration trong lượt này. |
| SQL silver và JSON dashboard sinh tự động | Là đầu vào provisioning thực tế; generator không thay thế artifact mà service đang đọc. |
| Logic validator khác production, helper secret ở nhiều service | Trùng lặp không đồng nghĩa dead code. Kiểm chứng cần độc lập; các service có dependency/build context riêng. |

## Metric chưa có consumer trong repo

| Metric | Đang thu thập từ Debezium | Quyết định hiện tại |
|---|---|---|
| `cdc_debezium_since_last_event_seconds` | `MilliSecondsSinceLastEvent` | Giữ để chẩn đoán; không tự cảnh báo pipeline hỏng chỉ vì simulation đang idle. |
| `cdc_debezium_connected` | `Connected` | Giữ; cần xác định giá trị bổ sung so với trạng thái connector/task trước khi thêm alert mới. |
| `cdc_debezium_events_seen_total` | `TotalNumberOfEventsSeen` | Giữ; giá trị reset theo task, không dùng làm tổng nghiệp vụ hay thay cho số event đã lưu. |

Cả ba vẫn có lệnh `.set(...)` trong exporter. Chưa tìm thấy truy vấn chúng trong dashboard, alert hay code khác của repo; không thể suy ra không ai truy vấn trực tiếp bằng Prometheus hoặc từ bên ngoài. Nếu muốn bỏ, cần ghi quyết định deprecate và kiểm tra consumer ngoài repo trước. Chi tiết toàn bộ metric đã rà nằm trong [log metric](../handoff/logs/2026-10-19-dead-code-metrics.txt).

## Kiểm chứng sau cleanup

| Kiểm tra | Kết quả |
|---|---|
| Ruff 0.17.1, `F401,F811,F821,F841` | Pass: không có import trùng/thừa, tên chưa định nghĩa hay biến cục bộ thừa trong phạm vi quét. |
| Vulture 2.16, mức tin cậy 80% | Không có finding. |
| Vulture mức 60% + kiểm tra caller | Helper `read_shop` đã hết finding sau khi xóa; phần còn lại được phân loại theo cơ chế framework/giao thức, không xóa hàng loạt. |
| Knip 6.42.0 | Sau cleanup không còn finding trong nhóm files, dependencies, unlisted, unresolved, exports, types, duplicates đã chọn. |
| `npm run build` (Node v22.22.1) | TypeScript và Vite pass. Vite còn cảnh báo bundle JS trên 500 kB; đây là việc tối ưu tải trang riêng. |
| Source API Docker build | Pass. |
| Import Source API từ image mới trong container tạm | Pass: 58 path duy nhất, có health và route `/v1`. |
| Contract trên PostgreSQL hiện tại | Pass: 31 bảng, 319 cột nguồn; schema, silver artifact và cấu hình capture phù hợp. |
| JSON + so sánh cấu hình Debezium | Parse được, mọi khóa active giống trước cleanup. |

Import lần đầu thất bại vì chạy thẳng Python bỏ qua entrypoint tạo thư mục multiprocess Prometheus. Chạy lại sau khi tạo `/tmp/prometheus` trong container tạm thì pass; giữ cả hai log để không nhầm với lỗi ứng dụng do cleanup. Docker build tạo image mới, service đang chạy chưa được recreate bằng image này.

Không chạy lại smoke ghi dữ liệu hoặc reset stack cho các thay đổi không có caller này. Kiểm tra import chỉ chứng minh app nạp và route đăng ký được, không thay thế test mọi luồng auth/nghiệp vụ. Static analysis không chứng minh toàn bộ repo không còn dead code; Python dependency gọi gián tiếp, API client bên ngoài, shell entrypoint và SQL được tạo động cần được đánh giá theo hợp đồng riêng.

## Hướng xử lý tiếp

1. Nếu bỏ auth mode hoặc API tương thích, mở đợt đổi hành vi riêng: kiểm kê client, quyết định hỗ trợ, cập nhật docs và chạy smoke/auth tương ứng.
2. Nếu xóa migration RawBLOB, cần xác nhận không còn volume từ schema cũ và lưu hướng nâng cấp có thể tìm lại.
3. Với ba metric trên, chọn giữ như dữ liệu chẩn đoán, thêm consumer có mục đích, hoặc deprecate; không coi thiếu panel là lý do đủ để xóa.
4. Tối ưu bundle frontend bằng phân tích kích thước và lazy loading khi cần; không gỡ Recharts khi LivePanel vẫn dùng.
5. Hợp nhất metadata bảng chỉ khi xác định nguồn chuẩn và giữ validator độc lập; đây là refactor, không phải xóa dead code đơn thuần.

Xem [bàn giao cho Claude](../handoff/2026-10-19-dead-code-audit.md) để có lệnh tái chạy, log và trạng thái working tree.
