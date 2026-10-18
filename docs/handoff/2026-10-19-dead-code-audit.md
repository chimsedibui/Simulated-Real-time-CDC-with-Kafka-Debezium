# Bàn giao Codex → Claude · dead code · 19/10/2026

[Nhật ký bàn giao](README.md) · [Báo cáo bảo trì](../maintenance/dead-code-audit-2026-10-19.md)

## Yêu cầu và trạng thái

User yêu cầu rà các phần dead code cần dọn và lưu log cho Claude. Đã rà static analysis, caller, frontend dependency/export và metric; đã dọn các phần đủ bằng chứng. User đã yêu cầu commit/push ngày 19/10/2026; cleanup được đóng thành commit riêng với webhook.

Base HEAD: `7c9d9ffbeb54fca55bfd9a801c762af8d8238446` trên main. Working tree từ trước chứa thay đổi Alert Webhook chưa commit; chúng được giữ nguyên, xem [bàn giao webhook](2026-10-19-alert-webhook.md). Không revert/ghi đè các file observability để tách cleanup.

## Thay đổi trong lượt này

- `apps/control-center/frontend/src/ui.tsx`: xóa ba thuộc tính format tiền không còn caller cùng helper/formatter của chúng; chuyển palette và tableColor thành nội bộ module, giữ cách tô màu bảng.
- `apps/control-center/frontend/src/api.ts`: bỏ export interface Seeding, vẫn dùng nội bộ.
- `source/source-api/app/auth.py`: xóa helper read_shop không có caller; giữ các guard hiện dùng và mọi auth mode.
- `debezium/postgres-connector.json`: xóa khối unwrap cũ gồm comment và bốn khóa bị vô hiệu hóa; active config không đổi.
- Thêm báo cáo audit ở maintenance, cập nhật kế hoạch/mục lục và nhật ký này.

Không xóa dependency Recharts, backend warehouse client, router FastAPI, Pydantic validator/field, SQL/JSON provisioning hoặc migration RawBLOB. Lý do chi tiết trong báo cáo.

Ba metric chưa có static consumer ngoài exporter được giữ: `cdc_debezium_since_last_event_seconds`, `cdc_debezium_connected`, `cdc_debezium_events_seen_total`. Chúng vẫn có collection logic. Không xóa theo mỗi tiêu chí thiếu panel.

## Tái chạy kiểm tra

Chạy từ repo root trừ lệnh frontend. Ruff/Vulture dùng uv tool run, không thêm dependency hay sửa lockfile dự án:

```bash
uv tool run --from ruff ruff check --select F401,F811,F821,F841 apps source warehouse quality observability

uv tool run --from vulture vulture apps/control-center/backend source/source-api/app source/simulator/sim source/identity/app warehouse/jobs observability/service observability/grafana quality warehouse/clickhouse/gen_silver.py warehouse/clickhouse/check_contract.py --min-confidence 80
```

Vulture mức 80 không có finding. Mức 60 trả exit 3 khi có candidate; phần lớn là decorator/framework/protocol. Không dùng output này làm danh sách xóa tự động.

Trong `apps/control-center/frontend`:

```bash
npx --yes knip --include files,dependencies,unlisted,unresolved,exports,types,duplicates --no-progress
npm run build
```

Knip pass sau cleanup; build TypeScript/Vite pass. Vite cảnh báo chunk trên 500 kB, chưa tối ưu bundle trong lượt này.

Kiểm tra image và route, từ repo root:

```bash
docker compose build source-api
docker compose run --rm --no-deps --entrypoint python source-api -c 'from pathlib import Path; Path("/tmp/prometheus").mkdir(exist_ok=True); from app.main import app; paths = sorted({r.path for r in app.routes}); assert "/health" in paths; assert any(p.startswith("/v1/") for p in paths); print("Source API import OK:", len(paths), "unique paths; health and /v1 routes registered")'
docker compose --profile tools run --rm --no-deps -v "$PWD:/workspace:ro" validator python /workspace/warehouse/clickhouse/check_contract.py
```

Kết quả: Source API import thành công với 58 path duy nhất. Contract holds: 31 bảng, 319 cột nguồn. Đã parse Python auth và JSON connector; so sánh toàn bộ khóa config không bắt đầu bằng `_` với base HEAD, bằng nhau.

Lần import đầu bỏ qua entrypoint nên thiếu `PROMETHEUS_MULTIPROC_DIR`; lần sau tạo thư mục trước import thì pass. Đây là khác biệt của harness kiểm tra, không cần sửa app. Một lần chạy local ban đầu cũng dùng sai working directory nên chưa áp dụng được edit; đã sửa cwd, áp dụng edit và chạy lại toàn bộ kiểm tra liên quan. Chỉ kết quả sau cleanup được dùng để kết luận.

## Bằng chứng lưu tại repo

| Log | Nội dung |
|---|---|
| [Ruff](logs/2026-10-19-dead-code-ruff.txt) | Pass sau cleanup |
| [Vulture 80](logs/2026-10-19-dead-code-vulture-80.txt) | Không có finding |
| [Vulture 60 trước](logs/2026-10-19-dead-code-vulture-60-before.txt), [sau](logs/2026-10-19-dead-code-vulture-60-after.txt) | Candidate và thay đổi sau khi bỏ read_shop |
| [Knip trước](logs/2026-10-19-dead-code-knip-before.txt), [sau](logs/2026-10-19-dead-code-knip-after.txt) | Hai export và một type export, sau cleanup hết finding |
| [Frontend build](logs/2026-10-19-dead-code-frontend-build.txt) | TypeScript/Vite và cảnh báo chunk |
| [Source build](logs/2026-10-19-dead-code-source-build.txt) | Docker image mới |
| [Import lần đầu](logs/2026-10-19-dead-code-source-import-first-attempt.txt), [lần thành công](logs/2026-10-19-dead-code-source-import.txt) | Nguyên nhân lỗi harness và kết quả route |
| [Contract](logs/2026-10-19-dead-code-contract.txt) | 31 bảng, 319 cột |
| [Metric](logs/2026-10-19-dead-code-metrics.txt) | Tham chiếu tên metric ngoài file khai báo trong file runtime/config/test tracked |

Tool version: Ruff 0.17.1, Vulture 2.16, Knip 6.42.0, Node v22.22.1. Công cụ chạy tạm qua uv/npx; không thêm cấu hình lint hay dependency mới cho repo.

## Tác động runtime và giới hạn

Không reset dữ liệu, không restart/recreate service đang chạy, không register connector, không chạy migration hay gửi Chat thử. Build đã cập nhật image tag local của source-api; container đang chạy vẫn dùng image trước đó. Chỉ các container kiểm tra tạm được tạo rồi xóa.

Không chạy smoke ghi dữ liệu vì phần xóa không có caller và config active không đổi. Import/contract/build không thay thế kiểm thử mọi hành vi nghiệp vụ. Không kết luận toàn repo hết dead code; đặc biệt external consumer, dynamic SQL và entrypoint vận hành phải được xác minh theo từng hợp đồng.

## Việc tiếp theo nếu tiếp tục dọn

1. Đọc báo cáo maintenance trước khi xóa candidate còn lại.
2. API tương thích/auth mode chỉ xóa sau khi quyết định bỏ hỗ trợ và kiểm kê client; khi đổi đường ghi phải chạy smoke/auth.
3. Migration RawBLOB chỉ bỏ sau khi xác nhận tuổi/schema của volume cần hỗ trợ.
4. Ba metric cần quyết định giữ diagnostic, thêm consumer hoặc deprecate; không tự xóa.
5. Cleanup ứng dụng được tách khỏi thay đổi webhook để review/rollback rõ; khi rollback, chọn revert commit tương ứng trong Git history. Không dùng `git restore` cả working tree vì sẽ mất công việc webhook.
