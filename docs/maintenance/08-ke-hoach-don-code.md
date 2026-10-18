# 08 · Kế hoạch dọn code và cấu hình cũ

[Tài liệu dự án](../README.md) / [Bảo trì](README.md)

## Mục tiêu và ranh giới

Giảm những chỗ phải sửa đồng thời khi thay đổi pipeline, loại bỏ đường chạy đã bỏ, và giữ generator cùng artifact nhất quán. Mỗi đợt phải có bằng chứng người dùng của code đó đã chuyển sang đường mới. Không xóa một hàm chỉ vì không tìm thấy lời gọi trực tiếp: FastAPI đăng ký handler qua decorator, ClickHouse materialized view qua DDL, Docker qua entrypoint, Prometheus qua tên metric.

Đã thực hiện một lượt rà soát và cleanup có bằng chứng ngày 19/10/2026; xem [kết quả chi tiết](dead-code-audit-2026-10-19.md). Các đợt dưới đây là khung bảo trì, không đồng nghĩa mọi ứng viên đều là dead code hoặc đã được xóa.

## Những gì đã dọn trong lịch sử

WarehousePanel và API chỉ phục vụ nó đã được xóa khi analytics chuyển sang Grafana. Một số import, constant, proxy seed và metric không còn người đọc cũng đã được dọn trước đó. Không khôi phục chúng chỉ để làm tài liệu cũ chạy đúng; cập nhật tài liệu theo kiến trúc hiện tại.

## Đợt 1: đường đọc tài liệu và artifact sinh tự động

**Vấn đề:** README cũ gộp nhiều lớp, khiến phần mới và hướng dẫn cũ dễ lệch nhau. Các file JSON dashboard và SQL silver được sinh từ chương trình nhưng trông giống file nguồn độc lập.

**Cách làm:** dùng README làm mục lục; giữ nội dung nghiệp vụ trong chương 01–07, backup thực hành trong chương 09. Với generator, xác định file nào là đầu vào chuẩn và file nào phải commit để provisioning dùng ngay. Không xóa dashboard JSON chỉ vì có script sinh nó: Grafana nạp chính JSON đó khi khởi động.

**Xác nhận:** kiểm tra link tương đối; chạy generator rồi kiểm tra diff; truy vấn dashboard qua Grafana. Nếu generator tạo khác artifact đang lưu thì giải quyết nguyên nhân trước khi commit.

**Hoàn thành khi:** người đọc đi từ mục lục tới hướng dẫn đang dùng; mỗi artifact có một nguồn sinh rõ ràng.

## Đợt 2: API tương thích và chế độ auth

**Ứng viên cần khảo sát:** Source API còn mount router không có `/v1` khi `AUTH_MODE` không phải `enforce`. Đây là đường tương thích có điều kiện, chưa đủ bằng chứng là dead code. Control Center, simulator, validator và hướng dẫn curl phải được kiểm tra cùng lúc.

**Cách làm:** thống kê caller theo path, đối chiếu OpenAPI và test với từng AUTH_MODE được hỗ trợ. Nếu quyết định chỉ giữ enforce thì ghi rõ quyết định đó rồi xóa nhánh cũ cùng cấu hình, hướng dẫn và test tương ứng. Không chỉ xóa nhánh middleware mà để client vẫn gọi đường cũ.

**Xác nhận:** API/stress, auth tests, seed từ volume sạch, checkout, callback payment/logistics và refresh token. Mọi thay đổi đường ghi phải qua smoke gate.

## Đợt 3: logic warehouse và script vận hành

**Ứng viên:** danh sách bảng được lặp ở contract, silver generator, job, validator và exporter; logic đọc secret lặp ở nhiều tiến trình; danh sách consumer trong backup script.

Lặp không tự động là lỗi. Validator có lý do tính độc lập với production để bắt sai định nghĩa. Không trích chung công thức GMV từ production vào bài kiểm tra rồi coi việc hai bên bằng nhau là bằng chứng.

**Cách làm:** chỉ hợp nhất metadata thuần túy khi có nguồn chuẩn rõ ràng. Giữ logic kiểm chứng độc lập. Kiểm tra script vận hành có còn gọi đúng init SQL, tên service và tài khoản; không bỏ script chỉ vì compose không gọi trực tiếp, vì người vận hành có thể gọi từ terminal.

**Xác nhận:** contract schema, generator diff, backup full/incremental, restore drill, reconcile 31 bảng và so sánh mart với gold tại cùng watermark.

## Đợt 4: metric, alert và dashboard

Trước khi xóa metric, tìm tên của nó trong exporter, PromQL, alert tests, dashboard generator, dashboard JSON và runbook. Metric không dùng trong dashboard vẫn có thể là đầu vào alert hoặc recording rule. Trước khi xóa mart, tìm cả SQL string trong Python và các query Grafana.

**Xác nhận:** promtool, alert rule tests, test bridge không gửi thông báo thật, generator diff và verifier query qua Grafana. Không trigger Google Chat chỉ để kiểm tra code nếu chưa có nhu cầu gửi tin.

## Đợt 5: dependency và file phát sinh

Kiểm tra Python requirements, frontend package và Docker build context. Dùng `rg` để lấy bằng chứng sử dụng; xem cả dynamic import, executable gọi từ shell, build tool và entrypoint. Build cache, `__pycache__`, log, backup và secret phải được gitignore. Lockfile và file provisioning được commit là artifact cần tái lập, không phải cache cần xóa.

**Xác nhận:** build service liên quan từ Dockerfile; chạy đúng bộ kiểm tra theo phạm vi thay đổi. Không nâng tất cả dependency trong cùng commit cleanup vì sẽ khó xác định nguyên nhân nếu gate lỗi.

## Cách chia commit và rollback

Một commit cho một đợt hoặc một hành vi. Ghi rõ file bỏ, caller đã chuyển đi đâu và bằng chứng kiểm thử. Tách đổi hành vi khỏi đổi định dạng. Với cleanup ứng dụng, revert commit là đủ; với schema và dữ liệu, cần migration hoặc restore đã thử trước. `down -v` là reset môi trường mô phỏng, không phải cách rollback trên server có dữ liệu thật.

Ưu tiên sau lượt rà soát: xử lý các nhánh tương thích theo quyết định hỗ trợ và bằng chứng caller; các kết quả backup/restore, smoke và tài liệu đã thực hiện được ghi trong nhật ký bàn giao. Chưa có cơ sở xóa đồng loạt phần HA được mô tả trong tài liệu vì đó là kế hoạch, không phải code chạy thừa.
