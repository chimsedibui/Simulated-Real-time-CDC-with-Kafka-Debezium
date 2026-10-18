# Q&A 07 · Q&A 07 · Triển khai, DevOps và khôi phục

[Tài liệu dự án](../README.md) / [Phỏng vấn](README.md)

Đọc cùng [chương 07](../deployment/07-trien-khai-va-devops.md) và [bài thực hành backup](../operations/09-thuc-hanh-backup-clickhouse.md). Phần server là thiết kế đề xuất; không trình bày nó như hạ tầng đã triển khai. Những con số chi phí trong tài liệu là ước lượng tại thời điểm viết, cần kiểm tra lại trước khi mua.

## Môi trường và ranh giới hệ thống

### Q1. Dự án chạy được local thì đã sẵn sàng production chưa?

Chưa. Local chứng minh luồng nghiệp vụ, CDC và kiểm chứng dữ liệu có thể chạy cùng nhau trên một máy. Production còn cần ranh giới mạng, quản lý quyền, lịch backup, khôi phục đã diễn tập, migration không mất dữ liệu, tài nguyên và SLO phù hợp. Khi phỏng vấn, nói rõ phần nào đã chạy thật và phần nào mới có phương án; một sơ đồ HA không chứng minh failover đã đúng.

### Q2. Vì sao không bắt đầu bằng Kubernetes?

Compose đủ để học các ranh giới giao dịch, offset và watermark trên một máy, trong khi Kubernetes thêm control plane, networking và vận hành stateful workload. Nếu chưa hiểu restore Kafka và ClickHouse thì đổi orchestrator không giải quyết được mất dữ liệu. Kubernetes có giá trị khi có nhiều môi trường, đội vận hành và nhu cầu điều phối vượt khả năng một host; quyết định theo nhu cầu đó.

### Q3. Một VM khác laptop ở điểm gì quan trọng nhất?

VM có thể tồn tại lâu và tiếp nhận truy cập từ bên ngoài; giả định reset để sửa mọi thứ không còn hợp lệ. Cần biết đĩa nào chứa dữ liệu, ai có quyền deploy, cổng nào được public, secret được lấy từ đâu, và khôi phục sau mất máy thế nào. Mọi thay đổi phải có phương án dữ liệu chứ không chỉ có image mới.

### Q4. Tại sao nên tách local, CI và staging?

Local tối ưu cho phát triển và giữ dữ liệu thử; CI phải dựng được từ repo sạch với secret dùng một lần; staging diễn tập release và migration gần với môi trường đích. Dùng chung Kafka hoặc secret giữa các môi trường có thể khiến một gate reset xóa nhầm hệ thống đang chạy. Tên project, volume, topic, tài khoản và bucket phải có ranh giới rõ.

### Q5. Đổi `docker compose -p` đã đủ tách môi trường chưa?

Chưa. Repo có `container_name`, cổng host và đường bind mount cố định; chỉ đổi project có thể vẫn đụng container, cổng hoặc thư mục PostgreSQL. Cần kiểm tra compose đã render, override hoặc bỏ tên cố định và đổi đường dữ liệu, rồi mới chạy đồng thời. Đây là ví dụ vì sao một câu lệnh deploy đúng cú pháp vẫn có thể sai vận hành.

### Q6. Stateful và stateless khác nhau thế nào trong pipeline này?

Kafka, PostgreSQL, ClickHouse và object storage giữ trạng thái cần phục hồi; xóa container không đồng nghĩa xóa dữ liệu nếu volume còn. Source API có thể dựng lại từ image nhưng vẫn phụ thuộc PostgreSQL, identity và secret. Connect worker tương đối dễ thay thế vì config/offset/status nằm trong Kafka, nhưng PostgreSQL slot và worker group cũng phải còn đúng lịch sử. Stateless không có nghĩa là không có dependency.

### Q7. Cần dự toán RAM và disk thế nào?

Đo mức nền và mức đỉnh trong từng tình huống: seed, tải, ClickHouse merge, backup và replay. Chừa khoảng cho page cache, WAL tăng khi connector dừng, Kafka retention và bản backup cùng tồn tại với dữ liệu chính. Tổng kích thước bảng không phải toàn bộ dung lượng host: còn Docker image, log, metadata S3 và các volume giám sát.

### Q8. Giới hạn dữ liệu khoảng 10 GB có tự được đảm bảo không?

Không. Đây là kỳ vọng sử dụng, chưa có quota toàn dự án. Bronze giữ lịch sử, incremental có thể lớn do merge part, và backup nằm ngoài named volume. Cần đo dung lượng thật, đặt cảnh báo, chọn retention và prune phù hợp; dừng simulator không thu hồi dữ liệu đã ghi.

## Image, cấu hình và release

### Q9. Vì sao không dùng image `latest`?

Nó có thể trỏ sang bản khác giữa hai lần dựng dù git không đổi, nên khó tái hiện lỗi và rollback. Tag phiên bản giảm biến động; digest còn xác định chính xác nội dung image. Tuy vậy pin phiên bản không thay thế quy trình cập nhật bản vá và kiểm thử tương thích Kafka, librdkafka, converter và ClickHouse.

### Q10. Vì sao build một lần rồi promote image?

Nếu staging và production tự build lại từ cùng commit, dependency không khóa hoặc base image thay đổi có thể tạo hai artifact khác nhau. Build một artifact có định danh, kiểm tra nó rồi promote giúp bằng chứng kiểm thử gắn với đúng image triển khai. Vẫn phải quản lý cấu hình và migration riêng vì image giống nhau không làm hai môi trường giống nhau hoàn toàn.

### Q11. Cấu hình nào thuộc git, cấu hình nào là secret?

Topology, tên service, cấu trúc schema và ngưỡng alert thường thuộc git. Mật khẩu, pepper, private key, client secret và webhook URL phải cấp riêng. File `.env` không tự an toàn vì có tên như vậy: repo này chuyển secret sang file mount, còn `.env` giữ thiết lập không bí mật. Phải kiểm tra nội dung thực tế và nơi nó được log.

### Q12. Secret file mount có được mã hóa tự động không?

Không. Read-only mount hạn chế thay đổi từ container và tránh đưa mật khẩu vào compose environment, nhưng file host vẫn chứa giá trị thật. Quyền file, quyền truy cập Docker, backup secret và bảo vệ host vẫn quan trọng. Với server, có thể dùng secret manager hoặc file được giải mã khi deploy; không coi thư mục gitignored là cơ chế mã hóa.

### Q13. Chạy `setup-env.sh` lại có rotate mật khẩu không?

Không. Chế độ bình thường chỉ tạo những file còn thiếu và chuyển giá trị cũ từ `.env` nếu có; nó cố giữ tương thích với dữ liệu đã khởi tạo. Rotation cần thao tác riêng. Trong repo, `--rotate` gắn với reset môi trường mô phỏng, không phải migration mật khẩu an toàn cho database production.

### Q14. Vì sao đổi file password mà database vẫn dùng password cũ?

Nhiều image chỉ đọc biến khởi tạo khi volume rỗng. Trên volume đã có dữ liệu, tài khoản và password nằm trong database; cập nhật file cho client không tự chạy ALTER USER. Muốn đổi trên hệ thống sống phải cập nhật server và client có thứ tự, kiểm tra reconnect và có rollback. Reset volume chỉ phù hợp dữ liệu mô phỏng đã được cho phép bỏ.

### Q15. Đổi PII salt ảnh hưởng gì tới warehouse?

Cùng email hoặc số điện thoại sẽ tạo giá trị hash khác, khiến các phép nối theo hash giữa hai thời kỳ có thể đứt. Salt không phải khóa mã hóa để giải mã lại PII. Với demo reset đồng bộ có thể chấp nhận; với dữ liệu sống lâu cần phiên bản hóa chính sách hash và kế hoạch chuyển dữ liệu. Backup cũ giữ hash cũ ngay cả khi secret hiện tại đổi.

### Q16. Rotation có xóa secret đã nằm trong lịch sử git không?

Không. Rotation làm credential cũ hết giá trị ở hệ thống đang dùng; lịch sử git vẫn giữ byte cũ. Nếu cần xóa lịch sử phải có công việc riêng, cân nhắc các clone và phối hợp rewrite. Không dùng việc rewrite thay cho revoke credential đã lộ, vì người khác có thể đã sao chép nó.

### Q17. Vì sao init script không thay cho migration?

Init chạy khi volume mới; hệ thống lâu dài chủ yếu chạy trên volume cũ. Migration cần mô tả thay đổi từ phiên bản này sang phiên bản khác, thứ tự với ứng dụng và CDC, tính tương thích cùng cách phục hồi. Một file CREATE TABLE đẹp trong repo không chứng minh database đang chạy đã có schema đó.

### Q18. Rollback image có rollback schema không?

Không. Image cũ có thể không đọc được cột hay cấu trúc dữ liệu đã đổi. Dùng thay đổi tương thích theo nhiều bước: thêm trước, ứng dụng hỗ trợ cả hai, backfill, chuyển người đọc rồi bỏ cũ sau. Với thay đổi mất dữ liệu, phải dựa vào backup hoặc migration bù được kiểm thử, không chỉ `docker compose up` bằng tag trước.

## Mạng, xác thực và quyền

### Q19. Vì sao bind các cổng quản trị vào localhost?

Kafka UI, Connect REST, Control Center hoặc cổng ClickHouse có thể không có lớp xác thực đủ cho mạng công khai. Bind localhost thu hẹp nơi truy cập, sau đó dùng SSH tunnel hay mạng riêng để quản trị từ xa. Docker network nội bộ vẫn cần được xét như một miền tin cậy riêng; localhost không ngăn một tiến trình đã chiếm host.

### Q20. HTTPS ở gateway đã mã hóa toàn pipeline chưa?

Chưa. TLS tại Traefik bảo vệ chặng client tới gateway, còn API tới PostgreSQL, Debezium tới Kafka và ClickHouse tới Kafka có cấu hình riêng. Nếu cần mã hóa những chặng đó phải bật và kiểm thử ở từng client/server. Không mô tả Kafka đã SASL/TLS chỉ vì website dùng HTTPS.

### Q21. Chứng chỉ tự ký dùng local có hợp với public không?

Nó hữu ích để học trust chain và gọi API local khi đã tin CA, nhưng browser ngoài không tự tin CA đó. Trên domain công khai cần cơ chế cấp và renew chứng chỉ được client tin cậy. Private key phải bảo vệ, còn public certificate có thể phân phối cho client; hai loại file có mục đích khác nhau.

### Q22. Vì sao không cho dashboard dùng ClickHouse admin?

Dashboard chỉ cần đọc bảng được cấp quyền; dùng admin biến một lỗi query hoặc lộ credential thành khả năng xóa hay sửa warehouse. `grafana_reader` tách quyền theo vai trò. Vẫn phải kiểm tra các view và bảng nguồn cần đọc, timeout và tài nguyên query để một dashboard nặng không làm ingestion suy giảm.

### Q23. JWT hợp lệ có nghĩa được phép gọi mọi API không?

Không. Xác minh chữ ký, issuer, audience và hạn dùng xác định token đáng tin; scope, actor và ownership quyết định hành vi được phép. Một buyer có JWT hợp lệ vẫn không được giao đơn của buyer khác hoặc thao tác của seller. Phải kiểm thử cả xác thực lẫn phân quyền, đặc biệt các internal endpoint.

### Q24. Vì sao webhook của đối tác cần kiểm tra replay?

Chữ ký đúng chứng minh payload do bên giữ key ký, nhưng kẻ khác có thể gửi lại payload đã bắt được. Timestamp, cửa sổ chấp nhận, event ID và idempotency hạn chế việc xử lý lại. Bản ghi trạng thái cũng phải từ chối chuyển trạng thái không hợp lệ; một lớp kiểm chữ ký không thay thế state machine.

## CI, CD và bằng chứng kiểm thử

### Q25. CI của dự án đang chứng minh những gì?

Có workflow contract schema, observability và smoke. Contract kiểm tra schema nguồn với silver và connector; observability kiểm cấu hình, rule tests, bridge và artifact dashboard; smoke dựng stack sạch, tạo dữ liệu và kiểm tra end-to-end. Một workflow xanh chỉ chứng minh bộ kiểm tra đó trên commit và môi trường cụ thể. Cần đọc kết quả thật, không suy ra smoke xanh từ observability xanh.

### Q26. Vì sao smoke cần database sạch?

Schema, init script và thứ tự dependency chỉ bộc lộ một số lỗi khi volume rỗng. Volume dev có thể đã được vá tay hoặc giữ sẵn role cũ, khiến code không tự dựng được từ repo. Smoke vừa kiểm tra khả năng bootstrap vừa kiểm tra luồng CDC; vì có reset, không chạy nó vào project chứa dữ liệu cần giữ.

### Q27. `quick` có phải kiểm tra chỉ đọc không?

Không. Nó có thể rebuild service, chạy API/stress tests và simulator, nên có ghi dữ liệu và thay đổi clock. Chữ quick chỉ nói phạm vi và thời gian tương đối. Khi một phiên khác đang kiểm tra dashboard hay restore, cần phối hợp trước khi dùng gate này, dù nó giữ volume.

### Q28. Vì sao kiểm tra dữ liệu phải đợi scheduler sau tải?

Khi clock chạy ×1440 rồi hạ về ×1, những deadline vừa qua dưới clock nhanh có thể còn chờ job xử lý. Grace tính theo tốc độ mới nhỏ đi rất nhiều, khiến truy vấn snapshot bắt đúng khoảng chưa ổn định. Cần đợi có giới hạn trên kết nối live trước khi mở snapshot kiểm tra; nếu hết hạn mà vẫn còn backlog thì lỗi phải giữ nguyên. Tăng grace vô hạn sẽ che scheduler bị kẹt.

### Q29. Tại sao đợi trên cùng transaction REPEATABLE READ không giải quyết được backlog?

Snapshot cố định nên các lần đọc sau vẫn thấy trạng thái trước khi job commit. Polling phải dùng transaction mới hoặc kết nối autocommit, sau đó mới chụp snapshot dùng cho các invariant liên quan. Đây là vấn đề cách quan sát dữ liệu, không nhất thiết là job không chạy.

### Q30. Smoke pass có chứng minh Connect replay an toàn không?

Không nếu gate đó chưa chủ động tạo điều kiện offset được phát lại. Cần kịch bản restart hoặc rollback offset có kiểm soát, đo lặp tại bronze, kiểm tra silver, watermark và mart. Khớp số dòng một lần chưa chứng minh mọi kiểu transaction metadata trùng đều được xử lý; ghi rõ ca đã kiểm chứng và giới hạn của nó.

### Q31. CD khác CI ở đâu?

CI tạo bằng chứng thay đổi có thể tích hợp; CD đưa artifact được chọn tới môi trường cụ thể với cấu hình, secret, migration và health checks. Dự án có CI không có nghĩa push là deploy lên server. Nếu thêm CD, cần giới hạn credential deploy, xác định môi trường và quy trình rollback trước.

### Q32. Vì sao không đưa secret thật vào CI smoke?

CI chỉ cần dựng môi trường dùng một lần; credential thật làm tăng tác động nếu log, artifact hay pull request bị lạm dụng. Webhook Chat thật còn có thể gửi thông báo ngoài ý muốn. Tạo secret ngẫu nhiên, placeholder webhook và xóa môi trường sau kiểm tra giúp bằng chứng độc lập với tài khoản production.

## Backup, restore và replay

### Q33. Backup và HA giải quyết hai vấn đề khác nhau thế nào?

HA giảm thời gian ngừng khi node lỗi; backup cung cấp trạng thái quá khứ để khôi phục khi mất dữ liệu hoặc thao tác sai. Replica có thể sao chép cả lệnh xóa nhầm, còn backup không tự giữ dịch vụ đang chạy khi máy chết. Với local này ưu tiên ClickHouse backup và diễn tập, còn HA chưa triển khai.

### Q34. Full và incremental khác nhau thế nào trong ClickHouse?

Full chứa bộ dữ liệu cần phục hồi độc lập; incremental tham chiếu bản base và chỉ lưu phần cần thêm. ClickHouse lưu dữ liệu theo part bất biến và có background merge, nên thay đổi ở mức part quyết định kích thước backup, không chỉ số row mới. Mất base làm incremental không dùng được dù thư mục incremental vẫn còn.

### Q35. Vì sao không copy trực tiếp thư mục ClickHouse đang chạy?

Trong lúc copy, metadata, part, merge và S3 metadata có thể thay đổi, khiến bản sao không phải trạng thái phục hồi hợp lệ. BACKUP gốc của ClickHouse xử lý dữ liệu theo cơ chế engine, nhưng tính nhất quán nghiệp vụ xuyên bảng vẫn cần xem xét. Repo tạm dừng ingestion và warehouse-jobs để giảm thay đổi khi chụp.

### Q36. Vì sao backup phải dừng cả Kafka tables lẫn warehouse-jobs?

Nếu chỉ dừng job, bronze/silver vẫn tiếp tục nhận dữ liệu giữa các bảng. Nếu chỉ dừng ingestion, job có thể tạo mart, snapshot hoặc ops record trong lúc BACKUP đang chụp các database. Dừng hai phía tạo trạng thái tĩnh hơn cho warehouse. API và Debezium vẫn có thể chạy vì Kafka giữ sự kiện để đọc bù.

### Q37. Restore ClickHouse có cần rewind offset của Debezium không?

Không trong kịch bản cùng lịch sử Kafka. Sự kiện đã nằm ở Kafka; chỉ consumer group của ClickHouse cần quay về điểm sau backup. Rewind Connect có thể làm Debezium phát lại source change vào Kafka ở offset mới, tạo kiểu trùng khác. Phân biệt source offset của connector với consumer offset của warehouse là điểm quan trọng khi giải thích khôi phục.

### Q38. Vì sao phải lưu replay position cho cả partition chưa có dữ liệu trong bronze?

Không có dòng có thể nghĩa là partition trống, chưa ingest được hoặc chưa có message phù hợp bộ lọc. Nếu lúc restore mặc định lấy earliest hiện tại, retention có thể đã xóa những message tồn tại lúc backup, gây mất dữ liệu âm thầm. Lưu earliest tại thời điểm backup cho phép phát hiện sự trôi đó và từ chối đọc bù không đầy đủ.

### Q39. Kafka cluster ID có đủ chứng minh cùng lịch sử không?

Không trong repo này vì cluster ID được cấu hình cố định và có thể tái dùng khi reset volume. Topic ID thay đổi khi topic bị tạo lại nên phát hiện được lịch sử Kafka mới. Script còn kiểm tra offset retained; đúng topic ID vẫn không cứu được phần dữ liệu đã hết retention.

### Q40. Tại sao kiểm tra retention phải xảy ra trước DROP database?

Nếu đã xóa warehouse rồi mới phát hiện không thể replay, người vận hành mất trạng thái đang chạy và phải xử lý restore dở. Preflight không bảo đảm mọi I/O sau đó thành công, nhưng nó loại được những điều kiện chắc chắn không phục hồi được trước khi thay dữ liệu. Lỗi storage hay Kafka thay đổi trong lúc restore vẫn cần quy trình phục hồi và tránh thao tác cạnh tranh.

### Q41. `--force` nên dùng khi nào?

Khi cần khôi phục bản cũ để xem hoặc điều tra, chấp nhận warehouse ở trạng thái offline. Ở bản script được sửa trong lượt Codex, force không nối Kafka consumer và không bật warehouse-jobs, kể cả ID Kafka còn khớp. Nó không hứa bù được đoạn đã mất retention và không cho phép trộn lịch sử nguồn đã reset với backup cũ.

### Q42. Restore thành công có nghĩa dữ liệu đã bắt kịp chưa?

Chưa. RESTORE hoàn tất chỉ đưa bảng về trạng thái backup. Kafka lag còn phải về mức bình thường, watermark phải đi tiếp, snapshot/mart phải được làm mới và reconcile với PostgreSQL phải khớp. Cần đo cả thời gian đọc bù để đánh giá RTO; vài giây RESTORE không đại diện cho thời gian dịch vụ hữu ích trở lại.

### Q43. Làm sao kiểm chứng restore tốt hơn việc so tổng số row?

So số khóa và tổng tiền theo trạng thái ở bảng nghiệp vụ, so silver với PostgreSQL, mart với gold tại cùng watermark và kiểm tra transaction còn bị giữ. Bronze có thể nhận thêm heartbeat hay job event nên tổng row thay đổi không tự chứng minh sai. Ngược lại, tổng row bằng nhau có thể che một row mất và một row trùng; dùng cả fingerprint và kiểm tra định nghĩa nghiệp vụ.

### Q44. Backup trên host có bảo vệ khi chạy `down -v` không?

Thư mục bind mount `warehouse/backups` tồn tại qua việc xóa named volume. Nhưng nó vẫn nằm cùng đĩa và có thể bị người dùng xóa trực tiếp; mất VM hay ổ đĩa có thể mất cả hai. Sao chép off-site phải giữ dữ liệu backup, manifest, offset, topic IDs và full base của incremental cùng nhau.

### Q45. Có thể khôi phục backup cũ sau khi reset PostgreSQL và Kafka rồi chạy tiếp không?

Không thể coi đó là cùng một lịch sử. PostgreSQL có WAL và trạng thái mới, Kafka có topic IDs mới, còn backup mang hash và transaction cũ. Dùng offline restore để xem dữ liệu cũ, hoặc có một kế hoạch chuyển dữ liệu riêng với kiểm chứng đầy đủ. Không chỉ bỏ check ID rồi cho consumers chạy.

### Q46. RPO và RTO nên được giải thích bằng ví dụ nào?

Nếu backup mỗi đêm và mất toàn bộ Kafka giữa ngày, phần sau backup có thể mất đối với warehouse: đó là khoảng dữ liệu có nguy cơ mất, liên quan RPO. Nếu khôi phục file mất 5 phút nhưng đọc bù mất 20 phút, dịch vụ cần khoảng 25 phút để trở lại mục tiêu: liên quan RTO. Các con số phải dựa vào kịch bản và phép đo, không chỉ dựa vào lịch backup.

## Sự cố và quyết định vận hành

### Q47. Kafka dừng thì PostgreSQL có tiếp tục ghi được không?

Có thể tiếp tục trong một khoảng thời gian; Debezium không publish được thì replication slot giữ WAL chưa xác nhận. Khi WAL vượt giới hạn hoặc đĩa đầy, tình hình xấu hơn; `max_slot_wal_keep_size` có thể khiến slot không còn tiếp tục được sau checkpoint. Không hứa rằng một lần dừng dài sẽ luôn hồi phục không mất dữ liệu. Theo dõi WAL lag, thời gian dừng và khả năng resnapshot.

### Q48. ClickHouse dừng thì có mất sự kiện không?

Nếu Kafka còn giữ chúng và consumer offset còn đúng, ClickHouse có thể đọc bù khi lên. Nếu thời gian dừng vượt retention, hoặc volume Kafka cũng mất, bảo đảm đó không còn. Restore backup và đối chiếu nguồn là quy trình phục hồi, khác với việc restart một container.

### Q49. Có thể scale Connect worker để tăng tốc connector PostgreSQL không?

Không tự động. Connector PostgreSQL này thường chạy một task đọc một luồng logical replication; thêm worker chủ yếu tạo chỗ failover cho task. Muốn tăng throughput phải xác định bottleneck source, producer, broker hay warehouse. Tăng worker còn làm rebalance và replay trở thành ca cần kiểm thử.

### Q50. Vì sao ClickHouse HA khó hơn thêm replica rồi đọc bất kỳ node nào?

Các bảng replicate độc lập; transaction metadata có thể tới một replica trước dữ liệu của bảng khác. Nếu watermark chỉ nhìn thấy metadata mà chưa đủ row, báo cáo có thể sai. Cần thiết kế đọc/ghi và kiểm chứng watermark phù hợp replication, không chỉ kiểm tra node còn sống. Vì vậy HA ClickHouse là thay đổi tính đúng của pipeline cần diễn tập riêng.

### Q51. Khi đĩa đầy nên làm gì trước?

Xác định vùng đầy là log, WAL, Kafka, ClickHouse hay backup; tạo chỗ trống bằng artifact có thể tái tạo hoặc bản backup đã có bản an toàn khác. Không xóa tùy tiện file trong database hay log segment Kafka. Sau đó xử lý nguyên nhân như connector chết giữ WAL, log không rotation hoặc backup không prune; chỉ tăng disk mà không tìm nguyên nhân sẽ lặp lại sự cố.

### Q52. Một health check xanh có đủ kết luận pipeline khỏe không?

Không. HTTP 200 chỉ cho biết tiến trình hoặc dependency cơ bản trả lời. Connector có thể RUNNING nhưng lag cao; exporter trả 200 nhưng dữ liệu cache đã cũ; mart có thể query được nhưng không refresh. Cần kết hợp availability, freshness, completeness và correctness: tình trạng service, độ trễ, watermark, ingest errors và reconciliation.

### Q53. Khi alert kêu, nên restart ngay không?

Thu thập trạng thái, log và mốc thời gian trước để phân biệt lỗi dependency, backlog hay dữ liệu sai. Restart có thể làm mất bằng chứng hoặc tăng replay. Khi biết restart là biện pháp phù hợp, ghi lại tác động và kiểm chứng lag/watermark sau đó; một container xanh lại chưa khép sự cố nếu số liệu còn lệch.

### Q54. Dọn dead code cần bằng chứng gì?

Tìm caller ở code, shell, compose, provisioning, SQL string, alert và tài liệu vận hành. Các framework dùng decorator hay tên chuỗi nên tìm gọi trực tiếp không đủ. Xóa theo đợt nhỏ, giữ test độc lập với production logic và chọn gate theo tác động; đổi schema hoặc đường ghi cần smoke, không chỉ lint.

### Q55. Có nên hợp nhất công thức metric giữa production và validator cho đỡ lặp?

Nếu validator gọi đúng công thức production, hai bên có thể cùng sai mà vẫn khớp. Có thể chia sẻ metadata thuần túy như tên bảng khi có lý do rõ, nhưng giữ cách tính kiểm chứng độc lập cho tiền, trạng thái và invariant quan trọng. Mục tiêu cleanup là bớt khó bảo trì mà vẫn giữ khả năng phát hiện lỗi.

### Q56. Bàn giao một session làm dở nên ghi những gì?

Ghi mục tiêu, commit gốc, file đã thay, thay đổi nào thuộc người khác, lệnh và kết quả kiểm thử, thao tác dữ liệu đã thực hiện và việc chưa làm. Phân biệt dự định với bằng chứng đã có, chỉ ra log lưu ở đâu và không chép secret. Session tiếp theo phải đọc git hiện tại vì trạng thái có thể đã đổi kể từ lúc ghi log.

## Thực hành chuyển từ local lên staging

### Q57. Vì sao không thay tất cả localhost bằng domain?

Browser, container và operator có không gian mạng khác nhau. Browser cần public URL và TLS; container cần DNS nội bộ như kafka:9092; operator nên vào qua VPN/SSH. Thay đồng loạt làm client nội bộ đi vòng Internet hoặc khiến localhost trỏ nhầm container. Lập bảng ai gọi ai trước khi tham số hóa.

### Q58. Thêm BASE_DOMAIN vào env đã đổi được router Traefik chưa?

Chưa nếu file dynamic.yml được mount nguyên văn. Compose nội suy cấu hình Compose, không tự render mọi file bên trong container. Cần render template hoặc tạo file riêng, validate và kiểm tra Host/CORS/TLS thực tế. Biến env chỉ có ý nghĩa khi thành phần sử dụng thực sự đọc nó.

### Q59. Vì sao frontend lên server vẫn trỏ Grafana vào port 3000?

Frontend hiện tự ghép hostname browser với :3000. Domain HTTPS của Grafana cần được cấu hình riêng; đổi mạng server không sửa JS đã build. Phải chọn build-time config hay runtime config rồi kiểm tra bundle và request thật từ browser.

### Q60. Ufw đang deny incoming thì database publish bằng Docker đã an toàn chưa?

Chưa thể kết luận. Docker published ports có đường xử lý firewall riêng và có thể đi trước ufw. Dùng cloud firewall, hạn chế port publish, xét IPv6 và kiểm tra từ ngoài; khi thêm service phải kiểm lại compose render và cổng thực. Không tắt cơ chế firewall Docker một cách tùy tiện để xử lý triệu chứng.

### Q61. Deploy và smoke gate có thể dùng chung một script không?

Không được mang bước reset của smoke vào server sống lâu. Smoke kiểm bootstrap trên môi trường tạm; deployment giữ dữ liệu, chạy migration và verify có giới hạn. Nếu cần reset staging thì phải là thao tác riêng có lựa chọn rõ, không là side effect của mỗi release.

### Q62. Có thể trace một HTTP request qua WAL tự động không?

Không chỉ bằng cài HTTP instrumentation. WAL và row event không tự chứa trace context của request. Cần thiết kế correlation qua outbox/header và capture phù hợp, giữ schema/PII an toàn. Liên kết span bất đồng bộ khác việc giữ nguyên một span HTTP chờ mart hoàn thành.

### Q63. Vì sao không dùng order_id làm label metric để dễ tìm?

Mỗi đơn tạo một giá trị label mới, làm số time series tăng theo dữ liệu và tốn bộ nhớ/lưu trữ. Dùng metric theo route/service/env hữu hạn, còn order/request/trace ID để ở log hoặc trace rồi liên kết từ đó. Không vì dữ liệu chỉ simulation mà bỏ qua bài học cardinality.

### Q64. Cancel deployment cũ khi có commit mới có luôn tốt không?

Không nếu deployment cũ đang migration hoặc đổi state. Giới hạn concurrency theo môi trường, tránh cancel ngang bước có side effect và có lock server. Build/test cũ có thể hủy để tiết kiệm; deploy cần điểm dừng/phục hồi rõ ràng. Chỉ nhìn workflow status không biết database đã đổi đến đâu.

### Q65. Một VPS 16 GB có chắc đủ cho stack này không?

Không. Đó là cấu hình ứng viên, còn cần xét loại CPU, IO, workload, heap/cache, giới hạn container và peak merge/backup. Dữ liệu dưới 10 GB không có nghĩa chỉ cần disk 10 GB. Ghi phép đo staging riêng; không dùng số p95 trên laptop như cam kết của VM mới.

### Q66. Bằng chứng nào cho thấy đã tự triển khai được môi trường thật?

Có domain/TLS và xác thực đúng; release digest truy nguyên; restart không mất volume; synthetic flow đi tới mart; dashboard, log và trace có thể điều tra; backup off-site restore đã thử; có bước rollback và kiểm tra port từ ngoài. Một tài liệu thiết kế hoặc container xanh trên máy local chưa đủ để tự nhận đã làm những phần đó. Xem [lộ trình thực hành](../deployment/first-deployment.md).
