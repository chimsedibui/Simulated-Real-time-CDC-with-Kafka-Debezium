# Q&A 02 · Source API, Identity và bảo mật

[Tài liệu dự án](../README.md) / [Phỏng vấn](README.md)

Bộ câu hỏi ôn tập cho [02-source-api-va-bao-mat.md](../business/02-source-api-va-bao-mat.md). Mức độ: **Cơ bản** (biết là gì), **Trung bình** (giải thích cơ chế), **Nâng cao** (trade-off, tấn công, thiết kế mở). Luồng nghiệp vụ chi tiết: [01-nghiep-vu-va-du-lieu-nguon.md](../business/01-nghiep-vu-va-du-lieu-nguon.md).

## Mục lục

1. [Thiết kế Source API](#1-thiết-kế-source-api) (Q1–Q12)
2. [Transaction, lock, deadlock](#2-transaction-lock-deadlock) (Q13–Q24)
3. [Idempotency và outbox](#3-idempotency-và-outbox) (Q25–Q34)
4. [Identity: đăng ký, đăng nhập, mật khẩu](#4-identity-đăng-ký-đăng-nhập-mật-khẩu) (Q35–Q46)
5. [Token, JWT, refresh, client, exchange](#5-token-jwt-refresh-client-exchange) (Q47–Q61)
6. [Phân quyền trong API](#6-phân-quyền-trong-api) (Q62–Q73)
7. [Webhook đối tác](#7-webhook-đối-tác) (Q74–Q81)
8. [Gateway](#8-gateway) (Q82–Q87)
9. [Secret và database](#9-secret-và-database) (Q88–Q99)
10. [Simulator, test, vận hành](#10-simulator-test-vận-hành) (Q100–Q104)
11. [Kịch bản tấn công và câu hỏi mở](#11-kịch-bản-tấn-công-và-câu-hỏi-mở) (Q105–Q116)

---

## 1. Thiết kế Source API

### Q1. Source API là gì và vai trò của nó trong pipeline? [Cơ bản]
Source API là một service FastAPI, là **nơi duy nhất ghi vào** database nghiệp vụ `enterprise_db`. Simulator, bộ test và identity service đều phải đi qua nó. Nó áp các quy tắc của sàn thương mại điện tử (máy trạng thái đơn, tồn kho, voucher…) và ghi kèm một domain event vào outbox. Debezium sau đó đọc WAL của PostgreSQL để đưa thay đổi sang Kafka và warehouse.

### Q2. Vì sao không để simulator ghi thẳng vào PostgreSQL? [Trung bình]
Nếu nhiều nơi ghi thẳng, sẽ có dòng dữ liệu không qua quy tắc nghiệp vụ và không có sự kiện đi kèm, làm dữ liệu ở warehouse "vô lý" (đơn đổi trạng thái không có lịch sử, tồn kho đổi không có movement). Gom việc ghi vào một API giúp quy tắc nằm một chỗ, mọi thay đổi có event, và chỉ cần bảo vệ một cửa. Nó cũng mô phỏng đúng hệ thống thật, nơi database OLTP nằm sau một backend. Kể cả dữ liệu danh mục ban đầu cũng được tạo qua API, `init.sql` không seed dữ liệu nghiệp vụ.

### Q3. "Endpoint kiểu command" khác CRUD thế nào? Cho ví dụ. [Cơ bản]
Command biểu diễn một **ý định nghiệp vụ** thay vì sửa một trường. Ví dụ thay vì PATCH đơn hàng với `status=READY_TO_SHIP`, người bán gọi `POST /v1/orders/{id}/pack`; thay vì PUT tồn kho, gọi `POST /v1/skus/{id}/restock`. Nhờ đó API biết actor là ai, kiểm tra bước chuyển trạng thái có hợp lệ, tạo kèm kiện hàng và mã vận đơn, và ghi lịch sử có lý do. Client không thể nhảy cóc trạng thái bằng một giá trị tùy ý.

### Q4. Vì sao các route nằm dưới tiền tố /v1? [Cơ bản]
Version trong URL cho phép thay đổi hợp đồng API sau này (ra /v2) mà không phá client cũ. Trong dự án, khi bật xác thực, các route được chuyển sang /v1; route không version chỉ còn khi `AUTH_MODE` khác `enforce` (để client cũ chạy trong giai đoạn chuyển đổi) và bị ẩn khỏi OpenAPI. Ở `enforce` chúng biến mất, có test kiểm tra.

### Q5. Route `/v1/internal/customers` và `/v1/internal/sellers` dùng để làm gì? [Trung bình]
Đây là route **provisioning**: identity service gọi chúng khi một người đăng ký, để tạo bản ghi khách hàng hoặc seller + shop + ví trong database nghiệp vụ. Chúng đòi scope `customers:provision` / `sellers:provision`: client `identity` có hai scope này, và mọi principal có scope `admin` cũng qua được vì kiểm tra scope luôn cho admin đi qua (ví dụ client `sim-orchestrator` và `quality`, hay tài khoản admin). Người mua không gọi được, kể cả qua gateway (gateway có định tuyến /v1/ nhưng API sẽ trả 403 vì thiếu scope).

### Q6. Lỗi của API trả về dạng gì? [Cơ bản]
Mọi lỗi có cùng hình dạng: trường `detail` chứa `code` máy đọc được (như `out_of_stock`, `order_not_found`), `message` cho người đọc, và có thể thêm chi tiết (ví dụ danh sách SKU thiếu cùng số lượng còn). Client nên rẽ nhánh theo `code`, không theo message. 401 có header `WWW-Authenticate`, 503 có `Retry-After`.

### Q7. Phân biệt 401, 403, 404, 409, 422, 503 trong API này. [Trung bình]
401: không chứng minh được danh tính (thiếu/sai token, chữ ký webhook sai hoặc cũ). 403: có danh tính nhưng thiếu quyền chung (thiếu scope) hoặc đối tác báo về giao dịch không phải của mình. 404: không tồn tại hoặc thuộc về người khác. 409: vi phạm quy tắc nghiệp vụ (hết hàng, chuyển trạng thái sai, voucher hết). 422: dữ liệu vào sai, hoặc dùng lại Idempotency-Key cho request khác. 503: deadlock/serialization (`retry`) hoặc chờ connection DB quá 10 giây (`db_busy`) — được phép thử lại.

### Q8. Client nên xử lý retry thế nào? [Trung bình]
4xx là lỗi của client, gửi lại nguyên xi sẽ nhận lại lỗi cũ. 5xx, 503 và lỗi mạng thì thử lại với backoff, **giữ nguyên Idempotency-Key**, để nếu lần đầu thực ra đã commit thì lần sau chỉ replay phản hồi. Simulator làm đúng như vậy: thử lại tối đa 3 lần (tức tối đa 4 lần gọi tính cả lần đầu), backoff lũy thừa, cùng khóa. Riêng 401 thì lấy token mới một lần rồi thử lại.

### Q9. API chạy bao nhiêu worker và vì sao? [Trung bình]
6 worker uvicorn (image mặc định 2, compose đặt 6). Python chỉ dùng một core mỗi process do GIL; với 2 worker, load test chạm trần khoảng 380 request/giây vì CPU hai process bão hòa chứ không phải PostgreSQL. Tăng lên 6 giúp dùng nhiều core hơn. Mỗi worker là một process riêng, có pool, scheduler và cache JWKS riêng.

### Q10. Connection pool được cấu hình thế nào và vì sao cần semaphore? [Nâng cao]
Mỗi worker có pool tối đa 12 connection, 6 × 12 = 72 trên 200 connection tối đa của PostgreSQL, chừa chỗ cho Debezium, monitoring, identity, test. Pool của psycopg2 khi cạn sẽ ném lỗi ngay, nên API thêm một semaphore cùng kích thước: request xếp hàng chờ tối đa 10 giây, quá thì 503 `db_busy`. Đây là backpressure có kiểm soát: dưới tải đột biến, request chờ một chút thay vì lỗi ngẫu nhiên.

### Q11. Job nền chạy ở đâu, và tại sao chạy trong 6 worker cùng lúc mà không sai? [Nâng cao]
Mỗi worker khởi một thread scheduler, cứ 2 giây đồng hồ thật một lượt: hết hạn checkout chưa trả tiền, hủy đơn trễ SLA, tự hoàn tất đơn đã giao, bỏ giỏ, tự duyệt trả hàng, đóng yêu cầu bị từ chối, cộng dồn ví và payout, dọn Idempotency-Key. Mỗi item chạy trong transaction riêng và **kiểm lại trạng thái dưới lock**; worker đến sau thấy trạng thái đã đổi thì bỏ qua (lỗi nghiệp vụ hoặc deadlock cũng bị bỏ qua). Việc cộng dồn ví dùng `SKIP LOCKED` để chia việc. Trade-off: đơn giản, không thêm service, đổi lại có việc trùng lặp nhỏ; tắt được bằng `RUN_JOBS=false`.

### Q12. Business clock là gì? Vì sao token và webhook không dùng nó? [Trung bình]
Business clock là hàm `business_now()` trong PostgreSQL, chạy nhanh gấp `speed` lần đồng hồ thật (simulator đặt 240×) để một ngày mô phỏng trôi trong vài phút; đổi tốc độ sẽ neo lại nên thời gian không nhảy. Mọi mốc nghiệp vụ (hạn thanh toán, SLA, auto-complete) dùng nó. Nhưng thứ liên quan tới thời gian thật — hạn token, cửa sổ chữ ký webhook 5 phút, TTL Idempotency-Key, khóa tài khoản — dùng đồng hồ thật. Nếu dùng business clock ở 240×, cửa sổ chữ ký 5 phút sẽ thành 20 giờ thật, mở rộng cơ hội replay.

---

## 2. Transaction, lock, deadlock

### Q13. "Một command = một transaction" nghĩa là gì? [Cơ bản]
Mỗi command lấy một connection, chạy toàn bộ logic trong một transaction, commit nếu thành công và rollback nếu có bất kỳ lỗi nào. Ví dụ checkout tạo checkout, nhiều đơn, order items, giữ tồn kho, dùng voucher, tạo payment, ghi sự kiện — tất cả cùng có hoặc cùng không. Không có trạng thái làm dở để phải dọn.

### Q14. Vì sao kiểm tra phân quyền nằm bên trong transaction lại hợp lý? [Trung bình]
Nhiều quyết định chỉ có thể đưa ra sau khi đọc dữ liệu: muốn biết SKU có phải của shop người gọi không thì phải đọc SKU. Đọc nó với lock rồi kiểm tra ngay trong transaction đảm bảo chủ sở hữu không đổi giữa lúc kiểm và lúc ghi. Nếu bị từ chối, lỗi làm transaction rollback nên không có gì bị ghi — kể cả dòng Idempotency-Key đã claim.

### Q15. Khi nào API dùng `FOR UPDATE`? [Cơ bản]
Khi sắp đổi trạng thái một dòng "chủ": đơn hàng, checkout, payment, return, ví shop, kiện hàng nhận webhook. Đọc với `FOR UPDATE` để không ai khác đổi nó cùng lúc, rồi kiểm tra lại trạng thái dưới lock trước khi chuyển. Ví dụ hai webhook cùng báo một payment sẽ được xử lý tuần tự, cái sau thấy trạng thái đã đúng và trả `duplicate_ignored`.

### Q16. Checkout đọc giá SKU với `FOR SHARE` để làm gì? [Nâng cao]
Để giá trong đơn chính xác là giá có hiệu lực tại thời điểm checkout. Lệnh đổi giá giữ `FOR UPDATE` trên SKU; checkout với `FOR SHARE` sẽ chờ lệnh đổi giá đang chạy commit rồi đọc giá mới, còn lệnh đổi giá đến sau phải chờ checkout xong. Nhiều checkout có thể cùng giữ `FOR SHARE` một SKU nên không chặn nhau. Vì chỉ xung đột với lệnh đổi giá (lock đúng một dòng) nên không tạo vòng lock.

### Q17. Tại sao tồn kho không được lock ngay từ đầu checkout? [Nâng cao]
SKU hot (flash sale) bị hàng trăm checkout đụng cùng lúc; nếu mỗi checkout lock tồn kho suốt lúc tính tiền, chúng sẽ xếp hàng dài. API chỉ **đọc** tồn kho lúc đầu (để báo lỗi sớm), tính toàn bộ giá và voucher, rồi ở **cuối** transaction mới trừ tồn kho bằng một câu UPDATE có điều kiện "còn đủ". Dòng hot chỉ bị lock vài mili giây. Điều kiện trong UPDATE đảm bảo không bán vượt dù đã đọc không lock.

### Q18. Nếu 50 người cùng mua SKU flash sale còn 10 suất thì sao? [Trung bình]
Cả 50 đọc thấy còn hàng và tính tiền song song. Ở cuối, câu UPDATE "bán thêm nếu đã bán + số lượng ≤ quota" chỉ trúng cho 10 người. 40 người còn lại nhận 409 `flash_sold_out` (hoặc `out_of_stock`), và transaction của họ rollback sạch: không giữ tồn kho, không tốn lượt voucher, không có đơn. Bộ test stress kiểm tra đúng tình huống "rush" này.

### Q19. Thứ tự lock trong checkout là gì và vì sao phải cố định? [Nâng cao]
Advisory lock voucher theo khách (chỉ khi có voucher) → giỏ hàng của khách → các SKU (`FOR SHARE`, sắp theo id) → slot quota voucher (sắp theo voucher id) → tồn kho (sắp theo SKU id) → quota flash sale (sắp theo item id). Deadlock xảy ra khi hai transaction lock cùng tài nguyên theo thứ tự ngược nhau; nếu ai cũng lock theo cùng một thứ tự toàn cục thì không thể có vòng chờ. Vì vậy mọi danh sách đều được sắp xếp trước khi lock, và luồng trả lại (hủy checkout, trả quota) đi theo cùng thứ tự.

### Q20. `FOR NO KEY UPDATE` được dùng ở đâu và tại sao? [Nâng cao]
Khi hủy hoặc hết hạn checkout, API lock tồn kho và flash item với `FOR NO KEY UPDATE`. Một checkout khác đang chèn order item có foreign key trỏ tới flash item, nên PostgreSQL tự giữ `KEY SHARE` trên dòng đó. `FOR UPDATE` xung đột với `KEY SHARE` và từng gây deadlock; `FOR NO KEY UPDATE` không xung đột vì không đổi khóa chính. Đây cũng là mức lock mà câu UPDATE bình thường tự lấy, nên không mất tính đúng.

### Q21. Quota voucher được chia thành slot để giải quyết vấn đề gì? [Nâng cao]
Một voucher sàn phổ biến bị hàng nghìn checkout tăng cùng một bộ đếm; một dòng duy nhất sẽ thành nút cổ chai vì mọi người xếp hàng lock nó. Quota được chia tối đa 16 dòng (slot); mỗi lần lấy chọn ngẫu nhiên một slot còn chỗ và tăng có điều kiện. Một lần thử chỉ chờ trên một dòng và không giữ lock nếu thất bại, nên không có vòng lock. Tổng quota vẫn đúng, có test kiểm không bao giờ dùng vượt.

### Q22. Advisory lock theo khách hàng dùng để làm gì? [Trung bình]
Giới hạn "mỗi khách chỉ dùng voucher N lần" cần tuần tự hóa các checkout **của cùng một khách** (nếu không, hai checkout song song cùng thấy "chưa dùng" và cùng dùng). API lấy `pg_advisory_xact_lock` trên khóa băm từ id khách. Khách khác không bị ảnh hưởng, và lock tự nhả khi transaction kết thúc. Chỉ lấy khi checkout có voucher để không làm chậm checkout thường.

### Q23. Nếu vẫn xảy ra deadlock thì người dùng thấy gì? [Cơ bản]
PostgreSQL hủy một transaction; API bắt lỗi deadlock hoặc serialization failure, rollback và trả 503 mã `retry` kèm `Retry-After: 1`. Client gửi lại với cùng Idempotency-Key là an toàn. Với job nền, item đó bị bỏ qua và làm lại ở lượt sau. Theo README, kiểm tra chất lượng còn đòi PostgreSQL không ghi nhận deadlock nào trong luồng ghi.

### Q24. SAVEPOINT được dùng để làm gì? [Trung bình]
Khi một câu INSERT có thể vi phạm ràng buộc unique (email khách đã tồn tại, mã SKU trùng, mã voucher trùng), API đặt savepoint trước, bắt lỗi unique violation, lùi về savepoint và ném lỗi nghiệp vụ 409 có mã rõ ràng. Không có savepoint, transaction PostgreSQL ở trạng thái lỗi và người dùng nhận lỗi SQL thô 500. Cách này biến lỗi ràng buộc thành lỗi nghiệp vụ có nghĩa.

---

## 3. Idempotency và outbox

### Q25. Idempotency-Key giải quyết vấn đề gì? [Cơ bản]
Mạng có thể rớt sau khi server đã commit nhưng trước khi client nhận phản hồi; client gửi lại thì có thể tạo đơn thứ hai, trừ tiền hai lần. Với Idempotency-Key, lần gửi lại cùng khóa sẽ nhận **phản hồi đã lưu** của lần đầu (header `Idempotent-Replay: true`) thay vì chạy lại. Nhờ đó retry trở thành an toàn.

### Q26. Khóa được lưu như thế nào? [Trung bình]
Trong bảng `idempotency_keys`: khóa (đã ghép tiền tố người gọi, tối đa 100 ký tự), endpoint (gồm cả id trên path), hash SHA-256 của nội dung request (JSON sắp key), mã HTTP và phản hồi. Dòng được chèn **trong cùng transaction** với command, và phản hồi được ghi vào cuối transaction. Bảng này không đi vào CDC vì là dữ liệu kỹ thuật.

### Q27. Hai request cùng khóa tới gần như đồng thời thì sao? [Nâng cao]
Request đầu chèn dòng khóa và đang chạy. Request thứ hai chèn cùng khóa nên phải chờ khóa unique của dòng đó. Nếu request đầu commit, request thứ hai thấy dòng đã tồn tại và replay phản hồi. Nếu request đầu rollback (bị từ chối), dòng biến mất, request thứ hai chèn được và chạy lại từ đầu. Không bao giờ có hai lần thực thi thành công.

### Q28. Dùng lại một khóa cho request khác thì sao? [Cơ bản]
Nếu endpoint khác hoặc hash nội dung khác, API trả 422 `idempotency_key_reused`. Đây là lỗi của client (dùng khóa không duy nhất), và trả phản hồi cũ cho một request khác sẽ rất nguy hiểm. Vì vậy khóa nên là UUID mới cho mỗi ý định, chỉ dùng lại khi retry.

### Q29. Vì sao chỉ lưu phản hồi thành công? [Trung bình]
Command bị từ chối (409 hết hàng, 404, 403) làm transaction rollback, nên dòng khóa cũng biến mất. Lần thử sau với cùng khóa sẽ chạy lại thật. Điều này hợp lý: lỗi nghiệp vụ có thể là tạm thời (hết hàng rồi được nhập thêm), và không có gì đã được ghi nên chạy lại không gây trùng.

### Q30. Vì sao khóa được ghép tiền tố theo người gọi? [Nâng cao]
Nếu khóa là toàn cục, hai người dùng vô tình chọn cùng chuỗi sẽ đụng nhau (người sau nhận 422 hoặc tệ hơn nhận phản hồi của người trước). Nguy hiểm hơn, kẻ đoán được khóa của người khác có thể "replay" và đọc phản hồi checkout của họ (địa chỉ, số tiền). Tiền tố là 16 ký tự đầu SHA-256 của `sub` trong token, nên khóa của mỗi người gọi nằm trong không gian riêng. Có test: hai người mua dùng cùng chuỗi khóa, người thứ hai nhận 201 thật với giỏ riêng, không phải replay.

### Q31. Khóa sống bao lâu và vì sao dùng đồng hồ thật? [Cơ bản]
72 giờ theo đồng hồ thật, sau đó job nền xóa dần (tối đa 5000 dòng mỗi lượt). Client retry theo thời gian thực (giây, phút), nên TTL phải theo thời gian thật; nếu theo business clock chạy 240×, 72 giờ nghiệp vụ chỉ là 18 phút thật.

### Q32. Outbox pattern là gì và giải quyết vấn đề gì? [Cơ bản]
Mỗi command ghi một domain event (như `OrderPlaced`) vào bảng `outbox_events` trong **cùng transaction** với thay đổi dữ liệu. Đây là lời giải cho bài toán dual-write: nếu ghi DB rồi mới gửi Kafka, có thể DB thành công mà gửi Kafka thất bại (hoặc ngược lại). Cùng transaction đảm bảo sự kiện tồn tại khi và chỉ khi dữ liệu thay đổi. Debezium đọc sự kiện từ WAL và đẩy lên topic `events.<aggregatetype>`.

### Q33. Vì sao dòng outbox bị xóa ngay trong cùng transaction? [Nâng cao]
Debezium đọc WAL, nên vẫn thấy lệnh INSERT dù dòng bị DELETE ngay sau đó trong cùng transaction; Outbox Event Router chuyển INSERT thành message và bỏ qua DELETE. Nhờ vậy bảng outbox luôn rỗng, không phình ra và không cần job dọn. Đánh đổi: không thể truy vấn lại sự kiện từ bảng; nguồn sự thật cho sự kiện là Kafka.

### Q34. Thời điểm của sự kiện outbox được lấy thế nào? [Nâng cao]
Dùng business clock đọc **tại lúc gọi** (dựa trên `clock_timestamp`, không phải thời điểm bắt đầu transaction). Nếu dùng giờ bắt đầu transaction, một transaction bắt đầu sớm nhưng xử lý chậm sẽ có sự kiện "cũ hơn" sự kiện của transaction bắt đầu muộn mà kết thúc trước. Đọc tại lúc gọi giúp các sự kiện của cùng một aggregate được sắp theo đúng thứ tự chúng xảy ra (vì chúng bị tuần tự hóa bởi lock trên aggregate).

---

## 4. Identity: đăng ký, đăng nhập, mật khẩu

### Q35. Identity service làm những gì? [Cơ bản]
Đăng ký người mua và người bán, đăng nhập bằng mật khẩu, cấp và xoay refresh token, cấp token cho service client (client_credentials), cấp token "đóng vai" (token exchange), và công bố khóa công khai (JWKS) cùng discovery. Nó có database riêng `identity_db`. Source API chỉ cần issuer, audience và URL JWKS của nó.

### Q36. Đăng ký hoạt động như một saga thế nào? [Trung bình]
Đăng ký đụng hai database nên không gói trong một transaction. Bước 1: tạo user PENDING (mật khẩu đã băm) và commit. Bước 2: identity dùng token client của chính nó gọi `/v1/internal/customers` (hoặc `/sellers`) với Idempotency-Key gắn user id. Bước 3: tạo membership (user → khách/shop), chuyển ACTIVE, ghi audit, trả token. Nếu lỗi giữa chừng, đăng ký lại cùng email sẽ dùng lại user PENDING và cùng khóa idempotency, nên API không tạo khách hàng thứ hai.

### Q37. Vì sao identity không ghi thẳng vào bảng customers? [Trung bình]
Vì Source API là người ghi duy nhất vào `enterprise_db`: tạo khách hàng còn cần tạo địa chỉ mặc định và phát sự kiện `CustomerRegistered` qua outbox. Role DB của identity (`identity_app`) cũng không connect được `enterprise_db`. Gọi qua API với scope provisioning giữ ranh giới rõ: identity lo danh tính, API lo dữ liệu nghiệp vụ.

### Q38. Mật khẩu được lưu thế nào? [Cơ bản]
Mật khẩu được HMAC-SHA256 với một pepper (secret trong file, ngoài DB), rồi băm bằng argon2id với tham số tối thiểu của OWASP (19 MiB bộ nhớ, 2 lượt, 1 luồng). Mỗi hash có salt ngẫu nhiên riêng do argon2 tạo. Mật khẩu không bao giờ được gửi sang Source API.

### Q39. Pepper khác salt thế nào và tại sao cần cả hai? [Trung bình]
Salt là ngẫu nhiên riêng cho từng hash và nằm ngay trong hash (trong DB); nó chống rainbow table và làm hai người cùng mật khẩu có hash khác nhau. Pepper là một secret chung nằm **ngoài** DB (file mount vào container). Nếu kẻ tấn công chỉ lấy được bản dump DB, họ không thể thử mật khẩu offline vì thiếu pepper. Nhược điểm: đổi pepper làm mọi hash vô hiệu, nên pepper cần được quản lý cẩn thận.

### Q40. Vì sao chọn argon2id thay vì SHA-256 hay bcrypt? [Trung bình]
SHA-256 quá nhanh, GPU thử được hàng tỷ mật khẩu mỗi giây. bcrypt chậm nhưng tốn ít bộ nhớ nên vẫn song song hóa được trên GPU/ASIC. argon2id là hàm thắng cuộc thi Password Hashing Competition, "đắt" cả về thời gian lẫn **bộ nhớ** (19 MiB mỗi lần thử), làm tấn công phần cứng chuyên dụng tốn kém. Code còn tự rehash khi tham số thay đổi lúc người dùng đăng nhập thành công.

### Q41. Làm sao chống dò email đã đăng ký qua đường login? [Trung bình]
Email không tồn tại (hoặc tài khoản chưa ACTIVE) vẫn được cho chạy qua một lần verify argon2 với một hash giả, nên thời gian phản hồi giống email có thật (chống timing attack). Thông báo lỗi giống hệt: 401 `invalid_credentials` "Email hoặc mật khẩu sai". Tuy nhiên đường đăng ký vẫn trả 409 `email_taken`, và tài khoản bị khóa trả 423 — hai kênh này vẫn tiết lộ tồn tại.

### Q42. Khóa tài khoản hoạt động thế nào? [Cơ bản]
Mỗi lần sai mật khẩu, bộ đếm tăng nếu lần sai đầu còn trong cửa sổ 15 phút, không thì đếm lại từ 1. Đủ 10 lần sai trong 15 phút thì khóa 15 phút; trong thời gian đó, kể cả mật khẩu đúng cũng nhận 423 `account_locked`. Đăng nhập đúng xóa bộ đếm. Dòng user được lock `FOR UPDATE` để các lần thử song song đếm chính xác.

### Q43. Lỗi "bộ đếm sai bị rollback" là gì và sửa thế nào? [Nâng cao]
Ban đầu, khi mật khẩu sai, code raise lỗi 401 ngay trong transaction đang tăng bộ đếm; raise làm transaction rollback, nên bộ đếm không bao giờ được lưu và tài khoản không bao giờ bị khóa. Bản sửa: ghi nhận lỗi vào một biến, để khối transaction kết thúc bình thường (commit bộ đếm và audit), rồi mới trả lỗi bên ngoài. Bài học chung: muốn ghi lại "bằng chứng thất bại" thì việc ghi đó phải commit độc lập với việc trả lỗi.

### Q44. Giới hạn đăng nhập theo IP được làm ở đâu? [Trung bình]
Hai lớp. Gateway giới hạn đường login trung bình 5 request/giây, burst 10, theo IP. Identity còn tự giữ trong bộ nhớ mỗi worker một hàng đợi theo IP, mặc định 120 lần/phút, vượt thì 429 `rate_limited` với `Retry-After: 60`. Lớp trong bộ nhớ có điểm yếu: mỗi worker đếm riêng (4 worker → thực tế cao hơn) và mất khi restart.

### Q45. Tài khoản admin được tạo thế nào? [Cơ bản]
Khi identity khởi động, nó tạo một lần user `admin@cdc.local` với mật khẩu lấy từ `secrets/admin_password` và membership admin. Việc này được bảo vệ bằng advisory lock vì 4 worker khởi động cùng lúc. Token của admin có scope admin, ops:read, pii:read.

### Q46. Vì sao identity dùng database riêng `identity_db`? [Trung bình]
Publication logical replication của PostgreSQL thuộc về một database. Publication CDC nằm ở `enterprise_db`, nên mật khẩu băm, refresh token và audit trong `identity_db` **không thể** chảy vào Kafka/warehouse dù cấu hình connector sai thế nào. Đây là ranh giới vật lý, an toàn hơn việc nhớ thêm bảng vào danh sách loại trừ. Ngoài ra role `identity_app` không connect được database nghiệp vụ và ngược lại.

---

## 5. Token, JWT, refresh, client, exchange

### Q47. Access token chứa những gì? [Cơ bản]
JWT ký EdDSA với các claim: `iss` (identity), `aud` (`source-api`), `sub` (id user hoặc client), `iat`, `nbf`, `exp`, `jti`, `kind` (customer/seller/ops/admin/service), `cid` (id khách), `shops` (danh sách shop), `scope`, và `act` với token exchange. Claim được tính từ bảng memberships lúc cấp. Token sống 10 phút với user, 15 phút với client và exchange.

### Q48. Vì sao access token ngắn hạn còn refresh token dài hạn? [Cơ bản]
Access token được gửi theo mọi request và API verify cục bộ, không tra DB, nên không thu hồi được — giữ ngắn (10 phút) để giới hạn thiệt hại khi lộ. Refresh token chỉ gửi tới identity, được tra DB mỗi lần dùng nên thu hồi được; nó dài hạn (30 ngày) để người dùng không phải đăng nhập liên tục. Đây là cân bằng kinh điển giữa hiệu năng (verify không trạng thái) và khả năng thu hồi.

### Q49. Vì sao ký bằng EdDSA mà không dùng HS256? [Trung bình]
HS256 dùng một khóa chung để cả ký lẫn verify, nên mọi service cần verify cũng ký được token — lộ một service là lộ khả năng phát hành token admin. EdDSA là bất đối xứng: chỉ identity giữ khóa riêng, mọi bên khác chỉ cần khóa công khai qua JWKS. Ed25519 nhanh, chữ ký ngắn, không có bẫy về nonce như ECDSA.

### Q50. Source API verify token như thế nào? [Trung bình]
Đọc `kid` trong header, tìm khóa công khai trong cache; gặp `kid` lạ thì tải lại JWKS nhưng tối đa 30 giây một lần. Verify với thuật toán chỉ EdDSA, đúng issuer, đúng audience, bắt buộc có exp/iat/sub/aud/iss, cho lệch đồng hồ 60 giây. Thất bại → 401 `unauthenticated` với `WWW-Authenticate: Bearer error="invalid_token"`. Thành công → dựng Principal từ claim.

### Q51. Vì sao giới hạn tải lại JWKS 30 giây một lần? [Nâng cao]
Kẻ tấn công có thể gửi hàng loạt token với `kid` ngẫu nhiên. Nếu mỗi `kid` lạ đều kích hoạt tải JWKS, API sẽ thành công cụ khuếch đại tấn công vào identity, và mỗi request bị chậm. Giới hạn 30 giây vẫn đủ để đón khóa mới sau khi xoay khóa, trong khi chặn được spam.

### Q52. Nếu identity service chết thì API còn chạy không? [Trung bình]
Còn, với người đã có token. Verify token hoàn toàn cục bộ bằng khóa công khai đã cache, không gọi identity mỗi request. Không làm được: đăng nhập, refresh, đăng ký, xin token mới cho client — nên sau khi token hiện tại hết hạn (10–15 phút), người dùng sẽ bị 401. Một worker API mới khởi động khi identity chết sẽ không tải được JWKS nên từ chối mọi token (compose cho API chờ identity healthy khi khởi động).

### Q53. Chặn tấn công `alg=none` hay nhầm thuật toán ra sao? [Trung bình]
API chỉ chấp nhận đúng một thuật toán là EdDSA khi decode, bất kể header của token nói gì. Token `alg=none` (không chữ ký) hay token HS256 ký bằng khóa công khai (tấn công nhầm lẫn thuật toán) đều bị từ chối. Đây là lỗi kinh điển của các thư viện JWT cũ tin vào trường `alg` do token tự khai.

### Q54. Refresh token rotation là gì? [Cơ bản]
Mỗi lần dùng refresh token để lấy access token mới, token cũ bị đánh dấu "đã dùng" và một refresh token mới cùng family được cấp. Mỗi refresh token chỉ dùng được một lần. Refresh token là chuỗi ngẫu nhiên 32 byte, không phải JWT, và DB chỉ lưu SHA-256 của nó.

### Q55. Reuse detection hoạt động thế nào và vì sao thu hồi cả family? [Trung bình]
Nếu một refresh token **đã dùng** được gửi lại, chỉ có thể là bị đánh cắp hoặc bị replay. Identity không biết bên nào là người thật (kẻ trộm có thể dùng trước), nên thu hồi toàn bộ family — tức cả phiên đăng nhập. Kẻ trộm mất quyền truy cập; người thật bị buộc đăng nhập lại, đó là cái giá chấp nhận được. Việc thu hồi được ghi vào audit với `reuse_detected`.

### Q56. Vì sao nhánh reuse detection trả phản hồi trực tiếp chứ không raise lỗi? [Nâng cao]
Câu lệnh thu hồi family nằm trong transaction; nếu raise lỗi HTTP bên trong, transaction sẽ rollback và việc thu hồi bị mất — cùng loại lỗi với bộ đếm login. Trả một phản hồi 401 trực tiếp từ bên trong khối transaction cho phép nó kết thúc bình thường và commit. Đây là chi tiết mà review code dễ bỏ sót.

### Q57. Nếu client hợp lệ gửi hai request refresh đồng thời với cùng token thì sao? [Nâng cao]
Dòng token được lock `FOR UPDATE`, nên hai request được xử lý tuần tự. Request đầu thành công và đánh dấu đã dùng; request sau thấy token đã dùng và **thu hồi cả family** — người dùng bị đăng xuất dù không có kẻ trộm. Đây là trade-off đã biết của rotation nghiêm ngặt; nhiều hệ thống thêm "grace period" vài giây cho token vừa dùng. Client nên tuần tự hóa việc refresh.

### Q58. Logout làm gì? [Cơ bản]
Logout nhận refresh token và thu hồi cả family của nó, ghi audit. Access token đang có vẫn còn hiệu lực đến khi hết hạn (tối đa 10 phút) vì API không tra DB khi verify. Muốn thu hồi tức thì cần danh sách đen theo `jti` hoặc token introspection — chưa làm.

### Q59. client_credentials dùng cho ai và kiểm tra những gì? [Cơ bản]
Cho service không có người dùng: `sim-orchestrator` (simulator), `quality` (test), `identity` (provisioning). Client gửi id + secret tới `/v1/auth/token`; secret được so constant-time với file `secrets/clients/<id>`. Client có thể xin ít scope hơn nhưng không vượt danh sách được cấp — vượt thì 400 `invalid_scope`. Token client có `sub=client:<id>`, `kind=service`, sống 15 phút.

### Q60. Token exchange là gì, dùng để làm gì trong dự án? [Trung bình]
Một service đáng tin đổi token để **hành động thay** một khách hoặc shop: gửi `requested_subject=customer:<uuid>` hoặc `shop:<uuid>`. Token nhận được có `sub` là subject đó, `kind`, `cid` hoặc `shops` tương ứng, **không có scope**, và `act` ghi client thật. Simulator dùng nó để mỗi actor gọi API với đúng quyền của người mua/người bán đó, nhờ vậy mọi kiểm tra quyền sở hữu được thử thật — kết quả: simulator gây 0 lượt từ chối.

### Q61. Implementation này khác RFC 8693 chuẩn ở đâu? [Nâng cao]
Chuẩn yêu cầu gửi `subject_token` (token của chủ thể bị đóng vai, chứng minh họ đồng ý) và có thể `actor_token`. Ở đây client chỉ xác thực bằng secret của mình, có cờ "impersonate", và chọn subject tùy ý (chỉ kiểm định dạng `customer|shop:<uuid>`, không kiểm subject có tồn tại). Điều này chấp nhận được vì chỉ hai client nội bộ có cờ đó và chúng vốn đã có scope admin, nên exchange **thu hẹp** chứ không mở rộng quyền. Với hệ thống thật nên theo chuẩn đầy đủ và ghi `act` vào audit phía API.

---

## 6. Phân quyền trong API

### Q62. Principal được dựng thế nào cho mỗi request? [Trung bình]
Một dependency áp cho toàn app chạy trước mọi endpoint. Có Bearer token → verify → Principal gồm `sub`, `kind`, `customer_id`, tập `shop_ids`, tập `scopes`, `actor`. Không có token mà route là công khai hoặc webhook → Principal ẩn danh. Không có token ở route khác → 401. Principal được đặt vào context variable để handler, hàm kiểm quyền và lớp idempotency đọc được mà không cần truyền qua mọi hàm.

### Q63. Vì sao dependency xác thực phải là hàm async? [Nâng cao]
Endpoint đồng bộ được FastAPI chạy trong threadpool, với một bản sao context lấy tại lúc gọi. Dependency async chạy trong event loop, ngay trong context của request, nên giá trị context variable nó đặt sẽ được sao chép sang thread chạy endpoint. Nếu viết dạng sync, dependency chạy trong một thread riêng và giá trị đặt ở đó có thể không thấy được ở endpoint.

### Q64. Ba chế độ AUTH_MODE khác nhau thế nào? [Cơ bản]
`off`: không kiểm gì, mọi request là "system" (hành vi cũ). `permissive`: mọi quyết định vẫn được tính, would-deny được đếm vào `auth_decisions_total` và ghi log nhưng không chặn. `enforce`: từ chối thật bằng 401/403/404; là mặc định hiện tại. Lưu ý token sai chữ ký vẫn bị 401 kể cả ở permissive.

### Q65. Chế độ permissive hữu ích thế nào khi triển khai? [Trung bình]
Bật phân quyền lên một hệ thống đang chạy có rủi ro làm hỏng client hợp lệ. Permissive cho phép bật toàn bộ logic, quan sát metric would-deny và log để biết client nào sẽ bị chặn, sửa client, rồi mới chuyển sang enforce khi số would-deny bằng 0. Dự án làm đúng quy trình này: simulator gây 0 lượt từ chối, test auth 11/11 và test API đều qua ở enforce.

### Q66. Những loại kiểm tra quyền nào có trong API? [Trung bình]
Cần scope (admin, ops:read, sim:clock, provisioning). Là chính khách đó khi ghi (cid của token phải khớp). Là chủ shop khi ghi (shop của tài nguyên nằm trong `shops`). Đọc của khách/shop thì thêm ops được phép. Đọc thứ "giữa hai bên" (đơn, kiện hàng, yêu cầu trả hàng) thì người mua, shop, ops, admin đều được. Danh sách: ops thấy tất cả, người mua chỉ dòng của mình, người bán chỉ shop của mình.

### Q67. Vì sao tài nguyên của người khác trả 404 chứ không phải 403? [Trung bình]
403 xác nhận rằng tài nguyên **tồn tại**, chỉ là bạn không có quyền; với id hay mã đơn đoán được, kẻ tấn công có thể dò ra đơn nào tồn tại. Trả 404 với đúng mã như khi không tồn tại (`order_not_found`, `sku_not_found`) làm "không có" và "không phải của bạn" không phân biệt được. 403 chỉ dùng khi vấn đề là vai trò ở một route mà sự tồn tại không phải bí mật, như người mua gọi `/customers`.

### Q68. Người bán shop A gọi đổi giá SKU của shop B — chuyện gì xảy ra? [Cơ bản]
API mở transaction, đọc và lock SKU, thấy nó thuộc shop B. Shop B không có trong `shops` của token, nên trả 404 `sku_not_found` và transaction rollback (không đổi giá, không ghi lịch sử giá, không phát sự kiện). Kết quả giống hệt như SKU không tồn tại. Test `test_a_seller_manages_only_their_own_shop` kiểm tra cả đổi giá và nhập kho.

### Q69. Người mua gửi `customer_id` của người khác trong body thì sao? [Trung bình]
API vẫn nhận `customer_id`/`shop_id` trong body để tương thích client cũ, nhưng nếu có thì nó phải khớp token, không khớp → 404 `customer_not_found`. Nếu bỏ trống, API lấy id từ token. Người bán sở hữu nhiều shop phải ghi rõ shop. Thiết kế sạch hơn (chưa có) là route `/v1/me/...` và bỏ hẳn id khỏi body.

### Q70. Ops đọc dữ liệu cá nhân thế nào? [Trung bình]
Người có `ops:read` đọc được hồ sơ, đơn của mọi người, nhưng tên, email, số điện thoại, tên người nhận, địa chỉ bị che: giữ ký tự đầu, thêm "***" và giữ phần sau `@` của email hoặc hai ký tự cuối. Muốn thấy rõ cần `pii:read` (admin hoặc client `quality`). Đây là nguyên tắc "cần biết": hỗ trợ khách hàng cần tra cứu đơn nhưng hiếm khi cần địa chỉ đầy đủ. Theo code hiện tại người bán xem đơn của mình cũng thấy địa chỉ bị che vì chỉ `kind=customer` được miễn.

### Q71. Route nào công khai và vì sao? [Cơ bản]
`/health`, `/metrics`, danh mục, thương hiệu, voucher đang chạy, campaign và item của campaign, danh sách SKU, danh sách shop, đồng hồ, `/docs`, `/openapi.json`. Đây là catalogue mà khách chưa đăng nhập cũng phải xem được, cùng các probe kỹ thuật. Gateway không đưa `/health`, `/metrics` ra ngoài. Danh sách shop công khai nhưng ẩn số dư ví với người không phải ops hoặc chính shop.

### Q72. Admin có cần token exchange để làm việc thay khách không? [Trung bình]
Không bắt buộc: principal có scope admin vượt qua mọi kiểm tra sở hữu. Nhưng một số command của người mua lấy `customer_id` từ token khi body bỏ trống, mà token admin không có `cid`, nên admin phải ghi rõ `customer_id`. Simulator vẫn dùng exchange để thử phân quyền như người thật, và bộ test dùng người dùng thật hoặc exchange cho test auth.

### Q73. Nếu quên gọi hàm kiểm quyền trong một endpoint mới thì sao? [Nâng cao]
Dependency toàn app vẫn đòi token hợp lệ (trừ route công khai), nên người lạ không vào được, nhưng **mọi người dùng đã đăng nhập** sẽ thao tác được trên tài nguyên của người khác — một IDOR. Đây là điểm yếu của phân quyền "gọi tay trong từng handler". Biện pháp: test phân quyền cho mỗi route (như `test_auth.py`), metric `auth_decisions_total` (route không có quyết định nào là đáng ngờ), hoặc chuyển sang mô hình mặc định từ chối, ví dụ khai báo policy theo route.

---

## 7. Webhook đối tác

### Q74. Webhook được ký như thế nào? [Cơ bản]
Đối tác gửi header `X-Partner` (tên, ví dụ `MOMO`; J&T chuẩn hóa thành `JT`) và `X-Signature: t=<unix giây>,v1=<hex>`, với v1 là HMAC-SHA256 dùng khóa riêng của đối tác trên chuỗi timestamp, dấu chấm, và thân request thô. Mỗi đối tác có một file khóa trong `secrets/webhooks/`. API tính lại và so sánh constant-time.

### Q75. API kiểm tra webhook qua những bước nào? [Trung bình]
Tìm khóa theo `X-Partner`; phải có `t` và `v1`, `t` là số; |giờ thật − t| ≤ 300 giây; HMAC khớp. Sai bất kỳ bước nào → 401 `invalid_signature`. Đúng → principal `partner:<tên>`. Sau đó trong handler, khi đã tìm được giao dịch, đối tác phải là đơn vị xử lý nó: ví (WALLET) qua MOMO, thẻ (CARD) qua VNPAY, chuyển khoản qua NAPAS, kiện hàng theo hãng được gán; sai → 403 `wrong_partner`.

### Q76. Vì sao ký cả timestamp và ký trên thân thô? [Trung bình]
Ký thân để không ai sửa được nội dung (đổi FAILED thành SUCCEEDED, đổi số tiền). Ký timestamp để callback ghi lại được hôm qua không gửi lại được hôm nay (ngoài cửa sổ 5 phút). Ký trên byte thô chứ không phải JSON đã parse vì parse-rồi-serialize có thể đổi thứ tự key hay khoảng trắng, làm hai bên tính ra chữ ký khác nhau.

### Q77. Replay một webhook hợp lệ trong vòng 5 phút thì sao? [Nâng cao]
Chữ ký vẫn hợp lệ vì không có kho nonce. Nhưng xử lý idempotent ở hai tầng: cùng Idempotency-Key (đối tác gửi event id của họ) → replay phản hồi cũ; tầng nghiệp vụ: payment đã ở trạng thái đó → `duplicate_ignored`, sự kiện vận chuyển có `carrier_event_id` đã thấy → `duplicate_ignored`, sự kiện cũ hơn sự kiện mới nhất → lưu nhưng không áp. Vậy replay không gây tác dụng phụ. Muốn chặt hơn có thể lưu `(partner, t, v1)` đã thấy trong 5 phút.

### Q78. VNPAY gửi webhook báo một payment của MOMO thì sao? [Cơ bản]
Chữ ký của VNPAY hợp lệ (ký bằng khóa VNPAY) nên qua bước xác thực. Nhưng khi tìm payment, API thấy provider là MOMO, không phải VNPAY, nên trả 403 `wrong_partner` và rollback. Nhờ vậy lộ khóa của một đối tác chỉ ảnh hưởng tới giao dịch của đối tác đó. Test `test_webhooks_must_be_signed_by_the_right_partner` kiểm tra đúng trường hợp này.

### Q79. Thanh toán thành công tới sau khi checkout đã hết hạn thì xử lý thế nào? [Trung bình]
Tiền đã bị cổng thanh toán trừ, nên API vẫn ghi payment SUCCEEDED, nhưng vì checkout không còn ở trạng thái chờ thanh toán, nó tự tạo lệnh hoàn tiền và trả kết quả `refunded_late_payment`. Đơn không được kích hoạt lại. Đây là cách xử lý thực tế cho callback chậm.

### Q80. Vì sao cửa sổ chữ ký dùng đồng hồ thật? [Cơ bản]
Đối tác ký bằng giờ hệ thống thật của họ. Business clock chạy 240× sẽ làm phép so sánh vô nghĩa, và nếu tính cửa sổ theo business time thì 5 phút sẽ tương ứng 20 giờ thật, mở rộng cơ hội replay. Code ghi rõ điều này cạnh hằng số cửa sổ.

### Q81. Tên đối tác lấy từ header — có nguy cơ path traversal khi đọc file khóa không? [Nâng cao]
Code từ chối tên chứa dấu `/`, nên không thể trỏ ra ngoài thư mục khóa. Tuy nhiên tên `..` hoặc `.` (không có `/`) trỏ tới một thư mục; đọc thư mục ném lỗi khác "file không tồn tại", không được bắt, nên request trả 500 thay vì 401. Không lộ dữ liệu, nhưng là lỗi xử lý; nên dùng danh sách trắng các đối tác hoặc bắt mọi lỗi I/O. Simulator chuẩn hóa tên giống API (chỉ giữ chữ và số, viết hoa).

---

## 8. Gateway

### Q82. Gateway làm những gì? [Cơ bản]
Traefik v3 là cửa HTTPS duy nhất cho client ngoài, trên 127.0.0.1:8443. Nó kết thúc TLS, định tuyến theo hostname (`api.cdc.localhost` → Source API, `auth.cdc.localhost` → identity), giới hạn tốc độ theo IP, thêm security header, áp CORS, và ghi access log JSON không có header. Nó chỉ đưa `/v1/...` và docs ra ngoài, không đưa `/health`, `/metrics`.

### Q83. Chứng chỉ TLS lấy ở đâu? [Cơ bản]
`setup-env.sh` tạo một CA cục bộ (10 năm) và chứng chỉ server (825 ngày) do CA đó ký, với SAN gồm api/auth/app.cdc.localhost và localhost, đặt trong `secrets/tls`. Muốn gọi không cần `-k`, tin `secrets/tls/ca.crt` (ví dụ curl với `--cacert`). TLS tối thiểu 1.2. Khóa riêng của CA được giữ quyền riêng tư.

### Q84. Các mức rate limit là bao nhiêu và vì sao khác nhau? [Trung bình]
Login: trung bình 5/giây, burst 10; phần còn lại của identity: 50/giây, burst 100; API: 300/giây, burst 600, đều theo IP. Login thấp nhất vì là mục tiêu brute-force và credential stuffing, người thật không login 5 lần/giây. API cao vì một client thật (app) gọi nhiều. Rate limit ở gateway bổ sung cho khóa tài khoản ở identity: một bên chặn số lượng, một bên chặn theo tài khoản.

### Q85. CORS được cấu hình thế nào và chống được gì? [Trung bình]
Chỉ origin `https://app.cdc.localhost` được trình duyệt cho phép gọi, với các method thông dụng và header Authorization, Content-Type, Idempotency-Key; preflight cache 10 phút. CORS ngăn trang web lạ dùng trình duyệt của nạn nhân đọc phản hồi API. Vì API dùng Bearer token trong header chứ không dùng cookie, rủi ro CSRF vốn đã thấp; CORS là lớp bổ sung. CORS không bảo vệ khỏi client không phải trình duyệt (curl).

### Q86. Các security header có tác dụng gì? [Cơ bản]
HSTS 1 năm buộc trình duyệt chỉ dùng HTTPS với host này. `nosniff` ngăn trình duyệt đoán kiểu nội dung (chống một số XSS). `X-Frame-Options: DENY` chống clickjacking (không cho nhúng trong iframe). `Referrer-Policy: no-referrer` không rò URL sang trang khác. Xóa header `Server` để không quảng cáo phần mềm và phiên bản.

### Q87. Các service bên trong có đi qua gateway không? [Trung bình]
Không. Simulator, test, identity gọi API (và API gọi JWKS) trực tiếp bằng HTTP trong mạng Docker. Cổng trực tiếp 8100 (API) và 8300 (identity) vẫn mở trên 127.0.0.1 cho tiện debug — chúng bỏ qua TLS và rate limit. Đây là đánh đổi chấp nhận được cho môi trường một máy; ở production nên dùng mTLS giữa service hoặc chỉ mở cổng nội bộ.

---

## 9. Secret và database

### Q88. Secret được quản lý thế nào trong dự án? [Cơ bản]
Mỗi secret là một file trong `secrets/` (gitignored): mật khẩu role DB, ClickHouse, Grafana, PII salt, khóa S3, pepper, mật khẩu admin, secret client, khóa webhook, chứng chỉ TLS. Mỗi container chỉ mount read-only những file nó cần dưới `/run/secrets`. `.env` chỉ còn thiết lập không bí mật như `AUTH_MODE`.

### Q89. Biến `*_FILE` là gì và vì sao tốt hơn biến môi trường chứa mật khẩu? [Trung bình]
Thay vì truyền mật khẩu, ta truyền đường dẫn tới file chứa mật khẩu; ứng dụng đọc file khi khởi động. Biến môi trường có mật khẩu hiện trong `docker inspect`, `/proc/<pid>/environ`, file compose, và thường bị in ra log khi debug hoặc lọt vào báo lỗi. Với `*_FILE`, chỉ đường dẫn bị lộ; nội dung chỉ đọc được từ trong container có mount. PostgreSQL, ClickHouse có sẵn hỗ trợ; code Python của dự án đọc `XXX` nếu có, không thì đọc `XXX_FILE`.

### Q90. Debezium/Kafka Connect đọc mật khẩu thế nào? [Trung bình]
Qua DirectoryConfigProvider: trong cấu hình connector, mật khẩu là một tham chiếu "đọc file cdc_db_password trong /run/secrets". Worker Connect thay giá trị lúc chạy. Cấu hình lưu trong topic nội bộ của Connect và trả về qua REST API chỉ chứa tham chiếu, không chứa mật khẩu.

### Q91. Vì sao PII salt lại là ngoại lệ? [Nâng cao]
Masking của Debezium đặt salt trong **tên** của khóa cấu hình, mà config provider chỉ thay được **giá trị**. Vì vậy `debezium/register.sh` (hoặc Control Center) đọc file salt và chèn vào cấu hình lúc đăng ký connector. Hệ quả: salt nằm trong cấu hình connector, xem được qua REST của Connect (chỉ trên localhost, không đăng nhập). Ai có salt có thể dò ngược hash của email/số điện thoại bằng từ điển, nên đây là điểm cần bảo vệ.

### Q92. Grafana đọc mật khẩu thế nào? [Cơ bản]
Mật khẩu admin qua biến có hậu tố `__FILE` (quy ước của Grafana). Datasource ClickHouse dùng cú pháp `$__file{...}` trỏ tới file mật khẩu của user `grafana_reader`. Đăng ký người dùng mới bị tắt.

### Q93. setup-env.sh có ghi đè secret đang có không? [Cơ bản]
Không. Nó chỉ tạo file còn thiếu với giá trị ngẫu nhiên, nên chạy lại an toàn với stack đang chạy. Nếu `.env` cũ còn mật khẩu, giá trị đó được **chuyển** sang file và xóa khỏi `.env`, để không đổi mật khẩu của volume đang có. Nó cũng tạo `.env` từ `.env.example`, pepper, mật khẩu admin, secret client, khóa đối tác và chứng chỉ TLS.

### Q94. Đổi một mật khẩu trên volume đã có thì làm sao? [Trung bình]
Sửa file trong `secrets/` rồi chạy `./observability/start.sh`: nó chạy lại script roles, passwords, identity trong container PostgreSQL, đặt lại mật khẩu Grafana và cấu hình connector. Phải làm vậy vì script trong `docker-entrypoint-initdb.d` chỉ chạy khi volume còn trống. Sau đó restart các service dùng mật khẩu đó để chúng đọc file mới.

### Q95. Vì sao mọi cổng bind 127.0.0.1? [Cơ bản]
Để máy khác trong cùng mạng LAN/Wi-Fi không truy cập được, đặc biệt các UI không có đăng nhập (Control Center, Kafka UI, Connect REST). Mặc định Docker publish trên 0.0.0.0 và trên Linux còn chèn rule iptables **vượt qua** firewall của host, nên một cổng PostgreSQL "chỉ để dev" có thể lộ ra mạng. Bind localhost là thay đổi rẻ mà hiệu quả.

### Q96. Các role PostgreSQL có quyền gì? [Trung bình]
`admin` là superuser, chỉ dùng cho init, Adminer, test kiểm dữ liệu. `source_api` chỉ DML (SELECT/INSERT/UPDATE/DELETE) và dùng sequence, không sở hữu gì, không DDL. `app_reader` chỉ đọc, transaction mặc định read-only, có `pg_monitor`. `monitor_reader` đọc thống kê. `cdc_user` có REPLICATION + SELECT, chỉ ghi vào bảng heartbeat và signal của Debezium, không phải superuser. `identity_app` sở hữu `identity_db`.

### Q97. Vì sao `source_api` không được quyền DDL? [Trung bình]
Nếu API bị chiếm (SQL injection, RCE), kẻ tấn công vẫn không thể DROP/ALTER bảng, tạo function, sửa publication hay REPLICA IDENTITY — những thứ có thể phá cả pipeline CDC. Họ vẫn có thể sửa/xóa dữ liệu, nên đây là giảm thiểu chứ không phải chặn hoàn toàn. Đó là nguyên tắc least privilege: cấp đúng quyền cần cho công việc.

### Q98. `REVOKE CONNECT ... FROM PUBLIC` để làm gì? [Trung bình]
Mặc định PostgreSQL cho mọi role quyền connect vào mọi database. Thu hồi quyền đó khỏi PUBLIC rồi chỉ cấp cho đúng role cần đảm bảo `identity_app` không vào được `enterprise_db`, và các role nghiệp vụ không vào được `identity_db`. Kết hợp với `pg_hba.conf` bắt buộc scram-sha-256 cho mọi kết nối TCP.

### Q99. Vì sao các script init PostgreSQL được đánh số 01–05? [Trung bình]
Entrypoint chạy file theo thứ tự tên, sắp bằng glob theo collation của locale, mà collation này bỏ qua dấu câu khi so sánh. Tên cũ `02a-roles` vì thế được xếp trước `02-monitoring`; script roles chạy khi `monitor_reader` chưa tồn tại → GRANT lỗi → init volume mới thất bại. Đổi sang số thuần 01 init, 02 monitoring, 03 roles, 04 passwords, 05 identity giữ thứ tự đúng với mọi collation. Bài học: đừng phụ thuộc vào thứ tự sắp xếp chuỗi có dấu câu.

---

## 10. Simulator, test, vận hành

### Q100. Simulator xác thực như thế nào? [Trung bình]
Dùng client `sim-orchestrator`: token client_credentials cho việc admin và đọc danh sách toàn cục; token exchange cho từng khách/shop mà actor đóng vai. Token được cache theo subject đến 1 phút trước khi hết hạn, tối đa 20.000 mục. Đăng ký khách/người bán qua identity, một phần phiên dùng đăng nhập mật khẩu thật (thỉnh thoảng cố tình gõ sai), webhook được ký lại ở mỗi lần gửi.

### Q101. Simulator xử lý 401 thế nào? [Cơ bản]
Gặp 401 với request có token, nó bỏ token đó khỏi cache, lấy token mới và thử lại **một lần**; nếu vẫn 401 thì dừng. Nguyên nhân thường là token hết hạn sớm hơn dự kiến hoặc khóa ký bị thay. Giới hạn một lần tránh vòng lặp vô hạn khi lỗi là thật.

### Q102. Mật khẩu của người dùng giả lưu ở đâu? [Trung bình]
Không lưu ở đâu cả: mật khẩu được suy ra bằng HMAC trên email với khóa là secret của client simulator, cắt 24 ký tự. Simulator luôn tính lại được khi cần đăng nhập. Hệ quả: lộ secret của `sim-orchestrator` đồng nghĩa lộ mật khẩu mọi user giả (nhưng secret đó vốn đã có scope admin).

### Q103. test_auth.py kiểm những gì? [Trung bình]
11 test: gọi ẩn danh bị 401 (catalogue vẫn mở); token giả/hỏng bị 401; người mua chỉ thao tác chính mình (404 với tài nguyên người khác, 403 với danh sách toàn cục); đơn chỉ người mua và shop xem được; người bán chỉ quản lý shop mình và không thấy ví shop khác; webhook phải ký đúng đối tác; Idempotency-Key tách theo người gọi; route không version đã bỏ; refresh rotation + reuse detection; khóa tài khoản sau 10 lần sai; client không xin vượt scope. Tất cả qua ở enforce.

### Q104. Giám sát phần auth bằng metric nào? [Cơ bản]
`auth_decisions_total{outcome}` (allowed, denied, would_deny) ở API; `identity_logins_total{outcome}` (ok, failed, locked) và `identity_tokens_total{grant}` (user, client_credentials, token_exchange) ở identity, hiển thị trên dashboard Platform Health. Alert `LoginFailuresHigh` bắn khi phần lớn lượt đăng nhập thất bại — dấu hiệu brute-force hoặc lỗi cấu hình. Bảng `audit` trong `identity_db` lưu chi tiết từng sự kiện.

---

## 11. Kịch bản tấn công và câu hỏi mở

### Q105. Nếu access token của người mua bị lộ thì sao? [Trung bình]
Kẻ tấn công có thể làm mọi thứ người mua đó làm được (xem đơn, đặt hàng, hủy đơn) trong tối đa 10 phút còn lại của token. Họ không thể đụng tới người khác (kiểm quyền sở hữu) và không lấy được token mới nếu không có refresh token. Logout hay khóa user không chặn được token đang sống vì API verify không tra DB. Muốn thu hồi tức thì cần danh sách đen `jti` hoặc giảm TTL.

### Q106. Nếu refresh token bị đánh cắp thì sao? [Trung bình]
Kẻ trộm có thể dùng nó lấy access token và refresh token mới. Nhưng ngay khi người thật (hoặc kẻ trộm) dùng lại một token đã dùng, identity phát hiện reuse và thu hồi cả family: cả hai bên mất phiên, người thật đăng nhập lại. Nếu người thật không bao giờ refresh lại, kẻ trộm giữ được phiên — vì vậy cần thêm giới hạn tuổi tối đa của family (hiện mỗi lần xoay cấp lại 30 ngày, phiên có thể kéo dài vô hạn).

### Q107. Nếu khóa ký Ed25519 của identity bị lộ thì sao? [Nâng cao]
Đó là sự cố nghiêm trọng nhất: kẻ tấn công tự ký token với bất kỳ claim nào, kể cả scope admin. Phải tạo khóa mới (hiện tại là xóa file khóa trong volume và restart identity), API sẽ tải JWKS mới khi gặp `kid` mới; mọi token cũ không còn khóa trong JWKS nên bị từ chối. Hiện chưa có xoay khóa có giai đoạn chồng lấn và khóa nằm không mã hóa trong volume; production nên dùng KMS/HSM, JWKS nhiều khóa và lịch xoay định kỳ.

### Q108. Nếu khóa webhook của MOMO bị lộ thì sao? [Trung bình]
Kẻ tấn công có thể giả callback "thanh toán thành công" cho các payment ví (do MOMO xử lý) — nghĩa là nhận hàng mà không trả tiền. Họ không giả được callback cho VNPAY, NAPAS hay hãng vận chuyển nhờ kiểm tra `wrong_partner`. Cần thay khóa (sửa file ở cả hai phía) và đối soát payment với sao kê của đối tác. Trong thực tế còn có lớp đối soát định kỳ với cổng thanh toán.

### Q109. Kẻ tấn công lấy được bản dump `identity_db` thì sao? [Trung bình]
Họ có email, hash argon2id, hash SHA-256 của refresh token, audit. Không dùng được refresh token vì chỉ có hash. Không thử mật khẩu offline được nếu không có thêm pepper (nằm trong file ngoài DB); nếu có cả pepper, argon2id vẫn làm việc dò rất tốn kém. Dữ liệu nghiệp vụ không có trong đó vì database tách riêng.

### Q110. Brute-force mật khẩu một tài khoản cụ thể thì bị chặn ra sao? [Cơ bản]
Gateway giới hạn 5 lượt login/giây theo IP; identity giới hạn 120/phút theo IP mỗi worker; sau 10 lần sai trong 15 phút tài khoản bị khóa 15 phút, tức tối đa khoảng 40 lần thử mỗi giờ cho một tài khoản. argon2id làm mỗi lần thử tốn tài nguyên server. Điểm yếu: kẻ tấn công có thể dùng cơ chế khóa để cố tình khóa tài khoản người khác (DoS), và mã 423 tiết lộ tài khoản tồn tại.

### Q111. Kẻ tấn công trong mạng nội bộ có thể làm gì? [Nâng cao]
Bên trong mạng Docker không có TLS: Kafka không SASL/ACL, PostgreSQL/ClickHouse dùng mật khẩu qua kết nối không mã hóa, HTTP giữa service là bản rõ. Một container bị chiếm có thể nghe/đọc topic Kafka, gọi thẳng identity với `X-Forwarded-For` giả (identity tin header này từ mọi nguồn) để lách giới hạn IP. Trên một máy dev, chấp nhận được; production cần mTLS, SASL/ACL Kafka, network policy, và chỉ tin header forwarded từ IP gateway.

### Q112. Những gì chưa được bảo vệ trong dự án? [Nâng cao]
Không TLS/SASL bên trong (Kafka, DB, HTTP nội bộ); Control Center, Kafka UI, Connect REST không đăng nhập (chỉ localhost); giá trị secret cũ và PII salt còn trong lịch sử git nên phải rotate; chưa có `/v1/me` và API vẫn nhận id trong body (phải khớp token); không thu hồi được access token; chưa có xoay khóa ký; `act` của token exchange chưa được audit phía API; không MFA, xác minh email, quên mật khẩu; file secret trên host chmod 644. Nói thẳng những điều này khi phỏng vấn cho thấy hiểu rõ giới hạn của thiết kế.

### Q113. Vì sao giá trị secret cũ trong lịch sử git lại là vấn đề, và xử lý thế nào? [Trung bình]
Trước khi chuyển sang file, các mật khẩu phát triển và PII salt từng nằm trong repo; xóa khỏi bản hiện tại không xóa khỏi lịch sử, ai clone repo cũng đọc được. Cách xử lý đúng là **rotate**: tạo giá trị mới cho mọi môi trường dùng chung (sửa file, chạy `./observability/start.sh`). Viết lại lịch sử git (filter-repo) chỉ là phụ, vì các bản clone cũ vẫn còn. Riêng PII salt đổi thì mọi hash đổi, nên phải quyết định từ đầu cho mỗi môi trường.

### Q114. Bạn sẽ thiết kế auth cho API này thế nào nếu làm lại từ đầu? [Nâng cao]
Tách issuer chuẩn OIDC (Keycloak/Auth0 hoặc service riêng như hiện tại), JWT bất đối xứng với JWKS nhiều khóa và xoay định kỳ, access token ngắn + refresh rotation. Ở API: dependency xác thực toàn cục, **mặc định từ chối** với policy khai báo theo route thay vì gọi tay, mọi danh tính lấy từ token qua `/v1/me/...` (không nhận id trong body), 404 cho tài nguyên không thuộc mình, scope cho vai trò hệ thống. Ghi `act` vào audit và outbox để truy được ai đóng vai ai. Đối tác dùng HMAC có timestamp + nonce store, hoặc mTLS. Thêm danh sách đen `jti` cho thu hồi khẩn cấp và mTLS giữa service bên trong.

### Q115. Nếu phải tăng tải gấp 10 lần, phần auth có thành nút thắt không? [Nâng cao]
Verify EdDSA cục bộ rất rẻ, không gọi mạng, nên API mở rộng ngang được. Identity mới là điểm nóng: argon2id tốn 19 MiB và vài chục mili giây mỗi lần login, nên login hàng loạt cần nhiều CPU/RAM; client_credentials và exchange rẻ. Simulator cache token nên số lần xin token thấp. Giới hạn theo IP trong bộ nhớ không chia sẻ giữa worker/instance — khi scale ngang cần chuyển sang Redis hoặc dựa vào gateway.

### Q116. So sánh cách xác thực đối tác bằng HMAC với mTLS hoặc OAuth client_credentials. [Nâng cao]
HMAC: đơn giản, phổ biến (Stripe, GitHub, MOMO đều dùng), bảo vệ toàn vẹn từng message và chống replay bằng timestamp, nhưng cần chia sẻ khóa đối xứng và tự lo nonce. mTLS: xác thực ở tầng kết nối, không cần chia sẻ secret, nhưng vận hành chứng chỉ phức tạp và không ký từng message (proxy ở giữa có thể đổi nội dung sau khi kết thúc TLS). OAuth client_credentials: đối tác phải gọi identity lấy token, thêm phụ thuộc và độ trễ, và không ký nội dung. Với callback một chiều từ bên ngoài, HMAC trên thân + timestamp là lựa chọn thực dụng nhất.
