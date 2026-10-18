# Q&A 05 · Q&A 05 · Mart và metric

[Tài liệu dự án](../README.md) / [Phỏng vấn](README.md)

Câu hỏi ôn tập cho [05-mart-va-metric.md](../platform/05-mart-va-metric.md). Mức độ: [Cơ bản], [Trung bình], [Nâng cao]. Số trong ví dụ lấy từ ví dụ xuyên suốt ở mục 5 của tài liệu (đơn 2 dòng, subtotal 500.000, voucher shop 50.000, voucher sàn 25.000, phí ship 30.000, freeship 15.000, total 440.000, trả 1 áo hoàn 127.500).

## Mục lục

- [A. Định nghĩa metric](#a-định-nghĩa-metric) (Q1–Q22)
- [B. Lớp gold và thiết kế mart](#b-lớp-gold-và-thiết-kế-mart) (Q23–Q34)
- [C. Từng mart](#c-từng-mart) (Q35–Q50)
- [D. Cơ chế refresh và trade-off](#d-cơ-chế-refresh-và-trade-off) (Q51–Q62)
- [E. Chốt ngày nghiệp vụ](#e-chốt-ngày-nghiệp-vụ) (Q63–Q74)
- [F. Tình huống biên](#f-tình-huống-biên) (Q75–Q86)
- [G. Reconciliation](#g-reconciliation) (Q87–Q95)
- [H. Data contract và chất lượng dữ liệu](#h-data-contract-và-chất-lượng-dữ-liệu) (Q96–Q105)
- [I. Dashboard](#i-dashboard) (Q106–Q109)
- [J. Câu hỏi mở khi phỏng vấn](#j-câu-hỏi-mở-khi-phỏng-vấn) (Q110–Q116)

---

## A. Định nghĩa metric

### Q1. GMV trong dự án này được định nghĩa thế nào? [Cơ bản]
GMV là tổng `subtotal` (giá trị hàng = số lượng × đơn giá lúc mua, trước voucher, không gồm phí ship) của các đơn **confirmed**. Đơn confirmed là đơn đã thanh toán online hoặc đơn COD (COD được xác nhận ngay khi checkout). GMV không trừ đơn bị huỷ sau khi đã trả tiền và không trừ hàng trả. Vì thế GMV ổn định khi cửa sổ thanh toán đã đóng.

### Q2. Placed GMV khác GMV ở đâu? [Cơ bản]
Placed GMV cộng subtotal của **mọi** đơn đã đặt, kể cả đơn online chưa trả tiền rồi hết hạn. GMV chỉ cộng đơn confirmed. Placed GMV không bao giờ đổi sau khi đơn được tạo, nên dùng cho số theo giờ (traffic_by_hour), khi đơn của giờ gần nhất còn đang trong cửa sổ thanh toán 15 phút. Chênh lệch placed GMV − GMV xấp xỉ giá trị giỏ hàng bị bỏ dở ở bước thanh toán.

### Q3. NMV là gì và tính thế nào? [Cơ bản]
NMV là GMV của các đơn **confirmed và kept** (không CANCELLED, không RETURNED_TO_SELLER), trừ đi giá trị hàng hoá (số lượng trả × đơn giá, trước voucher) của các đơn vị đã được hoàn tiền qua return REFUNDED. Trong ví dụ, NMV = 500.000 − 1 × 150.000 = 350.000. NMV thay đổi liên tục theo huỷ và trả hàng, kể cả với ngày đã qua lâu.

### Q4. Net revenue khác NMV thế nào? [Trung bình]
NMV ở mức giá trước voucher và trừ hàng trả theo giá gốc. Net revenue là số người mua thực trả cho hàng: Σ `net_amount` của dòng (line_total trừ phần voucher shop và voucher sàn được phân bổ cho dòng), trừ số tiền thực hoàn. Với áo trong ví dụ: net 255.000 − 127.500 = 127.500, trong khi phần NMV của áo là 300.000 − 150.000 = 150.000. Net revenue dùng để xếp hạng sản phẩm và ngành hàng; cả hai đều không gồm phí ship.

### Q5. Completed revenue là gì, có trừ hoàn tiền không? [Trung bình]
Completed revenue là Σ `total` (số người mua phải trả: subtotal − voucher shop − voucher sàn + phí ship − trợ giá ship) của các đơn đang ở trạng thái COMPLETED. Nó **không** trừ hoàn tiền trả hàng một phần, vì đơn trả một phần vẫn chuyển về COMPLETED. Trong ví dụ, completed revenue = 440.000 dù đã hoàn 127.500. Đơn trả toàn bộ ở lại RETURN_REFUND nên không nằm trong completed revenue.

### Q6. "Confirmed" và "kept" nghĩa là gì, tại sao tách hai cờ? [Cơ bản]
Confirmed trả lời "khách đã cam kết mua chưa" (đã trả tiền online hoặc COD), kept trả lời "đơn có còn sống không" (không huỷ, không bị hoàn về shop). GMV chỉ cần confirmed; NMV, net revenue, promo cost cần cả hai. Tách hai cờ giúp đặt định nghĩa một lần trong `gold.order_facts` và mọi mart dùng chung, thay vì mỗi mart tự viết điều kiện trạng thái.

### Q7. Vì sao COD được coi là confirmed ngay lúc checkout? [Trung bình]
Với COD không có bước thanh toán trước; checkout chuyển thẳng sang COD_CONFIRMED và đơn vào TO_SHIP. Nếu chờ đến lúc thu tiền khi giao mới tính, GMV của một ngày sẽ thay đổi trong nhiều ngày sau đó và không thể chốt sớm. Cái giá là GMV chứa cả đơn COD về sau bị huỷ hoặc giao thất bại; những đơn đó bị loại ở NMV.

### Q8. Đơn RETURN_REFUND có nằm trong NMV không? [Trung bình]
Có, vì kept chỉ loại CANCELLED và RETURNED_TO_SELLER. Phần trả được trừ riêng theo giá trị từng đơn vị đã hoàn. Nếu trả toàn bộ, đơn ở lại RETURN_REFUND và đóng góp subtotal − toàn bộ giá trị trả = 0 vào NMV. Return chưa REFUNDED (mới yêu cầu, đang gửi hàng về) chưa làm giảm NMV.

### Q9. Take và take rate tính thế nào? [Trung bình]
Take = hoa hồng + phí thanh toán (2%) mà sàn thu từ người bán, lấy từ sổ cái người bán và đổi dấu thành số dương. Các bút toán này chỉ sinh khi đơn COMPLETED, tính trên SALE của người bán (giá trị hàng giữ lại trừ voucher shop). Take rate = take / Σ subtotal của đơn COMPLETED. Trong ví dụ (hoa hồng 5%): take = 15.750 + 6.300 = 22.050, đóng góp 22.050 / 500.000 = 4,41%.

### Q10. Vì sao mẫu số take rate là subtotal đơn COMPLETED chứ không phải toàn bộ GMV? [Nâng cao]
Take chỉ phát sinh khi đơn hoàn tất, nên chia cho toàn bộ GMV sẽ làm take rate thấp giả tạo với các đơn còn đang giao (chưa đến lúc thu phí). Ghép tử và mẫu cùng một tập đơn (đơn đã hoàn tất) cho tỷ lệ "sàn giữ được bao nhiêu trên mỗi đồng hàng đã bán xong". Tử số tính sau voucher shop và sau trả hàng còn mẫu số là subtotal gốc, nên take rate phản ánh cả tác động của voucher shop và trả hàng.

### Q11. Ba loại hoàn tiền trong P&L là gì và vì sao tách ra? [Trung bình]
Return refunds là hoàn tiền gắn với return (chi phí bán hàng thật). Cancellation refunds là hoàn đúng total cho đơn online đã trả mà bị huỷ hoặc giao thất bại. Late-payment refunds là hoàn toàn bộ khoản tiền về sau khi checkout đã hết hạn/bị thay thế. Chỉ loại đầu là chi phí của doanh số đã ghi nhận; hai loại sau là trả lại tiền của đơn chưa bao giờ là doanh số, gộp vào sẽ thổi phồng chi phí.

### Q12. Voucher shop và voucher sàn được xử lý khác nhau thế nào? [Trung bình]
Cả hai đều không trừ khỏi GMV/NMV nhưng đều trừ khỏi net revenue và total. Voucher shop do người bán gánh: trừ vào SALE của người bán và hiện ở seller_promo_cost. Voucher sàn và freeship do sàn gánh: hiện ở platform_promo_cost, người bán không bị trừ. Cả hai promo cost trong P&L chỉ tính trên đơn confirmed và kept.

### Q13. Voucher cấp đơn được phân bổ xuống dòng như thế nào, để làm gì? [Trung bình]
Voucher shop của đơn và phần voucher sàn của đơn được chia xuống các dòng theo tỷ lệ line_total, phần lẻ chia theo phương pháp largest remainder để tổng khớp tuyệt đối. Trong ví dụ, 50.000 voucher shop chia 30.000/20.000 theo tỷ lệ 300.000/200.000. Mục đích là khi trả một phần, hoàn đúng số đã trả cho từng đơn vị: hoàn = net của dòng × số lượng trả / số lượng đặt.

### Q14. Phí ship có trong GMV không? [Cơ bản]
Không. GMV, NMV, net revenue chỉ là giá trị hàng hoá. Phí ship có trong `total`, nên có trong completed revenue, tiền thu (collected) và hoàn tiền khi huỷ đơn đã trả. Trợ giá ship (freeship) là chi phí khuyến mãi của sàn.

### Q15. AOV tính trên tập đơn nào? [Cơ bản]
AOV = GMV / số đơn confirmed, làm tròn. Đơn chưa trả tiền bị hết hạn không vào tử lẫn mẫu. Lưu ý một checkout nhiều shop sinh nhiều đơn, nên AOV ở đây là giá trị trung bình một đơn của một shop, không phải một lần thanh toán.

### Q16. Buyers đếm gì? [Cơ bản]
Số khách hàng khác nhau có ít nhất một đơn confirmed, tính đếm chính xác (không xấp xỉ). Ở daily_sales là khách mua trong ngày; ở overview_kpis là khách mua trong toàn lịch sử. Tổng buyers các ngày lớn hơn buyers toàn kỳ vì khách quay lại.

### Q17. Unpaid cancelled khác cancelled orders thế nào? [Trung bình]
Cancelled orders đếm mọi đơn đang CANCELLED. Unpaid cancelled chỉ đếm đơn CANCELLED chưa bao giờ confirmed, tức bỏ dở thanh toán hoặc hết hạn 15 phút. Hiệu hai số là huỷ "thật" sau khi đã cam kết mua (khách huỷ, shop huỷ, quá SLA). Tách ra để không lẫn vấn đề thanh toán với vấn đề vận hành.

### Q18. Pack time đo từ đâu đến đâu? [Trung bình]
Từ lần đầu đơn vào TO_SHIP (lúc thanh toán xong, hoặc ngay checkout với COD) đến lần đầu vào READY_TO_SHIP, lấy từ lịch sử trạng thái đơn. Trung bình theo shop, quy ra giờ, chỉ tính đơn đã đóng gói. Bắt đầu từ TO_SHIP thay vì từ lúc đặt đơn để không phạt người bán vì thời gian khách chưa trả tiền.

### Q19. Return rate có mấy định nghĩa? [Nâng cao]
Hai. Theo cohort ngày giao: số đơn có bất kỳ yêu cầu trả nào / số đơn đã giao trong ngày đó, kèm cờ matured. Theo lý do: số return của lý do đó trên các đơn đã "chín" / tổng số đơn đã giao và đã "chín". "Returned" đếm return ở mọi trạng thái, kể cả bị từ chối, nên là tỷ lệ yêu cầu trả, không phải tỷ lệ hoàn tiền.

### Q20. Cohort "matured" nghĩa là gì? [Trung bình]
Một đơn đã giao có thể yêu cầu trả cho tới khi tự hoàn tất 3 ngày nghiệp vụ sau khi giao. Cohort ngày giao được coi là matured khi lần giao muộn nhất của cohort đã cách thời điểm đặt đơn mới nhất trong warehouse ít nhất 3 ngày. Tỷ lệ của cohort chưa chín còn tăng, so nó với cohort đã chín sẽ đánh giá thấp tỷ lệ trả.

### Q21. Sell-through của flash sale là gì? [Cơ bản]
Sell-through = đã bán / quota của các suất SKU trong chiến dịch. "Đã bán" do nguồn duy trì và bằng số đơn vị trên các đơn chưa huỷ (check_data kiểm). Doanh thu kèm theo là Σ đã bán × giá sốc, không trừ trả hàng hay voucher.

### Q22. Redemption rate của voucher là gì? [Cơ bản]
Redemption rate = số lượt dùng đang APPLIED / giới hạn lượt dùng. Lượt RELEASED (checkout kết thúc không trả tiền, voucher được trả lại) không tính. Discount cost là tổng tiền giảm của các lượt APPLIED.

## B. Lớp gold và thiết kế mart

### Q23. Gold trong dự án gồm những gì? [Cơ bản]
Một view trung gian `order_facts` (một dòng mỗi đơn, kèm cờ confirmed/kept và giá trị trả), 18 view mart, bảng `daily_sales_closed` cho ngày đã chốt, và `cdc_freshness` (độ trễ pipeline, không phải mart). Không có view theo từng thực thể kiểu star schema; các mart đọc thẳng `consistent.<table>(v)` của thực thể cần thiết.

### Q24. Grain của order_facts là gì, vì sao chọn đơn hàng? [Trung bình]
Một dòng mỗi đơn — đơn của một shop, không phải checkout. Hầu hết metric bán hàng (GMV, NMV, AOV, cancel rate theo shop, P&L) có grain tự nhiên là đơn, và trạng thái, voucher, phương thức thanh toán đều ở cấp đơn. Giá trị trả hàng được cộng sẵn lên cấp đơn để không phải join lại ở mỗi mart. Mart cần cấp dòng (product, category) thì join thêm order_items và lọc qua order_facts.

### Q25. Tham số watermark `v` trong view gold có tác dụng gì? [Trung bình]
Mọi view gold nhận `v` và truyền cùng `v` xuống mọi bảng `consistent.*` nó đọc. Nhờ vậy cả câu truy vấn thấy một trạng thái đã tồn tại thật ở PostgreSQL tại một điểm commit, không có transaction bị xé đôi giữa các topic Kafka. Không có `v`, mỗi subquery chọn "mới nhất" ở thời điểm riêng và có thể mâu thuẫn trong cùng một câu (chi tiết ở doc 04).

### Q26. gold_snap là gì và vì sao không viết tay? [Trung bình]
gold_snap là bản sao của mỗi view gold nhưng đọc bảng snapshot typed thay vì `consistent.*`. Nó được sinh tự động từ định nghĩa gold mỗi khi warehouse-jobs khởi động, bằng cách thay nguồn. Không viết tay để chỉ có một nơi định nghĩa metric; check_data còn so mart (tính từ gold_snap) với gold tính trực tiếp để chắc hai đường ra cùng kết quả.

### Q27. Vì sao định nghĩa metric phải nằm ở một chỗ? [Cơ bản]
Nếu mỗi dashboard/mart tự viết "đơn hợp lệ là gì", sớm muộn hai báo cáo sẽ ra hai GMV khác nhau và không ai biết cái nào đúng. Ở đây các định nghĩa ghi ở đầu file gold, cờ confirmed/kept ở order_facts, và check_data cài lại độc lập trong PostgreSQL để đối chiếu. Mart không có logic riêng, chỉ chụp kết quả gold.

### Q28. Tại sao doanh số quy theo ngày đặt đơn chứ không theo ngày thanh toán hay hoàn tiền? [Trung bình]
Đây là cách nhìn theo cohort đơn hàng: mỗi đơn thuộc đúng một ngày, ngày đó không đổi, dễ so sánh và dễ chốt. Theo ngày thanh toán thì đơn COD thuộc ngày giao; theo ngày hoàn tiền thì doanh số âm có thể rơi vào ngày khác. Cái giá là số của ngày cũ (NMV) còn thay đổi, nên mới cần bản chốt.

### Q29. Mọi mart có cùng phạm vi thời gian không? [Trung bình]
Không. daily_sales theo ngày đặt, return_rate_by_cohort theo ngày giao, traffic_by_hour là 48 giờ gần nhất; order_status_summary, inventory_risk, customer_tiers là trạng thái hiện tại; còn lại là tích luỹ toàn lịch sử. Bộ chọn thời gian của Grafana không lọc mart nào.

### Q30. "Bây giờ" trong các cửa sổ thời gian của gold là gì? [Nâng cao]
Là thời điểm đặt đơn mới nhất trong dữ liệu tại watermark, không phải giờ máy. Đồng hồ nghiệp vụ là đồng hồ mô phỏng có thể chạy nhanh gấp nhiều lần; nếu dùng giờ máy, cửa sổ 48 giờ hay 3 ngày sẽ sai theo tốc độ mô phỏng và trôi khi simulator dừng. Dùng dữ liệu làm mốc còn giúp kết quả tái lập được: cùng `v` luôn ra cùng mart.

### Q31. Tại sao hoàn tiền trả hàng chỉ tính return có trạng thái REFUNDED? [Trung bình]
Chỉ REFUNDED là tiền đã thực trả lại; REQUESTED, APPROVED, RETURNING, RECEIVED còn có thể kết thúc khác, REJECTED/CLOSED thì không hoàn gì. Trừ sớm sẽ làm NMV giảm rồi tăng lại nếu return bị từ chối. Trong returns_summary các trạng thái khác vẫn được đếm riêng là open/rejected để thấy dòng chảy.

### Q32. Vì sao NMV trừ hàng trả theo giá trước voucher, còn net revenue trừ theo tiền hoàn thực? [Nâng cao]
Mỗi metric giữ nhất quán với cơ sở của nó. NMV là giá trị hàng hoá trước voucher, nên phần trả cũng phải là giá trị hàng trước voucher (số lượng × đơn giá). Net revenue là tiền thực thu sau voucher, nên trừ đúng số tiền thực hoàn (đã gồm phần voucher phân bổ). Trộn hai cơ sở sẽ khiến NMV âm hoặc net revenue bị trừ thừa phần voucher.

### Q33. Có metric nào gold không tự tính mà lấy từ nguồn? [Trung bình]
Có: total_spent và hạng của khách (customer_tiers), sold của flash sale, penalty_points của shop, và các bút toán sổ cái người bán (SALE, COMMISSION, PAYMENT_FEE). Những số này do ứng dụng nguồn duy trì trong cùng transaction nghiệp vụ. Warehouse tin chúng nhưng check_data kiểm công thức của chúng ở PostgreSQL (nhóm money, promo, settlement).

### Q34. Tại sao gold đọc consistent.* chứ không đọc silver FINAL trực tiếp? [Trung bình]
Mỗi bảng nguồn có topic riêng, ClickHouse tiêu thụ độc lập, nên silver có thể đã thấy đơn COMPLETED mà chưa thấy bút toán SALE cùng transaction. Một probe dưới tải đo được 20 đơn như vậy tại một thời điểm. `consistent.*(v)` ẩn các transaction chưa về đủ, nên P&L hay settlement không bao giờ lệch nhau vì đọc giữa chừng.

## C. Từng mart

### Q35. overview_kpis có những gì và panel nào dùng? [Cơ bản]
Một dòng toàn lịch sử: orders, GMV, NMV, completed revenue, AOV, buyers, cancelled, active customers, số shop. Dashboard chỉ dùng bốn ô: Confirmed GMV, NMV, Orders, Buyers. Mart này còn được dùng để quyết định có bỏ qua lần refresh hay không.

### Q36. daily_sales có cột nào? [Cơ bản]
Mỗi ngày nghiệp vụ một dòng: orders, confirmed orders, buyers, placed GMV, GMV, NMV, completed revenue, cancelled orders, unpaid cancelled, AOV. Panel vẽ placed GMV, GMV, NMV theo ngày. Đây là số sống; số đã chốt ở `daily_sales_closed`.

### Q37. Tính GMV theo ngày như thế nào nếu viết lại từ đầu? [Trung bình]
Lấy mọi đơn tại một watermark, đổi thời điểm đặt sang giờ Việt Nam để lấy ngày, giữ đơn đã thanh toán online hoặc là COD, cộng subtotal theo ngày. Không lọc trạng thái hiện tại (đơn đã trả tiền rồi huỷ vẫn tính). Cẩn thận dùng thời điểm nghiệp vụ, không dùng thời điểm ingest; và đọc mọi bảng ở cùng một watermark.

### Q38. Tính NMV của một shop như thế nào? [Trung bình]
Lấy các đơn của shop đã confirmed và kept, lấy subtotal của mỗi đơn trừ giá trị hàng của các đơn vị đã hoàn tiền (số lượng trả × đơn giá trên dòng, chỉ return REFUNDED), rồi cộng lại. Đây chính là cột nmv trong shop_performance. Nếu muốn theo ngày thì nhóm thêm theo ngày đặt giờ VN.

### Q39. traffic_by_hour dùng để làm gì và có bẫy gì? [Trung bình]
Thấy cao điểm trong ngày, hiệu ứng flash sale, ngày đôi như 11.11: mỗi giờ trong 48 giờ gần nhất có số đơn, placed GMV và số đơn có dùng voucher. Dùng placed GMV vì đơn của giờ mới nhất còn trong cửa sổ thanh toán. Bẫy: giờ được nhóm trên mốc UTC, ranh giới trùng giờ VN (lệch tròn 7 giờ) nhưng nhãn hiển thị là giờ UTC.

### Q40. Funnel có 5 bước nào và vì sao không chia thẳng ra tỷ lệ chuyển đổi? [Trung bình]
Giỏ tạo → checkout → checkout đã trả hoặc COD đã xác nhận → đơn đã giao cho vận chuyển → đơn hoàn tất. Ba bước đầu đếm giỏ/checkout, hai bước cuối đếm đơn, và một checkout nhiều shop sinh nhiều đơn. Vì vậy bước 4 có thể lớn hơn bước 3; tiêu đề panel ghi "counts have different grains".

### Q41. payment_summary tính success rate thế nào? [Cơ bản]
Success rate = lượt thành công / lượt đã có kết quả (bỏ PENDING). Bỏ PENDING vì lượt đang chờ chưa thể nói thành hay bại, đặc biệt COD ở PENDING tới khi giao. Mart còn có collected (tổng tiền lượt thành công) và refunded (mọi hoàn tiền trên lượt của phương thức đó).

### Q42. Khác biệt grain thanh toán online và COD ảnh hưởng gì tới payment_summary? [Nâng cao]
Thanh toán online gắn với checkout: một lượt trả cho mọi đơn của checkout, nên collected là grand total của checkout. COD có đúng một lượt thu cho mỗi đơn. Vì vậy số lượt của hai nhóm không so trực tiếp với số đơn; muốn so phải quy về cùng grain.

### Q43. category_sales gộp ngành hàng thế nào? [Cơ bản]
Mỗi sản phẩm thuộc một ngành lá; nếu ngành lá có cha thì gộp về cha, không có cha thì giữ chính nó. Measure như product_sales: units sold, gross revenue, net revenue, trên đơn confirmed và kept, trừ phần đã hoàn.

### Q44. product_sales có grain gì? [Trung bình]
Tên sản phẩm × ngành lá × shop, sắp theo net revenue giảm dần. Measure: units sold (đặt − hoàn), gross revenue (Σ line_total), net revenue (Σ net − tiền hoàn), số đơn khác nhau. Gộp theo tên nên hai sản phẩm trùng tên cùng shop và ngành sẽ chung một dòng.

### Q45. shop_performance đo cancel rate thế nào, có công bằng với shop không? [Nâng cao]
Cancel rate = đơn CANCELLED / mọi đơn của shop, gồm cả đơn khách không trả tiền và hết hạn — lỗi không phải của shop. Muốn đánh giá shop công bằng hơn nên dùng sla_breaches (đơn bị huỷ vì shop không đóng gói kịp SLA 2 ngày) hoặc tính cancel rate trên đơn confirmed. Bảng điểm còn có pack time, điểm phạt, rating trung bình và số đánh giá.

### Q46. inventory_risk liệt kê những SKU nào? [Cơ bản]
Mọi dòng tồn kho (SKU tại shop), kèm available, reserved, reorder point và cờ needs_reorder (available ≤ reorder point), sắp theo available − reorder point tăng dần. Chú thích SQL nói chỉ SKU dưới ngưỡng nhưng view thực tế trả mọi SKU; panel lấy 200 dòng đầu nên hiển thị nhóm rủi ro nhất.

### Q47. platform_pnl có grain gì và có những cột nào? [Cơ bản]
Một dòng cho toàn lịch sử: GMV, NMV, take, platform promo cost, seller promo cost, ba loại hoàn tiền, tổng đã chi cho người bán, take rate. Không có chiều thời gian; muốn P&L theo tháng phải thêm grain ngày vào order_facts và các nguồn khác.

### Q48. delivery_performance tính success rate và thời gian vận chuyển thế nào? [Trung bình]
Chỉ kiện chiều đi. Success rate = kiện giao thành công / (thành công + trả về người gửi), bỏ kiện đang đi để không làm loãng. Thời gian vận chuyển trung bình = giao xong − lấy hàng, quy ra giờ nghiệp vụ, chỉ kiện đã giao. Có thêm tổng số lần giao thất bại.

### Q49. seller_settlement balance nghĩa là gì? [Trung bình]
Balance = tổng mọi bút toán sổ cái của shop (SALE dương, COMMISSION, PAYMENT_FEE, PAYOUT âm) = số sàn còn nợ shop. Nó gồm cả phần bút toán chưa được cộng vào ví (ví được cộng theo lô). Trong ví dụ, sau một đơn: 315.000 − 15.750 − 6.300 = 292.950 trước khi chi trả.

### Q50. Vì sao discount cost ở voucher_performance có thể lớn hơn promo cost ở P&L? [Nâng cao]
Lượt dùng voucher của checkout đã thanh toán vẫn ở APPLIED kể cả khi đơn sau đó bị huỷ. voucher_performance cộng mọi lượt APPLIED, nên là "chi phí đã cam kết". P&L chỉ tính promo cost trên đơn confirmed và kept, tức chi phí thực của doanh số còn sống. Hai số đo hai thứ khác nhau, không phải lỗi.

## D. Cơ chế refresh và trade-off

### Q51. Một lần refresh mart diễn ra thế nào? [Trung bình]
Mỗi 15 giây warehouse-jobs chốt một watermark, đưa snapshot tới watermark đó, rồi với từng mart chèn toàn bộ kết quả gold_snap gắn refresh_id = watermark. Sau cùng nó ghi một dòng vào `ops.mart_refreshes`. Nếu lần trước đã ở cùng watermark và đã có dữ liệu thì bỏ qua.

### Q52. Vì sao dòng ops.mart_refreshes được ghi sau cùng? [Trung bình]
Nó là dấu xuất bản. Người đọc chỉ lấy refresh_id lớn nhất trong bảng đó, nên chỉ thấy những lần refresh mà cả 18 mart đã được ghi xong. Nếu job chết giữa chừng, các dòng nửa vời có refresh_id mới nhưng không ai đọc tới.

### Q53. Lần refresh hỏng giữa chừng được xử lý thế nào? [Nâng cao]
Lần sau thử lại cùng watermark: với mỗi mart, nếu đã có dòng mang refresh_id đó thì xoá chúng (chờ xoá xong) rồi mới chèn lại. Đây là kiểu delete-then-insert theo khoá refresh_id, làm việc chèn idempotent. Chỉ xoá khi thực sự có dòng sót, vì mutation trong ClickHouse đắt, không nên làm mỗi lần.

### Q54. Mart có incremental không? [Nâng cao]
Phần incremental nằm ở snapshot: mỗi lượt chỉ đọc lại các key bị transaction mới chạm vào, rebuild toàn bộ mỗi giờ hoặc sau lỗi. Bản thân mart được tính lại toàn bộ mỗi lần, nhưng từ bảng typed nên chỉ mất 0,3–1 giây cho 18 mart. Cách này đơn giản và đúng hơn so với cập nhật tăng dần từng mart, vì nhiều mart có tỷ lệ, distinct count, cửa sổ thời gian rất khó cập nhật tăng dần chính xác. Chi tiết snapshot ở doc 04.

### Q55. Vì sao vật chất hoá mart thay vì để Grafana gọi view gold? [Trung bình]
Tính gold trực tiếp từ consistent.* mất khoảng 4,6 giây cho 18 mart ở 35 nghìn đơn và tăng theo lịch sử; nhân với số người xem, số panel và mỗi 30 giây thì không chịu nổi. Mart biến mỗi panel thành đọc một bảng nhỏ (SLO p95 dashboard 500 ms). Thêm nữa mọi panel đọc cùng refresh_id nên nhất quán, và kết quả đã xuất bản vẫn còn khi job hoặc ClickHouse gặp sự cố.

### Q56. Đánh đổi của mart vật chất hoá là gì? [Trung bình]
Độ tươi: dashboard chậm hơn nguồn cỡ chu kỳ refresh (15 giây) cộng độ trễ CDC. Thêm một service phải vận hành, cần alert khi refresh dừng (MartRefreshStale). Và tốn chỗ cho các bản refresh cũ trong tối đa khoảng một giờ.

### Q57. Vì sao chỉ chạy một replica warehouse-jobs? [Trung bình]
Hai replica sẽ cùng refresh một watermark, cùng phát hiện "dòng sót" của nhau và xoá lẫn nhau, cùng chốt ngày. Logic không có khoá phân tán. Một replica đủ vì một lượt chỉ mất khoảng 1–2 giây; muốn HA thì cần leader election.

### Q58. Bản refresh cũ được dọn thế nào? [Cơ bản]
Mỗi giờ job xoá các dòng mart có refresh_id khác bản mới nhất và đã quá 1 giờ. Bảng ops.mart_refreshes có TTL 1 ngày. Mart không có TTL nên không bị mất khi restart.

### Q59. Snapshot, mart, chốt ngày, reconciliation có dùng cùng watermark không? [Trung bình]
Trong một lượt thì có: job lấy watermark một lần, đưa snapshot tới đó, rồi refresh mart, chốt ngày và (nếu đến hạn) reconciliation đều đọc snapshot tại watermark đó. Nhờ vậy ngày chốt bằng đúng daily_sales của lần refresh cùng lượt.

### Q60. Nếu không có commit mới thì sao? [Cơ bản]
Watermark không đổi; job thấy lần refresh gần nhất đã ở đúng watermark và đã có dòng overview_kpis nên bỏ qua. Snapshot cũng không chạy lại nếu chưa tới hạn rebuild. Dashboard vẫn hiện bản cũ, đúng vì dữ liệu không đổi.

### Q61. _row trong mart dùng để làm gì? [Cơ bản]
Là số thứ tự dòng khi chèn, giữ đúng thứ tự sắp xếp của view gold (ví dụ product_sales theo net revenue giảm dần). Bảng mart sắp theo refresh_id và _row, panel Grafana sắp theo _row nên hiện đúng thứ tự gold mà không cần biết cột sắp.

### Q62. Đổi định nghĩa một mart thì mart cũ xử lý thế nào? [Nâng cao]
Bảng mart được tạo nếu chưa có, theo schema của view gold lúc đó, và không tự đổi schema. Đổi cột thì phải migrate tường minh (xoá hoặc alter bảng mart) trước khi job tạo lại. gold_snap được sinh lại từ gold mỗi khi job khởi động nên logic mới có hiệu lực sau restart.

## E. Chốt ngày nghiệp vụ

### Q63. "Chốt ngày" là gì? [Cơ bản]
Chép dòng daily_sales của một ngày đã kết thúc vào `daily_sales_closed`, kèm thời điểm chốt, một lần duy nhất. Từ đó dòng chốt không bao giờ sửa, là con số "đã báo cáo". daily_sales sống vẫn tiếp tục đổi bên cạnh.

### Q64. Điều kiện để một ngày được chốt là gì? [Trung bình]
Ba điều kiện cùng lúc: ngày đã kết thúc ít nhất 1 giờ nghiệp vụ (đã có đơn đặt lúc từ 01:00 ngày hôm sau); ngày chưa được chốt; và không còn đơn nào của ngày đó ở trạng thái UNPAID. Kiểm tra chạy sau mỗi lần refresh mart, đọc snapshot tại cùng watermark.

### Q65. Vì sao phải đóng băng số ngày? [Cơ bản]
Báo cáo đã gửi đi phải tái lập được: hỏi "hôm 10/03 báo GMV bao nhiêu" phải ra đúng số đã báo, không phải số đã bị trả hàng làm thay đổi. Nó cũng tách rõ "đã báo cáo gì" và "thực tế bây giờ"; chênh lệch NMV giữa hai bản là tác động của huỷ/trả hàng phát sinh sau.

### Q66. Những cột nào của ngày đã chốt vẫn khớp bản sống, cột nào không? [Trung bình]
Orders, placed GMV, confirmed orders, GMV, buyers, AOV cố định ở cả hai bản (check_data khẳng định placed GMV và GMV không lệch). NMV và cancelled orders ở bản chốt là số lúc chốt, bản sống còn đổi theo huỷ/trả. Completed revenue lúc chốt gần như bằng 0 vì đơn chưa giao xong, bản sống tăng dần trong vài ngày.

### Q67. Vì sao chờ 1 giờ mà không chốt ngay lúc 00:00? [Trung bình]
Đơn online đặt lúc 23:55 còn 15 phút để trả tiền; chốt ngay thì GMV của ngày thiếu các khoản trả muộn hợp lệ. Một giờ là dư so với cửa sổ 15 phút và cho dữ liệu kịp về warehouse. Điều kiện UNPAID bổ sung trường hợp transaction thanh toán commit muộn hơn nữa.

### Q68. Kể lại sự cố 15 ngày bị chốt sai. [Nâng cao]
Ban đầu chỉ có điều kiện thời gian: qua 1 giờ là chốt. Khi đồng hồ mô phỏng chạy rất nhanh (ví dụ ×1440, 1 giờ nghiệp vụ chỉ khoảng 2,5 giây thật), một transaction thanh toán chậm commit sau khi ngày đã bị chốt, nên GMV sống lớn hơn GMV đã chốt. Kiểm tra "placed GMV và GMV của ngày chốt không bao giờ đổi" trong check_data bắt được 15 ngày lệch. Sửa bằng điều kiện "không còn đơn UNPAID", bỏ các dòng chốt sai và chốt lại.

### Q69. Bài học từ sự cố đó là gì? [Nâng cao]
Điều kiện "đã đủ lâu" theo đồng hồ là giả định về độ dài transaction, và giả định đó vỡ khi tải cao hoặc đồng hồ nhanh. Nên chốt theo trạng thái nghiệp vụ: không còn gì dở dang có thể thay đổi số. Và phải có kiểm tra tự động khẳng định số đã chốt không đổi, vì lỗi kiểu này không ai nhìn bằng mắt mà thấy.

### Q70. Vì sao chốt ngày chạy trong service thay vì refreshable materialized view của ClickHouse? [Nâng cao]
Chốt phải đọc tại một watermark đã chốt và truyền nó xuống gold; materialized view tự động không có cách pin tham số đó, và mỗi subquery sẽ tự chọn "mới nhất". Service còn kiểm được điều kiện UNPAID trên cùng snapshot và đảm bảo chèn đúng một lần.

### Q71. Làm sao biết một ngày đã chốt chưa và chốt lúc nào? [Cơ bản]
Tra bảng `gold.daily_sales_closed` theo ngày: có dòng là đã chốt, cột closed_at là thời điểm chốt (giờ máy, UTC). Mỗi ngày chỉ được có một dòng; check_data báo lỗi nếu có ngày bị chốt hai lần.

### Q72. Dashboard có hiển thị ngày đã chốt không? [Cơ bản]
Không. Dashboard Business Analytics chỉ đọc mart sống; mô tả dashboard ghi rõ NMV thay đổi theo trả hàng và đây không phải snapshot ngày đã chốt. Muốn xem số đã báo cáo thì truy vấn bảng closed.

### Q73. Nếu simulator dừng thì ngày cuối có được chốt không? [Trung bình]
Không, cho tới khi có đơn mới. Điều kiện thời gian đo theo thời điểm đặt đơn mới nhất, không theo giờ máy; khi không có đơn mới, "bây giờ" của warehouse đứng yên. Đây là thiết kế có chủ ý để tốc độ đồng hồ mô phỏng không ảnh hưởng tới kết quả.

### Q74. Có trường hợp nào số chốt vẫn sai không? [Nâng cao]
Còn một khe nhỏ: một transaction tạo đơn mà commit muộn hơn 1 giờ nghiệp vụ so với thời điểm đơn được đóng dấu sẽ làm placed GMV đổi, điều kiện UNPAID không bắt vì đơn chưa tồn tại lúc chốt. Với đồng hồ nhanh, 1 giờ nghiệp vụ có thể chỉ vài giây thật. Kiểm tra closed-day trong check_data sẽ phát hiện nếu xảy ra; cách chặt hơn là chốt khi watermark đã vượt qua mọi transaction bắt đầu trước cuối ngày.

## F. Tình huống biên

### Q75. Đơn bị huỷ sau khi ngày đã chốt thì số nào đổi? [Trung bình]
Bản chốt không đổi gì. Bản sống của ngày đặt đơn: cancelled orders tăng 1, NMV giảm đúng giá trị đơn (đơn không còn kept), GMV không đổi (đơn vẫn confirmed). Nếu đơn đã trả online, P&L có thêm một khoản cancellation refund bằng total.

### Q76. Trả hàng hoàn tiền vào ngày khác thì ghi nhận vào ngày nào? [Trung bình]
Vào ngày **đặt đơn** của đơn đó trong daily_sales (NMV giảm), không phải ngày hoàn tiền. Trong ví dụ, hoàn 127.500 ngày 14/03 làm NMV ngày 10/03 giảm 150.000. Nếu cần báo cáo theo dòng tiền (refund theo ngày hoàn) thì phải làm mart khác theo ngày của refund.

### Q77. Webhook thanh toán đến trễ sau khi checkout hết hạn thì sao? [Nâng cao]
Checkout đã hết hạn, các đơn đã CANCELLED và không có paid_at, nên chúng không confirmed và không vào GMV. Khoản tiền về muộn được hoàn toàn bộ với lý do late_payment và hiện ở late-payment refunds trong P&L, không phải chi phí bán hàng. check_data có kiểm tra "late / superseded online payments are refunded in full".

### Q78. Thanh toán thành công nhưng transaction commit chậm, sau đơn đặt sau nó — ảnh hưởng gì? [Nâng cao]
Trong khoảng đó warehouse vẫn thấy đơn UNPAID, nên GMV của ngày thấp hơn thực tế một chút. Watermark đảm bảo không thấy nửa transaction, nhưng không thể thấy một transaction chưa commit. Vì vậy ngày chỉ được chốt khi không còn đơn UNPAID — chính là bài học 15 ngày.

### Q79. Đơn đặt lúc 00:30 giờ VN thuộc ngày nào? [Cơ bản]
Thuộc ngày đó theo giờ VN, dù giờ lưu là 17:30 UTC của ngày hôm trước. Mọi đơn từ 00:00 đến 06:59 giờ VN đều có ngày UTC là hôm trước. Gold luôn đổi sang Asia/Ho_Chi_Minh trước khi lấy ngày, và check_data cũng nhóm theo giờ VN ở PostgreSQL để đối chiếu.

### Q80. Đơn COD bị giao thất bại và hoàn về shop ảnh hưởng metric thế nào? [Trung bình]
Đơn vẫn confirmed nên còn trong GMV, nhưng RETURNED_TO_SELLER không phải kept nên rời NMV. Lượt thu COD chuyển CANCELLED, không có hoàn tiền vì chưa thu. Ở delivery_performance, kiện tính là returned_to_sender và kéo success rate xuống.

### Q81. Khách trả toàn bộ đơn thì sao? [Trung bình]
Return REFUNDED với mọi đơn vị, đơn ở lại RETURN_REFUND (không về COMPLETED), nên không được quyết toán cho người bán và không có take. NMV của đơn = 0, net revenue các dòng = 0, units sold = 0. Return refunds trong P&L = tổng net của các dòng (425.000 trong ví dụ), không gồm phí ship.

### Q82. Return bị shop từ chối thì có ảnh hưởng metric bán hàng không? [Trung bình]
Không ảnh hưởng NMV hay net revenue vì không có hoàn tiền. Nhưng return vẫn được đếm trong return_rate_by_cohort (đơn có yêu cầu trả) và trong returns_summary ở cột open (khi đang REJECTED chờ khiếu nại) rồi rejected (khi CLOSED). Đơn về COMPLETED khi return đóng.

### Q83. Đơn có voucher sàn chia cho nhiều shop trong một checkout bị huỷ một đơn thì sao? [Nâng cao]
Voucher sàn được chia cho các đơn theo phần còn lại sau voucher shop. Huỷ một đơn sau thanh toán: đơn đó rời NMV và phần voucher sàn của nó không còn trong platform promo cost (chỉ tính đơn kept), còn lượt dùng voucher vẫn APPLIED nên discount cost ở voucher_performance không đổi. Đơn được hoàn đúng total của nó.

### Q84. Giá SKU đổi giữa lúc khách đang checkout thì GMV dùng giá nào? [Trung bình]
GMV dùng đơn giá ghi trên dòng đơn lúc checkout, không tra lại giá hiện tại. Nguồn đã sửa để checkout khoá đọc giá SKU, nên đổi giá phải chờ checkout xong và dòng luôn mang giá hiệu lực lúc đặt. Hai đơn cũ trước bản sửa vẫn mang giá cũ và là kiểm tra duy nhất đang fail.

### Q85. Một dòng bị xoá cứng ở nguồn thì mart thấy gì? [Trung bình]
Debezium phát sự kiện xoá, silver đánh dấu xoá và `consistent.*` không còn trả dòng đó từ watermark sau; snapshot ghi một dòng "đã mất" cho key đó. Mart tính lại toàn bộ nên dòng tự biến khỏi kết quả ở lần refresh sau. Bảng có xoá cứng thường xuyên (cart_items) bị loại khỏi reconciliation liên tục.

### Q86. Snapshot rebuild giữa chừng thì dashboard có thấy số lạ không? [Nâng cao]
Không. Rebuild xảy ra trước khi refresh mart trong cùng lượt; nếu lỗi giữa chừng, lượt đó không ghi dấu xuất bản và lần sau bắt buộc rebuild toàn bộ. Dashboard luôn đọc refresh_id đã xuất bản gần nhất, tức một lần refresh hoàn chỉnh.

## G. Reconciliation

### Q87. Reconciliation so sánh những gì? [Cơ bản]
Mỗi 30 giây, với 28 bảng, so số dòng có created_at đến một cutoff giữa PostgreSQL và snapshot ClickHouse tại watermark; với bảng có cột không bao giờ đổi sau khi chèn thì so thêm tổng cột đó (ví dụ total của orders, net_amount của order_items, amount của payments và refunds). Kết quả ghi vào `ops.reconciliation`.

### Q88. Vì sao chỉ cộng cột không đổi sau khi chèn? [Trung bình]
Hai bên đọc ở hai thời điểm khác nhau: PostgreSQL là hiện tại, ClickHouse chậm vài giây. Cột như status đổi liên tục, so tổng sẽ lệch dù pipeline đúng. Cột bất biến cộng trên tập dòng tạo trước cutoff thì hai bên phải bằng nhau tuyệt đối nếu không mất dữ liệu.

### Q89. Cutoff được chọn thế nào? [Trung bình]
Cutoff = thời điểm đặt đơn mới nhất trong warehouse trừ 1 giờ nghiệp vụ, so cùng mốc UTC ở hai bên. Dòng tạo trước đó đã commit đủ lâu để chắc chắn đã về warehouse. Dùng mốc dữ liệu thay vì giờ máy để không phụ thuộc tốc độ đồng hồ mô phỏng.

### Q90. Dung sai của reconciliation là bao nhiêu? [Trung bình]
Số phải khớp tuyệt đối. Sự khoan dung nằm ở hai chỗ: cutoff lùi 1 giờ, và một bảng chỉ bị coi là failing khi hai lần liên tiếp đều lệch. Một lần lệch có thể do một dòng bị xoá ở PostgreSQL giữa hai lần đọc; hai lần liên tiếp mới là trôi dữ liệu thật.

### Q91. Bảng nào bị loại khỏi reconciliation, vì sao? [Trung bình]
Bảng không có created_at (inventory, shop_wallets) vì không có cutoff để so, và cart_items vì bị xoá cứng liên tục nên hiện tại ở PostgreSQL và vài giây trước ở ClickHouse không bao giờ khớp khi đang có traffic. Những bảng này được check_data so lúc hệ thống nghỉ.

### Q92. Reconciliation kết nối PostgreSQL bằng quyền gì? [Cơ bản]
Bằng user chỉ đọc `app_reader`, phiên read-only và autocommit. Job phân tích không có quyền ghi vào database nghiệp vụ, nên lỗi ở job không thể làm hỏng dữ liệu nguồn, và có thể cấp quyền tối thiểu.

### Q93. Khi reconciliation lệch thì ai được báo? [Trung bình]
Job ghi log error tên các bảng lệch hai lần liên tiếp. Exporter đưa số bảng failing lên Prometheus, alert WarehouseDataDrift (critical) bắn khi còn bảng failing sau 2 phút. Có thêm ReconciliationStale nếu reconciliation không chạy thành công trong 3 phút, và panel trên dashboard Platform Health.

### Q94. Xem kết quả reconciliation ở đâu? [Cơ bản]
`ops.reconciliation_status` cho kết quả mới nhất mỗi bảng trong 1 ngày qua: ok, failing, số dòng và tổng hai bên. `ops.reconciliation` giữ lịch sử từng lần chạy trong 7 ngày.

### Q95. Reconciliation khác kiểm tra pipeline trong check_data thế nào? [Nâng cao]
Reconciliation chạy liên tục khi hệ thống đang có traffic, nên chỉ so được phần dữ liệu cũ và cột bất biến. Kiểm tra pipeline trong check_data chạy khi simulator đã dừng, so toàn bộ 31 bảng, cả cột thay đổi được (status, tồn kho…), và thử lại tới khi hội tụ. Một cái để phát hiện sớm khi vận hành, một cái để chứng minh đúng tuyệt đối trước khi phát hành.

## H. Data contract và chất lượng dữ liệu

### Q96. check_data.py kiểm tra những nhóm gì? [Cơ bản]
money, state, stock, logistics, promo, returns, settlement, write-path (dữ liệu nguồn đúng luật nghiệp vụ); pipeline (warehouse là bản sao đúng của nguồn); metrics (gold bằng cùng định nghĩa tính ở PostgreSQL); ops (mart bằng gold, snapshot bằng consistent, reconciliation đạt, không lộ PII). Mã thoát 0 nghĩa là mọi kiểm tra đạt.

### Q97. Vì sao phải dừng simulator khi chạy check_data? [Cơ bản]
Để pipeline lắng: so sánh hai hệ thống chỉ có nghĩa khi không còn dữ liệu đang trên đường. Ngay cả khi simulator dừng, job nền (hết hạn, tự hoàn tất…) vẫn ghi, nên các kiểm tra PostgreSQL đọc một snapshot REPEATABLE READ, còn các kiểm tra so hai bên thì thử lại tới khi hội tụ hoặc hết thời gian.

### Q98. Nhóm metrics kiểm tra gì cụ thể? [Trung bình]
Viết lại độc lập trong PostgreSQL rồi so với gold tại watermark: daily sales (orders, placed GMV, GMV, NMV, unpaid cancelled theo ngày giờ VN), P&L (GMV, NMV, take, promo, ba loại hoàn tiền, payout), product sales (tổng units, gross, net), pack time mỗi shop (dung sai 0,011 giờ), return cohorts. Thêm: mỗi ngày chốt đúng một lần và placed GMV, GMV của ngày chốt bằng bản sống.

### Q99. Vì sao pack time có dung sai còn các metric khác không? [Cơ bản]
Gold làm tròn pack time tới 0,01 giờ còn bản PostgreSQL không làm tròn, nên chênh tối đa khoảng 0,005 giờ; dung sai 0,011 giờ vừa đủ. Các metric tiền là số nguyên VND nên phải khớp tuyệt đối.

### Q100. Kiểm tra "marts equal gold" làm gì và có hạn chế gì? [Nâng cao]
Lấy lần refresh đầu tiên ở hoặc sau watermark hiện tại, so từng mart (bỏ cột kỹ thuật) với gold tính trực tiếp tại cùng watermark. Nó chứng minh đường snapshot + gold_snap ra cùng kết quả với định nghĩa gốc. Hạn chế: so một chiều (dòng có trong mart mà không có trong gold), nên một dòng gold bị thiếu trong mart sẽ không bị bắt; nên so cả hai chiều hoặc so thêm số dòng.

### Q101. Kiểm tra "snapshot equals consistent" làm thế nào? [Trung bình]
Với mỗi bảng snapshot, so số dòng và tổng hash của toàn bộ cột với `consistent.<table>` tại watermark mà snapshot đã đạt. Hash toàn cột bắt được cả sai giá trị, không chỉ sai số dòng. Nó bảo đảm cập nhật theo key không bỏ sót và không để thừa phiên bản cũ.

### Q102. Kiểm tra nào đang fail và vì sao? [Trung bình]
Kiểm tra promo "dòng hàng thường mang giá SKU hiệu lực lúc đặt" fail với 2 đơn lịch sử 27011000045946 và 27012200082718. Một lần đổi giá commit trong lúc checkout đang chạy nên hai đơn giữ giá cũ. Nguyên nhân đã sửa ở nguồn (checkout khoá đọc giá SKU), hai đơn biến mất khi reset dữ liệu.

### Q103. Vì sao không sửa tay hai đơn đó cho check_data xanh? [Nâng cao]
Chúng là dữ liệu thật đã xảy ra: khách đã trả theo giá đó, sổ cái và hoàn tiền đều dựa trên giá đó. Sửa tay ở nguồn sẽ tạo mâu thuẫn mới với thanh toán, hoặc phải nới kiểm tra và che mất loại lỗi này nếu tái diễn. Giữ kiểm tra đỏ có giải thích rõ ràng tốt hơn một kiểm tra xanh giả.

### Q104. Trùng lặp ở bronze có làm sai metric không? [Trung bình]
Không. Kafka giao at-least-once nên bronze có thể có bản trùng; check_data chỉ cảnh báo (WARN), không fail. Silver lấy phiên bản theo LSN, bản trùng có cùng phiên bản nên không đổi kết quả; domain events được đếm theo event_id.

### Q105. check_contract.py khác check_data.py thế nào? [Trung bình]
check_contract là hợp đồng **schema**: chạy trong CI, fail khi nguồn có bảng/cột mới hoặc đổi kiểu mà silver chưa khai báo, hoặc PII hash chưa khai báo đúng. check_data là hợp đồng **dữ liệu**: chạy trên stack thật, khẳng định giá trị đúng luật nghiệp vụ và warehouse/mart khớp nguồn. Một cái ngăn thay đổi phá pipeline, một cái chứng minh số đúng.

## I. Dashboard

### Q106. Dashboard Business Analytics đọc dữ liệu thế nào? [Cơ bản]
Có một biến "Completed refresh" lấy refresh_id mới nhất trong `ops.mart_refreshes`; mọi panel đọc mart của đúng refresh_id đó. Bốn ô số đọc overview_kpis, bốn biểu đồ cột đọc daily_sales, traffic_by_hour, order_status_summary, funnel, còn lại là bảng cho 13 mart khác và một bảng thông tin lần xuất bản.

### Q107. Vì sao mọi panel dùng chung một refresh_id? [Trung bình]
Để mọi con số trên trang là cùng một trạng thái dữ liệu: tổng GMV ở ô số khớp tổng theo ngày, số đơn ở pipeline khớp tổng đơn. Nếu mỗi panel tự lấy bản mới nhất ở thời điểm riêng, hai panel có thể lệch một lần refresh và người xem tưởng số sai.

### Q108. Bộ chọn thời gian của Grafana có tác dụng không? [Cơ bản]
Không với mart nghiệp vụ. Mart đã có grain riêng (ngày, giờ, tích luỹ), và ngày là ngày nghiệp vụ theo đồng hồ mô phỏng, không phải giờ máy. Mô tả dashboard ghi rõ ngày không bị lọc theo bộ chọn thời gian.

### Q109. Panel daily sales có giới hạn gì? [Trung bình]
Panel sắp ngày tăng dần và lấy tối đa 200 dòng, nên nếu dữ liệu vượt 200 ngày nghiệp vụ (dễ xảy ra với đồng hồ nhanh) thì chỉ hiện 200 ngày cũ nhất. Cách sửa là lấy 200 ngày mới nhất rồi sắp lại tăng dần. Panel cũng là số sống, không phải ngày đã chốt.

## J. Câu hỏi mở khi phỏng vấn

### Q110. Làm sao đảm bảo số trên dashboard đúng? [Nâng cao]
Theo từng lớp: nguồn đúng luật nghiệp vụ (check_data nhóm money, state, promo…); warehouse là bản sao đúng (kiểm tra pipeline lúc nghỉ, reconciliation liên tục lúc chạy, alert khi lệch hai lần liên tiếp); đọc không bị xé transaction (watermark); định nghĩa metric một chỗ và được cài lại độc lập trong PostgreSQL để đối chiếu; mart bằng gold và snapshot bằng consistent; mọi panel đọc cùng một lần refresh đã xuất bản. Thêm alert khi refresh hoặc reconciliation dừng, để "số cũ" không bị tưởng là "số đúng".

### Q111. Nếu sếp hỏi "sao GMV hôm qua trên dashboard khác báo cáo sáng nay?" thì trả lời thế nào? [Nâng cao]
Trước hết xác định đang so bản nào: báo cáo sáng là số đã chốt, dashboard là số sống. GMV và placed GMV của ngày đã chốt không được phép khác (có kiểm tra tự động); nếu khác là lỗi, như sự cố 15 ngày. Còn NMV, số đơn huỷ, completed revenue thì khác là bình thường do huỷ/trả hàng phát sinh sau; chênh lệch đó giải thích được bằng danh sách đơn huỷ/hoàn tiền của ngày.

### Q112. Bạn sẽ thêm metric "doanh thu thuần theo tháng" thế nào? [Trung bình]
Định nghĩa trước: theo tháng đặt đơn hay tháng hoàn tiền, giờ VN, gồm hay không gồm phí ship. Thêm view gold đọc order_facts và return_items với cùng cờ confirmed/kept, nhóm theo tháng giờ VN; thêm tên vào danh sách mart để job vật chất hoá; viết lại định nghĩa trong check_data để đối chiếu; thêm panel. Nếu cần số "đã báo cáo" thì cân nhắc chốt tháng giống chốt ngày.

### Q113. Nếu dữ liệu lớn gấp 100 lần thì cái gì vỡ trước? [Nâng cao]
Mart tích luỹ tính lại toàn bộ mỗi 15 giây sẽ đắt dần theo lịch sử dù snapshot tăng dần. Hướng xử lý: chia mart theo ngày và chỉ tính lại các ngày có key thay đổi (dirty dates), ngày cũ giữ nguyên; hoặc tăng chu kỳ refresh cho mart ít quan trọng. Reconciliation đếm toàn bảng ở PostgreSQL cũng nặng dần, có thể chuyển sang so theo khoảng thời gian gần.

### Q114. Vì sao không dùng dbt cho lớp gold? [Trung bình]
Gold ở đây là view có tham số watermark, đọc consistent.* và phải tính lại mỗi 15 giây tại một điểm commit; bài toán là nhất quán và độ tươi, không phải DAG batch. dbt hợp với batch theo lịch và kiểm thử khai báo; có thể dùng nó để quản lý định nghĩa view, nhưng vẫn cần một service pin watermark, xuất bản nguyên tử và chốt ngày.

### Q115. Metric nào dễ bị hiểu sai nhất trong dự án và bạn sẽ truyền đạt thế nào? [Trung bình]
GMV thường bị hiểu là "doanh thu" — thực ra nó gồm đơn đã huỷ sau thanh toán và hàng bị trả, và chưa trừ voucher. Completed revenue không trừ hoàn tiền trả một phần, discount cost của voucher khác promo cost của P&L. Cách truyền đạt: từ điển metric một chỗ, mô tả trên panel, và một ví dụ số đi qua mọi mart như mục 5 của tài liệu.

### Q116. Nếu được làm lại, bạn sẽ thay đổi gì ở lớp mart? [Nâng cao]
So mart và gold hai chiều trong check_data; thêm chiều thời gian cho P&L và payment_summary; nhóm product_sales theo id thay vì tên; tính cancel rate theo shop trên đơn confirmed; sửa panel daily sales lấy ngày mới nhất; đưa ngày đã chốt lên dashboard cạnh số sống. Về vận hành, thêm leader election nếu cần nhiều replica warehouse-jobs.
