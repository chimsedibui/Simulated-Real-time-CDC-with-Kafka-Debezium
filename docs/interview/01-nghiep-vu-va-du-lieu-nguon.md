# Q&A 01 · Nghiệp vụ và dữ liệu nguồn

[Tài liệu dự án](../README.md) / [Phỏng vấn](README.md)

Bộ câu hỏi ôn tập cho [01-nghiep-vu-va-du-lieu-nguon.md](../business/01-nghiep-vu-va-du-lieu-nguon.md). Mỗi câu có mức độ [Cơ bản], [Trung bình] hoặc [Nâng cao]. Câu trả lời được viết để có thể nói lại thành lời trong buổi phỏng vấn.

## Mục lục

- [A. Tổng quan và tác nhân](#a-tổng-quan-và-tác-nhân)
- [B. Đồng hồ nghiệp vụ](#b-đồng-hồ-nghiệp-vụ)
- [C. Danh mục, giá, tồn kho](#c-danh-mục-giá-tồn-kho)
- [D. Giỏ hàng](#d-giỏ-hàng)
- [E. Checkout, voucher, flash sale](#e-checkout-voucher-flash-sale)
- [F. Thanh toán và webhook](#f-thanh-toán-và-webhook)
- [G. Vận chuyển](#g-vận-chuyển)
- [H. Hủy đơn, hoàn tất, hạng thành viên](#h-hủy-đơn-hoàn-tất-hạng-thành-viên)
- [I. Trả hàng và hoàn tiền](#i-trả-hàng-và-hoàn-tiền)
- [J. Quyết toán người bán](#j-quyết-toán-người-bán)
- [K. Đồng thời, idempotency, outbox](#k-đồng-thời-idempotency-outbox)
- [L. Mô hình dữ liệu và CDC](#l-mô-hình-dữ-liệu-và-cdc)
- [M. Simulator](#m-simulator)
- [N. Câu hỏi phỏng vấn mở](#n-câu-hỏi-phỏng-vấn-mở)

---

## A. Tổng quan và tác nhân

### Q1. Hệ thống nguồn của dự án mô phỏng nghiệp vụ gì? [Cơ bản]

Một sàn thương mại điện tử nhiều shop kiểu Shopee / TikTok Shop ở Việt Nam, đa ngành hàng (điện thoại, laptop, thời trang, mỹ phẩm, gia dụng, mẹ & bé, bách hóa). Người mua mua hàng từ nhiều shop trong một lần thanh toán, mỗi shop tự đóng gói và gửi qua hãng vận chuyển, sàn thu tiền rồi quyết toán lại cho shop sau khi trừ hoa hồng và phí. Dữ liệu được lưu ở PostgreSQL 15, schema `ecommerce`, và là nguồn cho pipeline CDC.

### Q2. Vì sao không chỉ sinh dữ liệu giả bằng các lệnh INSERT ngẫu nhiên? [Cơ bản]

Vì CDC chỉ đáng giá khi dữ liệu có hành vi thật: một dòng bị cập nhật nhiều lần theo vòng đời, có xóa, có transaction đụng nhiều bảng, có tranh chấp đồng thời, có sự kiện đến trễ hoặc trùng. INSERT ngẫu nhiên không tạo ra những tình huống đó, nên pipeline sẽ trông chạy được nhưng chưa từng bị thử thách. Hơn nữa dữ liệu ngẫu nhiên không có bất biến nghiệp vụ, nên không thể đối soát kho dữ liệu với nguồn một cách có ý nghĩa.

### Q3. Các tác nhân trên sàn là ai và mỗi bên làm gì? [Cơ bản]

Người mua thêm giỏ, checkout, thanh toán, hủy, xác nhận nhận hàng, trả hàng, đánh giá. Người bán (shop) đăng sản phẩm, đổi giá, nhập hàng, tạo voucher shop, đóng gói, hủy khi hết hàng, xử lý hàng trả. Cổng thanh toán (MOMO, VNPAY, NAPAS) và hãng vận chuyển (GHN, GHTK, SPX, J&T, VTP) gọi webhook. Vận hành sàn tạo danh mục, voucher sàn, flash sale, phân xử tranh chấp. Cuối cùng là SYSTEM — các job nền thực thi luật theo thời gian như hết hạn thanh toán hay tự hoàn tất đơn.

### Q4. "Source API là nơi duy nhất ghi vào database" nghĩa là gì và tại sao quan trọng? [Cơ bản]

Không service nào khác, kể cả simulator, được kết nối và ghi trực tiếp vào PostgreSQL; mọi thay đổi phải là một lệnh nghiệp vụ qua HTTP. Nhờ vậy mọi luật nghiệp vụ, thứ tự khóa và việc phát domain event chỉ nằm ở một chỗ, không thể bị bỏ qua. Ngay cả dữ liệu khởi tạo cũng đi qua API, nên mỗi dòng trong DB đều có event tương ứng trong outbox — điều kiện để đối soát "một event tạo cho mỗi dòng" ở phía kho.

### Q5. Một lệnh nghiệp vụ tương ứng với bao nhiêu transaction? [Cơ bản]

Đúng một. Ví dụ checkout ghi vào khoảng mười bảng (checkout, đơn, dòng đơn, lịch sử, tồn kho, sổ kho, lượt voucher, thanh toán, giỏ, outbox) và tất cả commit hoặc rollback cùng nhau. Điều này giữ cho DB luôn nhất quán, và giúp phía CDC dựng lại "trạng thái tại một mốc commit" mà không bao giờ thấy nửa transaction.

### Q6. Vì sao checkout tách thành một đơn cho mỗi shop? [Cơ bản]

Vì mỗi shop đóng gói, gửi hàng, chịu SLA, nhận tiền và bị phạt độc lập. Một đơn gộp nhiều shop sẽ không có một trạng thái đúng duy nhất (shop A đã giao, shop B chưa đóng gói). Checkout vẫn là đơn vị thanh toán chung, còn đơn là đơn vị hoàn thành. Ví dụ giỏ có hàng của 2 shop trả bằng ví sẽ tạo 1 checkout, 2 đơn và 1 payment.

### Q7. Tiền được lưu thế nào? [Cơ bản]

VND kiểu BIGINT (số nguyên), đã gồm VAT. Không dùng số thực để tránh sai số làm tròn. Mọi phép chia (phần trăm voucher, phân bổ giảm giá, hoàn tiền một phần) đều làm tròn xuống số nguyên và có cơ chế phân bổ để tổng các phần khớp tuyệt đối.

### Q8. Máy trạng thái kiểm tra "actor" để làm gì? [Trung bình]

Ngoài việc bước chuyển có hợp lệ không, hệ thống còn kiểm tra ai được phép làm bước đó. Chỉ PAYMENT mới đưa đơn từ UNPAID sang TO_SHIP, chỉ LOGISTICS mới báo đã giao, chỉ SELLER mới đóng gói. Nếu thiếu kiểm tra này, người bán có thể tự đánh dấu đã giao để nhận tiền sớm. Actor còn được ghi vào lịch sử đơn, là dữ liệu quý cho phân tích (ví dụ tỉ lệ hủy do SYSTEM vì SLA).

### Q9. Bối cảnh Việt Nam thể hiện ở đâu trong dữ liệu? [Cơ bản]

17 tỉnh/thành chia 3 vùng Bắc–Trung–Nam để tính phí ship và thời gian giao; hãng vận chuyển và cổng thanh toán nội địa; COD chiếm khoảng 45%; tên người, đầu số điện thoại, đường phố, quận huyện thật; thương hiệu và mẫu sản phẩm thật với giá tham khảo; giờ trong ngày tính theo UTC+7; kịch bản Siêu Sale 11.11.

### Q10. Phân khúc khách và hạng thành viên khác nhau thế nào? [Trung bình]

Phân khúc (NEW, REGULAR, BARGAIN_HUNTER, LOYAL, BUSINESS) là thuộc tính hành vi gán lúc đăng ký và quyết định cách simulator cư xử (tần suất ghé, tỉ lệ checkout, dùng voucher). Hạng (BRONZE, SILVER, GOLD, PLATINUM) là kết quả tính từ tổng chi tiêu trên các đơn đã hoàn tất, với ngưỡng 5, 20 và 50 triệu đồng. Phân khúc không đổi, còn hạng được tính lại mỗi khi một đơn hoàn tất.

## B. Đồng hồ nghiệp vụ

### Q11. Đồng hồ nghiệp vụ là gì? [Cơ bản]

Một bảng một dòng giữ mốc thời gian thực, mốc thời gian nghiệp vụ tương ứng và hệ số tốc độ. Hàm `business_now()` trả về mốc nghiệp vụ cộng với thời gian thực đã trôi qua nhân tốc độ. Mọi timestamp nghiệp vụ, hạn chót và job theo thời gian đều dùng hàm này. Ở ×240, một ngày nghiệp vụ trôi qua trong 6 phút thực.

### Q12. Tại sao không rút ngắn luật (ví dụ SLA 30 giây) thay vì tăng tốc đồng hồ? [Trung bình]

Rút ngắn luật làm dữ liệu mất tính thật: metric như "thời gian đóng gói trung bình" hay "đơn theo giờ trong ngày" sẽ vô nghĩa, và tỉ lệ giữa các luật bị méo. Tăng tốc đồng hồ giữ nguyên độ dài thật của mọi luật (15 phút thanh toán, 2 ngày đóng gói, 3 ngày tự hoàn tất) nên báo cáo trông như của sàn thật, chỉ là thời gian chạy nhanh hơn.

### Q13. Đổi tốc độ đồng hồ giữa chừng có làm thời gian nhảy không? [Trung bình]

Không. Khi đổi tốc độ, hệ thống neo lại: mốc nghiệp vụ mới bằng thời điểm nghiệp vụ hiện tại, mốc thực mới bằng bây giờ, rồi mới áp tốc độ mới. Thời gian nghiệp vụ vì thế luôn liên tục và đơn điệu tăng; chỉ "độ dốc" thay đổi.

### Q14. Độ trễ pipeline được đo bằng thời gian nào? [Trung bình]

Bằng thời gian thực. Debezium ghi lại thời điểm commit thực của mỗi thay đổi, còn kho dữ liệu đo lúc nhận được, nên độ trễ tính bằng giây thật. Thời gian nghiệp vụ chỉ dùng cho metric nghiệp vụ. Tách hai trục này giúp tốc độ ×1440 không làm "độ trễ" bị phóng đại 1440 lần.

### Q15. Vì sao `business_now()` đọc đồng hồ tại thời điểm gọi chứ không phải đầu transaction? [Nâng cao]

Hàm thời gian chuẩn của PostgreSQL trả về giờ bắt đầu transaction. Nếu dùng nó, một transaction bắt đầu sớm nhưng làm việc lâu sẽ đóng dấu event sớm hơn một transaction khác thật ra xảy ra trước nó, làm thứ tự event của cùng một đối tượng bị đảo. Đọc đồng hồ thực tại lúc gọi khiến event được xếp theo đúng lúc nó xảy ra.

### Q16. Những gì cố ý không dùng thời gian nghiệp vụ? [Trung bình]

Bảng idempotency key (hết hạn sau 72 giờ thực, vì client retry theo giờ thực), kiểm tra độ lệch giờ của chữ ký webhook (nếu dùng giờ nghiệp vụ thì cửa sổ 5 phút sẽ bị nới ra theo tốc độ), nhịp chạy job (2 giây thực) và thời gian commit mà Debezium ghi lại.

### Q17. Simulator dựa vào đồng hồ thế nào? [Trung bình]

Mỗi tick nó đọc đồng hồ và tính số giờ nghiệp vụ đã trôi qua kể từ tick trước. Mọi hành vi trong cấu hình là tần suất mỗi giờ nghiệp vụ, được chuyển thành xác suất cho khoảng thời gian đó. Vì vậy đổi tốc độ chỉ làm mọi thứ nhanh hơn chứ không đổi tỉ lệ, và khoảng thời gian bị chặn tối đa 6 giờ để một lần tạm dừng dài không gây bùng nổ hành động.

## C. Danh mục, giá, tồn kho

### Q18. Sản phẩm và SKU khác nhau thế nào? [Cơ bản]

Sản phẩm là một tin đăng của shop (ví dụ "Samsung Galaxy A55 5G"), thuộc một danh mục lá và có thể có thương hiệu. SKU là biến thể cụ thể có thể bán được ("Đen / 256GB") với mã duy nhất toàn sàn, giá niêm yết, giá bán, cân nặng và một dòng tồn kho riêng. Giỏ, đơn, tồn kho, flash sale đều làm việc ở cấp SKU.

### Q19. `list_price` và `price` là gì, có ràng buộc gì? [Cơ bản]

`list_price` là giá niêm yết (giá gạch ngang), `price` là giá bán thực tế. Giá bán phải dương và không vượt giá niêm yết; ràng buộc này được kiểm tra cả ở API và CHECK của DB. Khi đổi giá cao hơn giá niêm yết, giá niêm yết được nâng theo để luật vẫn đúng.

### Q20. Ba con số `on_hand`, `reserved`, `available` nghĩa là gì? [Cơ bản]

`on_hand` là hàng thật trên kệ, `reserved` là phần đã giữ cho đơn đã đặt nhưng chưa xuất kho, `available` là phần còn bán được và luôn bằng `on_hand − reserved`. DB có CHECK bắt buộc đẳng thức này và cả ba không âm. Checkout kiểm tra `available`, không phải `on_hand`.

### Q21. Khi nào tồn kho thực sự bị trừ? [Trung bình]

Không phải lúc checkout. Checkout chỉ tăng `reserved` (giữ hàng). Hàng chỉ rời kho, tức `on_hand` và `reserved` cùng giảm, khi hãng vận chuyển báo đã lấy hàng. Nếu đơn bị hủy trước đó, phần giữ được giải phóng mà `on_hand` không đổi. Mô hình này phản ánh đúng thực tế: hàng của đơn chưa gửi vẫn nằm trong kho.

### Q22. Sổ kho `inventory_movements` dùng để làm gì? [Trung bình]

Ghi mọi biến động tồn kho: loại (LISTED, RESTOCK, RESERVE, RELEASE, SHIP, RETURN_TO_SENDER, CUSTOMER_RETURN), delta của on_hand và reserved, số dư sau biến động, và nguồn gốc (đăng bán, phiếu nhập, đơn, yêu cầu trả). Mọi thay đổi tồn kho đi qua một hàm duy nhất luôn ghi kèm một dòng sổ, nên tồn kho hiện tại luôn bằng tổng delta. Phía kho dữ liệu dùng sổ này để phân tích luồng hàng và để kiểm tra bất biến.

### Q23. Làm sao đảm bảo tồn kho không bao giờ âm? [Trung bình]

Hai lớp. Lệnh cập nhật tồn kho có điều kiện "available sau thay đổi ≥ 0" ngay trong câu UPDATE; nếu không thỏa thì không dòng nào được cập nhật và lệnh báo hết hàng. Bên dưới còn CHECK của DB không cho on_hand, reserved, available âm. Vì điều kiện nằm trong chính câu UPDATE (được thực hiện dưới khóa dòng), nó đúng cả khi hai checkout cùng lúc nhắm vào một SKU.

### Q24. Đổi giá có ảnh hưởng đơn đã đặt không? [Cơ bản]

Không. Dòng đơn chụp lại giá niêm yết và đơn giá tại thời điểm checkout. Đổi giá chỉ cập nhật SKU, thêm một dòng `price_history` và phát `SkuPriceChanged`. Doanh thu quá khứ vì thế không bao giờ bị viết lại.

### Q25. `price_history` ghi gì? [Cơ bản]

Mỗi lần giá bán của SKU thay đổi: giá cũ, giá mới, lý do và thời điểm. Dòng đầu tiên được tạo lúc đăng bán với giá cũ rỗng và lý do "listing". Đây là bảng chỉ-thêm; nhờ nó, có thể kiểm tra rằng mỗi đơn mang đúng giá có hiệu lực tại thời điểm đặt.

### Q26. Nhập hàng (restock) hoạt động thế nào? Có nhà cung cấp không? [Cơ bản]

Người bán nhập một số lượng cho SKU; hệ thống cộng `on_hand` qua sổ kho với loại RESTOCK và một mã phiếu nhập mới, cập nhật thời điểm nhập gần nhất và phát `InventoryRestocked`. Không có nhà cung cấp hay đơn mua hàng: trên marketplace, nguồn hàng là việc riêng của shop. Simulator nhập 30–300 đơn vị cho các SKU có available không vượt ngưỡng nhập lại.

### Q27. Danh mục được tổ chức thế nào? [Cơ bản]

Cây hai cấp: 8 ngành gốc và 20 ngành con, định danh bằng slug duy nhất. Sản phẩm luôn gắn vào ngành con. Tạo danh mục hay thương hiệu là "upsert": gửi lại slug/tên đã có thì nhận về bản ghi cũ, không lỗi và không phát event mới.

### Q28. Có thể ngừng bán một SKU hay shop không? [Trung bình]

Schema có cờ hoạt động cho SKU và shop, trạng thái ACTIVE/INACTIVE cho sản phẩm, và cả thêm giỏ lẫn checkout đều từ chối món không còn bán (`sku_unavailable`). Tuy nhiên hiện chưa có endpoint nào tắt các cờ đó, nên trong dữ liệu thực tế mọi thứ đều đang bán. Đây là chỗ đã chừa sẵn cho mở rộng.

## D. Giỏ hàng

### Q29. Luật "một giỏ ACTIVE mỗi khách" được đảm bảo thế nào? [Cơ bản]

Bằng unique index một phần trên `customer_id` chỉ áp cho dòng có trạng thái ACTIVE. Khách có thể có nhiều giỏ CHECKED_OUT hay ABANDONED trong lịch sử, nhưng DB không cho tồn tại hai giỏ ACTIVE cùng lúc. Giỏ được tạo bằng lệnh "chèn nếu chưa có", dựa đúng vào index này.

### Q30. Thêm cùng một SKU hai lần thì sao? [Cơ bản]

Dòng giỏ là duy nhất theo cặp (giỏ, SKU), nên lần thêm thứ hai cộng dồn số lượng vào dòng có sẵn, tối đa 100. Event `CartItemAdded` mang cả số lượng vừa thêm và số lượng của dòng sau khi cộng.

### Q31. Xóa món khỏi giỏ làm gì với dữ liệu? Tại sao quan trọng với CDC? [Trung bình]

Đó là DELETE cứng dòng `cart_items`, kèm event `CartItemRemoved`. Đây là xóa cứng nghiệp vụ duy nhất được stream, nên nó là "bài kiểm tra" cho đường xử lý delete của pipeline: sự kiện delete với dòng cũ đầy đủ, tombstone, và tầng silver phải xóa đúng dòng. Nếu mọi thứ đều chỉ là soft delete, phần đó của pipeline sẽ không bao giờ được thử.

### Q32. Giỏ có lưu giá không? [Trung bình]

Không. Giỏ chỉ lưu SKU và số lượng; giá được đọc tại thời điểm checkout. Đây là lựa chọn giống sàn thật: khách thấy giá mới khi vào thanh toán. Tài liệu thiết kế cũ có chụp giá vào giỏ, nhưng code hiện tại không làm vậy.

### Q33. Khi nào giỏ thành ABANDONED? [Cơ bản]

Khi giỏ ACTIVE có ít nhất một món và không được động tới trong 1 ngày nghiệp vụ, job SYSTEM chuyển nó sang ABANDONED và phát `CartAbandoned`. Giỏ rỗng không bị đánh dấu. Lần thêm món sau đó của khách sẽ tạo giỏ ACTIVE mới.

### Q34. Sau checkout, các dòng giỏ có bị xóa không? [Trung bình]

Không. Giỏ chuyển CHECKED_OUT và các dòng giữ nguyên làm lịch sử. Mỗi giỏ chỉ checkout được một lần vì `checkouts.cart_id` là duy nhất. Điều này cho phép phân tích tỉ lệ chuyển đổi giỏ → đơn.

## E. Checkout, voucher, flash sale

### Q35. Hãy mô tả các bước của checkout. [Trung bình]

Kiểm tra khách còn hoạt động và lấy địa chỉ (mặc định nếu không chỉ định). Nếu có voucher: khóa theo khách, nạp voucher và kiểm tra còn lượt mà không khóa. Khóa giỏ ACTIVE, đọc SKU và tồn kho (khóa chia sẻ trên SKU), từ chối nếu món không còn bán hoặc thiếu hàng. Chốt thời điểm t, tìm giá flash sale, nhóm theo shop, tính phí ship, áp voucher. Ghi checkout, đơn, dòng đơn, lịch sử, event, payment COD. Cuối cùng mới lấy quota voucher, giữ hàng và tăng quota flash sale bằng UPDATE có điều kiện, rồi ghi lượt voucher, payment online và đóng giỏ.

### Q36. Phí ship được tính thế nào? [Cơ bản]

Theo tuyến và tổng cân nặng các món của cùng một shop. Cùng tỉnh: 15.000 ₫ cho 500 g đầu, mỗi 500 g thêm 2.500 ₫. Cùng vùng khác tỉnh: 22.000 ₫ và 4.000 ₫. Khác vùng: 32.000 ₫ và 5.000 ₫. Ví dụ gói 1,2 kg từ TP.HCM ra Hà Nội là 42.000 ₫. Mỗi đơn (mỗi shop) có phí ship riêng.

### Q37. Có những loại voucher nào? [Cơ bản]

Ba loại: FIXED (giảm số tiền cố định), PERCENT (giảm theo phần trăm, lưu bằng basis point, thường có trần) và FREESHIP (trợ giá phí ship). Hai phạm vi: SHOP do shop tạo và tự tài trợ, PLATFORM do sàn tạo và tài trợ. FREESHIP chỉ có ở phạm vi sàn. Mỗi voucher có khung hiệu lực, chi tiêu tối thiểu, tổng lượt và lượt mỗi khách.

### Q38. Luật chồng voucher là gì? [Trung bình]

Mỗi checkout được tối đa một voucher shop cho mỗi shop trong giỏ, một voucher giảm giá của sàn và một voucher freeship. Gửi hai voucher cùng loại bị từ chối với lý do "stacking". Voucher shop của một shop không có trong giỏ bị từ chối "wrong_shop".

### Q39. Voucher được áp theo thứ tự nào và vì sao? [Nâng cao]

Đầu tiên voucher shop, tính trên tiền hàng của chính shop đó. Sau đó voucher sàn, tính trên tổng tiền hàng còn lại sau voucher shop và chia cho các đơn theo tỉ lệ phần còn giảm được. Cuối cùng freeship, tính trên tổng phí ship và chia theo phí ship từng đơn. Thứ tự này đảm bảo voucher sàn không giảm trên phần shop đã giảm (tránh giảm hai lần trên cùng một đồng) và khớp cách các sàn thật tính.

### Q40. Tại sao voucher không bao giờ kéo đơn xuống dưới 1.000 ₫? [Trung bình]

Để mỗi đơn luôn có giá trị dương: DB yêu cầu tổng đơn > 0, và một đơn 0 đồng sẽ làm hỏng các phép chia tỉ lệ (hoa hồng, hoàn tiền) cũng như metric. Khoản 1.000 ₫ tối thiểu được trừ ra khỏi "phần có thể giảm" của mỗi đơn khi áp voucher shop và voucher sàn.

### Q41. Phân bổ giảm giá xuống từng dòng đơn để làm gì? [Nâng cao]

Để khi khách trả một phần, hệ thống hoàn đúng số tiền khách thật sự đã trả cho từng đơn vị, và để quyết toán cho shop chính xác. Mỗi phần giảm cấp đơn được chia cho các dòng theo tỉ lệ thành tiền bằng phương pháp phần dư lớn nhất: lấy phần nguyên của mỗi phần, rồi chia các đồng còn dư cho những phần có phần thập phân lớn nhất. Tổng các phần vì thế khớp tuyệt đối với số tổng, không lệch một đồng.

### Q42. Ai chịu chi phí của từng loại giảm giá? [Trung bình]

Voucher shop do người bán chịu, ghi ở `shop_discount` và bị trừ vào doanh thu của shop khi quyết toán. Voucher sàn (`platform_discount`) và trợ giá ship (`shipping_subsidy`) do sàn chịu, không ảnh hưởng tiền của shop. Phân biệt này quan trọng cho P&L của sàn và cho định nghĩa GMV/NMV ở tài liệu mart.

### Q43. Hãy tính một ví dụ checkout hai shop có voucher. [Nâng cao]

Khách ở Hà Nội mua của shop A ở TP.HCM 1.328.000 ₫ hàng (ship khác vùng 32.000 ₫) và của shop B ở Hà Nội 229.000 ₫ (ship cùng tỉnh 15.000 ₫). Voucher shop A giảm 50.000 ₫; voucher sàn 10% tối đa 100.000 ₫ chạm trần và được chia 84.850 ₫ cho A, 15.150 ₫ cho B; freeship 30.000 ₫ chia 20.426 ₫ cho A, 9.574 ₫ cho B theo phí ship. Đơn A còn 1.204.724 ₫, đơn B 219.276 ₫, tổng 1.424.000 ₫. Kết quả: 1 checkout, 2 đơn, 3 lượt voucher và 1 payment (online) hoặc 2 payment COD.

### Q44. Một voucher tính ra 0 ₫ có tốn lượt không? [Trung bình]

Không. Lượt chỉ được lấy cho voucher thật sự được áp với số tiền dương; ví dụ freeship cho checkout có phí ship 0 sẽ không tạo lượt dùng. Điều này vừa công bằng với khách, vừa giảm số lần phải đụng tới dòng quota.

### Q45. Giới hạn lượt dùng mỗi khách được đảm bảo thế nào khi khách bấm checkout hai lần cùng lúc? [Nâng cao]

Khi checkout có voucher, transaction lấy một khóa tư vấn (advisory lock) gắn với khách đó, nên hai checkout có voucher của cùng khách chạy tuần tự, còn khách khác không bị chặn. Bên trong, hệ thống đếm số lượt APPLIED của khách với voucher đó. Nếu không có khóa này, hai checkout song song đều đếm thấy 0 lượt và cùng dùng một voucher giới hạn 1 lần/khách.

### Q46. Tổng lượt của voucher được quản lý thế nào? [Nâng cao]

Tổng lượt được chia vào tối đa 16 dòng "slot", sức chứa các slot chênh nhau tối đa 1 và cộng lại đúng bằng tổng lượt. Mỗi checkout lấy một lượt từ một slot ngẫu nhiên còn chỗ bằng UPDATE có điều kiện "đã dùng < sức chứa"; nếu slot vừa đầy thì chọn lại. Bảng slot không được stream qua CDC vì `voucher_usages` đã mang cùng thông tin.

### Q47. Tại sao chia quota voucher thành nhiều slot? [Nâng cao]

Vì một voucher sàn trong phút đầu 11.11 bị hàng nghìn checkout cùng nhắm tới. Nếu quota là một dòng, mọi checkout phải xếp hàng chờ khóa dòng đó, tức là tuần tự hóa toàn bộ checkout dùng voucher. Chia 16 slot giảm tranh chấp khoảng 16 lần. Đánh đổi là việc đọc "còn bao nhiêu lượt" phải cộng các slot, và cần cẩn thận để không giữ khóa slot này trong khi chờ slot khác.

### Q48. Load test phát hiện gì về voucher, và đã sửa thế nào? [Nâng cao]

Load test cho thấy dòng quota voucher là hot row số một, vì trước đây nó bị khóa ngay đầu checkout và giữ tới khi commit. Sau khi sửa, checkout chỉ kiểm tra quota sớm mà không khóa (để từ chối nhanh nếu hết), và chỉ lấy lượt ở cuối transaction, chỉ cho những voucher thật sự được áp. Thời gian giữ khóa từ cả checkout rút xuống vài mili giây; checkout p99 ở 6 simulator giảm từ 1,96 s xuống 0,82 s.

### Q49. Giá flash sale được áp dụng khi nào? [Trung bình]

Khi SKU nằm trong một chiến dịch đang chạy tại thời điểm checkout và quota còn đủ cho số lượng khách muốn mua. Đơn giá khi đó là giá sale (nếu nhiều chiến dịch, lấy giá thấp nhất) và dòng đơn lưu tham chiếu tới mục flash sale. Giá sale lúc tạo chiến dịch phải thấp hơn giá hiện tại của SKU.

### Q50. Điều gì xảy ra nếu quota flash sale hết ngay giữa lúc checkout? [Nâng cao]

Checkout đọc quota không khóa ở đầu, nên có thể thấy "còn" nhưng tới cuối, khi tăng số đã bán bằng UPDATE có điều kiện "đã bán + số lượng ≤ quota", điều kiện không còn thỏa. Khi đó API trả `flash_sold_out` và cả transaction rollback (không giữ hàng, không lấy voucher). Khách thử lại sẽ không còn thấy giá sale và trả giá thường. Đây là đánh đổi có chủ ý: thỉnh thoảng phải thử lại, đổi lấy việc không khóa SKU nóng suốt cả checkout.

### Q51. Hủy đơn có trả lại quota flash sale không? [Trung bình]

Có. Khi đơn bị hủy (trước khi xuất kho), mỗi dòng có tham chiếu flash sale trả lại số lượng vào quota, để người khác mua được. Lượt voucher thì khác: chỉ được trả khi cả checkout chưa thanh toán bị đóng.

### Q52. Vì sao checkout lấy tài nguyên khan hiếm ở cuối chứ không phải đầu? [Nâng cao]

Khóa dòng được giữ tới khi transaction kết thúc. Nếu khóa tồn kho hay quota ngay đầu, khóa bị giữ suốt các bước tính giá, áp voucher, ghi đơn — với SKU bán chạy, mọi checkout khác phải chờ. Lấy ở cuối bằng UPDATE có điều kiện thì khóa chỉ giữ vài mili giây trước commit, mà vẫn không bán quá vì điều kiện được kiểm tra dưới khóa. Đổi lại, kiểm tra sớm có thể "lạc quan" và thỉnh thoảng thất bại muộn.

## F. Thanh toán và webhook

### Q53. COD và thanh toán online khác nhau thế nào về dữ liệu? [Cơ bản]

Online: một payment cho cả checkout (đúng tổng tiền), đơn ở UNPAID chờ webhook, hạn thanh toán 15 phút. COD: mỗi đơn có một payment COD ở PENDING ngay từ checkout, đơn vào thẳng TO_SHIP, và payment chỉ thành SUCCEEDED khi hãng báo đã giao. Lý do COD tách theo đơn: mỗi shipper thu tiền đơn của mình.

### Q54. Điều gì xảy ra khi webhook báo thanh toán thành công? [Trung bình]

Trong một transaction, hệ thống khóa checkout rồi payment, đánh dấu payment SUCCEEDED, hủy các lần thử khác còn PENDING, chuyển checkout sang PAID và đưa từng đơn UNPAID sang TO_SHIP với actor PAYMENT. Mỗi đơn nhận hạn đóng gói 2 ngày tính từ lúc trả tiền. Event `PaymentSucceeded` và các `OrderStatusChanged` được phát cùng lúc.

### Q55. Thanh toán thất bại thì sao? Có thử lại được không? [Cơ bản]

Payment chuyển FAILED kèm lý do (hết tiền, thẻ bị từ chối, hết hạn OTP…), checkout vẫn chờ. Người mua có thể thử lại trước hạn: hệ thống tạo payment mới và hủy lần thử còn PENDING. Sau hạn `pay_by` thì không thử lại được nữa. Simulator cho khoảng một nửa số người bị lỗi thử lại.

### Q56. Nếu không ai trả tiền trong 15 phút thì sao? [Cơ bản]

Job SYSTEM tìm checkout PENDING_PAYMENT quá hạn, khóa và kiểm tra lại, rồi đóng nó: checkout EXPIRED, payment PENDING EXPIRED, trả lượt voucher, hủy mọi đơn với lý do "payment_timeout" — việc hủy giải phóng hàng giữ và quota flash sale. Event `CheckoutClosed` được phát.

### Q57. Cổng báo thành công sau khi checkout đã hết hạn thì xử lý thế nào? [Nâng cao]

Tiền đã bị cổng trừ thật, nên hệ thống không được bỏ qua callback. Nó ghi payment SUCCEEDED rồi lập tức tạo refund toàn bộ với lý do `late_payment`, không gắn với đơn nào vì các đơn đã bị hủy. Cách này giữ cho sổ sách khớp với cổng thanh toán: mọi khoản tiền vào đều có khoản hoàn tương ứng. Simulator cố ý tạo tình huống này cho khoảng 2% thanh toán.

### Q58. Thành công cho một lần thử đã bị thay thế (superseded) thì sao? [Nâng cao]

Giống thanh toán trễ: payment đó đang CANCELLED nhưng tiền đã bị trừ, nên nó được ghi SUCCEEDED và hoàn ngay. Checkout chỉ giữ đúng một payment thành công được tính là tiền của đơn; khi cần hoàn tiền cho đơn, hệ thống tìm đúng payment đó, bỏ qua những payment đã hoàn vì đến trễ.

### Q59. Webhook bị gửi trùng thì sao? [Trung bình]

Có hai lớp. Nếu cổng gửi lại với cùng event id (dùng làm Idempotency-Key), API trả lại nguyên phản hồi đã lưu mà không chạy lại. Nếu gửi lại với event id mới nhưng kết quả giống trạng thái hiện tại của payment, đó là no-op "duplicate_ignored". Không trường hợp nào áp dụng thanh toán hai lần.

### Q60. Nhận "FAILED" cho payment đã SUCCEEDED (hoặc ngược lại) thì sao? [Trung bình]

Bị từ chối với `payment_already_final`: kết quả cuối không bị lật. Riêng FAILED đến cho một payment đã EXPIRED hay CANCELLED thì được bỏ qua êm ("stale_failure_ignored") vì nó không thay đổi gì về nghiệp vụ.

### Q61. Làm sao chắc chắn chỉ đúng cổng mới báo được kết quả cho payment của mình? [Trung bình]

Webhook được ký HMAC theo partner, và sau khi tìm thấy payment, API kiểm tra partner đã ký phải là nhà cung cấp của payment đó (MOMO cho ví, VNPAY cho thẻ, NAPAS cho chuyển khoản). Như vậy MOMO không thể báo thành công cho giao dịch thẻ. Chi tiết chữ ký ở tài liệu 02.

### Q62. Vì sao webhook khóa theo thứ tự checkout → payment → đơn? [Nâng cao]

Đó là một phần của thứ tự khóa toàn cục. Job hết hạn và lệnh hủy của người mua cũng khóa checkout trước. Nếu webhook khóa payment trước rồi mới tới checkout, trong khi job khóa checkout rồi mới cập nhật payment, hai bên có thể chờ nhau theo vòng và deadlock. Cùng thứ tự thì một bên chỉ đơn giản chờ bên kia xong.

## G. Vận chuyển

### Q63. Vận đơn được tạo khi nào? [Cơ bản]

Khi người bán đóng gói đơn TO_SHIP: trong cùng transaction, đơn sang READY_TO_SHIP và sàn tạo vận đơn chiều FORWARD với hãng được chọn theo tỉ trọng (GHN 30, GHTK 25, SPX 25, J&T 15, VTP 5), mã vận đơn có tiền tố của hãng, tỉnh đi là tỉnh shop và tỉnh đến là tỉnh nhận. Khi một yêu cầu trả được duyệt, sàn tạo vận đơn chiều RETURN theo hướng ngược lại.

### Q64. Các sự kiện tracking ảnh hưởng tới đơn thế nào? [Trung bình]

Lấy hàng đưa đơn sang SHIPPED và trừ kho. Đang vận chuyển không đổi trạng thái đơn. Đã giao đưa đơn sang DELIVERED và thu COD. Giao thất bại đưa đơn sang DELIVERY_FAILED và tăng số lần. Đi giao lại sau thất bại đưa đơn về SHIPPED. Hoàn về người gửi đưa đơn sang RETURNED_TO_SELLER, hàng về kho và tiền được hoàn hoặc COD bị hủy.

### Q65. Hãng gửi trùng một sự kiện thì sao? [Cơ bản]

Mỗi sự kiện có mã riêng của hãng và mã này là duy nhất trong `shipment_events`. Nếu mã đã có, API trả "duplicate_ignored" và không ghi gì thêm. Ngoài ra simulator dùng mã sự kiện làm Idempotency-Key, nên lần gửi lại giống hệt còn được trả phản hồi cũ.

### Q66. Hãng gửi một sự kiện cũ đến muộn, sau sự kiện mới hơn, thì sao? [Nâng cao]

Hệ thống so sánh giờ xảy ra của sự kiện (theo hãng) với giờ của sự kiện mới nhất đã áp dụng. Nếu không mới hơn, sự kiện vẫn được lưu vào `shipment_events` với cờ applied = false nhưng không thay đổi vận đơn hay đơn ("stale_ignored"). Lưu lại để không mất dữ liệu cho đối soát và phân tích, nhưng không áp dụng để trạng thái không bị kéo lùi, ví dụ từ đã giao về đang vận chuyển.

### Q67. Vì sao cho phép bỏ qua bước (ví dụ PICKED_UP thẳng tới DELIVERED)? [Trung bình]

Vì hãng thật không phải lúc nào cũng gửi đủ mọi lần quét; có khi giao thẳng từ hub, hoặc sự kiện trung gian bị mất. Nếu bắt buộc đủ chuỗi, một sự kiện bị mất sẽ làm vận đơn kẹt mãi. Bảng "sự kiện nào được phép sau trạng thái nào" vì thế chấp nhận các bước nhảy hợp lý nhưng vẫn chặn những bước vô lý như giao thất bại khi chưa đi giao.

### Q68. Tối đa bao nhiêu lần giao? Sau đó thế nào? [Cơ bản]

Tối đa 3 lần giao thất bại. Sau lần thứ ba, hệ thống không cho đưa đơn đi giao lại nữa; hãng sẽ báo hoàn về người gửi, đơn sang RETURNED_TO_SELLER, hàng được cộng lại vào kho và tiền được xử lý (hủy COD hoặc hoàn online).

### Q69. Có thể hủy đơn sau khi đã đóng gói không? [Trung bình]

Không. Máy trạng thái không có bước chuyển từ READY_TO_SHIP sang CANCELLED: hàng đã được bàn giao (hoặc sắp bàn giao) cho hãng vận chuyển. Nếu khách không muốn nhận, đơn sẽ đi theo đường giao thất bại rồi hoàn về, hoặc khách nhận rồi yêu cầu trả hàng.

### Q70. `last_event_time` trên vận đơn dùng để làm gì? [Trung bình]

Lưu giờ xảy ra (theo hãng) của sự kiện mới nhất đã áp dụng. Nó là mốc để quyết định một sự kiện mới đến là "mới" hay "cũ đến muộn". Lưu ý đây là giờ của hãng, khác `created_at` của sự kiện là giờ sàn nhận được callback.

### Q71. Vận đơn trả hàng có gì khác vận đơn giao? [Trung bình]

Nó chỉ nhận sự kiện lấy hàng, vận chuyển, đang giao và đã giao — không có giao thất bại hay hoàn về. Lấy hàng đưa yêu cầu trả sang RETURNING; giao tới shop đưa sang RECEIVED. Mỗi đơn có tối đa một vận đơn mỗi chiều, do ràng buộc duy nhất trên (đơn, chiều).

## H. Hủy đơn, hoàn tất, hạng thành viên

### Q72. Ai được hủy đơn và ở trạng thái nào? [Cơ bản]

Người mua hủy được khi UNPAID hoặc TO_SHIP. Người bán hủy được khi TO_SHIP, với lý do hết hàng, khách yêu cầu hoặc sai giá. SYSTEM hủy đơn UNPAID khi hết hạn thanh toán và đơn TO_SHIP khi quá SLA đóng gói. Sau READY_TO_SHIP thì không ai hủy được.

### Q73. Vì sao hủy một đơn UNPAID lại hủy cả checkout? [Trung bình]

Các đơn của một checkout online chung một lần thanh toán chưa xảy ra. Không có cách trả "một nửa" payment, nên giữ lại một phần sẽ để checkout ở trạng thái vô nghĩa. Hủy cả checkout: trả lượt voucher, hủy các lần thử thanh toán, hủy mọi đơn UNPAID. Sau khi đã thanh toán, các đơn độc lập và hủy đơn nào thì chỉ hoàn tiền đơn đó.

### Q74. Hủy một đơn đã thanh toán online thì tiền đi đâu? [Trung bình]

Hệ thống tạo refund bằng đúng tổng tiền của đơn (đã trừ giảm giá, gồm phí ship) trên payment đã giữ tiền của checkout, phát `RefundIssued`. Payment vẫn ở SUCCEEDED; refund là dòng riêng. Lượt voucher không được trả lại, nhưng vì tiền hoàn là số sau giảm giá nên khách không lợi gì.

### Q75. Khi đơn quá SLA đóng gói, điều gì xảy ra với shop? [Cơ bản]

Job SYSTEM hủy đơn với lý do "seller_sla_breach", giải phóng hàng giữ, hoàn tiền nếu đã trả online, và cộng 1 điểm phạt cho shop (event `ShopPenalized`). SLA là 2 ngày nghiệp vụ tính từ lúc thanh toán (online) hoặc lúc checkout (COD). Simulator tạo khoảng 10% shop NORMAL "lười" để tình huống này xảy ra thường xuyên.

### Q76. Khi nào đơn COMPLETED? [Cơ bản]

Khi người mua xác nhận đã nhận hàng, hoặc tự động sau 3 ngày nghiệp vụ kể từ lúc giao. Ngoài ra đơn đang RETURN_REFUND sẽ COMPLETED khi yêu cầu trả bị từ chối hẳn hoặc chỉ được hoàn một phần.

### Q77. Việc gì xảy ra khi đơn COMPLETED? [Trung bình]

Trong cùng transaction: ghi mốc hoàn tất, cộng chi tiêu cho khách (tổng đơn trừ các khoản đã hoàn của đơn) và tính lại hạng, phát `CustomerSpendUpdated` với hạng trước/sau; rồi quyết toán cho shop bằng các bút toán SALE, COMMISSION, PAYMENT_FEE và phát `OrderSettled`.

### Q78. Vì sao cộng chi tiêu và quyết toán ở COMPLETED chứ không phải lúc thanh toán? [Trung bình]

Vì trước khi hoàn tất, số tiền còn có thể thay đổi: đơn có thể bị hủy, hoàn về, hoặc bị trả một phần. Ghi nhận sớm thì phải viết bút toán đảo ngược và điều chỉnh hạng ngược, phức tạp và dễ sai. COMPLETED là thời điểm số tiền đã cuối cùng, nên mỗi đơn chỉ được ghi nhận đúng một lần.

## I. Trả hàng và hoàn tiền

### Q79. Khi nào người mua được yêu cầu trả hàng? [Cơ bản]

Chỉ khi đơn đang DELIVERED, tức là trong khoảng từ lúc giao tới khi đơn hoàn tất (tối đa 3 ngày nghiệp vụ nếu khách không bấm xác nhận). Mỗi đơn tối đa một yêu cầu trả, nhưng có thể chỉ trả một số dòng hoặc một số lượng nhỏ hơn đã mua.

### Q80. Mô tả luồng trả hàng đầy đủ. [Trung bình]

Khách yêu cầu: đơn sang RETURN_REFUND, tạo yêu cầu REQUESTED với hạn trả lời 2 ngày. Shop duyệt (hoặc tự duyệt khi quá hạn) → APPROVED và tạo vận đơn trả; hoặc từ chối → REJECTED. Bị từ chối, khách có 1 ngày khiếu nại → DISPUTED, sàn phân xử hoàn hay bác. Hàng trả được lấy → RETURNING, tới shop → RECEIVED. Shop kiểm hàng, nhập lại kho phần còn bán được, khách được hoàn → REFUNDED. Đơn ở lại RETURN_REFUND nếu trả hết, hoặc sang COMPLETED nếu trả một phần.

### Q81. Số tiền hoàn được tính thế nào? [Trung bình]

Theo số tiền khách thực trả cho dòng (thành tiền trừ phần voucher shop và voucher sàn đã phân bổ), nhân với số đơn vị trả và chia cho số đã mua, làm tròn xuống. Ví dụ dòng 2 cáp 238.000 ₫ đã gánh 8.961 ₫ voucher shop và 15.207 ₫ voucher sàn, thực trả 213.832 ₫; trả 1 cáp được hoàn 106.916 ₫. Phí ship không được hoàn.

### Q82. Hàng trả về bị hỏng thì khách có được hoàn không? [Trung bình]

Có, khách được hoàn toàn bộ số tiền dự kiến bất kể tình trạng. Tình trạng chỉ quyết định tồn kho: dòng được đánh dấu SELLABLE thì nhập lại kho (CUSTOMER_RETURN), DAMAGED thì không. Rủi ro hàng hỏng thuộc về người bán, giống chính sách của các sàn lớn.

### Q83. Shop không trả lời yêu cầu trả hàng thì sao? [Cơ bản]

Sau 2 ngày nghiệp vụ, job SYSTEM tự duyệt với lý do "seller_no_response" và tạo vận đơn trả. Luật này bảo vệ người mua khỏi shop "im lặng" để hết hạn.

### Q84. Khách không khiếu nại khi bị từ chối thì sao? [Cơ bản]

Sau 1 ngày nghiệp vụ kể từ khi bị từ chối, job SYSTEM đóng yêu cầu (CLOSED, "no_dispute") và đơn chuyển COMPLETED: khách giữ hàng, shop được quyết toán toàn bộ.

### Q85. Khi trả toàn bộ đơn, đơn kết thúc ở đâu và shop nhận gì? [Nâng cao]

Đơn ở lại RETURN_REFUND như trạng thái cuối, không bao giờ COMPLETED, nên không có bút toán quyết toán nào cho shop và khách không được cộng chi tiêu. Điều này hợp lý: shop đã nhận lại hàng và khách đã nhận lại tiền hàng. Trả một phần thì đơn COMPLETED và quyết toán chỉ tính phần shop giữ lại.

### Q86. Tiền hoàn cho đơn COD đi từ payment nào? [Nâng cao]

Từ payment COD của chính đơn đó, vốn đã SUCCEEDED khi hãng giao hàng và thu tiền. Với đơn online, refund trỏ tới payment thành công được giữ của checkout. Hệ thống có một hàm tìm "payment đang giữ tiền của đơn" dùng chung cho hủy, hoàn về và trả hàng.

### Q87. Vai trò của sàn (PLATFORM) trong trả hàng là gì? [Trung bình]

Sàn chỉ xuất hiện khi khách khiếu nại một yêu cầu bị từ chối. Sàn có thể xử cho khách (yêu cầu sang APPROVED và đi tiếp như được duyệt) hoặc bác (CLOSED, đơn COMPLETED). Simulator phân xử khoảng 60% có lợi cho khách.

## J. Quyết toán người bán

### Q88. Shop nhận được bao nhiêu tiền khi đơn hoàn tất? [Trung bình]

SALE bằng tiền hàng của các đơn vị shop giữ lại trừ voucher shop; COMMISSION bằng SALE nhân tỉ lệ hoa hồng theo hạng (MALL 8%, PREFERRED 6%, NORMAL 4%); PAYMENT_FEE bằng 2% SALE. Ví dụ đơn 1.328.000 ₫ có voucher shop 50.000 ₫ ở shop NORMAL: SALE 1.278.000, hoa hồng 51.120, phí 25.560, shop thực nhận 1.201.320 ₫.

### Q89. Vì sao voucher sàn không bị trừ vào tiền của shop? [Cơ bản]

Vì sàn là bên tài trợ voucher sàn và trợ giá ship để kích cầu. Shop vẫn nhận doanh thu như khi khách trả giá gốc (trừ voucher của chính shop). Phần chênh lệch là chi phí khuyến mãi của sàn, thể hiện trong P&L của sàn.

### Q90. Sổ cái người bán được thiết kế thế nào và vì sao? [Nâng cao]

Mọi biến động ví là một bút toán chỉ-thêm trong `seller_ledger_entries` (SALE dương, COMMISSION, PAYMENT_FEE, PAYOUT âm), mỗi bút toán kèm một dòng `wallet_pending`. Job nền cuộn các dòng chờ vào `shop_wallets.balance` theo lô. Lý do: nếu mỗi đơn hoàn tất cập nhật thẳng dòng ví, shop lớn sẽ có hàng trăm transaction xếp hàng chờ khóa một dòng. Ghi chỉ-thêm không tranh chấp, cộng theo lô thì mỗi shop chỉ bị khóa một lần mỗi lô.

### Q91. Số dư thật của ví lúc bất kỳ là bao nhiêu? [Trung bình]

Bằng số dư đã cuộn trong `shop_wallets` cộng tổng các dòng còn chờ trong `wallet_pending` của shop đó. Danh sách shop của API trả về đúng con số tổng này. Sổ cái còn cho phép kiểm tra lại: số dư phải bằng tổng mọi bút toán của shop.

### Q92. Chi trả cho shop (payout) diễn ra thế nào? [Trung bình]

Job nền chọn shop có số dư dương, chưa được chi trả trong 1 ngày nghiệp vụ và có bút toán cũ hơn một ngày. Với mỗi shop, nó cuộn nốt phần chờ của shop, khóa ví, tạo `seller_payouts` bằng toàn bộ số dư, ghi bút toán PAYOUT âm và trừ ví về 0, phát `PayoutPaid`. Bút toán PAYOUT được cộng thẳng vào ví vì job đang giữ khóa ví.

### Q93. Phí thanh toán 2% có áp cho đơn COD không? [Cơ bản]

Có. Trong mô hình này phí giao dịch tính trên SALE của mọi đơn hoàn tất, không phân biệt phương thức. Đó là một quyết định đơn giản hóa; sàn thật có thể có phí khác nhau theo phương thức.

## K. Đồng thời, idempotency, outbox

### Q94. Hệ thống dùng mức cô lập transaction nào? Làm sao đúng mà không cần SERIALIZABLE? [Nâng cao]

Mức mặc định READ COMMITTED. Tính đúng đắn đến từ khóa dòng có thứ tự cố định, UPDATE có điều kiện (kiểm tra điều kiện ngay trong câu lệnh, dưới khóa dòng), ràng buộc CHECK/UNIQUE ở DB và khóa tư vấn theo khách cho voucher. SERIALIZABLE sẽ sinh nhiều lỗi serialization phải retry dưới tải cao; cách hiện tại cho thông lượng tốt hơn và lỗi có thể dự đoán.

### Q95. Thứ tự khóa toàn cục là gì và giải quyết vấn đề gì? [Nâng cao]

Slot quota voucher → tồn kho theo mã SKU tăng dần → mục flash sale; với thanh toán là checkout → payment → đơn. Mọi luồng (checkout, hủy, hết hạn, webhook) lấy khóa theo cùng thứ tự nên không thể chờ nhau theo vòng, tức là không deadlock. Ví dụ checkout giữ hàng các SKU theo thứ tự sắp xếp, và việc đóng checkout cũng khóa trước mọi SKU của nó theo đúng thứ tự đó trước khi hủy từng đơn.

### Q96. Tại sao đóng checkout khóa trước SKU bằng "FOR NO KEY UPDATE" mà không phải "FOR UPDATE"? [Nâng cao]

Khi một checkout khác chèn dòng đơn tham chiếu tới mục flash sale, PostgreSQL lấy khóa KEY SHARE trên mục đó để bảo vệ khóa ngoại. FOR UPDATE xung đột với KEY SHARE, nên đóng checkout sẽ chờ checkout kia, trong khi checkout kia có thể đang chờ một dòng tồn kho mà đóng checkout đang giữ → deadlock. FOR NO KEY UPDATE là đúng chế độ mà các lệnh UPDATE sau đó vẫn sẽ lấy, và nó không xung đột với KEY SHARE.

### Q97. Race về giá giữa checkout và đổi giá là gì, sửa thế nào? [Nâng cao]

Lần đổi giá đóng dấu thời gian nghiệp vụ trước khi commit. Nếu một checkout xảy ra sau mốc đó nhưng đọc trước khi lần đổi giá commit, nó sẽ thấy giá cũ, và đơn mang giá cũ trong khi lịch sử giá nói giá mới đã có hiệu lực từ trước. Sửa: checkout đọc SKU với FOR SHARE, phải chờ lần đổi giá đang diễn ra commit rồi đọc giá mới; lần đổi giá đến sau phải chờ checkout xong. FOR SHARE chỉ xung đột với cập nhật giá, vốn chỉ khóa một dòng, nên không gây vòng khóa.

### Q98. Race về giỏ hàng là gì? [Nâng cao]

Thêm món làm hai bước: "chèn giỏ ACTIVE nếu chưa có" (bị bỏ qua vì đã có giỏ), rồi khóa giỏ ACTIVE. Giữa hai bước, một checkout song song có thể commit và đóng giỏ đó; bước khóa lúc này không tìm thấy giỏ ACTIVE nào. Sửa: lặp lại tối đa 3 lần — lần sau, lệnh chèn thành công vì giỏ cũ không còn ACTIVE, và món được thêm vào giỏ mới.

### Q99. Nếu deadlock vẫn xảy ra thì sao? [Trung bình]

API bắt lỗi deadlock hoặc serialization, rollback toàn bộ và trả 503 kèm "Retry-After". Retry với cùng Idempotency-Key là an toàn vì lệnh chưa được áp dụng. Bộ kiểm thử stress yêu cầu không có deadlock nào trong PostgreSQL, và gate dừng lại ngay khi thấy một deadlock.

### Q100. Idempotency-Key hoạt động thế nào? [Trung bình]

Client gửi một khóa duy nhất cho mỗi lệnh. Trong chính transaction của lệnh, API chèn khóa (gắn tiền tố danh tính người gọi) cùng endpoint và hash của nội dung. Nếu khóa đã có: cùng nội dung thì trả lại phản hồi đã lưu với header Idempotent-Replay, khác nội dung thì từ chối 422. Nếu chưa có: chạy lệnh, lưu phản hồi, commit cả hai cùng lúc. Khóa bị xóa sau 72 giờ thực.

### Q101. Hai request cùng Idempotency-Key đến cùng lúc thì sao? [Nâng cao]

Request thứ hai chèn khóa và đụng unique index của khóa mà request đầu đã chèn nhưng chưa commit, nên nó phải chờ. Khi request đầu commit, request thứ hai thấy khóa đã tồn tại và trả lại phản hồi đã lưu. Nếu request đầu thất bại và rollback, khóa biến mất, request thứ hai chèn thành công và chạy lệnh từ đầu. Không bao giờ có hai lần áp dụng.

### Q102. Vì sao chỉ lưu phản hồi thành công? [Trung bình]

Vì một lệnh bị từ chối (ví dụ hết hàng, voucher hết lượt) thường có thể thành công sau khi client sửa tình huống hoặc trạng thái thay đổi. Nếu lưu cả lỗi, retry với cùng khóa sẽ mãi nhận lỗi cũ. Lệnh bị từ chối rollback luôn cả việc giành khóa, nên retry chạy lại thật.

### Q103. Tại sao khóa idempotency gắn tiền tố danh tính người gọi? [Trung bình]

Để một người gọi không thể đoán hay dùng lại khóa của người khác và nhận phản hồi của họ (có thể chứa dữ liệu riêng). Mỗi người gọi có không gian khóa riêng; hai người vô tình dùng cùng chuỗi khóa cũng không đụng nhau.

### Q104. Outbox pattern ở đây hoạt động thế nào? [Trung bình]

Mỗi domain event được chèn vào `outbox_events` và xóa ngay trong cùng transaction với thay đổi nghiệp vụ. Debezium đọc lệnh chèn từ WAL, Outbox Event Router chuyển nó thành message trên topic `events.<aggregate>` và bỏ qua lệnh xóa. Bảng luôn rỗng, không cần job dọn, mà event vẫn có trong WAL.

### Q105. Outbox giải quyết vấn đề gì so với việc API tự gửi event lên Kafka? [Trung bình]

Vấn đề dual-write: ghi DB và gửi Kafka là hai hệ thống, không có transaction chung. Ghi DB xong mà gửi Kafka lỗi thì mất event; gửi Kafka xong mà DB rollback thì có event "ma". Với outbox, event là một phần của transaction DB, nên nó tồn tại khi và chỉ khi thay đổi được commit; Debezium đảm bảo đọc mọi thứ đã commit.

### Q106. Đã có CDC theo bảng rồi, cần domain event làm gì? [Nâng cao]

CDC theo bảng cho biết dòng thay đổi thế nào, nhưng không cho biết ý nghĩa nghiệp vụ: một lần UPDATE `orders` có thể là thanh toán, đóng gói hay hủy, và một checkout đụng mười bảng. Domain event nói thẳng "OrderPlaced", "PaymentSucceeded" với payload nghiệp vụ gọn, dễ dùng cho phân tích luồng và cho các hệ thống khác. Kho dữ liệu dùng cả hai: bảng cho trạng thái, event cho ý nghĩa và đối soát.

## L. Mô hình dữ liệu và CDC

### Q107. Liệt kê các bảng theo miền. [Cơ bản]

Khách hàng: `customers`, `customer_addresses`. Người bán: `sellers`, `shops`. Danh mục và kho: `categories`, `brands`, `products`, `product_skus`, `price_history`, `inventory`, `inventory_movements`. Giỏ: `carts`, `cart_items`. Khuyến mãi: `vouchers`, `voucher_quota_slots`, `voucher_usages`, `campaigns`, `flash_sale_items`. Đơn và thanh toán: `checkouts`, `orders`, `order_items`, `order_status_history`, `payments`, `refunds`. Vận chuyển: `shipments`, `shipment_events`. Trả hàng: `returns`, `return_items`. Đánh giá: `product_reviews`. Quyết toán: `seller_ledger_entries`, `seller_payouts`, `shop_wallets`, `wallet_pending`. Hạ tầng: `outbox_events`, `idempotency_keys`, `sim_clock`, `cdc_heartbeat`, `debezium_signals`.

### Q108. Bảng nào là chỉ-thêm (append-only)? [Trung bình]

Thật sự chỉ-thêm: `price_history`, `inventory_movements`, `order_status_history`, `shipment_events`, `seller_ledger_entries`. Ngoài ra một số bảng hiện chỉ được chèn trong thực tế (`order_items`, `refunds`, `seller_payouts`, `product_reviews`, `vouchers`, `campaigns`, `sellers`, `categories`, `brands`, `products`) dù có trigger cập nhật `updated_at`. Nhóm đầu được cấu hình replica identity mặc định để tiết kiệm WAL.

### Q109. Bảng nào bị cập nhật nhiều nhất? [Trung bình]

`orders` (mỗi lần chuyển trạng thái, kèm các mốc thời gian và version), `inventory` (mỗi lần giữ hàng, giải phóng, xuất kho, nhập), `payments`, `shipments`, `checkouts`, `carts`, `flash_sale_items`, `returns`. Với CDC, mỗi lần cập nhật là một sự kiện, nên một đơn có thể sinh hàng chục sự kiện trên nhiều bảng trong vòng đời.

### Q110. Bảng nào bị xóa cứng? [Trung bình]

Trong dữ liệu nghiệp vụ được stream, chỉ có `cart_items` (khi xóa món). Ngoài ra `outbox_events` chèn rồi xóa trong cùng transaction (router bỏ qua lệnh xóa), `wallet_pending` bị xóa khi cuộn ví và `idempotency_keys` bị xóa khi hết hạn — hai bảng này không được stream.

### Q111. REPLICA IDENTITY FULL và DEFAULT được dùng ở đâu, vì sao? [Nâng cao]

Bảng trạng thái dùng FULL để mỗi sự kiện update/delete mang toàn bộ dòng cũ ở phần "before"; tầng silver cần nó để xử lý delete và tầng "consistent" cần nó để dựng lại trạng thái tại một mốc. Bảng chỉ-thêm và outbox dùng DEFAULT (chỉ khóa chính) vì chúng không bao giờ bị cập nhật hay xóa có ý nghĩa, nên phần "before" đầy đủ chỉ tốn WAL.

### Q112. Bảng nào không được stream và vì sao? [Trung bình]

`voucher_quota_slots` và `wallet_pending` là chi tiết kỹ thuật để tránh hot row; thông tin của chúng đã có trong `voucher_usages` và `seller_ledger_entries`. `idempotency_keys`, `sim_clock`, `cdc_heartbeat` là hạ tầng. `debezium_signals` được capture để chạy incremental snapshot nhưng không đưa vào mô hình kho. Tổng cộng 31 bảng nghiệp vụ được đưa vào kho.

### Q113. Vì sao đơn hàng chép cứng địa chỉ giao thay vì chỉ lưu id địa chỉ? [Cơ bản]

Vì khách có thể sửa hoặc thêm địa chỉ sau khi đặt; đơn phải giữ đúng nơi đã giao tại thời điểm đó. Đây là kỹ thuật snapshot quen thuộc cho dữ liệu giao dịch. Checkout vẫn giữ id địa chỉ, còn đơn có tên, chuỗi địa chỉ và tỉnh nhận (tỉnh dùng cho phí ship và phân tích theo vùng).

### Q114. Cột `version` trên `orders` có ý nghĩa gì? [Trung bình]

Nó tăng 1 mỗi lần đơn chuyển trạng thái. Phía downstream có thể dùng nó để biết bản ghi nào mới hơn và phát hiện bỏ sót hay đảo thứ tự cập nhật. Cùng với `order_status_history`, nó cho biết đơn đã qua bao nhiêu bước.

### Q115. Domain event có những aggregate nào? [Cơ bản]

13 aggregate: customer, shop, catalog, product, inventory, cart, checkout, order, payment, promotion, shipment, return, review — tương ứng 13 topic `events.<aggregate>`, với tổng cộng 33 loại event. Ví dụ aggregate order có OrderPlaced và OrderStatusChanged; payment có PaymentCreated, PaymentSucceeded, PaymentFailed, RefundIssued.

### Q116. Có event riêng cho việc giữ hàng hay xuất kho không? [Trung bình]

Không. Giữ hàng được thể hiện qua OrderPlaced (kèm danh sách món) và qua sự kiện CDC trên `inventory`, `inventory_movements`; xuất kho đi kèm OrderStatusChanged sang SHIPPED. Event chỉ có ở những chỗ mang ý nghĩa nghiệp vụ rõ ràng; chi tiết cơ học để cho CDC theo bảng. Riêng nhập hàng có InventoryRestocked vì đó là hành động chủ động của người bán.

### Q117. Những ràng buộc ở DB nào bảo vệ tính đúng của tiền? [Trung bình]

Tổng đơn phải bằng tiền hàng trừ voucher shop trừ voucher sàn cộng phí ship trừ trợ giá ship; dòng đơn có thành tiền bằng số lượng nhân đơn giá và số thực trả bằng thành tiền trừ các phần giảm (không âm); các khoản không âm; tổng checkout dương. Nếu code tính sai, DB từ chối chứ không lưu số sai.

## M. Simulator

### Q118. Simulator làm gì và gọi API bằng danh tính nào? [Cơ bản]

Nó đóng mọi tác nhân của sàn và tạo toàn bộ lưu lượng. Lệnh của người mua mang token của đúng người mua đó, lệnh của shop mang token của shop, webhook được ký HMAC bằng khóa của cổng hoặc hãng, còn việc admin và các truy vấn danh sách dùng token riêng của simulator. Nhờ vậy API kiểm tra quyền như với người dùng thật. Chi tiết ở tài liệu 02.

### Q119. Tần suất hành vi được chuyển thành hành động thế nào? [Trung bình]

Mỗi hành vi là tần suất mỗi giờ nghiệp vụ. Với một ứng viên (ví dụ một đơn TO_SHIP), xác suất hành động trong tick là 1 − e^(−rate·Δt), với Δt là số giờ nghiệp vụ đã trôi qua. Số phiên mua mới lấy theo phân phối Poisson. Công thức này đảm bảo kết quả không phụ thuộc độ dài tick hay tốc độ đồng hồ.

### Q120. Lưu lượng thay đổi theo thời gian thế nào? [Cơ bản]

Theo hồ sơ 24 giờ giờ VN: đêm khuya rất thấp (khoảng 0,05), tăng dần buổi sáng, đỉnh nhỏ buổi trưa (1,25), cao điểm buổi tối (1,8 lúc 21 giờ). Cuối tuần ×1,2. Flash sale ×2, mega sale ×3, giờ đầu mega sale ×8. Ở ×240 với 150 phiên/giờ, trung bình khoảng 10 phiên mua mỗi giây thực vào giờ chuẩn.

### Q121. Vì sao dùng phân phối Zipf cho độ phổ biến SKU? [Trung bình]

Vì doanh số thật có "đuôi dài": vài sản phẩm bán chạy chiếm phần lớn đơn. Zipf tạo đúng hình dạng đó, và quan trọng hơn, nó tạo ra hot row thật trên tồn kho của SKU bán chạy — chính là thứ cần để kiểm thử khóa và hiệu năng. Phân phối đều sẽ che giấu nút thắt này.

### Q122. Cổng thanh toán giả lập "xấu tính" thế nào? [Trung bình]

Callback đến sau 10–300 giây nghiệp vụ; 92% thành công; 6% người mua không bao giờ trả (checkout hết hạn); một nửa người bị lỗi thử lại; 5% callback gửi trùng, nửa cùng event id nửa id mới; 2% thành công đến sau 1.500 giây, quá hạn 15 phút, buộc sàn tự hoàn tiền. Mỗi kiểu xấu tính kiểm chứng một nhánh xử lý của webhook.

### Q123. Hãng vận chuyển giả lập "xấu tính" thế nào? [Trung bình]

Thời gian vận chuyển theo tuyến (12, 30, 60 giờ), 7% mỗi lần giao thất bại, giao lại, hoàn về sau lần thất bại thứ ba. 3% sự kiện gửi trùng cùng mã; 4% sự kiện giao có thêm một lần quét cũ đến muộn với giờ sớm hơn, để kiểm tra việc lưu nhưng không áp dụng. Giờ sự kiện luôn nằm trong khoảng vừa trôi qua và sau sự kiện trước.

### Q124. Người mua giả lập phản ứng thế nào khi checkout bị từ chối? [Trung bình]

Như người thật dùng app: voucher bị từ chối thì bỏ voucher đó và thử lại; hết hàng thì xóa các món thiếu khỏi giỏ và thử lại, bỏ cuộc nếu mọi món đều hết; flash sale hết quota thì thử lại với giá thường; giỏ rỗng thì thôi. Mọi từ chối 4xx được đếm như lưu lượng bình thường, không phải lỗi.

### Q125. Shop "lười" là gì và để làm gì? [Cơ bản]

Khoảng 10% shop NORMAL (chọn ổn định theo id shop) đóng gói với tần suất rất thấp (0,01 đơn mỗi giờ thay vì 0,1). Chúng thường vượt SLA 2 ngày, nên đơn bị SYSTEM hủy và shop bị phạt; khách cũng chấm chúng thấp hơn. Mục đích là tạo dữ liệu thật cho metric hủy do SLA, thời gian đóng gói theo shop và điểm đánh giá.

### Q126. Seeding tạo những gì và có an toàn khi chạy lại không? [Cơ bản]

Danh mục và thương hiệu, rồi shop (đăng ký qua identity, đăng sản phẩm tới đủ số SKU của preset, 40% có voucher shop), rồi khách hàng — tất cả qua API. Preset medium mặc định là 50 shop, 1.000 SKU, 5.000 khách. Chạy lại an toàn vì seeding chỉ tạo phần còn thiếu so với preset và danh mục là upsert.

### Q127. Mega sale 11.11 trong simulator gồm những gì? [Cơ bản]

Ba voucher sàn lớn (giảm 100.000 ₫ cho đơn từ 800.000 ₫ với 2.000 lượt; giảm 15% tối đa 200.000 ₫ với 3.000 lượt; freeship 50.000 ₫ với 10.000 lượt) và một chiến dịch 24 giờ với 60 SKU giảm giá. Lưu lượng ×8 giờ đầu rồi ×3. Đây là bài kiểm tra hot row trên voucher và SKU flash sale.

### Q128. Vận hành sàn tự động làm gì mỗi ngày? [Cơ bản]

Ở mỗi nửa đêm nghiệp vụ, phát 3 voucher sàn có hiệu lực tới hết ngày và lên lịch 3 khung flash sale lúc 0h, 12h, 20h, mỗi khung 2 giờ với 15 SKU giảm 30–50% và quota 20–80. Trong ngày nó phân xử các khiếu nại trả hàng.

### Q129. Nút "tua nhanh" trên Control Center làm gì? [Trung bình]

Nó cho cổng thanh toán, người bán và hãng vận chuyển hành động như thể một ngày nghiệp vụ đã trôi qua: gửi các callback thanh toán đến hạn trong một ngày, đóng gói và đẩy vận đơn với Δt bằng 24 giờ. Đồng hồ nghiệp vụ không bị đổi; chỉ là xác suất hành động được tính cho khoảng dài hơn. Dùng để demo nhanh trọn vòng đời đơn.

## N. Câu hỏi phỏng vấn mở

### Q130. Hãy mô tả luồng checkout từ đầu đến cuối, bao gồm cả xử lý đồng thời. [Nâng cao]

Checkout là một transaction: kiểm tra khách và địa chỉ, khóa tư vấn theo khách nếu có voucher, kiểm tra voucher sớm không khóa, khóa giỏ, đọc SKU với khóa chia sẻ để không lệch giá, kiểm tra còn bán và đủ hàng, chốt thời điểm t, áp giá flash sale, tách đơn theo shop, tính ship theo rate card, áp voucher theo thứ tự shop → sàn → freeship và phân bổ xuống từng dòng. Sau khi ghi checkout, đơn, dòng, lịch sử và event, nó mới lấy tài nguyên khan hiếm ở cuối theo thứ tự khóa toàn cục: quota voucher (slot ngẫu nhiên), giữ hàng theo SKU, quota flash sale — đều là UPDATE có điều kiện nên không bán quá. Hết giữa chừng thì rollback toàn bộ. Cuối cùng tạo payment online (hoặc COD theo đơn), đóng giỏ. Kết quả là p99 khoảng 133–162 ms với một simulator.

### Q131. Nếu được thiết kế lại, bạn sẽ đổi gì trong mô hình nguồn? [Nâng cao]

Vài hướng hợp lý: thêm luồng ngừng bán SKU/shop thật sự để tạo thêm loại thay đổi; tách tồn kho theo kho nếu shop có nhiều kho; cho phép trả lượt voucher khi hủy đơn đã thanh toán theo chính sách; ghi refund có trạng thái PENDING để mô phỏng hoàn tiền bất đồng bộ qua cổng. Về hiệu năng, có thể chia nhỏ tồn kho của SKU cực nóng thành nhiều "bucket" giống quota voucher, vì đó là nút thắt hiện tại. Mỗi thay đổi đều phải cân nhắc độ phức tạp tăng thêm cho pipeline và cho bộ đối soát.

### Q132. Nút thắt hiệu năng hiện tại ở đâu và bạn sẽ xử lý thế nào? [Nâng cao]

Với 8 simulator, hệ thống đạt khoảng 1.800 sự kiện CDC mỗi giây; nút thắt là đường ghi, chủ yếu chờ khóa trên dòng `inventory` của SKU bán chạy, chứ không phải Debezium hay ClickHouse. Hướng xử lý: chia tồn kho SKU nóng thành nhiều bucket như quota voucher, giữ hàng theo lô cho flash sale, hoặc tách việc giữ hàng thành bước bất đồng bộ có thời hạn. Mỗi cách đánh đổi độ đơn giản của bất biến "tồn kho = tổng sổ kho" lấy thông lượng.

### Q133. Làm sao bạn biết dữ liệu nguồn luôn đúng? [Nâng cao]

Ba lớp. Ràng buộc ở DB (CHECK, UNIQUE, khóa ngoại) chặn số sai ngay khi ghi. Một hàm duy nhất cho mỗi loại thay đổi (tồn kho, trạng thái đơn, trạng thái trả) giữ bất biến ở một chỗ. Và bộ kiểm tra dữ liệu đọc PostgreSQL trong một snapshot nhất quán để xác minh hàng loạt bất biến: tiền khớp, tồn kho bằng sổ kho, lịch sử trạng thái phát lại được qua bảng chuyển, voucher không vượt lượt, giá đơn khớp lịch sử giá, quyết toán khớp. Thêm vào đó là bài stress cố ý dồn nhiều checkout, hủy và thanh toán vào vài SKU và voucher, yêu cầu không deadlock và không lệch quota.

### Q134. Bạn xử lý callback trùng, trễ, sai thứ tự từ bên thứ ba thế nào? [Nâng cao]

Ba cơ chế. Idempotency theo event id của partner để khử trùng lần gửi lại giống hệt. Kiểm tra trạng thái nghiệp vụ để biến callback lặp kết quả thành no-op. So sánh giờ xảy ra với sự kiện mới nhất đã áp dụng để lưu nhưng không áp dụng sự kiện cũ. Riêng tiền thì nguyên tắc là "tiền đã bị trừ thì phải có hạch toán": thanh toán thành công đến trễ vẫn được ghi nhận và hoàn ngay, không bị bỏ qua.

### Q135. Vì sao thiết kế này hữu ích cho việc kiểm thử pipeline CDC? [Trung bình]

Nó sinh ra mọi kiểu thay đổi mà một pipeline phải chịu: insert, nhiều update trên cùng dòng, delete cứng, transaction lớn đụng nhiều bảng, bảng chỉ-thêm khối lượng lớn, hot row, đỉnh tải 11.11, sự kiện đến trễ từ partner, và cả domain event qua outbox. Vì nguồn có bất biến chặt và thời gian nghiệp vụ tách khỏi thời gian thực, có thể đối soát kho với nguồn một cách chính xác và đo độ trễ bằng giây thật.

### Q136. Hai buyer cùng mua món cuối cùng của một SKU. Điều gì xảy ra? [Trung bình]

Cả hai có thể cùng đọc thấy available = 1 ở đầu checkout. Tới cuối, cả hai cùng chạy lệnh giữ hàng có điều kiện; lệnh đầu lấy khóa dòng tồn kho và thành công, lệnh sau chờ, rồi kiểm tra lại điều kiện trên dữ liệu mới và thấy không còn đủ. Checkout thứ hai bị rollback với `out_of_stock` kèm số còn lại là 0. Không có cách nào bán ra hai món.

### Q137. Người mua hủy đơn đúng lúc webhook thanh toán thành công tới. Kết quả thế nào? [Nâng cao]

Với đơn UNPAID, cả hai đều khóa checkout trước, nên chạy tuần tự. Nếu hủy thắng: checkout CANCELLED, đơn hủy, voucher và hàng được trả; webhook đến sau thấy checkout không còn chờ thanh toán, ghi payment thành công và hoàn ngay (late_payment). Nếu webhook thắng: checkout PAID, đơn TO_SHIP; lệnh hủy đang chờ khóa thấy checkout không còn PENDING_PAYMENT và bị từ chối, người mua bấm lại thì lần này đơn đã TO_SHIP nên chỉ đơn đó bị hủy và được hoàn tiền. Cả hai kết cục đều nhất quán về tiền.

### Q138. Shop đóng gói đúng lúc job SLA định hủy đơn. Kết quả thế nào? [Nâng cao]

Cả hai khóa dòng đơn. Job xử lý mỗi đơn trong transaction riêng, khóa lại và kiểm tra lại trạng thái. Nếu shop đóng gói trước, job thấy đơn đã READY_TO_SHIP và bỏ qua. Nếu job trước, đơn đã CANCELLED và lệnh đóng gói bị từ chối vì bước chuyển không hợp lệ. Không có trạng thái lai.

### Q139. Khách bấm "đã nhận hàng" đúng lúc job tự hoàn tất chạy. Có bị quyết toán hai lần không? [Nâng cao]

Không. Cả hai đi qua hàm chuyển trạng thái, khóa dòng đơn. Bên đến sau thấy đơn đã COMPLETED; bước COMPLETED → COMPLETED không tồn tại nên bị từ chối (job thì bỏ qua êm). Bút toán quyết toán và cộng chi tiêu chỉ được ghi trong transaction thực hiện bước chuyển, nên chỉ có đúng một lần.

### Q140. Khách yêu cầu trả hàng đúng lúc đơn sắp tự hoàn tất. Chuyện gì xảy ra? [Nâng cao]

Job tự hoàn tất chọn ứng viên trước, rồi khóa từng đơn và kiểm tra lại trạng thái. Nếu khách đã kịp yêu cầu trả, đơn đang RETURN_REFUND nên job bỏ qua — dù RETURN_REFUND → COMPLETED là bước hợp lệ cho SYSTEM, job chỉ hoàn tất đơn đang DELIVERED. Nếu job thắng, đơn COMPLETED và yêu cầu trả bị từ chối vì chỉ đơn DELIVERED mới trả được.

### Q141. Hãy giải thích vì sao checkout tạo một payment online nhưng nhiều payment COD. [Trung bình]

Online, khách trả một lần cho cả giỏ qua cổng, nên một payment cho cả checkout là đúng thực tế; khi cần hoàn cho một đơn, refund trỏ tới payment chung đó với số tiền của đơn. COD, tiền được thu bởi shipper của từng đơn vào lúc giao, và các đơn của các shop được giao vào thời điểm khác nhau bởi có thể là các hãng khác nhau, nên mỗi đơn cần một payment riêng để ghi nhận thu tiền độc lập.

### Q142. Tài liệu thiết kế cũ khác code hiện tại ở những điểm chính nào? [Trung bình]

Bản cũ là cửa hàng linh kiện máy tính một người bán, có nhà cung cấp, kho, đơn mua hàng, danh mục nhiều-nhiều, chụp giá vào giỏ, và đích là Flink → Iceberg → MinIO. Code hiện tại là sàn nhiều shop đa ngành, không có nhà cung cấp/kho/đơn mua, giỏ không lưu giá, checkout tách đơn theo shop, có quyết toán, ví, trả hàng có tranh chấp, đồng hồ nghiệp vụ, và đích là Kafka → ClickHouse. Khi có mâu thuẫn, code là chuẩn.
