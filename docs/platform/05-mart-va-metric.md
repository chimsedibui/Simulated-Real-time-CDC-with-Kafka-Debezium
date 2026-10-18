# 05 · Mart và metric: nghiệp vụ xử lý từ gold ra mart

[Tài liệu dự án](../README.md) / [Data platform](README.md)

## Đọc file này để biết gì

- Lớp **gold** biểu diễn gì về mặt nghiệp vụ, được dựng từ `consistent.<table>(v)` ra sao.
- **18 mart**: mỗi mart trả lời câu hỏi kinh doanh nào, grain là gì, dimension/measure định nghĩa chính xác thế nào, đọc từ đâu, panel Grafana nào dùng nó.
- **Từ điển metric**: placed GMV, GMV, NMV, net revenue, take rate, return rate… và cách voucher, phí ship, hoàn tiền, múi giờ được xử lý.
- **Chốt ngày nghiệp vụ** (business-day close): chốt là gì, điều kiện chốt, vì sao phải đóng băng số liệu, và bài học "15 ngày bị chốt sai".
- **Reconciliation** PostgreSQL ↔ ClickHouse và **data contract** (`quality/check_data.py`): mỗi kiểm tra khẳng định điều gì, vì sao.
- Mart được refresh thế nào (ở mức đủ hiểu), dashboard Business Analytics đọc mart nào.

Không lặp lại ở đây: luồng nghiệp vụ nguồn ([01](../business/01-nghiep-vu-va-du-lieu-nguon.md)), CDC/ingestion ([03](03-cdc-ingestion.md)), các lớp warehouse, watermark và snapshot ([04](04-warehouse.md)), vận hành ([06](../operations/06-van-hanh.md)). Câu hỏi ôn tập: [Q&A · 05-mart-va-metric.md](../interview/05-mart-va-metric.md).

## Mục lục

1. [Bức tranh tổng thể](#1-bức-tranh-tổng-thể)
2. [Quy ước chung: tiền, thời gian, "bây giờ"](#2-quy-ước-chung-tiền-thời-gian-bây-giờ)
3. [Từ điển metric](#3-từ-điển-metric)
4. [Lớp gold](#4-lớp-gold)
5. [Ví dụ số xuyên suốt: một đơn, hai dòng, voucher, trả một phần](#5-ví-dụ-số-xuyên-suốt)
6. [18 mart](#6-18-mart)
7. [Mart được refresh thế nào](#7-mart-được-refresh-thế-nào)
8. [Chốt ngày nghiệp vụ](#8-chốt-ngày-nghiệp-vụ)
9. [Reconciliation PostgreSQL ↔ ClickHouse](#9-reconciliation-postgresql--clickhouse)
10. [Data contract và kiểm tra chất lượng](#10-data-contract-và-kiểm-tra-chất-lượng)
11. [Dashboard theo vai trò nghiệp vụ](#11-dashboard-theo-vai-trò-nghiệp-vụ)
12. [Chỗ README cũ lệch với SQL](#12-chỗ-readme-cũ-lệch-với-sql)

---

## 1. Bức tranh tổng thể

```
consistent.<table>(v)      silver "như PostgreSQL tại watermark v" — không có transaction bị xé đôi (doc 04)
        │  cùng một v cho cả câu truy vấn
        ▼
gold.*  (view có tham số v)   định nghĩa nghiệp vụ: order_facts + 18 view mart + daily_sales_closed
        │  warehouse-jobs đọc bản gold_snap.* (cùng logic, đọc snap.<table> thay vì consistent.*)
        ▼
mart.<tên>                 bảng vật lý, mỗi lần refresh gắn refresh_id = v
        │
        ▼
Grafana "Marketplace · Business Analytics"  — mọi panel đọc cùng một refresh_id
```

Ba ý cần nhớ:

- **Định nghĩa nằm một chỗ**: toàn bộ công thức metric ở gold. Mart chỉ là bản chụp kết quả gold tại một watermark; không có logic nghiệp vụ riêng.
- **Một watermark cho mọi thứ**: một lần refresh tính cả 18 mart ở cùng một `v`, nên các panel trên dashboard luôn nhìn cùng một trạng thái dữ liệu (không có chuyện panel GMV đã thấy đơn mới còn panel đơn hàng chưa thấy).
- **Hai loại số theo ngày**: `gold.daily_sales` là số **sống** (thay đổi khi có huỷ/trả hàng), `gold.daily_sales_closed` là số **đã chốt** ("as reported"), không bao giờ đổi.

## 2. Quy ước chung: tiền, thời gian, "bây giờ"

| Quy ước | Nội dung |
|---|---|
| Tiền | VND, số nguyên (Int64). Không có số thập phân, không đổi tiền tệ. |
| Thời gian nghiệp vụ | Mọi mốc thời gian (`created_at`, `paid_at`, `delivered_at`…) là **giờ nghiệp vụ** do đồng hồ mô phỏng của nguồn đóng dấu (`business_now()`, có thể chạy nhanh gấp nhiều lần giờ thật, xem [01](../business/01-nghiep-vu-va-du-lieu-nguon.md)). Không bao giờ dùng giờ ingest hay giờ máy ClickHouse để quy ngày. |
| Ngày nghiệp vụ | Ngày theo múi giờ **Asia/Ho_Chi_Minh** (UTC+7) của thời điểm đặt đơn (`created_at`). Dữ liệu lưu ở UTC, chỉ đổi sang giờ Việt Nam khi quy ra ngày. |
| "Bây giờ" của warehouse | Các cửa sổ thời gian (48 giờ gần nhất, cohort đã "chín" 3 ngày, chốt ngày sau 1 giờ, cutoff reconciliation 1 giờ) đo so với **thời điểm đặt đơn mới nhất** đã thấy trong warehouse, không phải đồng hồ thật. Nhờ vậy kết quả không phụ thuộc tốc độ đồng hồ mô phỏng và không trôi khi simulator dừng. |
| Doanh số thuộc ngày nào | Theo **ngày đặt đơn**. Huỷ hay trả hàng xảy ra mấy ngày sau vẫn làm thay đổi số của ngày đặt đơn (không phải ngày hoàn tiền). Đây là cách nhìn theo cohort đơn hàng, không phải theo dòng tiền. |

Một số khái niệm của đơn hàng mà mọi metric dựa vào (trong `gold.order_facts`):

- **confirmed** (đơn đã xác nhận): đã thanh toán online (có `paid_at`), **hoặc** là đơn COD (COD được xác nhận ngay lúc checkout). Đơn online chưa trả tiền rồi hết hạn 15 phút → không confirmed.
- **kept** (đơn được giữ lại): trạng thái hiện tại **không phải** `CANCELLED` (huỷ) và không phải `RETURNED_TO_SELLER` (giao thất bại, hàng quay về shop). Đơn `RETURN_REFUND` (đang/đã trả hàng) vẫn tính là kept — phần trả được trừ riêng theo từng đơn vị hàng.
- **returned_gross**: giá trị hàng hoá (số lượng trả × đơn giá lúc mua, **trước voucher**) của các đơn vị đã được hoàn tiền qua return có trạng thái `REFUNDED`.
- **returned_net**: số tiền thực hoàn cho các đơn vị đó (**sau voucher**, bằng net của dòng chia theo tỷ lệ số lượng trả).

## 3. Từ điển metric

Định nghĩa gốc nằm ở đầu `warehouse/clickhouse/init/03-gold.sql`; `check_data.py` tính lại một số metric chính ngay trong PostgreSQL để đối chiếu (mục 10).

| Metric | Định nghĩa chính xác | Ghi chú nghiệp vụ |
|---|---|---|
| **Placed GMV** | Tổng `subtotal` (giá trị hàng = Σ số lượng × đơn giá, trước voucher, không gồm phí ship) của **mọi** đơn đã đặt. | Không bao giờ đổi sau khi đơn được tạo. Dùng cho số theo giờ, khi đơn mới còn đang trong cửa sổ thanh toán. |
| **GMV** | Placed GMV của các đơn **confirmed**. | Không trừ đơn bị huỷ sau khi đã trả tiền, không trừ hàng trả. Ổn định khi cửa sổ thanh toán (15 phút nghiệp vụ) của mọi đơn trong ngày đã đóng. |
| **NMV** (net merchandise value) | GMV của các đơn **confirmed và kept**, trừ đi **returned_gross** (giá trị hàng của đơn vị đã hoàn tiền qua return). | Thay đổi liên tục theo huỷ/trả hàng. Vẫn là giá trị trước voucher. |
| **Net revenue** | Σ `net_amount` của dòng hàng (= `line_total` − phần voucher shop − phần voucher sàn phân bổ cho dòng) trên đơn **confirmed và kept**, trừ đi số tiền hoàn qua return của dòng đó. | "Người mua thực trả cho hàng hoá". Dùng để xếp hạng sản phẩm/ngành hàng. Không gồm phí ship. |
| **Gross revenue** (product/category) | Σ `line_total` (trước voucher) trên đơn confirmed và kept. | Không trừ hàng trả. |
| **Units sold** | Σ số lượng đặt − số lượng đã hoàn tiền, trên đơn confirmed và kept. | |
| **Completed revenue** | Σ `total` (số người mua phải trả: subtotal − voucher shop − voucher sàn + phí ship − trợ giá ship) của đơn có trạng thái **COMPLETED**. | Không trừ hoàn tiền trả hàng một phần (đơn trả một phần vẫn về COMPLETED). |
| **AOV** (avg order value) | GMV / số đơn confirmed, làm tròn. | |
| **Buyers** | Số khách hàng khác nhau có ít nhất một đơn confirmed. | |
| **Cancelled orders** | Số đơn có trạng thái hiện tại `CANCELLED`. | Gồm cả đơn chưa trả tiền bị hết hạn. |
| **Unpaid cancelled** | Đơn `CANCELLED` mà chưa bao giờ confirmed (bỏ dở thanh toán/hết hạn). | Tách khỏi huỷ "thật" sau khi đã cam kết mua. |
| **Take** | Hoa hồng (COMMISSION) + phí thanh toán (PAYMENT_FEE) sàn thu từ người bán, lấy từ sổ cái người bán (seller ledger), đổi dấu thành số dương. | Chỉ phát sinh khi đơn COMPLETED; tính trên doanh thu của seller (sau voucher shop, sau phần trả). Phí thanh toán = 2%, hoa hồng theo tỷ lệ của shop. |
| **Take rate** | Take / Σ `subtotal` của các đơn COMPLETED. | Làm tròn 4 chữ số. |
| **Platform promo cost** | Σ voucher sàn + trợ giá phí ship (freeship) trên đơn confirmed và kept. | Chi phí khuyến mãi sàn tự gánh. |
| **Seller promo cost** | Σ voucher shop trên đơn confirmed và kept. | Người bán gánh (bị trừ vào SALE trong sổ cái). |
| **Return refunds** | Hoàn tiền gắn với một return. | **Chi phí bán hàng** duy nhất trong ba loại hoàn tiền. |
| **Cancellation refunds** | Hoàn tiền không gắn return và không phải lý do `late_payment`: đơn online đã trả tiền rồi bị huỷ hoặc giao thất bại → hoàn **đúng bằng total** (gồm phí ship). | Trả lại tiền của đơn không bao giờ thành doanh số (không confirmed-and-kept). |
| **Late-payment refunds** | Tiền về sau khi checkout đã hết hạn/bị thay thế → hoàn toàn bộ. | Đơn không confirmed, không ở GMV. |
| **Pack time** | Từ lúc đơn vào `TO_SHIP` (thanh toán xong, hoặc ngay checkout với COD) đến `READY_TO_SHIP`, lấy từ lịch sử trạng thái, trung bình theo shop, đơn vị giờ. | Chỉ tính đơn đã đóng gói. |
| **Return rate (cohort)** | Theo ngày giao hàng: số đơn có **bất kỳ** yêu cầu trả hàng nào / số đơn đã giao. | Cohort "matured" khi lần giao cuối của cohort cách thời điểm hiện tại ≥ 3 ngày (cửa sổ trả hàng đã đóng). |
| **Return rate (theo lý do)** | Số return của lý do đó trên các đơn đã "chín" / tổng số đơn đã giao và đã "chín". | Chỉ tính đơn đã qua cửa sổ trả hàng để tỷ lệ không bị loãng. |
| **Sell-through** (flash sale) | Đã bán / quota. "Đã bán" = số đơn vị trên các đơn chưa huỷ. | |
| **Redemption rate** (voucher) | Lượt dùng đang hiệu lực (`APPLIED`) / giới hạn lượt dùng. | Lượt dùng bị nhả (`RELEASED`, checkout kết thúc không trả tiền) không tính. |
| **Payment success rate** | Lượt thanh toán thành công / lượt đã có kết quả (không tính `PENDING`). | |
| **Delivery success rate** | Kiện giao thành công / (thành công + trả về người gửi). | Chỉ kiện đã kết thúc. |

Cách xử lý từng khoản tiền, tóm gọn:

| Khoản | Trong GMV/NMV? | Trong net revenue? | Trong completed revenue / tiền thu? | Ghi nhận ở đâu |
|---|---|---|---|---|
| Voucher shop | Không trừ | Trừ | Trừ | seller promo cost; trừ vào SALE của người bán |
| Voucher sàn | Không trừ | Trừ | Trừ | platform promo cost |
| Phí ship | Không cộng | Không cộng | Cộng | — |
| Freeship (trợ giá ship) | — | — | Trừ | platform promo cost |
| Hoàn tiền trả hàng | NMV trừ theo giá trước voucher | Trừ theo số tiền hoàn thực | Không trừ | return refunds (P&L), `refunded` (payment) |
| Hoàn tiền khi huỷ đơn đã trả | Đơn rời NMV (không kept), GMV giữ nguyên | Đơn rời khỏi | — | cancellation refunds |
| Hoàn tiền thanh toán trễ | Đơn không confirmed nên không vào GMV | — | — | late-payment refunds |

## 4. Lớp gold

### 4.1. Gold là gì trong dự án này

Gold **không** có từng view theo thực thể (order, payment, shipment…) như một star schema. Gold gồm:

- **`gold.order_facts`** — view trung gian duy nhất, **một dòng mỗi đơn hàng** (đơn của một shop; một checkout nhiều shop sinh nhiều đơn). Cột: mã đơn, shop, khách, trạng thái, phương thức thanh toán, thời điểm đặt, `subtotal`, voucher shop, voucher sàn, trợ giá ship, `total`, hai cờ **confirmed**/**kept**, và **returned_gross**/**returned_net** (cộng từ các dòng trả hàng thuộc return `REFUNDED`, nối qua dòng đơn để lấy đơn giá). Mọi metric bán hàng (daily_sales, shop_performance, platform_pnl, overview_kpis, product/category sales) lọc qua view này, nên "confirmed" và "kept" chỉ được định nghĩa một lần.
- **18 view mart** — mỗi view là một câu hỏi kinh doanh (mục 6). Chúng đọc trực tiếp `consistent.<table>(v)` của các thực thể cần thiết và/hoặc `order_facts`.
- **`gold.daily_sales_closed`** — **bảng** (không phải view) chứa ngày đã chốt (mục 8).
- **`gold.cdc_freshness`** — sức khoẻ pipeline (độ trễ CDC 15 phút gần nhất), không thuộc nghiệp vụ, không phải mart.

### 4.2. Thực thể nguồn và mart dùng chúng

| Thực thể (consistent.*) | Ý nghĩa nghiệp vụ | Mart dùng |
|---|---|---|
| orders | Đơn hàng của một shop, trạng thái, tiền, các mốc thời gian | order_facts → hầu hết mart bán hàng; order_status_summary; funnel; traffic_by_hour; return_rate_by_cohort; returns_summary |
| order_items | Dòng hàng: SKU, số lượng, đơn giá lúc mua, phần voucher phân bổ, net | order_facts, product_sales, category_sales |
| returns, return_items | Yêu cầu trả hàng (một đơn tối đa một return) và các dòng trả | order_facts, product/category sales, returns_summary, return_rate_by_cohort |
| payments, refunds | Lượt thanh toán (online theo checkout, COD theo đơn) và hoàn tiền kèm lý do | payment_summary, platform_pnl |
| shipments | Kiện hàng chiều đi (FORWARD) và chiều trả (RETURN) | delivery_performance |
| order_status_history | Lịch sử chuyển trạng thái | shop_performance (pack time) |
| inventory, product_skus, products | Tồn kho theo SKU-shop, biến thể, sản phẩm | inventory_risk, product_sales |
| categories | Cây ngành hàng (lá và gốc) | product_sales, category_sales |
| customers | Khách, hạng thành viên, tổng chi tiêu | customer_tiers, overview_kpis |
| shops | Shop, hạng shop, tỉnh, điểm phạt | shop_performance, product_sales, seller_settlement, overview_kpis |
| product_reviews | Đánh giá sao | shop_performance |
| carts, checkouts | Giỏ hàng, phiên thanh toán | funnel |
| vouchers, voucher_usages | Voucher và lượt dùng | voucher_performance |
| campaigns, flash_sale_items | Chiến dịch flash/mega sale và suất giá sốc | flash_sale_performance |
| seller_ledger_entries, seller_payouts | Sổ cái người bán (append-only) và các lần chi trả | seller_settlement, platform_pnl |

### 4.3. Dựng từ `consistent.<table>(v)` như thế nào

- Mỗi view gold nhận tham số watermark `v` và truyền **cùng một `v`** xuống mọi bảng `consistent.*` (và xuống `order_facts`) nó đọc. Kết quả vì vậy là một trạng thái đã thực sự tồn tại trong PostgreSQL tại một thời điểm commit — không bao giờ thấy đơn đã COMPLETED mà chưa có dòng SALE trong sổ cái (chi tiết ở [04](04-warehouse.md)).
- Warehouse-jobs không đọc gold trực tiếp mà đọc **`gold_snap.<view>`**: cùng định nghĩa, được sinh **tự động** từ định nghĩa gold lúc service khởi động, chỉ thay nguồn `consistent.<table>(v)` bằng bảng snapshot `snap.<table>` (đã được đưa tới đúng `v`). Không ai viết tay hai bản, nên không thể lệch logic. Snapshot là gì, cập nhật theo key ra sao: [04](04-warehouse.md).

## 5. Ví dụ số xuyên suốt

Khách **An** đặt một đơn ở **Shop A** (hoa hồng 5%), thanh toán bằng thẻ, ngày 10/03 lúc 23:40 giờ Việt Nam (16:40 UTC).

| Dòng | SKU | SL × đơn giá | line_total | Voucher shop phân bổ | Voucher sàn phân bổ | net_amount |
|---|---|---|---|---|---|---|
| 1 | Áo | 2 × 150.000 | 300.000 | 30.000 | 15.000 | 255.000 |
| 2 | Quần | 1 × 200.000 | 200.000 | 20.000 | 10.000 | 170.000 |
| **Đơn** | | | **subtotal 500.000** | **50.000** | **25.000** | 425.000 |

Phí ship 30.000, voucher freeship trợ 15.000 → `total` = 500.000 − 50.000 − 25.000 + 30.000 − 15.000 = **440.000**. Voucher ở cấp đơn được chia xuống dòng theo tỷ lệ `line_total` (60%/40%), để trả hàng một phần hoàn đúng số đã trả cho từng đơn vị.

**Diễn biến**: trả tiền ngay → giao hàng 12/03 → An trả **1 áo** (lý do CHANGED_MIND), shop nhận và hoàn tiền: 255.000 × 1/2 = **127.500**. Vì chỉ trả một phần, đơn về **COMPLETED** và được quyết toán.

| Mart / số | Giá trị | Giải thích |
|---|---|---|
| daily_sales, ngày **10/03** | orders 1, confirmed 1, buyers 1 | Quy ngày theo giờ VN (xem ghi chú múi giờ bên dưới) |
| placed_gmv | 500.000 | subtotal |
| gmv | 500.000 | confirmed |
| nmv | 500.000 − 1 × 150.000 = **350.000** | trừ giá trị hàng trả, trước voucher |
| completed_revenue | **440.000** | total của đơn COMPLETED, không trừ 127.500 |
| avg_order_value | 500.000 | |
| daily_sales_closed, ngày 10/03 | nmv **500.000**, completed_revenue **0** | Chốt lúc ~01:00 ngày 11/03: chưa có trả hàng, đơn chưa hoàn tất. Số chốt giữ nguyên mãi. |
| product_sales, Áo | units 1, gross 300.000, net 255.000 − 127.500 = **127.500** | |
| product_sales, Quần | units 1, gross 200.000, net 170.000 | |
| platform_pnl | platform_promo_cost 40.000 (25.000 + 15.000), seller_promo_cost 50.000, return_refunds 127.500 | |
| seller_settlement, Shop A | SALE = (300.000 − 30.000) − 135.000 + (200.000 − 20.000) = **315.000**; commission 15.750; payment fee 6.300; balance 292.950 (trước payout) | Người bán hưởng phần hàng giữ lại trừ voucher shop; voucher sàn và freeship do sàn trả. |
| take, take_rate | take 22.050; đóng góp take_rate = 22.050 / 500.000 = 4,41% | Mẫu số là subtotal đơn COMPLETED, không trừ hàng trả. |
| payment_summary, CARD | attempts 1, succeeded 1, collected 440.000, refunded 127.500 | |
| returns_summary, CHANGED_MIND | returns 1, refunded 1, refund_amount 127.500 | |
| return_rate_by_cohort, ngày giao 12/03 | delivered 1, returned 1 | |
| customer_tiers (hạng của An) | lifetime_spent tăng 440.000 − 127.500 = 312.500 | total_spent nguồn tính trên đơn COMPLETED trừ hoàn tiền. |

**Ghi chú múi giờ**: nếu An đặt lúc 00:30 ngày 11/03 giờ VN thì giờ lưu là 17:30 UTC ngày **10/03**. Quy ngày theo UTC sẽ đếm đơn vào ngày 10/03 (sai); quy theo giờ VN đếm vào 11/03 (đúng). Mọi đơn từ 00:00 đến 06:59 giờ VN đều rơi vào trường hợp này.

**Biến thể** để so sánh:

| Tình huống | placed_gmv | gmv | nmv | cancelled | unpaid_cancelled | Hoàn tiền |
|---|---|---|---|---|---|---|
| Online, không trả tiền, hết hạn 15 phút | 500.000 | 0 | 0 | 1 | 1 | — |
| Online, đã trả, shop huỷ vì quá SLA | 500.000 | 500.000 | 0 | 1 | 0 | cancellation 440.000 |
| COD, khách huỷ trước khi giao | 500.000 | 500.000 | 0 | 1 | 0 | không (COD chưa thu) |
| COD, giao thất bại → RETURNED_TO_SELLER | 500.000 | 500.000 | 0 | 0 | 0 | không |
| Trả toàn bộ 3 đơn vị | 500.000 | 500.000 | 0 | 0 | 0 | return 425.000; đơn ở lại RETURN_REFUND, không quyết toán |
| Tiền thẻ về sau khi checkout đã hết hạn | 500.000 | 0 | 0 | 1 | 1 | late_payment = số tiền đã trừ |

## 6. 18 mart

### 6.1. Bảng tóm tắt

Tất cả mart có cùng cơ chế refresh: tính lại toàn bộ từ snapshot tăng dần mỗi ~15 giây, gắn `refresh_id` = watermark (mục 7). "Tích luỹ" = toàn bộ lịch sử, không theo khoảng thời gian.

| # | Mart | Câu hỏi | Grain (một dòng là) | Phạm vi thời gian | Panel Grafana |
|---|---|---|---|---|---|
| 1 | overview_kpis | Toàn sàn đang lớn cỡ nào? | 1 dòng duy nhất | Tích luỹ | 4 ô số: Confirmed GMV, NMV, Orders, Buyers |
| 2 | daily_sales | Mỗi ngày bán được bao nhiêu? | ngày nghiệp vụ | Theo ngày đặt | Daily sales · business dates (cột) |
| 3 | traffic_by_hour | Cao điểm trong ngày, flash sale ra sao? | giờ | 48 giờ gần nhất | Orders by business hour (cột) |
| 4 | order_status_summary | Pipeline đơn đang kẹt ở đâu? | trạng thái đơn | Trạng thái hiện tại | Order pipeline (cột) |
| 5 | funnel | Giỏ → checkout → trả tiền → giao → hoàn tất | bước phễu | Tích luỹ | Purchase funnel (cột) |
| 6 | payment_summary | Phương thức nào thanh toán thành công, thu/hoàn bao nhiêu? | phương thức thanh toán | Tích luỹ | Payment attempts and refunds |
| 7 | category_sales | Ngành hàng gốc nào mang doanh thu? | ngành hàng gốc | Tích luỹ | Category sales |
| 8 | product_sales | Sản phẩm nào bán chạy? | sản phẩm × ngành lá × shop | Tích luỹ | Product sales |
| 9 | shop_performance | Người bán nào tốt/kém? | shop | Tích luỹ | Shop scorecard |
| 10 | inventory_risk | SKU nào sắp hết hàng? | SKU tại một shop | Hiện tại | Inventory risk |
| 11 | customer_tiers | Cơ cấu khách theo hạng thành viên | hạng | Hiện tại | Customer loyalty |
| 12 | platform_pnl | Sàn thu, chi, hoàn bao nhiêu? | 1 dòng duy nhất | Tích luỹ | Platform economics |
| 13 | delivery_performance | Hãng vận chuyển nào tốt? | hãng vận chuyển | Tích luỹ | Carrier performance |
| 14 | returns_summary | Vì sao khách trả hàng? | lý do trả | Tích luỹ | Returns by reason |
| 15 | return_rate_by_cohort | Tỷ lệ trả theo ngày giao | ngày giao (giờ VN) | Theo ngày giao | Return rate by delivery cohort |
| 16 | voucher_performance | Voucher nào được dùng, tốn bao nhiêu? | voucher (code, phạm vi, loại) | Tích luỹ | Voucher performance |
| 17 | flash_sale_performance | Chiến dịch flash sale bán hết bao nhiêu? | chiến dịch | Tích luỹ | Flash-sale performance |
| 18 | seller_settlement | Mỗi shop được bao nhiêu, đã chi bao nhiêu? | shop | Tích luỹ | Seller settlement |

### 6.2. overview_kpis

- **Câu hỏi**: các con số đầu trang của dashboard.
- **Grain**: một dòng cho toàn bộ lịch sử.
- **Measure**: `orders` (mọi đơn đã đặt), `gmv`, `nmv`, `completed_revenue`, `avg_order_value`, `buyers`, `cancelled` (đơn đang CANCELLED), `active_customers` (khách đang active), `shops` (tổng số shop). Định nghĩa đúng như từ điển.
- **Nguồn**: order_facts, customers, shops.
- **Dashboard**: chỉ 4 ô số dùng `gmv`, `nmv`, `orders`, `buyers`; các cột còn lại có trong mart nhưng không lên panel.
- **Lưu ý**: refresh có bị bỏ qua hay không cũng dựa trên mart này — nếu đã có dòng `overview_kpis` ở watermark hiện tại thì lần refresh coi như đã làm (mục 7).

### 6.3. daily_sales

- **Câu hỏi**: doanh số theo ngày nghiệp vụ, số liệu **sống**.
- **Grain**: một dòng mỗi ngày (giờ VN của `created_at`), mới nhất trước.
- **Measure**: `orders`, `confirmed_orders`, `buyers` (khách khác nhau có đơn confirmed trong ngày), `placed_gmv`, `gmv`, `nmv`, `completed_revenue`, `cancelled_orders`, `unpaid_cancelled`, `avg_order_value`.
- **Nguồn**: order_facts.
- **Dashboard**: biểu đồ cột với `placed_gmv`, `gmv`, `nmv` theo ngày.
- **Lưu ý**: số của một ngày cũ vẫn đổi khi có huỷ/trả hàng (NMV giảm, cancelled tăng, completed_revenue tăng dần khi đơn hoàn tất). Muốn "số đã báo cáo" thì đọc `gold.daily_sales_closed` (mục 8) — bảng này không có trên dashboard. Panel sắp ngày tăng dần và giới hạn 200 dòng, nên nếu dữ liệu vượt 200 ngày thì panel chỉ hiện 200 ngày **cũ nhất**.

### 6.4. traffic_by_hour

- **Câu hỏi**: cao điểm trong ngày, hiệu ứng flash sale / 11.11.
- **Grain**: một dòng mỗi giờ, trong 48 giờ trước đơn mới nhất.
- **Measure**: `orders`, `placed_gmv` (dùng placed vì đơn của giờ gần nhất có thể còn trong cửa sổ thanh toán), `orders_with_voucher` (đơn có bất kỳ voucher shop, voucher sàn hoặc trợ giá ship nào).
- **Nguồn**: orders.
- **Dashboard**: cột `orders` theo giờ.
- **Lưu ý**: nhóm giờ trên mốc UTC; vì UTC+7 lệch tròn giờ nên ranh giới giờ trùng với giờ VN, nhưng nhãn giờ hiển thị là giờ UTC.

### 6.5. order_status_summary

- **Câu hỏi**: hiện có bao nhiêu đơn ở mỗi trạng thái (UNPAID, TO_SHIP, READY_TO_SHIP, SHIPPED, DELIVERY_FAILED, DELIVERED, COMPLETED, CANCELLED, RETURNED_TO_SELLER, RETURN_REFUND).
- **Grain**: trạng thái hiện tại.
- **Measure**: `orders`, `amount` = Σ total.
- **Nguồn**: orders. **Dashboard**: Order pipeline.

### 6.6. funnel

- **Câu hỏi**: phễu mua hàng.
- **Grain**: 5 bước: giỏ hàng đã tạo → checkout → checkout đã trả tiền hoặc COD đã xác nhận → đơn đã giao cho vận chuyển (có `shipped_at`) → đơn COMPLETED.
- **Measure**: `n` mỗi bước.
- **Nguồn**: carts, checkouts, orders.
- **Lưu ý**: hai bước cuối đếm **đơn**, ba bước đầu đếm **giỏ/checkout**; một checkout nhiều shop sinh nhiều đơn, nên tỷ lệ giữa bước 3 và 4 không phải tỷ lệ chuyển đổi thuần (panel ghi rõ "counts have different grains").

### 6.7. payment_summary

- **Câu hỏi**: mỗi phương thức (COD, WALLET, CARD, BANK_TRANSFER) có bao nhiêu lượt, thành công bao nhiêu, thu và hoàn bao nhiêu.
- **Grain**: phương thức.
- **Measure**: `attempts`; số lượt `succeeded`, `failed`, `expired`, `cancelled`, `pending`; `success_rate` = thành công / lượt không còn pending; `collected` = Σ số tiền lượt thành công; `refunded` = Σ mọi hoàn tiền (mọi lý do) trên các lượt của phương thức đó.
- **Nguồn**: payments, refunds.
- **Lưu ý**: thanh toán online gắn với **checkout** (một lượt trả cho cả checkout nhiều đơn); COD có đúng một lượt thu mỗi đơn, ở PENDING tới khi giao, thành SUCCEEDED khi giao, CANCELLED khi đơn huỷ/hoàn về.

### 6.8. category_sales

- **Câu hỏi**: ngành hàng gốc nào mang doanh thu.
- **Grain**: ngành hàng **gốc** (nếu ngành lá có cha thì gộp về cha; nếu không thì là chính nó).
- **Measure**: `units_sold`, `gross_revenue`, `net_revenue` — cùng cơ sở với product_sales.
- **Nguồn**: order_items, order_facts (lọc confirmed và kept), return_items/returns (REFUNDED), products, categories.

### 6.9. product_sales

- **Câu hỏi**: sản phẩm nào bán chạy, theo doanh thu thực.
- **Grain**: tên sản phẩm × ngành hàng lá × shop. Sắp theo net revenue giảm dần.
- **Measure**: `units_sold` (đặt − hoàn), `gross_revenue` (Σ line_total), `net_revenue` (Σ net_amount − số tiền hoàn), `orders` (số đơn khác nhau).
- **Nguồn**: order_items, order_facts (confirmed và kept), returns/return_items, products, categories, shops.

### 6.10. shop_performance

- **Câu hỏi**: bảng điểm người bán.
- **Grain**: shop (tên, hạng MALL/PREFERRED/NORMAL, tỉnh).
- **Measure**: `orders` (mọi đơn), `gmv`, `nmv`, `completed`, `cancel_rate` = đơn CANCELLED / mọi đơn (gồm cả đơn hết hạn thanh toán), `sla_breaches` = đơn bị hệ thống huỷ vì shop không đóng gói kịp SLA (2 ngày), `avg_hours_to_pack` (pack time trung bình, giờ, null nếu chưa đóng gói đơn nào), `penalty_points`, `rating` = trung bình sao, `reviews` = số đánh giá.
- **Nguồn**: order_facts, orders, shops, product_reviews, order_status_history.

### 6.11. inventory_risk

- **Câu hỏi**: SKU nào cần nhập thêm.
- **Grain**: một dòng tồn kho (SKU tại shop), sắp theo `available − reorder_point` tăng dần (rủi ro nhất trước).
- **Measure**: `available`, `reserved` (đang giữ cho đơn chưa giao), `reorder_point`, `needs_reorder` = available ≤ reorder point.
- **Nguồn**: inventory, product_skus, products, shops.
- **Lưu ý**: view liệt kê **mọi** SKU kèm cờ, không chỉ SKU dưới ngưỡng (khác với chú thích trong SQL); panel lấy 200 dòng đầu nên thực tế hiển thị nhóm rủi ro nhất.

### 6.12. customer_tiers

- **Câu hỏi**: cơ cấu khách theo hạng BRONZE/SILVER/GOLD/PLATINUM.
- **Grain**: hạng.
- **Measure**: `active_customers`, `total_customers`, `lifetime_spent` = Σ total_spent, `avg_spent`.
- **Nguồn**: customers. `total_spent` và hạng do **nguồn** duy trì (Σ total − hoàn tiền của đơn COMPLETED; ngưỡng 5/20/50 triệu), warehouse không tính lại; check_data kiểm tra công thức ở PostgreSQL.

### 6.13. platform_pnl

- **Câu hỏi**: kinh tế của sàn.
- **Grain**: một dòng, toàn lịch sử.
- **Measure**: `gmv`, `nmv`, `take`, `platform_promo_cost`, `seller_promo_cost`, `return_refunds`, `cancellation_refunds`, `late_payment_refunds`, `paid_to_sellers` (Σ payout), `take_rate`.
- **Nguồn**: order_facts, seller_ledger_entries, refunds, seller_payouts.
- **Lưu ý**: hoàn tiền tách theo **nguyên nhân** vì chỉ hoàn tiền trả hàng mới là chi phí của doanh số đã ghi nhận; hai loại còn lại là trả lại tiền của đơn chưa bao giờ là doanh số (gộp lại sẽ thổi phồng "chi phí").

### 6.14. delivery_performance

- **Câu hỏi**: bảng điểm hãng vận chuyển (GHN, GHTK, J&T, SPX, VTP).
- **Grain**: hãng, chỉ kiện **chiều đi**.
- **Measure**: `parcels`, `delivered`, `returned_to_sender`, `failed_attempts` (Σ số lần giao thất bại), `success_rate`, `avg_hours_in_transit` = trung bình (giao thành công − lấy hàng) theo giờ nghiệp vụ, chỉ kiện đã giao.
- **Nguồn**: shipments.

### 6.15. returns_summary

- **Câu hỏi**: lý do trả hàng (DAMAGED, WRONG_ITEM, NOT_AS_DESCRIBED, MISSING_PARTS, CHANGED_MIND) và kết cục.
- **Grain**: lý do.
- **Measure**: `returns`, `refunded`, `rejected` (return kết thúc CLOSED: shop từ chối mà không khiếu nại kịp, hoặc sàn bác khiếu nại), `open` (mọi trạng thái khác: REQUESTED, APPROVED, REJECTED chờ khiếu nại, DISPUTED, RETURNING, RECEIVED), `refund_amount`, `return_rate` (mục từ điển).
- **Nguồn**: returns, orders.

### 6.16. return_rate_by_cohort

- **Câu hỏi**: tỷ lệ trả của các đơn giao cùng ngày; cohort nào đã đủ "chín" để đọc.
- **Grain**: ngày giao (giờ VN của `delivered_at`).
- **Measure**: `delivered`, `returned` (đơn có return ở **bất kỳ** trạng thái nào, kể cả bị từ chối), `return_rate`, `matured`.
- **Nguồn**: orders, returns.
- **Lưu ý**: một đơn đã giao có thể trả cho tới khi tự hoàn tất 3 ngày nghiệp vụ sau giao; cohort chưa matured sẽ còn tăng, đừng so với cohort đã chín.

### 6.17. voucher_performance

- **Câu hỏi**: voucher nào được dùng, tốn bao nhiêu.
- **Grain**: voucher (code, phạm vi PLATFORM/SHOP, loại FIXED/PERCENT/FREESHIP).
- **Measure**: `usage_limit`, `redemptions` (lượt APPLIED), `discount_cost` = Σ số tiền giảm của lượt APPLIED, `redemption_rate`.
- **Nguồn**: voucher_usages, vouchers.
- **Lưu ý**: lượt dùng của checkout đã trả tiền vẫn APPLIED kể cả khi đơn sau đó bị huỷ, nên discount_cost ở đây là "chi phí đã cam kết", có thể lớn hơn promo cost trong P&L (P&L chỉ tính đơn confirmed và kept).

### 6.18. flash_sale_performance

- **Câu hỏi**: mỗi chiến dịch flash/mega sale bán được bao nhiêu suất.
- **Grain**: chiến dịch (tên, loại, giờ bắt đầu), mới nhất trước.
- **Measure**: `skus` (số suất SKU), `quota`, `sold`, `sell_through`, `revenue_at_flash_price` = Σ đã bán × giá sốc.
- **Nguồn**: flash_sale_items, campaigns. `sold` do nguồn duy trì = đơn vị trên đơn chưa huỷ (check_data kiểm tra).

### 6.19. seller_settlement

- **Câu hỏi**: ví người bán.
- **Grain**: shop (tên, hạng).
- **Measure**: `sales` (Σ SALE), `commission`, `payment_fees`, `paid_out` (đổi dấu thành số dương), `balance` = Σ mọi bút toán = số sàn còn nợ shop (gồm cả phần chưa cộng vào ví).
- **Nguồn**: seller_ledger_entries, shops. Sổ cái chỉ thêm, không sửa, nên tổng luôn đúng theo thời gian.

## 7. Mart được refresh thế nào

Service `warehouse-jobs` chạy vòng lặp 5 giây; cứ mỗi **15 giây** (`MART_REFRESH_S`) nó làm một lượt:

1. **Chốt một watermark `v`** cho cả lượt.
2. **Đưa snapshot tới `v`**: chỉ đọc lại các key bị các transaction mới chạm vào (tăng dần), và rebuild toàn bộ mỗi giờ, lúc khởi động hoặc sau lỗi. Chi tiết: [04](04-warehouse.md).
3. **Bỏ qua** nếu lần refresh gần nhất đã ở đúng `v` và đã có dữ liệu (không có commit mới).
4. Với từng mart trong 18: nếu đã có dòng mang `refresh_id = v` (do một lượt trước hỏng giữa chừng) thì **xoá các dòng đó trước** (đồng bộ), rồi **chèn toàn bộ kết quả** của view `gold_snap` tương ứng, gắn `refresh_id = v`, thời điểm refresh, và số thứ tự dòng `_row` (giữ thứ tự sắp xếp của gold).
5. **Ghi một dòng vào `ops.mart_refreshes` cuối cùng**. Đây là "dấu xuất bản": người đọc chỉ lấy `refresh_id` lớn nhất trong bảng này, nên không bao giờ thấy một lần refresh mới viết được nửa chừng.
6. Chạy **chốt ngày** (mục 8) ngay sau đó, cùng `v`.
7. Mỗi giờ dọn các refresh cũ (giữ bản mới nhất, xoá bản khác đã quá 1 giờ).

"Incremental" nằm ở **snapshot** (chỉ key thay đổi); bản thân mart được tính lại toàn bộ mỗi lần, nhưng tính trên bảng typed nên rẻ (0,3–1 giây cho 18 mart, so với ~4,6 giây khi tính thẳng từ `consistent.*` lúc 35 nghìn đơn). Mart không có TTL nên tồn tại qua restart; chỉ chạy **một** replica warehouse-jobs.

Vì sao vật chất hoá thay vì để Grafana gọi view gold:

- Mỗi người xem, mỗi panel, mỗi 30 giây sẽ tính lại gold → tải nhân theo số người xem; mart biến dashboard thành đọc bảng nhỏ (SLO p95 truy vấn dashboard 500 ms).
- Mọi panel đọc cùng `refresh_id` → nhất quán chéo panel.
- Kết quả đã xuất bản không mất khi ClickHouse bận hoặc job lỗi; dashboard chỉ "cũ đi" và alert `MartRefreshStale` báo.

## 8. Chốt ngày nghiệp vụ

### 8.1. "Chốt" là gì

Chốt một ngày = chép dòng của ngày đó từ `daily_sales` (tại watermark của lượt chạy) vào bảng `gold.daily_sales_closed`, kèm thời điểm chốt, **một lần duy nhất**. Sau đó dòng ấy không bao giờ bị sửa: đó là con số "đã báo cáo" cho ngày đó. `daily_sales` sống vẫn tiếp tục thay đổi bên cạnh.

### 8.2. Điều kiện để một ngày được chốt

Một ngày D được chốt khi **đồng thời**:

1. D đã kết thúc ít nhất **1 giờ nghiệp vụ**: ngày D phải nhỏ hơn ngày (giờ VN) của "thời điểm đặt đơn mới nhất trừ 1 giờ". Nói cách khác, chỉ khi đã có đơn đặt lúc ≥ 01:00 ngày D+1. Một giờ là dư so với cửa sổ thanh toán 15 phút.
2. D **chưa được chốt** trước đó.
3. **Không còn đơn nào của ngày D ở trạng thái UNPAID** trong snapshot. Còn một đơn đang chờ thanh toán thì GMV của ngày còn có thể đổi, nên chờ.

Đi kèm trong cùng lượt refresh (mỗi 15 giây), đọc snapshot tại cùng `v`.

### 8.3. Số nào cố định, số nào không

| Cột | Ở bản chốt | Ở bản sống sau đó |
|---|---|---|
| orders, placed_gmv | Cố định | Không đổi (đơn đã đặt không mất) |
| confirmed_orders, gmv, buyers, AOV | Cố định | Không đổi, nhờ điều kiện 3 (check_data khẳng định placed GMV và GMV không bao giờ lệch) |
| nmv, cancelled_orders | Số tại lúc chốt | Đổi theo huỷ/trả hàng |
| completed_revenue | Gần như 0 (đơn chưa giao xong) | Tăng dần khi đơn hoàn tất (3 ngày sau giao) |

### 8.4. Vì sao cần đóng băng

- Báo cáo ngày đã gửi đi (cho quản lý, cho đối soát) phải tái lập được: hỏi "hôm 10/03 báo GMV bao nhiêu" phải ra đúng con số đã báo, không phải con số đã bị trả hàng làm thay đổi.
- Tách hai câu hỏi: "đã báo cáo gì" (closed) và "thực tế bây giờ là bao nhiêu" (live). Chênh lệch NMV giữa hai bản chính là ảnh hưởng của huỷ/trả hàng phát sinh sau.
- Chốt chạy trong warehouse-jobs (không phải refreshable materialized view) vì cần chốt watermark và truyền nó xuống.

### 8.5. Bài học: 15 ngày bị chốt sai

- **Điều kiện cũ** chỉ có (1) và (2): ngày đã qua 1 giờ nghiệp vụ là chốt.
- **Sự cố**: khi đồng hồ mô phỏng chạy rất nhanh (ví dụ ×1440, 1 giờ nghiệp vụ chỉ còn khoảng 2,5 giây thật), một transaction thanh toán chậm có thể **commit sau** khi đơn đặt sau nó đã xuất hiện và sau khi ngày đã bị chốt. Đơn được đánh dấu đã trả tiền muộn → GMV sống của ngày lớn hơn GMV đã chốt.
- **Phát hiện**: kiểm tra "closed days frozen once; placed GMV and GMV never change afterwards" trong check_data báo lệch ở 15 ngày.
- **Sửa**: thêm điều kiện (3) — không còn đơn UNPAID của ngày đó. Các dòng chốt sai được bỏ và các ngày ấy được chốt lại theo điều kiện mới.
- **Bài học**: điều kiện "đã đủ lâu" theo thời gian là không đủ với hệ thống có transaction dài; phải chốt theo **trạng thái nghiệp vụ** (không còn gì đang dở dang), và phải có kiểm tra tự động khẳng định số đã chốt không đổi.

## 9. Reconciliation PostgreSQL ↔ ClickHouse

**Mục đích**: phát hiện warehouse lệch nguồn (mất sự kiện, slot replication bị tạo lại, lỗi schema…) trong lúc hệ thống đang chạy, không đợi kiểm tra thủ công.

**Cách làm** (mỗi 30 giây, `RECONCILE_S`, cùng watermark với snapshot):

- Kết nối PostgreSQL bằng user **chỉ đọc** `app_reader`, phiên read-only.
- **Cutoff** = thời điểm đặt đơn mới nhất trong warehouse **trừ 1 giờ nghiệp vụ**. Chỉ so các dòng có `created_at` ≤ cutoff — đã commit đủ lâu để cả hai bên chắc chắn phải có.
- Với **28 bảng**: so số dòng; và với bảng có một cột **không bao giờ đổi sau khi chèn**, so thêm tổng cột đó (ví dụ: tổng `total` của orders, `net_amount` của order_items, `amount` của payments/refunds/ledger/payouts, `on_hand_delta` của inventory_movements, `new_price` của price_history, `quantity` của return_items, `rating` của reviews…). Cột thay đổi được (như status) không dùng vì hai bên đọc ở hai thời điểm khác nhau.
- Phía ClickHouse đọc bảng snapshot tại `v` (bằng `consistent.<table>(v)`).
- **Bỏ qua**: bảng không có `created_at` (inventory, shop_wallets) và cart_items (bị xoá cứng liên tục; để check_data so lúc hệ thống nghỉ).

**Dung sai**: số phải **khớp tuyệt đối**; sự "khoan dung" nằm ở chỗ khác:

- Cutoff lùi 1 giờ để loại dữ liệu đang trên đường.
- Một bảng chỉ bị coi là **failing** khi **hai lần liên tiếp** đều lệch: một lần lệch có thể do một dòng bị xoá ở PostgreSQL giữa hai lần đọc; hai lần liên tiếp mới là trôi dữ liệu thật.

**Bảng ops**:

- `ops.reconciliation`: mỗi lần chạy × mỗi bảng một dòng (pg_rows, ch_rows, pg_sum, ch_sum, ok), giữ 7 ngày.
- `ops.reconciliation_status`: kết quả mới nhất mỗi bảng trong 1 ngày qua và cờ `failing`.

**Cảnh báo**:

| Alert | Khi nào |
|---|---|
| WarehouseDataDrift (critical) | Có bảng failing kéo dài 2 phút; job cũng ghi log error tên bảng. |
| ReconciliationStale | Không có lần reconciliation nào thành công trong 3 phút. |
| MartRefreshStale | Mart không được refresh quá 2 phút trong khi CDC vẫn có sự kiện. |
| WarehouseJobsFailing | warehouse-jobs lỗi ≥ 3 lần trong 5 phút. |

Panel tương ứng ở dashboard Platform Health: Reconciliation execution age, Reconciliation failing tables, Mart refresh age/duration, Snapshot run duration, Keys re-read per delta snapshot.

## 10. Data contract và kiểm tra chất lượng

Có hai lớp "hợp đồng":

- **Hợp đồng schema** (`warehouse/clickhouse/check_contract.py`, chạy trong CI): nguồn đổi schema mà warehouse chưa sẵn sàng thì fail. Thuộc [03](03-cdc-ingestion.md)/[04](04-warehouse.md).
- **Hợp đồng dữ liệu** (`quality/check_data.py`): chạy khi simulator đã dừng, đợi pipeline lắng, rồi khẳng định hàng loạt bất biến. Mã thoát 0 = mọi kiểm tra đạt. Các kiểm tra trên PostgreSQL đọc **một snapshot REPEATABLE READ**; các kiểm tra so sánh hai hệ thống thì thử lại tới khi hội tụ (job nền vẫn ghi sau khi simulator dừng).

### 10.1. Các nhóm kiểm tra

| Nhóm | Khẳng định | Vì sao quan trọng cho mart |
|---|---|---|
| money | line_total = SL × đơn giá; net = line − voucher phân bổ; subtotal và voucher của đơn = tổng các dòng; total đúng công thức; checkout = tổng các đơn; checkout PAID có đúng một thanh toán online = grand_total; hoàn tiền huỷ = đúng total; hoàn ≤ thanh toán; COD đúng một lượt thu; total_spent và hạng khách đúng. | Mọi metric tiền dựa vào các cột này. Nếu nguồn sai, gold đúng định nghĩa vẫn ra số sai. |
| state | Trạng thái checkout khớp các đơn; mốc thời gian theo thứ tự; luật thời gian (hết hạn thanh toán, SLA, tự hoàn tất, trả lời return) được áp đúng hạn. | Cờ confirmed/kept và các cửa sổ thời gian dựa vào đó. |
| stock | Tồn kho = tổng sổ kho; mỗi biến động khớp đơn/return gây ra nó. | inventory_risk. |
| logistics | Kiện hàng khớp trạng thái đơn/return; sự kiện tracking bị bỏ qua đúng là cũ. | delivery_performance. |
| promo | Bộ đếm voucher, giới hạn, cửa sổ, phân bổ xuống dòng; flash sold ≤ quota; **giá trên dòng = giá SKU hiệu lực lúc đặt**. | voucher/flash mart; GMV dựa trên đơn giá. |
| returns | Dòng trả ≤ SL đặt; hoàn = net × SL trả ÷ SL đặt; kết cục đơn theo return. | NMV, net revenue, returns mart. |
| settlement | Đơn COMPLETED có SALE/COMMISSION/FEE đúng; ví + pending = sổ cái; payout = bút toán; review chỉ trên đơn đã hoàn tất. | take, seller_settlement, rating. |
| write-path | Mỗi dòng có đúng một domain event; lịch sử trạng thái phát lại qua các bước và actor hợp lệ; không deadlock. | Pack time dựa vào lịch sử trạng thái. |
| pipeline | Silver FINAL = PostgreSQL (31 bảng, đếm và tổng); mọi domain event tới bronze; không lỗi ingest; watermark không giữ lại gì và `consistent.*(v)` = silver; trùng lặp ở bronze chỉ cảnh báo (at-least-once). | Warehouse đúng là bản sao của nguồn. |
| metrics | Xem 10.2. | |
| ops | Xem 10.3. | |

### 10.2. Nhóm metrics: gold = cùng định nghĩa tính trong PostgreSQL

Mỗi metric được viết lại **độc lập** bằng SQL PostgreSQL trên bảng nguồn và so với gold tại watermark hiện tại (thử lại tối đa 60 giây):

| Kiểm tra | So gì |
|---|---|
| daily sales | Theo ngày giờ VN: orders, placed GMV, GMV, NMV, unpaid cancelled. |
| P&L | GMV, NMV, take, promo sàn, promo shop, ba loại hoàn tiền, tổng payout. |
| product sales | Tổng units, gross, net revenue. |
| pack time | Giờ đóng gói trung bình mỗi shop, dung sai 0,011 giờ (gold làm tròn 0,01). |
| return cohorts | delivered và returned theo ngày giao. |
| closed days | Mỗi ngày chốt đúng một lần; placed GMV và GMV ở bản chốt bằng bản sống. |

Ý nghĩa: hai cách cài đặt độc lập của cùng định nghĩa ra cùng số → định nghĩa được cài đúng ở cả hai phía, và pipeline không làm sai lệch.

### 10.3. Nhóm ops

| Kiểm tra | Khẳng định |
|---|---|
| marts = gold | Lấy lần refresh đầu tiên ở hoặc sau watermark hiện tại; với mỗi bảng mart, các dòng mart (bỏ cột kỹ thuật) phải đều có trong gold tính trực tiếp tại cùng `v`. Bảo đảm đường snapshot + gold_snap không làm sai kết quả. |
| snapshot = consistent | Mỗi bảng snapshot (FINAL) có cùng số dòng và cùng tổng hash của mọi cột với `consistent.<table>` tại watermark của snapshot. Bảo đảm cập nhật theo key không bỏ sót/không thừa. |
| reconciliation | Có ít nhất một lần reconciliation sau khi lắng, và lần mới nhất mọi bảng đều ok. |
| PII | Không có email, số điện thoại, địa chỉ dạng rõ trong silver hay bronze. |

Lưu ý trung thực: kiểm tra "marts = gold" là so **một chiều** (dòng có trong mart mà không có trong gold); một dòng gold bị thiếu trong mart sẽ không bị bắt. Muốn chặt hơn thì so cả hai chiều hoặc so thêm số dòng.

### 10.4. Lỗi lịch sử đã xử lý bằng reset

`promo · regular lines priced at the SKU price in force when the order was placed` từng fail với **2 đơn lịch sử: 27011000045946 và 27012200082718**. Hai đơn này mang **giá cũ** của SKU dù được đặt sau thời điểm giá mới có hiệu lực: một lần đổi giá commit **trong lúc** checkout đang chạy (thấy ở ×1440 với 8 simulator). Nguyên nhân đã được sửa ở nguồn (checkout khoá đọc giá SKU dạng chia sẻ, nên đổi giá phải chờ checkout xong), nhưng hai đơn cũ vẫn còn trong dữ liệu. Sau reset trong lượt bàn giao Codex ngày 18/10/2026, smoke đạt 63/63 checks và không còn hai lỗi này. Đây là lỗi dữ liệu nguồn có thật, không phải lỗi của warehouse; mart phản ánh đúng giá đã bán.

## 11. Dashboard theo vai trò nghiệp vụ

Grafana hiện có **năm dashboard nghiệp vụ**, ngoài dashboard Platform Health dùng Prometheus. Các trang nghiệp vụ được sinh từ `business_dashboards.py` qua `generate_dashboards.py`, dùng múi giờ Asia/Ho_Chi_Minh và tự làm mới mỗi phút. Chúng chỉ đọc các mart đã tính sẵn, không chạy lại gold hoặc truy vấn trực tiếp PostgreSQL.

| Dashboard | Câu hỏi vận hành | Mart chính |
|---|---|---|
| Điều hành sàn (`cdc-business`) | GMV/NMV đổi thế nào, mất giá trị ở giai đoạn nào, đội nào cần xử lý? | daily_sales, overview_kpis, traffic_by_hour, order_status_summary |
| Đơn hàng & trải nghiệm (`cdc-operations`) | Backlog ở đâu, vận chuyển và trả hàng có vấn đề gì? | order_status_summary, delivery_performance, returns_summary, return_rate_by_cohort |
| Danh mục & chiến dịch (`cdc-growth`) | Danh mục/sản phẩm nào đóng góp, voucher và flash sale dùng quota ra sao? | category_sales, product_sales, voucher_performance, flash_sale_performance, customer_tiers |
| Nhà bán & tồn kho (`cdc-sellers`) | Shop nào cần hỗ trợ, SKU nào thiếu tồn khả dụng? | shop_performance, inventory_risk |
| Thu phí & dòng tiền (`cdc-finance`) | Thu phí, tài trợ khuyến mại, hoàn tiền và ví seller đang ở trạng thái nào? | platform_pnl, payment_summary, seller_settlement |

Mỗi trang ghim một `refresh_id` đã hoàn tất từ `ops.mart_refreshes`, nên các panel trên trang dùng cùng một bản dữ liệu. Hai trang mở ở hai thời điểm khác nhau có thể chọn hai refresh khác nhau. Ngày nghiệp vụ chạy theo đồng hồ mô phỏng; bộ chọn thời gian wall-clock được ẩn. Chọn 7/14/30 ngày ở trang điều hành chỉ lọc phần so sánh doanh số theo ngày, không tự đổi tất cả bảng lũy kế thành số của cùng kỳ.

So sánh kỳ loại ngày nghiệp vụ mới nhất vì ngày đó có thể chưa kết thúc. Số quá khứ vẫn là giá trị sống, có thể đổi khi hoàn/hủy, không phải báo cáo ngày đã đóng băng. Không cộng daily distinct buyers thành số khách duy nhất của một kỳ; không lấy trung bình tỷ lệ cohort thay cho tổng numerator chia tổng denominator. Kỳ NMV mới cũng có thể chưa hết cửa sổ trả hàng.

Các màu và ngưỡng trên bảng là tín hiệu khám phá, không tự tạo thêm SLA hay chính sách phạt seller. Tồn kho thiếu để vượt reorder point không phải dự báo nhu cầu; voucher cost không phải ROI; giá trị hàng ròng không phải tiền đã thu. Chi tiết từng giới hạn và quy trình đọc nằm trong [hướng dẫn Grafana](../../observability/grafana/README.md).

Control Center giữ hai liên kết đầu vào: nghiệp vụ và nền tảng. Từ trang nghiệp vụ có thể chuyển sang bốn trang theo vai trò còn lại. Kiểm tra tích hợp bằng `python3 observability/grafana/verify_business_dashboards.py`; verifier truy vấn datasource Grafana thật, không ghi dữ liệu nguồn.

## 12. Chỗ README cũ lệch với SQL

Bảng dưới ghi lại các sai lệch của README gộp trước khi tài liệu được tách. README hiện đã là mục lục; dùng các chương mới làm mô tả hiện hành.

| README nói | Code thực tế |
|---|---|
| Business-day close chạy mỗi 5 giây | Chạy ngay sau mỗi lần refresh mart (15 giây); 5 giây chỉ là nhịp vòng lặp. |
| Không nhắc điều kiện "không còn đơn UNPAID" | Có điều kiện này (mục 8.2). |
| Net revenue = net_amount của đơn **kept** trừ hoàn tiền | Đơn **confirmed và kept**. |
| Return rate = return trên đơn đã giao của cohort đã chín (một định nghĩa) | Hai định nghĩa: cohort rate cho **mọi** cohort (kèm cờ matured) và rate theo lý do chỉ trên đơn đã chín; "returned" đếm return **mọi trạng thái** (cả bị từ chối, đang mở), không chỉ REFUNDED. |
| Reconciliation so PostgreSQL với `consistent.<table>(v)` | So với bảng snapshot tại `v` (tương đương theo thiết kế, được check_data kiểm). |
| GMV "final once the 15-minute payment window closes" | Gần đúng; transaction thanh toán commit trễ vẫn có thể đổi GMV, nên ngày chỉ được chốt khi hết đơn UNPAID. |
| Không định nghĩa completed revenue | = Σ total của đơn COMPLETED (gồm phí ship, sau voucher, **không** trừ hoàn tiền trả một phần). |

Ngoài README: chú thích của `inventory_risk` trong SQL nói "SKU ở/dưới ngưỡng" nhưng view trả về mọi SKU kèm cờ `needs_reorder`.
