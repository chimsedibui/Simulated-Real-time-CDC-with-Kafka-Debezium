#!/usr/bin/env python3
"""
generate_data.py — CDC input data generator for the ecommerce schema.

Simulates realistic e-commerce activity in a continuous loop:
  - New customer registrations
  - New orders with line items + inventory reservation
  - Order lifecycle progression: PENDING → PROCESSING → SHIPPED → DELIVERED
  - Order cancellations
  - Inventory restocking
  - Customer tier upgrades on delivery

Usage:
    pip install psycopg2-binary
    python generate_data.py [options]

    python generate_data.py --interval 3 --orders-per-tick 5
    python generate_data.py --host localhost --port 5432 --once
"""

import argparse
import logging
import random
import time

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vietnamese data pools — consistent with existing seed data style
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "An", "Binh", "Chau", "Dung", "Giang", "Hoa", "Khang", "Lan",
    "Minh", "Nga", "Phuc", "Quyen", "Son", "Thu", "Vy", "Bao",
    "Cuong", "Diem", "Hien", "Khanh", "Linh", "Nam", "Nhu", "Phuong",
    "Quoc", "Tam", "Tuan", "Uyen", "Xuan", "Yen", "Hung", "Long",
    "Huy", "Dat", "Trang", "Ngoc", "Mai", "Huong", "Thao", "Hai",
]

LAST_NAMES = [
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Do", "Bui",
    "Dang", "Ngo", "Duong", "Ly", "Mai", "Phan", "Trinh", "Dinh",
]

STREET_NAMES = [
    # TP.HCM
    "Le Loi", "Tran Hung Dao", "Nguyen Hue", "Pham Ngu Lao",
    "Vo Van Tan", "Nam Ky Khoi Nghia", "Cach Mang Thang 8", "Nguyen Thi Minh Khai",
    "Dien Bien Phu", "Le Van Sy", "Nguyen Dinh Chieu", "Vo Thi Sau",
    "Tran Quoc Toan", "Dong Khoi", "Nguyen Cong Tru", "Mac Dinh Chi",
    "Pasteur", "Nguyen Van Cu", "Ba Thang Hai", "Su Van Hanh",
    # Ha Noi
    "Ly Thuong Kiet", "Hung Vuong", "Nguyen Trai", "Le Duan",
    "Hoang Dieu", "Cau Giay", "Xuan Thuy", "Tran Duy Hung",
    "Nguyen Chi Thanh", "Kim Ma", "Giang Vo", "Lang Ha",
    "Doi Can", "Phan Dinh Phung", "Dinh Tien Hoang", "Tran Quang Khai",
    "To Huu", "Ha Huy Tap", "Truong Chinh", "Nguyen Khuyen",
    # Da Nang / other cities
    "Bach Dang", "Hai Ba Trung", "Nguyen Van Linh", "Tran Phu",
    "Phan Chau Trinh", "Le Hong Phong", "Nguyen Tat Thanh", "Quang Trung",
    "Hung Vuong", "Ly Tu Trong",
]

DISTRICTS = [
    # TP.HCM
    "Quan 1, TP.HCM", "Quan 3, TP.HCM", "Quan 4, TP.HCM",
    "Quan 5, TP.HCM", "Quan 6, TP.HCM", "Quan 7, TP.HCM",
    "Quan 8, TP.HCM", "Quan 10, TP.HCM", "Quan 12, TP.HCM",
    "Binh Thanh, TP.HCM", "Go Vap, TP.HCM", "Tan Binh, TP.HCM",
    "Tan Phu, TP.HCM", "Binh Tan, TP.HCM", "Thu Duc, TP.HCM",
    "Nha Be, TP.HCM", "Binh Chanh, TP.HCM",
    # Ha Noi
    "Hoan Kiem, Ha Noi", "Hai Ba Trung, Ha Noi", "Dong Da, Ha Noi",
    "Ba Dinh, Ha Noi", "Tay Ho, Ha Noi", "Cau Giay, Ha Noi",
    "Thanh Xuan, Ha Noi", "Long Bien, Ha Noi", "Hoang Mai, Ha Noi",
    "Ha Dong, Ha Noi", "Nam Tu Liem, Ha Noi", "Bac Tu Liem, Ha Noi",
    # Da Nang
    "Hai Chau, Da Nang", "Thanh Khe, Da Nang", "Son Tra, Da Nang",
    "Ngu Hanh Son, Da Nang", "Cam Le, Da Nang", "Lien Chieu, Da Nang",
    # Other cities
    "Ninh Kieu, Can Tho", "Binh Thuy, Can Tho",
    "Bien Hoa, Dong Nai", "Thu Dau Mot, Binh Duong",
    "Nha Trang, Khanh Hoa", "Hue, Thua Thien Hue",
    "Vung Tau, Ba Ria - Vung Tau", "Da Lat, Lam Dong",
]

ORDER_NOTES = [
    "Giao hang nhanh giup toi",
    "De ngoai cua neu khong co nha",
    "Goi dien truoc khi giao",
    "Hang de vo, can dong goi can than",
    "Thanh toan khi nhan hang",
    "Giao buoi sang truoc 10 gio",
    "Vui long giao buoi chieu sau 13 gio",
    "Boc hang can than",
    "Khong giao qua buu dien, giao truc tiep",
    "Nhan hang tai tang 1, toa nha A",
    "Giao vao buoi toi sau 18 gio",
    "Kiem tra hang truoc khi nhan",
    "Dong goi them lop bao ve, hang dien tu",
    "Giao len tang 5, khong co thang may",
    "Vui long nhan ho neu khong co ai o nha",
    "Lien he truoc 30 phut truoc khi giao",
    "De o an ninh cua toa nha neu vang mat",
    "Hang mua lam qua, xin vui long goi dep",
    "Giao truoc 12 gio trua cang tot",
    "Chi nhan hang vao thu 2 va thu 4",
    "Khong giao vao cuoi tuan",
    "Can hoa don VAT kem theo",
    "Kiem tra so luong khi giao",
    "Goi tui kin, tranh am uot",
    "Nhan hang tai le tan nha hang xom so 12",
    "Giao trong gio hanh chinh (8-17h)",
    "Xin vui long bam chuong truoc khi giao",
    "Dat hang truoc cua, khong can ky nhan",
    "Giao hang thu tu tuan sau",
    "Vui long xac nhan truoc khi giao",
    "Nhan hang tai bao ve tang tret",
    "Luu y: dia chi moi, GPS co the sai",
    "Goi cho so dien thoai phu neu khong lien lac duoc",
    "Can bien lai giao hang",
    "Khong giao cho nguoi khac, chi nhan truc tiep",
    "Tra hang neu bi loi, cam on",
    "Giao hang vao sang som (truoc 8h) neu co the",
    "Dat qua cua hang xom neu vang nha",
    "Hang can bao quan lanh, vui long uu tien",
    "Thanh toan bang chuyen khoan, khong nhan tien mat",
    "Can hop le cung hang",
    "Khong dong goi chung voi san pham khac",
    "Giao hang vao ngay 15 hoac cuoi thang",
    "Hang tet, giao truoc 27 thang chap",
    "Dia chi trong hem hep, xe may di duoc",
    "Goi cho so 0909 xxx xxx neu khong gap chu nha",
    "Can chu ky nguoi nhan, khong de truoc cua",
    "Hang cho khach hang khac, vui long xac nhan ten",
    "Giao tang 8 toa B, lau 8",
    "Bam so phong 801 tren bo chuong",
]


PHONE_PREFIXES = [
    "032", "033", "034", "035", "036", "037", "038", "039",
    "086", "096", "097", "098",
    "070", "076", "077", "078", "079", "089", "090", "093",
    "081", "082", "083", "084", "085", "088", "091", "094",
    "052", "056", "058", "092",
    "055", "059", "099",
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "mail.com", "outlook.com",
    "hotmail.com", "icloud.com", "proton.me", "live.com",
    "me.com", "vnn.vn",
]

TIER_THRESHOLDS = [
    (10000, "PLATINUM"),
    (5000,  "GOLD"),
    (1000,  "SILVER"),
    (0,     "BRONZE"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_phone() -> str:
    return random.choice(PHONE_PREFIXES) + str(random.randint(1_000_000, 9_999_999))


def make_address() -> str:
    return f"{random.randint(1, 999)} {random.choice(STREET_NAMES)}, {random.choice(DISTRICTS)}"


def calc_tier(total_spent: float) -> str:
    for threshold, tier in TIER_THRESHOLDS:
        if total_spent >= threshold:
            return tier
    return "BRONZE"


def get_conn(cfg: dict):
    return psycopg2.connect(**cfg)


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------

def op_insert_customer(cur) -> bool:
    """Register a new customer. Uses a savepoint to skip on email collision."""
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    suffix = random.randint(10, 99999)
    domain = random.choice(EMAIL_DOMAINS)
    email   = f"{fn.lower()}.{ln.lower()}{suffix}@{domain}"[:30]
    phone   = make_phone()
    address = make_address()[:255]

    try:
        cur.execute("SAVEPOINT sp_customer")
        cur.execute(
            """INSERT INTO ecommerce.customers
               (firstname, lastname, email, phone, address, tier, total_spent, is_active)
               VALUES (%s, %s, %s, %s, %s, 'BRONZE', 0, TRUE)""",
            (fn[:20], ln[:20], email, phone, address),
        )
        log.info(f"[INSERT] customer: {fn} {ln} <{email}>")
        return True
    except psycopg2.errors.UniqueViolation:
        cur.execute("ROLLBACK TO SAVEPOINT sp_customer")
        log.debug(f"Email collision for {email}, skipped")
        return False


def op_create_order(cur, customer_ids: list, product_prices: dict) -> bool:
    """Place a new order with 1-5 random products, reserve inventory."""
    if not customer_ids or not product_prices:
        return False

    cust_id    = str(random.choice(customer_ids))
    num_items  = random.randint(1, min(5, len(product_prices)))
    products   = random.sample(list(product_prices.keys()), num_items)

    lines      = []
    subtotal   = 0.0
    for pid in products:
        qty        = random.randint(1, 3)
        unit_price = product_prices[pid]
        line_total = round(qty * unit_price, 2)
        subtotal  += line_total
        lines.append((pid, qty, unit_price, line_total))

    tax_amount      = round(subtotal * 0.10, 2)
    shipping_amount = round(random.uniform(15.0, 50.0), 2)
    total_amount    = round(subtotal + tax_amount + shipping_amount, 2)

    cur.execute("SELECT address FROM ecommerce.customers WHERE id = %s", (cust_id,))
    row          = cur.fetchone()
    ship_address = (row[0] if row else make_address())[:70]
    notes        = random.choice(ORDER_NOTES) if random.random() < 0.25 else None

    # order_number is auto-generated by trigger when empty string
    cur.execute(
        """INSERT INTO ecommerce.orders
           (order_number, customer_id, total_amount, tax_amount, shipping_amount,
            status, shipping_address, notes)
           VALUES ('', %s, %s, %s, %s, 'PENDING', %s, %s)
           RETURNING id""",
        (cust_id, total_amount, tax_amount, shipping_amount, ship_address, notes),
    )
    order_id = str(cur.fetchone()[0])

    for pid, qty, unit_price, line_total in lines:
        cur.execute(
            """INSERT INTO ecommerce.order_items
               (order_id, product_id, quantity, unit_price, total_price)
               VALUES (%s, %s, %s, %s, %s)""",
            (order_id, str(pid), qty, unit_price, line_total),
        )
        # Reserve stock (skip silently if not enough available)
        cur.execute(
            """UPDATE ecommerce.inventory
               SET reserved_quantity  = reserved_quantity  + %s,
                   available_quantity = available_quantity - %s
               WHERE product_id = %s
                 AND available_quantity >= %s""",
            (qty, qty, str(pid), qty),
        )

    log.info(
        f"[INSERT] order {order_id[:8]}… "
        f"${total_amount:.2f}  {len(lines)} item(s)  customer={cust_id[:8]}…"
    )
    return True


def op_advance_orders(cur):
    """Move a random subset of orders to the next status in the lifecycle."""
    transitions = [
        ("PENDING",    "PROCESSING"),
        ("PROCESSING", "SHIPPED"),
        ("SHIPPED",    "DELIVERED"),
    ]
    for from_s, to_s in transitions:
        cur.execute(
            "SELECT id FROM ecommerce.orders WHERE status = %s ORDER BY RANDOM() LIMIT 4",
            (from_s,),
        )
        for (oid,) in cur.fetchall():
            if random.random() < 0.55:
                cur.execute(
                    "UPDATE ecommerce.orders SET status = %s WHERE id = %s",
                    (to_s, str(oid)),
                )
                log.info(f"[UPDATE] order {str(oid)[:8]}…  {from_s} → {to_s}")

                if to_s == "DELIVERED":
                    _credit_customer(cur, str(oid))


def _credit_customer(cur, order_id: str):
    """Update customer total_spent and tier after order delivery."""
    cur.execute(
        "SELECT total_amount, customer_id FROM ecommerce.orders WHERE id = %s",
        (order_id,),
    )
    row = cur.fetchone()
    if not row:
        return
    amount, cust_id = float(row[0]), str(row[1])

    cur.execute(
        "SELECT total_spent FROM ecommerce.customers WHERE id = %s",
        (cust_id,),
    )
    row = cur.fetchone()
    if not row:
        return

    new_spent = float(row[0]) + amount
    new_tier  = calc_tier(new_spent)
    cur.execute(
        "UPDATE ecommerce.customers SET total_spent = %s, tier = %s WHERE id = %s",
        (new_spent, new_tier, cust_id),
    )
    log.info(
        f"[UPDATE] customer {cust_id[:8]}…  "
        f"total_spent=${new_spent:.2f}  tier={new_tier}"
    )


def op_cancel_order(cur):
    """Cancel a random PENDING order."""
    cur.execute(
        """UPDATE ecommerce.orders SET status = 'CANCELLED'
           WHERE id = (
               SELECT id FROM ecommerce.orders WHERE status = 'PENDING'
               ORDER BY RANDOM() LIMIT 1
           ) RETURNING id""",
    )
    row = cur.fetchone()
    if row:
        log.info(f"[UPDATE] order {str(row[0])[:8]}…  PENDING → CANCELLED")


def op_restock_inventory(cur):
    """Restock the product with the lowest available quantity."""
    qty = random.randint(30, 100)
    cur.execute(
        """UPDATE ecommerce.inventory
           SET quantity            = quantity + %s,
               available_quantity  = available_quantity + %s,
               last_restocked_at   = NOW()
           WHERE product_id = (
               SELECT product_id FROM ecommerce.inventory
               ORDER BY available_quantity ASC
               LIMIT 1
           )""",
        (qty, qty),
    )
    log.info(f"[UPDATE] inventory  restocked lowest-stock product  +{qty} units")


def op_deactivate_customer(cur):
    """Deactivate a random BRONZE customer who has never spent anything."""
    cur.execute(
        """UPDATE ecommerce.customers
           SET is_active = FALSE
           WHERE id = (
               SELECT id FROM ecommerce.customers
               WHERE tier = 'BRONZE' AND total_spent = 0 AND is_active = TRUE
               ORDER BY RANDOM() LIMIT 1
           ) RETURNING id""",
    )
    row = cur.fetchone()
    if row:
        log.info(f"[UPDATE] customer {str(row[0])[:8]}…  deactivated (inactive BRONZE)")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_tick(cur, customer_ids: list, product_prices: dict, cfg: dict):
    """Execute one tick of operations."""

    # New customers (40% chance)
    if random.random() < 0.40:
        for _ in range(cfg["new_customers_per_tick"]):
            op_insert_customer(cur)

    # New orders
    for _ in range(cfg["orders_per_tick"]):
        op_create_order(cur, customer_ids, product_prices)

    # Advance order lifecycle
    op_advance_orders(cur)

    # Cancel a pending order (5% chance)
    if random.random() < 0.05:
        op_cancel_order(cur)

    # Restock inventory (20% chance)
    if random.random() < 0.20:
        op_restock_inventory(cur)

    # Deactivate dormant customer (5% chance)
    if random.random() < 0.05:
        op_deactivate_customer(cur)


def run(db_cfg: dict, interval: float, orders_per_tick: int,
        new_customers_per_tick: int, once: bool):

    tick_cfg = {
        "orders_per_tick":        orders_per_tick,
        "new_customers_per_tick": new_customers_per_tick,
    }

    log.info(
        "Generator started — "
        f"interval={interval}s  orders_per_tick={orders_per_tick}  "
        f"new_customers_per_tick={new_customers_per_tick}  once={once}"
    )

    tick = 0
    while True:
        tick += 1
        log.info(f"──── Tick {tick} ────")

        try:
            conn = get_conn(db_cfg)
            cur  = conn.cursor()

            cur.execute(
                "SELECT id FROM ecommerce.customers WHERE is_active = TRUE"
            )
            customer_ids = [row[0] for row in cur.fetchall()]

            cur.execute(
                "SELECT id, price FROM ecommerce.products WHERE is_available = TRUE"
            )
            product_prices = {row[0]: float(row[1]) for row in cur.fetchall()}

            # Bootstrap: ensure we have active customers
            if not customer_ids:
                log.warning("No active customers — bootstrapping 5 records")
                for _ in range(5):
                    op_insert_customer(cur)
                conn.commit()
                cur.execute(
                    "SELECT id FROM ecommerce.customers WHERE is_active = TRUE"
                )
                customer_ids = [row[0] for row in cur.fetchall()]

            run_tick(cur, customer_ids, product_prices, tick_cfg)

            conn.commit()
            cur.close()
            conn.close()

        except Exception as exc:
            log.exception(f"Tick {tick} failed: {exc}")
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass

        if once:
            log.info("--once flag set, exiting after first tick")
            break

        time.sleep(interval)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Generate continuous CDC input data for the ecommerce schema",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--interval",            type=float, default=5.0,
                   help="Seconds between ticks")
    p.add_argument("--orders-per-tick",     type=int,   default=2,
                   help="Orders created per tick")
    p.add_argument("--new-customers",       type=int,   default=1,
                   help="New customers per tick when triggered")
    p.add_argument("--host",                default="localhost")
    p.add_argument("--port",                type=int,   default=5432)
    p.add_argument("--user",                default="admin")
    p.add_argument("--password",            default="admin")
    p.add_argument("--dbname",              default="enterprise_db")
    p.add_argument("--once",                action="store_true",
                   help="Run a single tick then exit (useful for testing)")
    args = p.parse_args()

    db_cfg = {
        "host":     args.host,
        "port":     args.port,
        "user":     args.user,
        "password": args.password,
        "dbname":   args.dbname,
    }

    run(
        db_cfg=db_cfg,
        interval=args.interval,
        orders_per_tick=args.orders_per_tick,
        new_customers_per_tick=args.new_customers,
        once=args.once,
    )


if __name__ == "__main__":
    main()
