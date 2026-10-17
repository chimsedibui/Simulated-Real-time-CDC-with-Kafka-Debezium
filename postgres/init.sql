-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- SCHEMA ecommerce
CREATE SCHEMA IF NOT EXISTS ecommerce;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_user') THEN
        CREATE ROLE cdc_user WITH LOGIN PASSWORD 'cdc_password' REPLICATION;
    END IF;
END
$$;

-- TABLE Customers
CREATE TABLE IF NOT EXISTS ecommerce.customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    firstname VARCHAR(20) NOT NULL,
    lastname VARCHAR(20) NOT NULL,
    email VARCHAR(30) NOT NULL UNIQUE,
    phone VARCHAR(12) NOT NULL,
    address VARCHAR(255) NOT NULL,
    tier VARCHAR(20) DEFAULT 'BRONZE' CHECK (tier IN ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM')),
    total_spent DECIMAL(10, 2) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_customers_email ON ecommerce.customers (email);
CREATE INDEX idx_customers_tier ON ecommerce.customers (tier);

INSERT INTO ecommerce.customers (firstname, lastname, email, phone, address, tier, total_spent, is_active) VALUES
('An', 'Nguyen', 'an.nguyen@mail.com', '0901234567', '123 Le Loi, Quan 1, TP.HCM', 'GOLD', 4520.00, TRUE),
('Binh', 'Tran', 'binh.tran@mail.com', '0912345678', '45 Tran Hung Dao, Hoan Kiem, Ha Noi', 'SILVER', 1890.50, TRUE),
('Chau', 'Le', 'chau.le@mail.com', '0923456789', '78 Nguyen Hue, Quan 1, TP.HCM', 'PLATINUM', 8750.00, TRUE),
('Dung', 'Pham', 'dung.pham@mail.com', '0934567890', '12 Bach Dang, Hai Chau, Da Nang', 'BRONZE', 320.00, TRUE),
('Giang', 'Hoang', 'giang.hoang@mail.com', '0945678901', '90 Cau Giay, Cau Giay, Ha Noi', 'SILVER', 2100.00, TRUE),
('Hoa', 'Vu', 'hoa.vu@mail.com', '0956789012', '34 Vo Van Tan, Quan 3, TP.HCM', 'GOLD', 5680.00, TRUE),
('Khang', 'Do', 'khang.do@mail.com', '0967890123', '56 Ly Thuong Kiet, Hai Ba Trung, Ha Noi', 'BRONZE', 150.00, FALSE),
('Lan', 'Bui', 'lan.bui@mail.com', '0978901234', '23 Tran Phu, Ninh Kieu, Can Tho', 'PLATINUM', 9200.00, TRUE),
('Minh', 'Dang', 'minh.dang@mail.com', '0989012345', '67 Hung Vuong, Hai Chau, Da Nang', 'SILVER', 1450.00, TRUE),
('Nga', 'Ngo', 'nga.ngo@mail.com', '0990123456', '89 Pham Ngu Lao, Quan 1, TP.HCM', 'GOLD', 3970.00, TRUE),
('Phuc', 'Duong', 'phuc.duong@mail.com', '0901112233', '101 Nguyen Trai, Thanh Xuan, Ha Noi', 'BRONZE', 0.00, TRUE),
('Quyen', 'Ly', 'quyen.ly@mail.com', '0902223344', '202 Dien Bien Phu, Binh Thanh, TP.HCM', 'SILVER', 2340.00, FALSE),
('Son', 'Mai', 'son.mai@mail.com', '0903334455', '303 Le Duan, Thanh Khe, Da Nang', 'GOLD', 6120.00, TRUE),
('Thu', 'Phan', 'thu.phan@mail.com', '0904445566', '404 Hoang Dieu, Hai Chau, Da Nang', 'PLATINUM', 11500.00, TRUE),
('Vy', 'Trinh', 'vy.trinh@mail.com', '0905556677', '505 Nguyen Van Cu, Long Bien, Ha Noi', 'BRONZE', 480.00, TRUE);


-- TABLE Products
CREATE TABLE IF NOT EXISTS ecommerce.products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    description TEXT NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    weight NUMERIC(5, 2),
    is_available BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_products_sku ON ecommerce.products (sku);
CREATE INDEX idx_products_category ON ecommerce.products (category);


INSERT INTO ecommerce.products (sku, name, description, category, price, weight) VALUES
('CPU-AMD-7800X3D', 'AMD Ryzen 7 7800X3D', 'CPU 8 nhân 16 luồng, 3D V-Cache, tối ưu cho gaming, socket AM5', 'CPU', 449.00, 0.08),
('CPU-INT-14700K', 'Intel Core i7-14700K', 'CPU 20 nhân (8P+12E), socket LGA1700, hỗ trợ DDR5', 'CPU', 409.00, 0.09),
('CPU-AMD-9950X', 'AMD Ryzen 9 9950X', 'CPU 16 nhân 32 luồng Zen 5, socket AM5, hiệu năng đa nhân mạnh', 'CPU', 649.00, 0.08),
('CPU-INT-14600KF', 'Intel Core i5-14600KF', 'CPU 14 nhân (6P+8E), không iGPU, hợp dựng máy gaming tầm trung', 'CPU', 269.00, 0.09),
('GPU-NV-4070S', 'NVIDIA GeForce RTX 4070 SUPER', 'Card đồ họa 12GB GDDR6X, chơi game 1440p mượt, DLSS 3', 'GPU', 599.00, 1.05),
('GPU-NV-4090', 'NVIDIA GeForce RTX 4090', 'Card đồ họa flagship 24GB GDDR6X, 4K cao nhất, AI/render', 'GPU', 1799.00, 2.20),
('GPU-AMD-7900XTX', 'AMD Radeon RX 7900 XTX', 'Card đồ họa 24GB GDDR6, hiệu năng 4K, raster mạnh', 'GPU', 949.00, 1.65),
('GPU-NV-4060', 'NVIDIA GeForce RTX 4060', 'Card đồ họa 8GB tầm phổ thông, gaming 1080p tiết kiệm điện', 'GPU', 299.00, 0.75),
('MB-ASUS-B650', 'ASUS TUF Gaming B650-PLUS WIFI', 'Bo mạch chủ socket AM5, DDR5, PCIe 5.0, WiFi 6', 'Mainboard', 199.00, 1.10),
('MB-MSI-Z790', 'MSI MAG Z790 Tomahawk WIFI', 'Bo mạch chủ LGA1700, DDR5, ép xung, WiFi 6E', 'Mainboard', 259.00, 1.25),
('MB-GB-X670E', 'Gigabyte X670E AORUS Master', 'Bo mạch chủ cao cấp AM5, PCIe 5.0, VRM mạnh cho OC', 'Mainboard', 389.00, 1.40),
('RAM-COR-32-6000', 'Corsair Vengeance 32GB DDR5-6000', 'Kit RAM 2x16GB DDR5 6000MHz CL30, tản nhiệt nhôm', 'RAM', 109.00, 0.18),
('RAM-GS-32-6400', 'G.Skill Trident Z5 32GB DDR5-6400', 'Kit RAM 2x16GB DDR5 6400MHz CL32, RGB', 'RAM', 134.00, 0.20),
('RAM-KING-16-3600', 'Kingston Fury Beast 16GB DDR4-3600', 'Kit RAM 2x8GB DDR4 3600MHz, tương thích rộng', 'RAM', 49.00, 0.14),
('SSD-SAM-990-2T', 'Samsung 990 PRO 2TB', 'SSD NVMe PCIe 4.0, đọc 7450MB/s, bền bỉ cho gaming/làm việc', 'Storage', 169.00, 0.01),
('SSD-WD-SN850-1T', 'WD Black SN850X 1TB', 'SSD NVMe PCIe 4.0 1TB, tốc độ cao, tản nhiệt tốt', 'Storage', 99.00, 0.01),
('SSD-CRU-P3-2T', 'Crucial P3 Plus 2TB', 'SSD NVMe PCIe 4.0 2TB tầm trung, giá tốt cho dung lượng lớn', 'Storage', 119.00, 0.01),
('HDD-SEA-4T', 'Seagate BarraCuda 4TB', 'Ổ cứng HDD 4TB 5400RPM, lưu trữ dữ liệu giá rẻ', 'Storage', 79.00, 0.49),
('PSU-COR-RM850', 'Corsair RM850e 850W', 'Nguồn 80 Plus Gold, full modular, ATX 3.0', 'PSU', 129.00, 1.80),
('PSU-SS-750', 'Seasonic Focus GX-750 750W', 'Nguồn 80 Plus Gold modular, bền bỉ, chạy êm', 'PSU', 109.00, 1.70),
('PSU-MSI-1000', 'MSI MPG A1000G 1000W', 'Nguồn 1000W 80 Plus Gold, ATX 3.0, hỗ trợ GPU cao cấp', 'PSU', 169.00, 2.10),
('CASE-NZXT-H7', 'NZXT H7 Flow', 'Vỏ case mid-tower thoáng khí, mặt lưới, dễ đi dây', 'Case', 129.00, 8.50),
('CASE-LL-O11', 'Lian Li O11 Dynamic EVO', 'Vỏ case kính cường lực, hỗ trợ tản nước, build đẹp', 'Case', 169.00, 12.00),
('CASE-COR-4000D', 'Corsair 4000D Airflow', 'Vỏ case mid-tower phổ biến, luồng khí tốt, giá hợp lý', 'Case', 94.00, 7.80),
('COOL-NOC-D15', 'Noctua NH-D15', 'Tản nhiệt khí song tháp, hiệu năng top, chạy cực êm', 'Cooler', 109.00, 1.32),
('COOL-AIO-280', 'Arctic Liquid Freezer III 280', 'Tản nhiệt nước AIO 280mm, hiệu năng cao, giá tốt', 'Cooler', 99.00, 1.55),
('MON-LG-27GP', 'LG 27GP850-B 27"', 'Màn hình 1440p 165Hz Nano IPS, 1ms, gaming sắc nét', 'Monitor', 349.00, 6.20),
('MON-DEL-4K', 'Dell UltraSharp U2723QE 27"', 'Màn hình 4K IPS Black, chuẩn màu cho công việc sáng tạo', 'Monitor', 569.00, 6.80),
('KB-KEY-Q1', 'Keychron Q1 Pro', 'Bàn phím cơ custom 75%, hotswap, kết nối không dây', 'Peripheral', 199.00, 1.60),
('MOU-LOG-G502', 'Logitech G502 X Lightspeed', 'Chuột gaming không dây, cảm biến HERO 25K, nhẹ', 'Peripheral', 139.00, 0.10);


-- TABLE Orders
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_number VARCHAR(20) NOT NULL UNIQUE,
    customer_id UUID NOT NULL REFERENCES ecommerce.customers(id),
    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount > 0),
    tax_amount DECIMAL(10, 2) NOT NULL CHECK (tax_amount > 0),
    shipping_amount DECIMAL(10, 2) NOT NULL CHECK (shipping_amount > 0),
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    shipping_address VARCHAR(70) NOT NULL,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_orders_order_number ON ecommerce.orders (order_number);
CREATE INDEX idx_orders_customer_id ON ecommerce.orders (customer_id);
CREATE INDEX idx_orders_status ON ecommerce.orders (status);
CREATE INDEX idx_orders_created_at ON ecommerce.orders (created_at DESC);


-- TABLE Order Items
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES ecommerce.orders(id),
    product_id UUID NOT NULL REFERENCES ecommerce.products(id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price > 0),
    total_price DECIMAL(10, 2) NOT NULL CHECK (total_price > 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_order_items_order_id ON ecommerce.order_items (order_id);
CREATE INDEX idx_order_items_product_id ON ecommerce.order_items (product_id);


-- TABLE inventory
CREATE TABLE IF NOT EXISTS ecommerce.inventory (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id UUID NOT NULL REFERENCES ecommerce.products(id),
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    reserved_quantity INTEGER NOT NULL CHECK (reserved_quantity >= 0),
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    reorder_point INTEGER NOT NULL CHECK (reorder_point >= 0),
    reorder_quantity INTEGER NOT NULL CHECK (reorder_quantity >= 0),
    warehouse_location VARCHAR(50),
    last_restocked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_inventory_product_id ON ecommerce.inventory (product_id);

INSERT INTO ecommerce.inventory (product_id, quantity, reserved_quantity, available_quantity, warehouse_location, reorder_point, reorder_quantity)
WITH stock AS (
    SELECT
        id,
        (RANDOM() * 200 + 50)::INTEGER AS quantity,
        (RANDOM() * 10)::INTEGER AS reserved_quantity
    FROM ecommerce.products
)
SELECT
    id,
    quantity,
    reserved_quantity,
    quantity - reserved_quantity AS available_quantity,
    CASE FLOOR(RANDOM() * 3)::INTEGER
        WHEN 0 THEN 'WAREHOUSE-A'
        WHEN 1 THEN 'WAREHOUSE-B'
        ELSE 'WAREHOUSE-C'
    END AS warehouse_location,
    (RANDOM() * 20 + 10)::INTEGER AS reorder_point,
    (RANDOM() * 50 + 20)::INTEGER AS reorder_quantity
FROM stock;


-- TABLE audit_logs
CREATE TABLE IF NOT EXISTS ecommerce.audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(50) NOT NULL,
    record_id UUID NOT NULL,
    action VARCHAR(10) NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_value JSONB,
    new_value JSONB,
    change_by VARCHAR(50) DEFAULT CURRENT_USER,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_audit_logs_table_name ON ecommerce.audit_logs (table_name);
CREATE INDEX idx_audit_logs_record_id ON ecommerce.audit_logs (record_id);
CREATE INDEX idx_audit_logs_updated_at ON ecommerce.audit_logs (updated_at DESC);

-- FUNCTION update_timestamp
CREATE OR REPLACE FUNCTION ecommerce.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER FOR UPDATING updated_at timestamp
CREATE TRIGGER trg_update_customers_timestamp
    BEFORE UPDATE ON ecommerce.customers
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

CREATE TRIGGER trg_update_products_timestamp
    BEFORE UPDATE ON ecommerce.products
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

CREATE TRIGGER trg_update_orders_timestamp
    BEFORE UPDATE ON ecommerce.orders
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

CREATE TRIGGER trg_update_order_items_timestamp
    BEFORE UPDATE ON ecommerce.order_items
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

CREATE TRIGGER trg_update_inventory_timestamp
    BEFORE UPDATE ON ecommerce.inventory
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

-- FUNCTION: GENERATE ORDER NUMBER
CREATE SEQUENCE IF NOT EXISTS ecommerce.order_number_seq START WITH 1;

CREATE OR REPLACE FUNCTION ecommerce.generate_order_number()
RETURNS TRIGGER AS $$
BEGIN
    NEW.order_number := CONCAT('ORD-', EXTRACT(YEAR FROM CURRENT_DATE), '-', LPAD(NEXTVAL('ecommerce.order_number_seq')::TEXT, 6, '0'));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGER FOR GENERATING ORDER NUMBER BEFORE INSERTING INTO orders table
CREATE TRIGGER trg_generate_order_number
    BEFORE INSERT ON ecommerce.orders
    FOR EACH ROW 
        WHEN (NEW.order_number IS NULL OR NEW.order_number = '')
        EXECUTE FUNCTION ecommerce.generate_order_number();


-- CDC HEART BEAT TABLE
CREATE TABLE IF NOT EXISTS ecommerce.cdc_heartbeat (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_cdc_heartbeat_ts ON ecommerce.cdc_heartbeat (ts DESC);
INSERT INTO ecommerce.cdc_heartbeat (id, ts) VALUES (1, NOW())
ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts;


CREATE TABLE IF NOT EXISTS ecommerce.debezium_signals (
    id VARCHAR(64) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    data TEXT
);

CREATE TABLE IF NOT EXISTS ecommerce.cdc_metrics (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_count BIGINT NOT NULL DEFAULT 1,
    measured_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_cdc_metrics_table_name ON ecommerce.cdc_metrics (table_name, operation);

CREATE PUBLICATION cdc_publication FOR TABLE
    ecommerce.customers,
    ecommerce.products,
    ecommerce.orders,
    ecommerce.order_items,
    ecommerce.inventory,
    ecommerce.debezium_signals;

GRANT USAGE ON SCHEMA ecommerce TO cdc_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce TO cdc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ecommerce TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce TO cdc_user;

CREATE OR REPLACE VIEW ecommerce.dashboard_metrics AS
SELECT 
    (SELECT COUNT(*) FROM ecommerce.customers WHERE is_active = TRUE) as active_customers,
    (SELECT COUNT(*) FROM ecommerce.products WHERE is_available = TRUE) as available_products,
    (SELECT COUNT(*) FROM ecommerce.orders WHERE created_at >= CURRENT_DATE) as orders_today,
    (SELECT COALESCE(SUM(total_amount), 0) FROM ecommerce.orders WHERE created_at >= CURRENT_DATE) as revenue_today;

GRANT SELECT ON ecommerce.dashboard_metrics TO cdc_user;

CREATE OR REPLACE VIEW ecommerce.cdc_replication_status AS
SELECT 
    slot_name,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size,
    EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())) as lag_seconds
FROM pg_replication_slots
WHERE slot_name LIKE 'debezium%';


CREATE OR REPLACE FUNCTION ecommerce.get_table_counts()
RETURNS TABLE (
    table_name TEXT,
    row_count BIGINT,
    last_updated TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'customers'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.customers
    UNION ALL
    SELECT 'products'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.products
    UNION ALL
    SELECT 'orders'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.orders
    UNION ALL
    SELECT 'order_items'::TEXT, COUNT(*)::BIGINT, MAX(created_at) FROM ecommerce.order_items
    UNION ALL
    SELECT 'inventory'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.inventory;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ecommerce.track_table_changes()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ecommerce.cdc_metrics (table_name, operation)
    VALUES (TG_TABLE_NAME, TG_OP);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Add tracking triggers 
CREATE TRIGGER trg_track_customers_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.customers
FOR EACH ROW EXECUTE FUNCTION ecommerce.track_table_changes();

CREATE TRIGGER trg_track_products_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.products
FOR EACH ROW EXECUTE FUNCTION ecommerce.track_table_changes();

CREATE TRIGGER trg_track_orders_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.orders
FOR EACH ROW EXECUTE FUNCTION ecommerce.track_table_changes();

CREATE TRIGGER trg_track_order_items_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.order_items
FOR EACH ROW EXECUTE FUNCTION ecommerce.track_table_changes();

CREATE TRIGGER trg_track_inventory_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.inventory
FOR EACH ROW EXECUTE FUNCTION ecommerce.track_table_changes();
