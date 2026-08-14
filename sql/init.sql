-- ============================================================
-- init.sql
-- Khởi tạo schema cho Instacart CDC Pipeline
-- Chạy tự động khi MySQL container start lần đầu
-- ============================================================

USE instacart;

-- Cấp quyền cho Debezium user đọc binlog
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

-- ─────────────────────────────────
-- Bảng chính: orders
-- ─────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id          INT          NOT NULL,
    user_id           INT          NOT NULL,
    eval_set          VARCHAR(10)  NOT NULL DEFAULT 'train',
    order_number      INT          NOT NULL DEFAULT 1,
    order_dow         INT          NOT NULL DEFAULT 0,
    order_hour_of_day INT          NOT NULL DEFAULT 12,
    days_since_prior  FLOAT                 DEFAULT NULL,
    status            VARCHAR(20)  NOT NULL DEFAULT 'completed',
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

-- ─────────────────────────────────
-- Bảng: order_products
-- ─────────────────────────────────
CREATE TABLE IF NOT EXISTS order_products (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    order_id          INT          NOT NULL,
    product_id        INT          NOT NULL,
    add_to_cart_order INT          NOT NULL DEFAULT 1,
    reordered         TINYINT(1)   NOT NULL DEFAULT 0,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_order_product (order_id, product_id),
    INDEX idx_order_id (order_id),
    INDEX idx_product_id (product_id)
) ENGINE=InnoDB;

-- ─────────────────────────────────
-- Bảng: products
-- ─────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id    INT          NOT NULL,
    product_name  VARCHAR(300) NOT NULL,
    aisle_id      INT          NOT NULL DEFAULT 1,
    department_id INT          NOT NULL DEFAULT 1,
    PRIMARY KEY (product_id)
) ENGINE=InnoDB;

-- Seed một vài sản phẩm mẫu
INSERT IGNORE INTO products (product_id, product_name, aisle_id, department_id) VALUES
(1, 'Chocolate Sandwich Cookies', 61, 19),
(2, 'All-Seasons Salt', 104, 13),
(3, 'Robust Golden Unsweetened Oolong Tea', 94, 7),
(4, 'Smart Ones Classic Favorites Mini Rigatoni', 38, 1),
(5, 'Green Chile Anytime Sauce', 5, 13),
(6, 'Dry Nose Oil', 11, 11),
(7, 'Pure Coconut Water With Orange', 98, 7),
(8, 'Cut Russet Potatoes Steam n Mash', 116, 1),
(9, 'Light Strawberry Blueberry Yogurt', 120, 16),
(10, 'Sparkling Orange Juice & Pamplemousse', 115, 7);
