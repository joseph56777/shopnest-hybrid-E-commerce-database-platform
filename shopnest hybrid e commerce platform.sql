-- ============================================================
-- ShopNest Hybrid E-Commerce Database
-- Student: [joseph antony | ID: [GH1053058]
-- Advanced Databases Project -- march 2026  |  MySQL 8.0+
-- ============================================================
-- STRUCTURE:
--   A  SQL side    : categories, users, addresses, orders, payments, shipping,
--                    order_items, inventory_log, audit_log
--   B  NoSQL side  : product_catalog, activity_logs, shopping_carts,
--                    product_reviews  (all using MySQL JSON columns)
--   C  Seed data   : 100+ rows per table via stored procedures
--   D  CRUD        : UPDATE and DELETE examples
--   E  SELECT      : aggregation, JOINs, CTEs, window functions
--   F  App layer   : helper functions + 5 stored procedures
--   G  Triggers    : audit trail, stock reduction, order guards
--   H  Transactions: COMMIT / ROLLBACK / SAVEPOINT demos
--   I  Indexes     : single, composite, expression, generated-column
--   J  Views       : order summary, customer stats, low-stock
--
-- ALL BUGS FIXED:
--   #1  Removed DEFAULT '[]' from JSON column            (Error 1101)
--   #2  Added proc_body label to sp_place_order          (LEAVE fix)
--   #3  Added proc_body label to sp_cancel_order         (LEAVE fix)
--   #4  Removed DETERMINISTIC from READ SQL DATA functions
--   #5  DATETIME variable in seed_payments               (was TIMESTAMP)
--   #6  DATETIME variable in seed_shipping               (was TIMESTAMP)
--   #7  sp_add_to_cart test uses user 101, not 121
--   #8  Removed duplicate stock deduction from sp_place_order
--   #9  Generated column uses CAST(..AS UNSIGNED), not = TRUE
--   #10 Removed FULLTEXT on generated column (unsupported)
--   #11 Replaced MD5() with SHA2(,256) in seed_users     (Error 1305)
--   #12 Running-total query wrapped in CTE               (Error 1055)
--       only_full_group_by rejects DATE_FORMAT() in SELECT
--       when GROUP BY uses DATE() -- solved by pre-grouping in CTE
-- ============================================================

DROP DATABASE IF EXISTS shopnest;
CREATE DATABASE shopnest CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE shopnest;


-- ============================================================
-- SECTION A  --  SQL SIDE  (Structured / ACID tables)
-- ============================================================

CREATE TABLE categories (
    category_id   INT         AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(80) NOT NULL UNIQUE,
    parent_id     INT         DEFAULT NULL,
    description   VARCHAR(200),
    is_active     TINYINT(1)  NOT NULL DEFAULT 1,
    FOREIGN KEY (parent_id) REFERENCES categories(category_id)
);

CREATE TABLE users (
    user_id       INT          AUTO_INCREMENT PRIMARY KEY,
    username      VARCHAR(50)  NOT NULL UNIQUE,
    email         VARCHAR(120) NOT NULL UNIQUE,
    password_hash VARCHAR(64)  NOT NULL,   -- SHA-256 hex = exactly 64 chars
    first_name    VARCHAR(60)  NOT NULL,
    last_name     VARCHAR(60)  NOT NULL,
    phone         VARCHAR(20),
    date_of_birth DATE,
    is_active     TINYINT(1)   NOT NULL DEFAULT 1,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login    TIMESTAMP    NULL
);

CREATE TABLE addresses (
    address_id INT          AUTO_INCREMENT PRIMARY KEY,
    user_id    INT          NOT NULL,
    label      VARCHAR(20)  NOT NULL DEFAULT 'Home',
    line1      VARCHAR(100) NOT NULL,
    line2      VARCHAR(100),
    city       VARCHAR(60)  NOT NULL,
    postcode   VARCHAR(10)  NOT NULL,
    country    VARCHAR(60)  NOT NULL DEFAULT 'United Kingdom',
    is_default TINYINT(1)   NOT NULL DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    CHECK (label IN ('Home','Work','Other'))
);

CREATE TABLE orders (
    order_id     INT           AUTO_INCREMENT PRIMARY KEY,
    user_id      INT           NOT NULL,
    address_id   INT           NOT NULL,
    status       VARCHAR(20)   NOT NULL DEFAULT 'Pending',
    total_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    discount_amt DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    notes        TEXT,
    placed_at    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id)    REFERENCES users(user_id),
    FOREIGN KEY (address_id) REFERENCES addresses(address_id),
    CHECK (status IN ('Pending','Confirmed','Shipped','Delivered','Cancelled','Refunded'))
);

CREATE TABLE order_items (
    item_id      INT           AUTO_INCREMENT PRIMARY KEY,
    order_id     INT           NOT NULL,
    product_ref  VARCHAR(20)   NOT NULL,   -- FK to product_catalog (NoSQL side)
    product_name VARCHAR(150)  NOT NULL,   -- price snapshot at purchase time
    quantity     SMALLINT      NOT NULL,
    unit_price   DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    CHECK (quantity > 0),
    CHECK (unit_price >= 0)
);

CREATE TABLE payments (
    payment_id      INT           AUTO_INCREMENT PRIMARY KEY,
    order_id        INT           NOT NULL UNIQUE,
    method          VARCHAR(20)   NOT NULL,
    status          VARCHAR(15)   NOT NULL DEFAULT 'Pending',
    amount          DECIMAL(10,2) NOT NULL,
    transaction_ref VARCHAR(60)   UNIQUE,
    paid_at         TIMESTAMP     NULL,
    created_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CHECK (method IN ('Card','PayPal','Bank Transfer','Wallet','UPI')),
    CHECK (status IN ('Pending','Completed','Failed','Refunded'))
);

CREATE TABLE shipping (
    shipping_id    INT         AUTO_INCREMENT PRIMARY KEY,
    order_id       INT         NOT NULL UNIQUE,
    carrier        VARCHAR(50) NOT NULL,
    tracking_no    VARCHAR(60) UNIQUE,
    status         VARCHAR(20) NOT NULL DEFAULT 'Preparing',
    estimated_date DATE,
    delivered_at   TIMESTAMP   NULL,
    created_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CHECK (status IN ('Preparing','Dispatched','In Transit','Delivered','Returned'))
);

-- Tracks every stock movement: negative = sold, positive = restocked/returned
CREATE TABLE inventory_log (
    log_id      INT         AUTO_INCREMENT PRIMARY KEY,
    product_ref VARCHAR(20) NOT NULL,
    change_qty  INT         NOT NULL,
    reason      VARCHAR(20) NOT NULL DEFAULT 'Sale',
    order_id    INT         DEFAULT NULL,
    logged_at   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CHECK (reason IN ('Sale','Restock','Return','Correction','Damage'))
);

-- Immutable record of every INSERT/UPDATE on sensitive tables
CREATE TABLE audit_log (
    log_id     BIGINT       AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(60)  NOT NULL,
    operation  VARCHAR(10)  NOT NULL,
    record_id  INT          NOT NULL,
    changed_by VARCHAR(100) NOT NULL DEFAULT 'system',
    changed_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    old_data   JSON,
    new_data   JSON
);


-- ============================================================
-- SECTION B  --  NoSQL SIDE  (JSON Document tables)
-- ============================================================
-- MySQL's native JSON column gives us MongoDB-style flexible schemas.
-- A phone row has ram/battery fields; a book row has author/isbn.
-- All coexist in the same table -- impossible with fixed SQL columns.

-- Like a MongoDB "products" collection: each doc has different spec fields
CREATE TABLE product_catalog (
    product_id  VARCHAR(20) PRIMARY KEY,
    category_id INT         NOT NULL,
    details     JSON        NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);

-- Like a MongoDB "logs" collection: login has {ip,device}; purchase has {order_id,total}
CREATE TABLE activity_logs (
    log_id     BIGINT      AUTO_INCREMENT PRIMARY KEY,
    user_id    INT         NOT NULL,
    event_type VARCHAR(30) NOT NULL,
    details    JSON        NOT NULL,
    created_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Cart items embedded in a JSON array (BUG #1: no DEFAULT on JSON column)
CREATE TABLE shopping_carts (
    cart_id     INT         AUTO_INCREMENT PRIMARY KEY,
    user_id     INT         NOT NULL UNIQUE,
    items       JSON        NOT NULL,
    coupon_code VARCHAR(20) DEFAULT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    expires_at  TIMESTAMP   NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE product_reviews (
    review_id   INT         AUTO_INCREMENT PRIMARY KEY,
    product_ref VARCHAR(20) NOT NULL,
    user_id     INT         NOT NULL,
    rating      TINYINT     NOT NULL,
    details     JSON        NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    CHECK (rating BETWEEN 1 AND 5)
);


-- ============================================================
-- SECTION C  --  SEED DATA  (100+ rows per table)
-- ============================================================

-- C1. Categories (20 rows) ----------------------------------------
INSERT INTO categories (category_name, parent_id, description) VALUES
    ('Electronics',    NULL, 'Gadgets and electronic devices'),
    ('Mobile Phones',  1,    'Smartphones and accessories'),
    ('Laptops',        1,    'Laptops and notebooks'),
    ('Audio',          1,    'Headphones, earbuds and speakers'),
    ('Cameras',        1,    'Digital cameras and lenses'),
    ('Clothing',       NULL, 'Mens, womens and kids clothing'),
    ('Mens Clothing',  6,    'Shirts, trousers and jackets'),
    ('Womens Clothing',6,    'Dresses, tops and skirts'),
    ('Footwear',       6,    'Shoes, boots and trainers'),
    ('Kids Clothing',  6,    'Clothing for children under 12'),
    ('Books',          NULL, 'Fiction, non-fiction and textbooks'),
    ('Fiction',        11,   'Novels and short story collections'),
    ('Non-Fiction',    11,   'Biographies, science and history'),
    ('Textbooks',      11,   'Academic and professional books'),
    ('Home & Garden',  NULL, 'Kitchen, furniture and garden'),
    ('Kitchen',        15,   'Cookware, appliances and utensils'),
    ('Furniture',      15,   'Sofas, tables and chairs'),
    ('Garden',         15,   'Outdoor and gardening tools'),
    ('Sports',         NULL, 'Fitness and outdoor sports'),
    ('Fitness',        19,   'Gym equipment and sportswear');


-- C2. Users (120 rows) -------------------------------------------
-- BUG #11 FIX: SHA2(,256) instead of MD5() -- MD5 causes Error 1305
-- inside stored procedures in some MySQL 8.0 configurations.
-- SHA-256 always resolves as a built-in and returns exactly 64 hex chars.
DELIMITER $$
CREATE PROCEDURE seed_users()
BEGIN
    DECLARE user_num   INT     DEFAULT 1;
    DECLARE first_name VARCHAR(30);
    DECLARE last_name  VARCHAR(30);
    WHILE user_num <= 120 DO
        SET first_name = ELT(((user_num-1) % 20)+1,
            'James','Emma','Oliver','Sophia','Liam','Ava','Noah','Isabella',
            'William','Mia','Benjamin','Charlotte','Ethan','Amelia','Lucas',
            'Harper','Mason','Evelyn','Logan','Abigail');
        SET last_name = ELT(((user_num-1) % 20)+1,
            'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller',
            'Davis','Wilson','Moore','Taylor','Anderson','Thomas','Jackson',
            'White','Harris','Martin','Lewis','Lee','Walker');
        INSERT INTO users (username, email, password_hash, first_name, last_name, phone, date_of_birth)
        VALUES (
            CONCAT(LOWER(first_name), LOWER(last_name), user_num),
            CONCAT(LOWER(first_name), '.', LOWER(last_name), user_num, '@shopnest.com'),
            SHA2(CONCAT('pass', user_num, 'secure'), 256),
            first_name, last_name,
            CONCAT('07', LPAD(7000000 + user_num, 9, '0')),
            DATE_SUB('2000-01-01', INTERVAL (MOD(user_num * 113, 10000)) DAY)
        );
        SET user_num = user_num + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_users();


-- C3. Addresses (150 rows: 120 Home + 30 Work) --------------------
DELIMITER $$
CREATE PROCEDURE seed_addresses()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE street VARCHAR(80);
    DECLARE ct VARCHAR(40);
    DECLARE pc VARCHAR(10);
    WHILE i <= 120 DO
        SET street = CONCAT(i, ' ', ELT(MOD(i-1,10)+1,
            'High Street','Baker Street','Church Lane','Park Road',
            'Victoria Road','Station Road','Green Lane','Mill Road','School Lane','King Street'));
        SET ct = ELT(MOD(i-1,15)+1,
            'London','Manchester','Birmingham','Leeds','Glasgow',
            'Sheffield','Edinburgh','Liverpool','Bristol','Cardiff',
            'Belfast','Nottingham','Leicester','Coventry','Oxford');
        SET pc = CONCAT(ELT(MOD(i-1,15)+1,
            'E1','M1','B1','LS1','G1','S1','EH1','L1','BS1','CF1',
            'BT1','NG1','LE1','CV1','OX1'), ' ', LPAD(MOD(i,100),2,'0'), 'A');
        INSERT INTO addresses (user_id, label, line1, city, postcode, is_default)
        VALUES (i, 'Home', street, ct, pc, 1);
        SET i = i + 1;
    END WHILE;
    -- 30 extra Work addresses for first 30 users
    SET i = 1;
    WHILE i <= 30 DO
        INSERT INTO addresses (user_id, label, line1, city, postcode, is_default)
        VALUES (i, 'Work', CONCAT('Floor ', MOD(i,10)+1, ', Central Business Park'),
            ELT(MOD(i-1,5)+1,'London','Manchester','Birmingham','Leeds','Glasgow'),
            CONCAT('WK', LPAD(i,2,'0'), ' 1AB'), 0);
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_addresses();


-- C4. Product Catalog (120 JSON documents, NoSQL side) ------------
-- Electronics get specs.ram/storage/battery; Clothing gets variants;
-- Books get specs.author/isbn; Home gets specs.wattage/capacity.
-- This flexible schema is impossible with fixed SQL columns -- it is
-- exactly what MongoDB does, implemented inside MySQL via JSON.
DELIMITER $$
CREATE PROCEDURE seed_product_catalog()
BEGIN
    DECLARE i   INT DEFAULT 1;
    DECLARE pid VARCHAR(20);
    DECLARE cat INT;
    DECLARE nm  VARCHAR(100);
    DECLARE pr  DECIMAL(10,2);
    DECLARE br  VARCHAR(50);
    DECLARE doc JSON;
    WHILE i <= 120 DO
        SET pid = CONCAT('PRD-', LPAD(i, 6, '0'));
        SET cat = ELT(MOD(i-1,10)+1, 2,3,4,5,7,8,9,12,13,16);
        SET nm  = ELT(MOD(i-1,20)+1,
            'Galaxy S24 Ultra','iPhone 16 Pro','MacBook Air M3','Dell XPS 15',
            'Sony WH-1000XM5','AirPods Pro 2','Canon EOS R50','Levi 501 Jeans',
            'Nike Air Max 90','Adidas Ultraboost 23','Atomic Habits',
            'Python Crash Course','KitchenAid Stand Mixer','Dyson V15 Detect',
            'Instant Pot Duo','Yoga Mat Premium','Waterproof Hiking Jacket',
            'iPad Pro 12.9','Samsung 65in OLED','Bosch Cordless Drill');
        SET br = ELT(MOD(i-1,10)+1,
            'Samsung','Apple','Apple','Dell','Sony','Apple','Canon','Levis','Nike','Adidas');
        SET pr = ROUND(19.99 + MOD(i * 37, 1400), 2);

        IF cat IN (2,3,4,5) THEN          -- Electronics
            SET doc = JSON_OBJECT(
                'sku',pid,'name',nm,'brand',br,'category','Electronics',
                'price',pr,'cost_price',ROUND(pr*0.55,2),
                'stock_qty',MOD(i*7,200)+1,'is_active',IF(MOD(i,12)=0,0,1),
                'rating_avg',ROUND(3.0+MOD(i,20)*0.1,1),'review_count',MOD(i*3,300),
                'specs',JSON_OBJECT(
                    'ram',   CONCAT(ELT(MOD(i,4)+1,'8','16','32','64'),'GB'),
                    'storage',CONCAT(ELT(MOD(i,3)+1,'128','256','512'),'GB'),
                    'screen',CONCAT(ROUND(5.5+MOD(i,5)*0.3,1),'in'),
                    'battery',CONCAT(3000+MOD(i*7,2500),'mAh')),
                'variants',JSON_ARRAY(
                    JSON_OBJECT('colour','Black', 'qty',MOD(i,50)+5),
                    JSON_OBJECT('colour','Silver','qty',MOD(i*2,40)+3)),
                'tags',JSON_ARRAY('electronics',LOWER(br)),
                'images',JSON_ARRAY(
                    CONCAT('https://cdn.shopnest.com/products/',i,'/main.jpg'),
                    CONCAT('https://cdn.shopnest.com/products/',i,'/alt.jpg')));

        ELSEIF cat IN (7,8,9) THEN        -- Clothing
            SET doc = JSON_OBJECT(
                'sku',pid,'name',nm,'brand',br,'category','Clothing',
                'price',pr,'cost_price',ROUND(pr*0.45,2),
                'stock_qty',MOD(i*11,180)+10,'is_active',IF(MOD(i,15)=0,0,1),
                'rating_avg',ROUND(3.2+MOD(i,18)*0.1,1),'review_count',MOD(i*2,200),
                'variants',JSON_ARRAY(
                    JSON_OBJECT('size','S', 'colour','Black','qty',MOD(i,20)+2),
                    JSON_OBJECT('size','M', 'colour','Black','qty',MOD(i,25)+5),
                    JSON_OBJECT('size','L', 'colour','Navy', 'qty',MOD(i,15)+3),
                    JSON_OBJECT('size','XL','colour','Navy', 'qty',MOD(i,10)+1)),
                'tags',JSON_ARRAY('clothing',LOWER(br)),
                'images',JSON_ARRAY(CONCAT('https://cdn.shopnest.com/products/',i,'/main.jpg')));

        ELSEIF cat IN (12,13) THEN        -- Books
            SET doc = JSON_OBJECT(
                'sku',pid,'name',nm,
                'brand',ELT(MOD(i,4)+1,'Penguin','HarperCollins','Pearson','OReilly'),
                'category','Books',
                'price',pr,'cost_price',ROUND(pr*0.40,2),
                'stock_qty',MOD(i*13,500)+20,'is_active',1,
                'rating_avg',ROUND(3.5+MOD(i,15)*0.1,1),'review_count',MOD(i*5,800),
                'specs',JSON_OBJECT(
                    'author', CONCAT('Author ',MOD(i,50)+1),
                    'pages',  150+MOD(i*7,600),
                    'isbn',   CONCAT('978-0-',LPAD(MOD(i*7,9999),4,'0'),'-',LPAD(MOD(i,999),3,'0'),'-',MOD(i,9)),
                    'edition',CONCAT(ELT(MOD(i,4)+1,'1st','2nd','3rd','4th'),' Edition'),
                    'language','English'),
                'tags',JSON_ARRAY('books',ELT(MOD(i,3)+1,'fiction','non-fiction','textbook')),
                'images',JSON_ARRAY(CONCAT('https://cdn.shopnest.com/products/',i,'/cover.jpg')));

        ELSE                              -- Home & Garden
            SET doc = JSON_OBJECT(
                'sku',pid,'name',nm,
                'brand',ELT(MOD(i,5)+1,'KitchenAid','Dyson','Bosch','Philips','Tefal'),
                'category','Home & Garden',
                'price',pr,'cost_price',ROUND(pr*0.50,2),
                'stock_qty',MOD(i*9,120)+5,'is_active',IF(MOD(i,20)=0,0,1),
                'rating_avg',ROUND(3.3+MOD(i,17)*0.1,1),'review_count',MOD(i*4,250),
                'specs',JSON_OBJECT(
                    'wattage', CONCAT(500+MOD(i*30,2000),'W'),
                    'capacity',CONCAT(ROUND(1.5+MOD(i,10)*0.5,1),'L'),
                    'colour',  ELT(MOD(i,5)+1,'Black','White','Red','Silver','Cream')),
                'variants',JSON_ARRAY(
                    JSON_OBJECT('colour','Black','qty',MOD(i,30)+5),
                    JSON_OBJECT('colour','White','qty',MOD(i,20)+3)),
                'tags',JSON_ARRAY('home','kitchen'),
                'images',JSON_ARRAY(CONCAT('https://cdn.shopnest.com/products/',i,'/main.jpg')));
        END IF;

        INSERT INTO product_catalog (product_id, category_id, details) VALUES (pid, cat, doc);
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_product_catalog();


-- C5. Orders (150 rows) ------------------------------------------
DELIMITER $$
CREATE PROCEDURE seed_orders()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE st VARCHAR(20);
    WHILE i <= 150 DO
        SET st = ELT(MOD(i*7,7)+1,
            'Pending','Confirmed','Shipped','Delivered','Delivered','Delivered','Cancelled');
        INSERT INTO orders (user_id, address_id, status, total_amount, discount_amt, placed_at)
        VALUES (
            MOD(i-1,120)+1, MOD(i-1,120)+1, st,
            ROUND(15 + MOD(i*31, 480), 2),
            IF(MOD(i,8)=0, ROUND(5 + MOD(i,20), 2), 0.00),
            DATE_SUB(NOW(), INTERVAL MOD(i,365) DAY));
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_orders();


-- C6. Order Items (~330 rows, 2-3 per order) ----------------------
DELIMITER $$
CREATE PROCEDURE seed_order_items()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE j INT;
    DECLARE rows_per_order INT;
    DECLARE pref  VARCHAR(20);
    DECLARE pname VARCHAR(150);
    WHILE i <= 150 DO
        SET rows_per_order = IF(MOD(i,3)=0, 3, 2);
        SET j = 1;
        WHILE j <= rows_per_order DO
            SET pref  = CONCAT('PRD-', LPAD(MOD(i + j*13, 120)+1, 6, '0'));
            SET pname = ELT(MOD(i+j,20)+1,
                'Galaxy S24 Ultra','iPhone 16 Pro','MacBook Air M3','Dell XPS 15',
                'Sony WH-1000XM5','AirPods Pro 2','Canon EOS R50','Levi 501 Jeans',
                'Nike Air Max 90','Adidas Ultraboost 23','Atomic Habits',
                'Python Crash Course','KitchenAid Stand Mixer','Dyson V15 Detect',
                'Instant Pot Duo','Yoga Mat Premium','Waterproof Jacket',
                'iPad Pro 12.9','Samsung 65in OLED','Bosch Cordless Drill');
            INSERT INTO order_items (order_id, product_ref, product_name, quantity, unit_price)
            VALUES (i, pref, pname, MOD(i+j,4)+1, ROUND(9.99 + MOD(i*j*17, 490), 2));
            SET j = j + 1;
        END WHILE;
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_order_items();


-- C7. Payments (150 rows) ----------------------------------------
-- BUG #5 FIX: pdate declared as DATETIME, not TIMESTAMP (NULL safety)
DELIMITER $$
CREATE PROCEDURE seed_payments()
BEGIN
    DECLARE i     INT DEFAULT 1;
    DECLARE meth  VARCHAR(20);
    DECLARE stat  VARCHAR(15);
    DECLARE pdate DATETIME DEFAULT NULL;
    WHILE i <= 150 DO
        SET meth  = ELT(MOD(i*3,5)+1,'Card','PayPal','Card','Card','Wallet');
        SET pdate = NULL;
        SELECT status INTO stat FROM orders WHERE order_id = i;
        SET stat = CASE
            WHEN stat IN ('Delivered','Shipped','Confirmed') THEN 'Completed'
            WHEN stat = 'Cancelled'                          THEN 'Refunded'
            ELSE 'Pending'
        END;
        IF stat != 'Pending' THEN
            SELECT DATE_ADD(placed_at, INTERVAL 30 MINUTE) INTO pdate
            FROM orders WHERE order_id = i;
        END IF;
        INSERT INTO payments (order_id, method, status, amount, transaction_ref, paid_at)
        VALUES (i, meth, stat,
            (SELECT total_amount FROM orders WHERE order_id = i),
            CONCAT('TXN', LPAD(i,10,'0')), pdate);
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_payments();


-- C8. Shipping (150 rows) ----------------------------------------
-- BUG #6 FIX: del_at declared as DATETIME, not TIMESTAMP (NULL safety)
DELIMITER $$
CREATE PROCEDURE seed_shipping()
BEGIN
    DECLARE i      INT DEFAULT 1;
    DECLARE car    VARCHAR(50);
    DECLARE sh_st  VARCHAR(20);
    DECLARE ord_st VARCHAR(20);
    DECLARE del_at DATETIME DEFAULT NULL;
    WHILE i <= 150 DO
        SET del_at = NULL;
        SET car    = ELT(MOD(i*5,6)+1,
            'Royal Mail','DHL','FedEx','UPS','Hermes','Amazon Logistics');
        SELECT status INTO ord_st FROM orders WHERE order_id = i;
        SET sh_st = CASE ord_st
            WHEN 'Delivered' THEN 'Delivered'
            WHEN 'Shipped'   THEN 'In Transit'
            WHEN 'Confirmed' THEN 'Dispatched'
            WHEN 'Cancelled' THEN 'Returned'
            ELSE 'Preparing'
        END;
        IF ord_st = 'Delivered' THEN
            SELECT DATE_ADD(placed_at, INTERVAL 4 DAY) INTO del_at
            FROM orders WHERE order_id = i;
        END IF;
        INSERT INTO shipping (order_id, carrier, tracking_no, status, estimated_date, delivered_at)
        VALUES (i, car, CONCAT('SHP', LPAD(i,12,'0')), sh_st,
            (SELECT DATE_ADD(placed_at, INTERVAL 3 DAY) FROM orders WHERE order_id = i),
            del_at);
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_shipping();


-- C9. Activity Logs (150 rows, NoSQL side) -----------------------
-- Each event type stores a different JSON shape -- the core NoSQL
-- flexible-schema advantage demonstrated inside MySQL.
DELIMITER $$
CREATE PROCEDURE seed_activity_logs()
BEGIN
    DECLARE i    INT DEFAULT 1;
    DECLARE evnt VARCHAR(30);
    DECLARE doc  JSON;
    WHILE i <= 150 DO
        SET evnt = ELT(MOD(i-1,8)+1,
            'login','page_view','search','add_to_cart',
            'remove_from_cart','checkout_start','purchase','logout');
        SET doc = CASE evnt
            WHEN 'login' THEN JSON_OBJECT(
                'method', ELT(MOD(i,3)+1,'email','google_oauth','facebook_oauth'),
                'success',IF(MOD(i,8)=0,0,1),
                'ip',     CONCAT(MOD(i*3,254)+1,'.',MOD(i*7,255),'.',MOD(i*11,255),'.1'),
                'device', ELT(MOD(i,3)+1,'mobile','desktop','tablet'))
            WHEN 'page_view' THEN JSON_OBJECT(
                'page',    ELT(MOD(i,5)+1,'/home','/products','/cart','/checkout','/account'),
                'duration',MOD(i*7,480)+5,
                'referrer',ELT(MOD(i,4)+1,'google','direct','social','email'),
                'device',  ELT(MOD(i,3)+1,'mobile','desktop','tablet'))
            WHEN 'search' THEN JSON_OBJECT(
                'query',        ELT(MOD(i,8)+1,'iphone','laptop','headphones',
                                    'jeans','trainers','books','kitchen','camera'),
                'results_count',MOD(i*3,85)+1,
                'filters_used', IF(MOD(i,3)=0,1,0),
                'device',       ELT(MOD(i,3)+1,'mobile','desktop','tablet'))
            WHEN 'add_to_cart' THEN JSON_OBJECT(
                'product_ref', CONCAT('PRD-',LPAD(MOD(i*7,120)+1,6,'0')),
                'product_name',ELT(MOD(i,10)+1,
                    'Galaxy S24','MacBook Air','Sony Headphones','Nike Trainers',
                    'Atomic Habits','KitchenAid Mixer','Dyson Vacuum',
                    'Levi Jeans','iPad Pro','AirPods Pro'),
                'quantity',MOD(i,3)+1, 'price',ROUND(9.99+MOD(i*23,490),2))
            WHEN 'purchase' THEN JSON_OBJECT(
                'order_id',      MOD(i,150)+1,
                'total_amount',  ROUND(20+MOD(i*31,500),2),
                'item_count',    MOD(i,5)+1,
                'payment_method',ELT(MOD(i,3)+1,'Card','PayPal','Wallet'))
            ELSE JSON_OBJECT('device',ELT(MOD(i,3)+1,'mobile','desktop','tablet'),'action',evnt)
        END;
        INSERT INTO activity_logs (user_id, event_type, details, created_at)
        VALUES (MOD(i-1,120)+1, evnt, doc, DATE_SUB(NOW(), INTERVAL MOD(i*3,30) DAY));
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_activity_logs();


-- C10. Shopping Carts (100 rows, NoSQL side) ----------------------
DELIMITER $$
CREATE PROCEDURE seed_shopping_carts()
BEGIN
    DECLARE i   INT DEFAULT 1;
    DECLARE doc JSON;
    WHILE i <= 100 DO
        SET doc = CASE MOD(i,3)
            WHEN 0 THEN JSON_ARRAY(
                JSON_OBJECT('product_ref',CONCAT('PRD-',LPAD(MOD(i,120)+1,6,'0')),
                            'name','Galaxy S24 Ultra','qty',1,'price',1199.00,
                            'added_at',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s')),
                JSON_OBJECT('product_ref',CONCAT('PRD-',LPAD(MOD(i+5,120)+1,6,'0')),
                            'name','Sony WH-1000XM5','qty',1,'price',299.00,
                            'added_at',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s')),
                JSON_OBJECT('product_ref',CONCAT('PRD-',LPAD(MOD(i+10,120)+1,6,'0')),
                            'name','Atomic Habits','qty',2,'price',12.99,
                            'added_at',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s')))
            WHEN 1 THEN JSON_ARRAY(
                JSON_OBJECT('product_ref',CONCAT('PRD-',LPAD(MOD(i*3,120)+1,6,'0')),
                            'name','Nike Air Max 90','qty',1,'price',109.99,
                            'added_at',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s')),
                JSON_OBJECT('product_ref',CONCAT('PRD-',LPAD(MOD(i*5,120)+1,6,'0')),
                            'name','Levi 501 Jeans','qty',1,'price',79.99,
                            'added_at',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s')))
            ELSE JSON_ARRAY(
                JSON_OBJECT('product_ref',CONCAT('PRD-',LPAD(MOD(i*7,120)+1,6,'0')),
                            'name','MacBook Air M3','qty',1,'price',1299.00,
                            'added_at',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s')))
        END;
        INSERT INTO shopping_carts (user_id, items, coupon_code, expires_at)
        VALUES (i, doc, IF(MOD(i,10)=0,'SAVE10',NULL), DATE_ADD(NOW(), INTERVAL 7 DAY));
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_shopping_carts();


-- C11. Product Reviews (150 rows, NoSQL side) --------------------
DELIMITER $$
CREATE PROCEDURE seed_product_reviews()
BEGIN
    DECLARE i   INT DEFAULT 1;
    DECLARE rat TINYINT;
    DECLARE doc JSON;
    WHILE i <= 150 DO
        SET rat = MOD(i * 7, 5) + 1;
        SET doc = JSON_OBJECT(
            'title', ELT(MOD(i,8)+1,
                'Great buy','Would buy again','Not what I expected','Top product',
                'Happy with this','Mixed feelings','Excellent quality','Average at best'),
            'body', ELT(MOD(i-1,10)+1,
                'Absolutely love this product. Exceeded my expectations.',
                'Good value for the price. Would recommend.',
                'Decent but the packaging could be better.',
                'Works exactly as described. No complaints.',
                'Had a few issues but customer service helped.',
                'Amazing quality. Bought this before and it never disappoints.',
                'Not bad but seen better at this price.',
                'Delivery was fast and item was well packaged.',
                'Five stars. Best purchase this year.',
                'Build quality feels cheap for the price paid.'),
            'verified_purchase',IF(MOD(i,4)=0,0,1),
            'helpful_votes',    MOD(i*3,45),
            'images',           IF(MOD(i,8)=0,
                JSON_ARRAY(CONCAT('https://cdn.shopnest.com/reviews/',i,'.jpg')),
                JSON_ARRAY()));
        INSERT INTO product_reviews (product_ref, user_id, rating, details, created_at)
        VALUES (CONCAT('PRD-', LPAD(MOD(i-1,120)+1, 6, '0')),
                MOD(i-1,120)+1, rat, doc,
                DATE_SUB(NOW(), INTERVAL MOD(i*5,300) DAY));
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;
CALL seed_product_reviews();


-- ============================================================
-- SECTION D  --  CRUD OPERATIONS
-- ============================================================
-- BUG #12 FIX (Error 1175): safe update mode blocks UPDATE/DELETE
-- that filter on non-key columns. We disable it for this block only,
-- then immediately re-enable it so the rest of the script is protected.

SET SQL_SAFE_UPDATES = 0;

-- Give a 5% loyalty discount to old pending orders
UPDATE orders
SET    discount_amt = ROUND(total_amount * 0.05, 2),
       notes        = 'Loyalty discount applied'
WHERE  status = 'Pending'
  AND  placed_at < DATE_SUB(NOW(), INTERVAL 7 DAY);

-- Restock any active product with fewer than 10 units (NoSQL side)
UPDATE product_catalog
SET    details = JSON_SET(details, '$.stock_qty',
                    CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED) + 50)
WHERE  CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED) < 10
  AND  CAST(JSON_EXTRACT(details,'$.is_active')  AS UNSIGNED) = 1;

-- Mark payments that have been Pending for more than 14 days as Failed
UPDATE payments
SET    status = 'Failed'
WHERE  status = 'Pending'
  AND  created_at < DATE_SUB(NOW(), INTERVAL 14 DAY);

-- Remove inactive products that have never received a single review
DELETE FROM product_catalog
WHERE CAST(JSON_EXTRACT(details,'$.is_active')   AS UNSIGNED) = 0
  AND CAST(JSON_EXTRACT(details,'$.review_count') AS UNSIGNED) = 0;

SET SQL_SAFE_UPDATES = 1;

-- INSERT: add a new product manually (NoSQL side)
INSERT INTO product_catalog (product_id, category_id, details)
VALUES ('PRD-999001', 2, JSON_OBJECT(
    'sku','PRD-999001','name','Google Pixel 9 Pro','brand','Google',
    'category','Mobile Phones','price',999.00,'cost_price',549.00,
    'stock_qty',75,'is_active',1,'rating_avg',4.7,'review_count',0,
    'specs',    JSON_OBJECT('ram','16GB','storage','256GB','screen','6.3in OLED','battery','4700mAh'),
    'variants', JSON_ARRAY(JSON_OBJECT('colour','Obsidian','qty',40),JSON_OBJECT('colour','Porcelain','qty',35)),
    'tags',     JSON_ARRAY('mobile-phones','google','android'),
    'images',   JSON_ARRAY('https://cdn.shopnest.com/products/pixel9/main.jpg')));


-- ============================================================
-- SECTION E  --  SELECT QUERIES
-- ============================================================

-- Delivered orders placed this calendar month
SELECT order_id, user_id, total_amount, placed_at
FROM   orders
WHERE  status = 'Delivered'
  AND  placed_at >= DATE_FORMAT(NOW(),'%Y-%m-01')
ORDER  BY placed_at DESC;

-- Top 20 products by price (NoSQL JSON query)
SELECT product_id,
       details->>'$.name'  AS product_name,
       details->>'$.brand' AS brand,
       CAST(JSON_EXTRACT(details,'$.price')     AS DECIMAL(10,2)) AS price,
       CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED)      AS stock
FROM   product_catalog
ORDER  BY price DESC LIMIT 20;

-- In-stock Electronics priced at or under £500 (NoSQL query)
SELECT product_id,
       details->>'$.name'  AS name,
       details->>'$.brand' AS brand,
       CAST(JSON_EXTRACT(details,'$.price') AS DECIMAL(10,2)) AS price
FROM   product_catalog
WHERE  details->>'$.category' = 'Electronics'
  AND  CAST(JSON_EXTRACT(details,'$.price')     AS DECIMAL(10,2)) <= 500
  AND  CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED) > 0
  AND  CAST(JSON_EXTRACT(details,'$.is_active') AS UNSIGNED) = 1
ORDER  BY price;

-- Activity logs showing flexible-schema benefit: same query returns
-- page, search term, and product name from different event types
SELECT log_id, event_type,
       details->>'$.page'         AS page_viewed,
       details->>'$.query'        AS search_query,
       details->>'$.product_name' AS product_added,
       details->>'$.total_amount' AS purchase_total,
       created_at
FROM   activity_logs
ORDER  BY created_at DESC LIMIT 20;

-- Revenue summary grouped by order status (GROUP BY + aggregation)
SELECT status,
       COUNT(*)                   AS order_count,
       ROUND(SUM(total_amount),2) AS revenue,
       ROUND(AVG(total_amount),2) AS avg_order_value
FROM   orders
GROUP  BY status
ORDER  BY revenue DESC;

-- Top 10 products by units sold (JOIN + GROUP BY)
SELECT oi.product_ref, oi.product_name,
       SUM(oi.quantity)                          AS units_sold,
       ROUND(SUM(oi.quantity * oi.unit_price),2) AS revenue
FROM   order_items oi
JOIN   orders o ON o.order_id = oi.order_id
WHERE  o.status NOT IN ('Cancelled','Refunded')
GROUP  BY oi.product_ref, oi.product_name
ORDER  BY units_sold DESC LIMIT 10;

-- Categories with more than 5 products (HAVING clause)
SELECT details->>'$.category' AS category,
       COUNT(*) AS product_count,
       ROUND(AVG(CAST(JSON_EXTRACT(details,'$.price') AS DECIMAL(10,2))),2) AS avg_price
FROM   product_catalog
GROUP  BY details->>'$.category'
HAVING COUNT(*) > 5
ORDER  BY product_count DESC;

-- Full order details: status, payment method, carrier (INNER JOIN)
SELECT o.order_id,
       CONCAT(u.first_name,' ',u.last_name) AS customer,
       o.status AS order_status, p.method AS payment_method,
       p.status AS payment_status, s.carrier, s.status AS shipping_status,
       o.total_amount
FROM   orders o
JOIN   users    u ON u.user_id  = o.user_id
JOIN   payments p ON p.order_id = o.order_id
JOIN   shipping s ON s.order_id = o.order_id
ORDER  BY o.placed_at DESC LIMIT 20;

-- Customers who have never placed an order (LEFT JOIN)
SELECT u.user_id, u.username, u.email, u.created_at
FROM   users u
LEFT   JOIN orders o ON o.user_id = u.user_id
WHERE  o.order_id IS NULL
ORDER  BY u.created_at DESC;

-- Monthly revenue for the past 12 months (GROUP BY + DATE_FORMAT)
SELECT DATE_FORMAT(placed_at,'%Y-%m') AS month,
       COUNT(*)                        AS orders_placed,
       ROUND(SUM(total_amount),2)      AS revenue
FROM   orders
WHERE  placed_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
  AND  status NOT IN ('Cancelled','Refunded')
GROUP  BY DATE_FORMAT(placed_at,'%Y-%m')
ORDER  BY month;

-- Customer lifetime spend and order-count rankings (window functions)
SELECT u.user_id,
       CONCAT(u.first_name,' ',u.last_name)          AS customer,
       COUNT(o.order_id)                              AS total_orders,
       ROUND(SUM(o.total_amount),2)                   AS lifetime_spend,
       RANK()       OVER (ORDER BY SUM(o.total_amount) DESC) AS spend_rank,
       DENSE_RANK() OVER (ORDER BY COUNT(o.order_id)  DESC) AS order_rank
FROM   users u
JOIN   orders o ON o.user_id = u.user_id
WHERE  o.status NOT IN ('Cancelled','Refunded')
GROUP  BY u.user_id, u.first_name, u.last_name
ORDER  BY spend_rank LIMIT 20;

-- Running total of daily revenue for Delivered orders (window function)
-- BUG #12 FIX (Error 1055 / only_full_group_by):
-- The original query grouped by DATE(placed_at) but selected
-- DATE_FORMAT(placed_at,...) -- two different expressions, which
-- only_full_group_by correctly rejects.
-- Solution: pre-aggregate by order_day in a CTE, then format in the
-- outer SELECT so every expression in SELECT is already aggregated.
WITH daily_revenue AS (
    SELECT DATE(placed_at)         AS order_day,
           SUM(total_amount)       AS day_total
    FROM   orders
    WHERE  status = 'Delivered'
    GROUP  BY DATE(placed_at)
)
SELECT DATE_FORMAT(order_day,'%Y-%m-%d')                     AS order_date,
       ROUND(day_total, 2)                                    AS daily_revenue,
       ROUND(SUM(day_total) OVER (ORDER BY order_day), 2)    AS running_total
FROM   daily_revenue
ORDER  BY order_day LIMIT 30;

-- Average order value per delivery city (CTE + JOIN)
WITH city_orders AS (
    SELECT a.city, o.total_amount
    FROM   orders o
    JOIN   addresses a ON a.address_id = o.address_id
    WHERE  o.status = 'Delivered'
)
SELECT city,
       COUNT(*)                   AS delivered_orders,
       ROUND(AVG(total_amount),2) AS avg_order_value,
       ROUND(SUM(total_amount),2) AS total_revenue
FROM   city_orders
GROUP  BY city
ORDER  BY total_revenue DESC;

-- Orders containing more than 2 line items (CTE + HAVING)
WITH big_orders AS (
    SELECT order_id,
           COUNT(item_id)             AS item_count,
           SUM(quantity * unit_price) AS items_total
    FROM   order_items
    GROUP  BY order_id
    HAVING COUNT(item_id) > 2
)
SELECT bo.order_id,
       CONCAT(u.first_name,' ',u.last_name) AS customer,
       bo.item_count,
       ROUND(bo.items_total, 2) AS cart_value
FROM   big_orders bo
JOIN   orders o ON o.order_id = bo.order_id
JOIN   users  u ON u.user_id  = o.user_id
ORDER  BY bo.item_count DESC;

-- Average rating and total reviews per category (JSON aggregation)
SELECT details->>'$.category'                                             AS category,
       COUNT(*)                                                            AS products,
       ROUND(AVG(CAST(JSON_EXTRACT(details,'$.rating_avg') AS DECIMAL(3,1))),2) AS avg_rating,
       SUM(CAST(JSON_EXTRACT(details,'$.review_count') AS UNSIGNED))      AS total_reviews
FROM   product_catalog
GROUP  BY details->>'$.category'
ORDER  BY avg_rating DESC;

-- Event frequency across activity log (NoSQL aggregation)
SELECT event_type, COUNT(*) AS event_count
FROM   activity_logs
GROUP  BY event_type
ORDER  BY event_count DESC;

-- Products with fewer than 15 units in stock that are still active
SELECT product_id,
       details->>'$.name'     AS name,
       details->>'$.category' AS category,
       CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED)      AS stock,
       CAST(JSON_EXTRACT(details,'$.price')     AS DECIMAL(10,2)) AS price
FROM   product_catalog
WHERE  CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED) < 15
  AND  CAST(JSON_EXTRACT(details,'$.is_active')  AS UNSIGNED) = 1
ORDER  BY stock;


-- ============================================================
-- SECTION F  --  FUNCTIONS & APPLICATION LAYER PROCEDURES
-- ============================================================
-- In a real system, Python/Node.js would connect separately to
-- MongoDB (NoSQL) and MySQL (SQL). Here, stored procedures play
-- that integration role entirely inside MySQL, bridging both sides
-- in a single transaction.

-- F1. Helper functions -------------------------------------------
-- BUG #4 FIX: removed DETERMINISTIC -- READS SQL DATA functions
-- cannot be deterministic because the underlying data can change.
DELIMITER $$

CREATE FUNCTION fn_lifetime_spend(p_user_id INT)
RETURNS DECIMAL(10,2) READS SQL DATA
BEGIN
    DECLARE v_total DECIMAL(10,2) DEFAULT 0.00;
    SELECT COALESCE(SUM(total_amount), 0.00) INTO v_total
    FROM   orders
    WHERE  user_id = p_user_id AND status NOT IN ('Cancelled','Refunded');
    RETURN v_total;
END$$

CREATE FUNCTION fn_active_order_count(p_user_id INT)
RETURNS INT READS SQL DATA
BEGIN
    DECLARE v_count INT DEFAULT 0;
    SELECT COUNT(*) INTO v_count
    FROM   orders
    WHERE  user_id = p_user_id AND status IN ('Pending','Confirmed','Shipped');
    RETURN v_count;
END$$
DELIMITER ;

SELECT user_id,
       fn_lifetime_spend(user_id)     AS spend,
       fn_active_order_count(user_id) AS active_orders
FROM   users LIMIT 5;


-- F2. sp_place_order (main integration: SQL + NoSQL in one tx) ---
-- 1. Validate user is active                    (SQL)
-- 2. Validate address belongs to user           (SQL)
-- 3. Check stock in product_catalog JSON        (NoSQL)
-- 4. Write order, items, payment, shipping      (SQL)
-- 5. Log stock movement to inventory_log        (SQL)
-- 6. Log purchase event to activity_logs JSON   (NoSQL)
-- 7. Commit or roll back the whole transaction
-- BUG #2: proc_body label needed for LEAVE to work
-- BUG #8: stock deduction removed -- trigger handles it
DELIMITER $$
CREATE PROCEDURE sp_place_order(
    IN  p_user_id        INT,
    IN  p_address_id     INT,
    IN  p_product_ref    VARCHAR(20),
    IN  p_product_name   VARCHAR(150),
    IN  p_qty            INT,
    IN  p_unit_price     DECIMAL(10,2),
    IN  p_payment_method VARCHAR(20),
    OUT p_order_id       INT,
    OUT p_message        VARCHAR(200)
)
proc_body: BEGIN
    DECLARE v_user_ok INT     DEFAULT 0;
    DECLARE v_addr_ok INT     DEFAULT 0;
    DECLARE v_stock   INT     DEFAULT 0;
    DECLARE v_total   DECIMAL(10,2);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
        ROLLBACK;
        SET p_message = 'Order failed - transaction rolled back.';
    END;

    SELECT COUNT(*) INTO v_user_ok FROM users
    WHERE  user_id = p_user_id AND is_active = 1;
    IF v_user_ok = 0 THEN
        SET p_message = 'User not found or account is inactive.'; LEAVE proc_body;
    END IF;

    SELECT COUNT(*) INTO v_addr_ok FROM addresses
    WHERE  address_id = p_address_id AND user_id = p_user_id;
    IF v_addr_ok = 0 THEN
        SET p_message = 'Address does not belong to this user.'; LEAVE proc_body;
    END IF;

    SELECT CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED) INTO v_stock
    FROM   product_catalog WHERE product_id = p_product_ref;
    IF v_stock IS NULL THEN
        SET p_message = CONCAT('Product ', p_product_ref, ' not found.'); LEAVE proc_body;
    END IF;
    IF v_stock < p_qty THEN
        SET p_message = CONCAT('Insufficient stock. Available: ', v_stock,', Requested: ', p_qty);
        LEAVE proc_body;
    END IF;

    SET v_total = ROUND(p_qty * p_unit_price, 2);
    START TRANSACTION;
        INSERT INTO orders (user_id, address_id, status, total_amount)
        VALUES (p_user_id, p_address_id, 'Confirmed', v_total);
        SET p_order_id = LAST_INSERT_ID();
        -- trigger trg_reduce_stock_on_item_insert fires automatically on this insert
        INSERT INTO order_items (order_id, product_ref, product_name, quantity, unit_price)
        VALUES (p_order_id, p_product_ref, p_product_name, p_qty, p_unit_price);
        INSERT INTO inventory_log (product_ref, change_qty, reason, order_id)
        VALUES (p_product_ref, -p_qty, 'Sale', p_order_id);
        INSERT INTO payments (order_id, method, status, amount, transaction_ref)
        VALUES (p_order_id, p_payment_method, 'Pending', v_total, CONCAT('TXN',LPAD(p_order_id,10,'0')));
        INSERT INTO shipping (order_id, carrier, tracking_no, status, estimated_date)
        VALUES (p_order_id, 'Royal Mail', CONCAT('SHP',LPAD(p_order_id,12,'0')),
                'Preparing', DATE_ADD(NOW(), INTERVAL 3 DAY));
        INSERT INTO activity_logs (user_id, event_type, details)
        VALUES (p_user_id, 'purchase', JSON_OBJECT(
            'order_id',p_order_id,'total_amount',v_total,
            'product_ref',p_product_ref,'product_name',p_product_name,
            'quantity',p_qty,'payment_method',p_payment_method));
    COMMIT;
    SET p_message = CONCAT('Order ', p_order_id, ' placed. Total: ', v_total);
END proc_body$$
DELIMITER ;

CALL sp_place_order(1, 1, 'PRD-000001', 'Galaxy S24 Ultra', 1, 1199.00, 'Card', @oid, @msg);
SELECT @oid AS new_order_id, @msg AS message;


-- F3. sp_cancel_order -------------------------------------------
-- BUG #3: proc_body label added for LEAVE
DELIMITER $$
CREATE PROCEDURE sp_cancel_order(IN p_order_id INT, OUT p_message VARCHAR(200))
proc_body: BEGIN
    DECLARE v_exists INT     DEFAULT 0;
    DECLARE v_status VARCHAR(20);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
        ROLLBACK;
        SET p_message = 'Cancellation failed - rolled back.';
    END;

    SELECT COUNT(*), status INTO v_exists, v_status
    FROM   orders WHERE order_id = p_order_id;
    IF v_exists = 0 THEN
        SET p_message = CONCAT('Order ', p_order_id, ' not found.'); LEAVE proc_body;
    END IF;
    IF v_status IN ('Delivered','Cancelled') THEN
        SET p_message = CONCAT('Cannot cancel order with status: ', v_status); LEAVE proc_body;
    END IF;

    START TRANSACTION;
        UPDATE orders    SET status = 'Cancelled' WHERE order_id = p_order_id;
        UPDATE payments  SET status = 'Refunded'  WHERE order_id = p_order_id AND status = 'Completed';
        UPDATE shipping  SET status = 'Returned'  WHERE order_id = p_order_id;
        -- Restore stock in product_catalog (NoSQL side)
        UPDATE product_catalog pc
        JOIN   order_items oi ON oi.product_ref = pc.product_id
        SET    pc.details = JSON_SET(pc.details,'$.stock_qty',
                   CAST(JSON_EXTRACT(pc.details,'$.stock_qty') AS UNSIGNED) + oi.quantity)
        WHERE  oi.order_id = p_order_id;
        INSERT INTO inventory_log (product_ref, change_qty, reason, order_id)
        SELECT product_ref, quantity, 'Return', p_order_id FROM order_items WHERE order_id = p_order_id;
    COMMIT;
    SET p_message = CONCAT('Order ', p_order_id, ' cancelled and stock restored.');
END proc_body$$
DELIMITER ;

CALL sp_cancel_order(5, @msg);
SELECT @msg;


-- F4. sp_add_to_cart (pure NoSQL operation) ----------------------
-- BUG #7: test call uses user 101, not 121 (121 does not exist)
DELIMITER $$
CREATE PROCEDURE sp_add_to_cart(
    IN p_user_id      INT,
    IN p_product_ref  VARCHAR(20),
    IN p_product_name VARCHAR(150),
    IN p_qty          INT,
    IN p_price        DECIMAL(10,2)
)
BEGIN
    DECLARE v_cart_exists INT  DEFAULT 0;
    DECLARE v_new_item    JSON;
    SET v_new_item = JSON_OBJECT(
        'product_ref',p_product_ref,'name',p_product_name,
        'qty',p_qty,'price',p_price,
        'added_at',DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s'));
    SELECT COUNT(*) INTO v_cart_exists FROM shopping_carts WHERE user_id = p_user_id;
    IF v_cart_exists = 0 THEN
        INSERT INTO shopping_carts (user_id, items, expires_at)
        VALUES (p_user_id, JSON_ARRAY(v_new_item), DATE_ADD(NOW(), INTERVAL 7 DAY));
    ELSE
        UPDATE shopping_carts
        SET    items = JSON_ARRAY_APPEND(items, '$', v_new_item), updated_at = NOW()
        WHERE  user_id = p_user_id;
    END IF;
    SELECT CONCAT('Item added to cart for user ', p_user_id) AS result;
END$$
DELIMITER ;
-- user 101 exists and has no cart yet (carts seeded for users 1-100)
CALL sp_add_to_cart(101, 'PRD-000010', 'Adidas Ultraboost 23', 1, 149.99);


-- F5. sp_sync_stock (reconciliation: SQL log vs NoSQL JSON) ------
DELIMITER $$
CREATE PROCEDURE sp_sync_stock(IN p_product_ref VARCHAR(20))
BEGIN
    DECLARE v_sql_net     INT          DEFAULT 0;
    DECLARE v_nosql_stock INT          DEFAULT 0;
    DECLARE v_diff        INT          DEFAULT 0;
    DECLARE v_name        VARCHAR(150) DEFAULT '';
    SELECT COALESCE(SUM(change_qty), 0) INTO v_sql_net
    FROM   inventory_log WHERE product_ref = p_product_ref;
    SELECT CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED), details->>'$.name'
    INTO   v_nosql_stock, v_name
    FROM   product_catalog WHERE product_id = p_product_ref;
    SET v_diff = v_sql_net - v_nosql_stock;
    SELECT p_product_ref AS product_ref, v_name AS product_name,
           v_sql_net     AS sql_inventory_net, v_nosql_stock AS nosql_stock_qty,
           v_diff        AS discrepancy, IF(v_diff=0,'OK','MISMATCH') AS sync_status;
    IF v_diff != 0 THEN
        INSERT INTO inventory_log (product_ref, change_qty, reason)
        VALUES (p_product_ref, v_diff, 'Correction');
        SELECT CONCAT('Correction logged for ', p_product_ref, '. Diff: ', v_diff) AS correction;
    END IF;
END$$
DELIMITER ;
CALL sp_sync_stock('PRD-000001');


-- F6. sp_user_dashboard (reads SQL + NoSQL side in one call) -----
DELIMITER $$
CREATE PROCEDURE sp_user_dashboard(IN p_user_id INT)
BEGIN
    -- User summary (SQL side)
    SELECT u.user_id, CONCAT(u.first_name,' ',u.last_name) AS full_name, u.email,
           fn_lifetime_spend(u.user_id)      AS lifetime_spend,
           fn_active_order_count(u.user_id)  AS active_orders
    FROM   users u WHERE u.user_id = p_user_id;
    -- Last 5 orders (SQL side)
    SELECT o.order_id, o.status, o.total_amount, o.placed_at, COUNT(oi.item_id) AS items
    FROM   orders o
    JOIN   order_items oi ON oi.order_id = o.order_id
    WHERE  o.user_id = p_user_id
    GROUP  BY o.order_id, o.status, o.total_amount, o.placed_at
    ORDER  BY o.placed_at DESC LIMIT 5;
    -- Current cart contents (NoSQL side)
    SELECT cart_id, JSON_LENGTH(items) AS items_in_cart, items AS cart_contents, updated_at
    FROM   shopping_carts WHERE user_id = p_user_id;
    -- Last 5 activity events (NoSQL side)
    SELECT event_type,
           details->>'$.page'         AS page,
           details->>'$.query'        AS search_query,
           details->>'$.product_name' AS product_interacted,
           created_at
    FROM   activity_logs WHERE user_id = p_user_id ORDER BY created_at DESC LIMIT 5;
END$$
DELIMITER ;
CALL sp_user_dashboard(1);


-- ============================================================
-- SECTION G  --  TRIGGERS
-- ============================================================
DELIMITER $$

-- G1. Audit trail: record every payment INSERT and UPDATE
CREATE TRIGGER trg_audit_payment_insert
AFTER INSERT ON payments FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, new_data)
    VALUES ('payments','INSERT', NEW.payment_id,
            JSON_OBJECT('order_id',NEW.order_id,'method',NEW.method,
                        'status',NEW.status,'amount',NEW.amount));
END$$

CREATE TRIGGER trg_audit_payment_update
AFTER UPDATE ON payments FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, old_data, new_data)
    VALUES ('payments','UPDATE', NEW.payment_id,
            JSON_OBJECT('status',OLD.status,'amount',OLD.amount),
            JSON_OBJECT('status',NEW.status,'amount',NEW.amount));
END$$

-- G2. Stock reduction: auto-fires when an order item is inserted
-- GREATEST(0,...) prevents stock going negative.
-- BUG #8: sp_place_order no longer does its own stock UPDATE,
-- so there is no double deduction.
CREATE TRIGGER trg_reduce_stock_on_item_insert
AFTER INSERT ON order_items FOR EACH ROW
BEGIN
    UPDATE product_catalog
    SET    details = JSON_SET(details,'$.stock_qty',
                        GREATEST(0,
                            CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED)
                            - NEW.quantity))
    WHERE  product_id = NEW.product_ref;
END$$

-- G3. Guard: prevent deleting a delivered order
CREATE TRIGGER trg_block_delivered_delete
BEFORE DELETE ON orders FOR EACH ROW
BEGIN
    IF OLD.status = 'Delivered' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Cannot delete a delivered order.';
    END IF;
END$$

-- G4. Auto-confirm order when its payment is marked Completed
CREATE TRIGGER trg_confirm_order_on_payment
AFTER UPDATE ON payments FOR EACH ROW
BEGIN
    IF NEW.status = 'Completed' AND OLD.status = 'Pending' THEN
        UPDATE orders SET status = 'Confirmed'
        WHERE  order_id = NEW.order_id AND status = 'Pending';
    END IF;
END$$

DELIMITER ;


-- ============================================================
-- SECTION H  --  TRANSACTIONS
-- ============================================================

-- TX1: mark a payment Completed and confirm its order atomically
START TRANSACTION;
    SAVEPOINT before_payment;
    UPDATE payments SET status = 'Completed', paid_at = NOW()
    WHERE  order_id = 2 AND status = 'Pending';
    UPDATE orders SET status = 'Confirmed'
    WHERE  order_id = 2 AND status = 'Pending';
    -- ROLLBACK TO SAVEPOINT before_payment;  -- uncomment to undo
COMMIT;

-- TX2: apply a 10% sale price to two products atomically (NoSQL side)
START TRANSACTION;
    UPDATE product_catalog
    SET    details = JSON_SET(details,'$.price',
               ROUND(CAST(JSON_EXTRACT(details,'$.price') AS DECIMAL(10,2)) * 0.90, 2))
    WHERE  product_id IN ('PRD-000003','PRD-000005');
COMMIT;

-- TX3: demonstrate ROLLBACK -- the discount is applied then immediately undone
START TRANSACTION;
    SAVEPOINT sp1;
    UPDATE orders SET total_amount = total_amount * 0.85 WHERE order_id = 10;
    ROLLBACK TO SAVEPOINT sp1;   -- undo the discount
COMMIT;


-- ============================================================
-- SECTION I  --  INDEXES
-- ============================================================

-- Single-column indexes for the most frequently filtered columns
CREATE INDEX idx_users_email     ON users         (email);
CREATE INDEX idx_users_username  ON users         (username);
CREATE INDEX idx_orders_user     ON orders        (user_id);
CREATE INDEX idx_orders_status   ON orders        (status);
CREATE INDEX idx_orders_placed   ON orders        (placed_at DESC);
CREATE INDEX idx_items_order     ON order_items   (order_id);
CREATE INDEX idx_items_product   ON order_items   (product_ref);
CREATE INDEX idx_payments_order  ON payments      (order_id);
CREATE INDEX idx_shipping_order  ON shipping      (order_id);
CREATE INDEX idx_inv_product     ON inventory_log (product_ref);
CREATE INDEX idx_activity_user   ON activity_logs (user_id);
CREATE INDEX idx_activity_event  ON activity_logs (event_type);
CREATE INDEX idx_reviews_product ON product_reviews (product_ref);

-- Composite indexes for multi-column query patterns
CREATE INDEX idx_orders_user_status ON orders   (user_id, status, placed_at DESC);
CREATE INDEX idx_payments_pending   ON payments (status, created_at);

-- Expression index for case-insensitive email lookups
CREATE INDEX idx_users_email_ci ON users ((LOWER(email)));

-- NoSQL side: generated (stored) columns extracted from JSON so MySQL
-- can index them. BUG #9: CAST(..AS UNSIGNED) not = TRUE.
ALTER TABLE product_catalog
    ADD COLUMN _price    DECIMAL(10,2) GENERATED ALWAYS AS (CAST(JSON_EXTRACT(details,'$.price')     AS DECIMAL(10,2))) STORED,
    ADD COLUMN _stock    INT UNSIGNED  GENERATED ALWAYS AS (CAST(JSON_EXTRACT(details,'$.stock_qty') AS UNSIGNED))      STORED,
    ADD COLUMN _active   TINYINT(1)    GENERATED ALWAYS AS (CAST(JSON_EXTRACT(details,'$.is_active') AS UNSIGNED))      STORED,
    ADD COLUMN _category VARCHAR(50)   GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(details,'$.category')))           STORED;

CREATE INDEX idx_catalog_price    ON product_catalog (_price);
CREATE INDEX idx_catalog_stock    ON product_catalog (_stock);
CREATE INDEX idx_catalog_active   ON product_catalog (_active);
CREATE INDEX idx_catalog_category ON product_catalog (_category);
-- Composite for the typical "browse catalogue" query pattern
CREATE INDEX idx_catalog_browse   ON product_catalog (_active, _category, _price);
-- BUG #10: FULLTEXT on generated/stored columns not supported in MySQL 8.

-- Confirm the query planner uses the index
EXPLAIN SELECT product_id, _price, _stock
FROM    product_catalog
WHERE   _active = 1 AND _category = 'Electronics'
ORDER   BY _price;


-- ============================================================
-- SECTION J  --  VIEWS
-- ============================================================

-- Full order summary: customer, payment, shipping in one place
CREATE OR REPLACE VIEW v_order_summary AS
SELECT o.order_id,
       CONCAT(u.first_name,' ',u.last_name) AS customer,
       u.email, a.city,
       o.status  AS order_status,
       p.method  AS payment_method, p.status AS payment_status,
       s.carrier, s.status AS shipping_status, s.tracking_no,
       COUNT(oi.item_id) AS item_count,
       o.total_amount, o.placed_at
FROM   orders o
JOIN   users       u  ON u.user_id    = o.user_id
JOIN   addresses   a  ON a.address_id = o.address_id
JOIN   payments    p  ON p.order_id   = o.order_id
JOIN   shipping    s  ON s.order_id   = o.order_id
JOIN   order_items oi ON oi.order_id  = o.order_id
GROUP  BY o.order_id, u.first_name, u.last_name, u.email, a.city,
          o.status, p.method, p.status, s.carrier, s.status,
          s.tracking_no, o.total_amount, o.placed_at;

-- Per-customer order counts and lifetime spend
CREATE OR REPLACE VIEW v_customer_stats AS
SELECT u.user_id, u.username,
       CONCAT(u.first_name,' ',u.last_name) AS full_name,
       u.email,
       COUNT(o.order_id)                                          AS total_orders,
       COUNT(CASE WHEN o.status='Delivered' THEN 1 END)           AS completed_orders,
       ROUND(COALESCE(SUM(
           CASE WHEN o.status NOT IN ('Cancelled','Refunded') THEN o.total_amount END
       ),0),2)                                                     AS lifetime_spend,
       MAX(o.placed_at)                                            AS last_order_date
FROM   users u
LEFT   JOIN orders o ON o.user_id = u.user_id
GROUP  BY u.user_id, u.username, u.first_name, u.last_name, u.email;

-- Active products with fewer than 15 units in stock
CREATE OR REPLACE VIEW v_low_stock AS
SELECT product_id,
       _category  AS category,
       _stock     AS stock_qty,
       _price     AS price,
       details->>'$.name'  AS product_name,
       details->>'$.brand' AS brand
FROM   product_catalog
WHERE  _stock < 15 AND _active = 1
ORDER  BY _stock;

-- Run all three views
SELECT * FROM v_order_summary  ORDER BY placed_at   DESC LIMIT 10;
SELECT * FROM v_customer_stats ORDER BY lifetime_spend DESC LIMIT 10;
SELECT * FROM v_low_stock;
