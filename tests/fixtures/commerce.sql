-- Synthetic records only; recreated with each disposable Compose database.
USE commerce_fixture;
CREATE TABLE users (
 user_id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL
);
CREATE TABLE products (
 product_id BIGINT PRIMARY KEY, category VARCHAR(100) NOT NULL,
 price DECIMAL(12,2) NOT NULL, updated_at DATETIME(6) NOT NULL
);
CREATE TABLE orders (
 order_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, product_id BIGINT NOT NULL,
 quantity INT NOT NULL, unit_price DECIMAL(12,2) NOT NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 FOREIGN KEY (user_id) REFERENCES users(user_id),
 FOREIGN KEY (product_id) REFERENCES products(product_id),
 INDEX orders_updated (updated_at, order_id)
);
CREATE INDEX users_updated ON users(updated_at, user_id);
INSERT INTO users VALUES
 (1, ' ALICE@example.test ', '2026-01-01', '2026-01-01'),
 (2, 'bob@example.test', '2026-01-02', '2026-01-02');
INSERT INTO products VALUES
 (1, ' books ', 19.95, '2026-01-01'),
 (2, 'ELECTRONICS', 250.00, '2026-01-01');
INSERT INTO orders VALUES
 (1, 1, 1, 2, 19.95, '2026-01-02', '2026-01-02'),
 (2, 2, 2, 1, 250.00, '2026-01-03', '2026-01-03'),
 (3, 1, 1, 1, 19.95, '2026-01-03', '2026-01-03');
