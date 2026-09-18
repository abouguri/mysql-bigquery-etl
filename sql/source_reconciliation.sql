-- MySQL source-side baseline. Run in a consistent source snapshot and record the bounds.
SELECT COUNT(*) AS users FROM users;
SELECT COUNT(*) AS products FROM products;
SELECT COUNT(*) AS orders, SUM(quantity * unit_price) AS net_revenue_usd FROM orders;
-- Seeded fixture: 2 users, 2 products, 3 orders, revenue 309.85 USD.
-- Compare with destination counts and SUM(total_amount) at matching source bounds.
