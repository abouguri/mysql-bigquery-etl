-- Execute after all tables have published. Tables are not cross-table atomic.
ASSERT (SELECT COUNT(*) - COUNT(DISTINCT user_id) FROM `{{project}}.{{dataset}}.users`) = 0 AS 'Duplicate user keys';
ASSERT (SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM `{{project}}.{{dataset}}.orders`) = 0 AS 'Duplicate order keys';
ASSERT (SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM `{{project}}.{{dataset}}.products`) = 0 AS 'Duplicate product keys';
ASSERT (SELECT COUNT(*) FROM `{{project}}.{{dataset}}.orders` WHERE order_id IS NULL OR user_id IS NULL OR product_id IS NULL OR total_amount IS NULL) = 0 AS 'Required order value missing';
ASSERT (SELECT COUNT(*) FROM `{{project}}.{{dataset}}.orders` o LEFT JOIN `{{project}}.{{dataset}}.users` u USING(user_id) WHERE u.user_id IS NULL) = 0 AS 'Missing user reference';
ASSERT (SELECT COUNT(*) FROM `{{project}}.{{dataset}}.orders` o LEFT JOIN `{{project}}.{{dataset}}.products` p USING(product_id) WHERE p.product_id IS NULL) = 0 AS 'Missing product reference';
ASSERT (SELECT COUNT(*) FROM `{{project}}.{{dataset}}.orders` WHERE total_amount != quantity * unit_price) = 0 AS 'Revenue arithmetic mismatch';
ASSERT (SELECT COALESCE(SUM(net_revenue_usd), NUMERIC '0') FROM `{{project}}.{{dataset}}.revenue_daily`) = (SELECT COALESCE(SUM(total_amount), NUMERIC '0') FROM `{{project}}.{{dataset}}.orders`) AS 'Revenue mart mismatch';
